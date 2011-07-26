
import threading, time, atexit, traceback, datetime, os
import sysv_ipc
import rsslib, feedlib
# importing dbimpl further down

from config import *

# ----- RECEIVING MESSAGE QUEUE

class ReceivingMessageQueue:

    def __init__(self):
        # create queue, and fail if it already exists
        self._mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER, sysv_ipc.IPC_CREX,
                                             0666)

        self._queue = [None, [], []] # one queue per type. 0 is blank

    def get_next_message(self):
        self._gather_into_queue()

        return (self._get_from_queue(2) or self._get_from_queue(1))

    def get_queue_size(self):
        return len(self._queue[1] + self._queue[2])

    def remove(self):
        self._mqueue.remove()

    def _gather_into_queue(self):
        try:
            (msg, type) = self._receive()
            while msg:
                self._queue[type].append(msg)
                (msg, type) = self._receive()
        except sysv_ipc.BusyError:
            pass
        
    def _receive(self):
        "Returns (msg, type) tuple."
        return self._mqueue.receive(False)

    def _get_from_queue(self, type):
        if self._queue[type]:
            msg = self._queue[type][0]
            self._queue[type] = self._queue[type][1 : ]
            return msg
        else:
            return None
    
def queue_worker():
    while not stop:
        msg = recv_mqueue.get_next_message()
        if not msg:
            time.sleep(0.01)
            continue

        tokens = msg.split()
        key = tokens[0]

        print msg, "(%s)" % recv_mqueue.get_queue_size()
        start = time.time()
        try:
            apply(msg_dict[key].invoke, tokens[1 : ])
        except:
            print "ERROR"
            outf = open("dbqueue.err", "a")
            outf.write(("-" * 75) + "\n")
            outf.write(msg + "\n")
            traceback.print_exc(None, outf)
            outf.close()
        spent = time.time() - start
        print "  time: ", spent

        stats.task_sample(key, spent)

# ----- RECEIVABLE MESSAGES

class FindFeedsToCheck:

    def invoke(self):
        dbimpl.cur.execute("""
        select id from feeds
          where
          last_read is null or
          last_read + (time_to_wait * interval '1 second') < now() 
        """)
        for (id) in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("CheckFeed %s" % id)

class AgePosts:

    def invoke(self):
        dbimpl.cur.execute("select feed, username from subscriptions")
        for row in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("AgeSubscription %s %s" % row)

class AgeSubscription:

    def invoke(self, feedid, username):
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        user = dbimpl.User(username)
        sub = dbimpl.Subscription(feed, user)
        for item in sub.get_rated_posts():
            item.age()
        dbimpl.conn.commit()
            
class PurgePosts:

    def invoke(self):
        for feedid in dbimpl.query_for_list("select id from feeds", ()):
            dbimpl.mqueue.send("PurgeFeed %s" % feedid)

class PurgeFeed:

    def invoke(self, feedid):
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed or not feed.get_max_posts():
            return

        items = feed.get_items()
        for ix in range(feed.get_max_posts(), len(items)):
            items[ix].delete()

class CheckFeed:

    def invoke(self, feedid):
        feedid = int(feedid)

        # get feed
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        if not feed: # might have been gc-ed in the meantime
            return
        
        items = {} # url -> item (so we can check for new ones)
        for item in feed.get_items():
            items[item.get_link()] = item
        
        # read xml
        try:
            site = rsslib.read_feed(feed.get_url(), rsslib.DefaultFactory(),
                                    rsslib.httplib_loader)
            feed.set_error(None)
        except Exception, e:
            # we failed, so record the failure and move on
            #traceback.print_exc()
            feed.set_error(str(e))
            feed.save()
            return

        # store all new items
        newposts = False
        for newitem in site.get_items():
            if items.has_key(newitem.get_link()):
                continue

            parsed_date = feedlib.parse_date(newitem.get_pubdate())
            newposts = True
            itemobj = dbimpl.Item(None, newitem.get_title(),
                                  newitem.get_link(), newitem.get_description(),
                                  parsed_date, newitem.get_author(), feed)
            itemobj.save()
        
        # update feed row
        feed.set_title(site.get_title())
        feed.set_link(site.get_link())
        feed.set_max_posts(feedlib.compute_max_posts(site))
        feed.is_read_now()
        feed.save()
            
        # recalc all subs on this feed (if new posts, that is)
        if newposts:
            dbimpl.cur.execute("""select username from subscriptions where
                               feed = %s""", (feed.get_local_id(), ))
            for (user, ) in dbimpl.cur.fetchall():
                dbimpl.mqueue.send("RecalculateSubscription %s %s" %
                                   (feed.get_local_id(), user))

class RecalculateSubscription:

    def invoke(self, feedid, username):
        feedid = int(feedid)

        user = dbimpl.User(username)
        feed = dbimpl.feeddb.get_feed_by_id(feedid)
        sub = dbimpl.Subscription(feed, user)
        
        # load all already rated posts on this subscription 
        ratings = {} # int(postid) -> ratedpost
        dbimpl.cur.execute("""
          select username, post, points,
                 id, title, link, descr, pubdate, author
          from rated_posts r
          join posts p on post = id
          where username = %s and r.feed = %s
        """, (username, feedid))
        for (username, postid, points,
             id, title, link, descr, date, author) in dbimpl.cur.fetchall():
            post = dbimpl.Item(id, title, link, descr, date, author, feed)
            ratings[postid] = dbimpl.RatedPost(user, post, sub, points)

        # load all seen posts
        seen = dbimpl.query_for_set("""
          select post from read_posts
          where username = %s and feed = %s
        """, (username, feedid))

        for item in feed.get_items():
            id = int(item.get_local_id())
            if id in seen:
                continue # user has already read item, nothing further to do
            
            rating = ratings.get(id)
            if not rating:
                rating = dbimpl.RatedPost(user, item, sub)
            rating.recalculate()
            rating.save()

        dbimpl.conn.commit()

class RecalculateAllPosts:

    def invoke(self, username):
        allsubs = dbimpl.query_for_list("""select feed from subscriptions
                                           where username = %s""",
                                        (username, ))
        for feedid in allsubs:
            dbimpl.mqueue.send("RecalculateSubscription %s %s" %
                               (feedid, username))

class RemoveDeadFeeds:

    def invoke(self):
        for feedid in dbimpl.query_for_list("""
             select id from feeds where not exists
               (select * from subscriptions where feed = id)
           """, ()):
            dbimpl.update("delete from feeds where id = %s", (feedid, ))
        dbimpl.conn.commit()

class RecordVote:

    def invoke(self, username, postid, vote):
        user = dbimpl.User(username)
        link = user.get_rated_post_by_id(postid)
        link.seen()
        if vote != "read":            
            link.record_vote(vote)
            link.get_subscription().record_vote(vote)
            dbimpl.mqueue.send("RecalculateAllPosts " + username)

class StatsReport:

    def invoke(self):
        outf = open(os.path.join(STATS_DIR, "queue-stats.html"), "w")
        outf.write("""
        <title>Whazzup queue stats</title>
        <style>td { padding-right: 12pt }</style>
        <h1>Whazzup queue stats</h1>

        <table>
        <tr><th>Task <th>Acc <th>Avg <th>Max <th>Min <th>Count
        """)

        tasks = feedlib.sort(stats.get_tasks(), TaskStats.get_sum)
        tasks.reverse()
        for task in tasks:
            outf.write("<tr><td>%s <td>%s <td>%s <td>%s <td>%s <td>%s\n" %
                       (task.get_name(),
                        str(task.get_sum())[ : 5],
                        str(task.get_average())[ : 5],
                        str(task.get_max())[ : 5],
                        str(task.get_min())[ : 5],
                        task.get_count()))

        outf.write("""
        </table>
        """)
        outf.close()

# ----- STATISTICS COLLECTOR

class StatisticsCollector:

    def __init__(self):
        self._tasks = {} # key -> TaskStats

    def task_sample(self, key, secs):
        stats = self._tasks.get(key)
        if not stats:
            stats = TaskStats(key)
            self._tasks[key] = stats
        stats.sample(secs)

    def get_tasks(self):
        return self._tasks.values()

class TaskStats:

    def __init__(self, key):
        self._key = key
        self._min = 10000000.0
        self._max = 0
        self._sum = 0
        self._count = 0

    def sample(self, secs):
        if secs < self._min:
            self._min = secs
        if secs > self._max:
            self._max = secs
        self._sum += secs
        self._count += 1

    def get_name(self):
        return self._key

    def get_sum(self):
        return self._sum

    def get_average(self):
        return self._sum / self._count

    def get_max(self):
        return self._max

    def get_min(self):
        return self._min

    def get_count(self):
        return self._count
    
# ----- CLEAN STOPPING

stop = False
import signal
def signal_handler(signal, frame):
    global stop
    print "%s received" % signal
    stop = True
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ------ SET UP MESSAGING

msg_dict = {
    "FindFeedsToCheck" : FindFeedsToCheck(),
    "AgePosts" : AgePosts(),
    "PurgePosts" : PurgePosts(),
    "CheckFeed" : CheckFeed(),
    "RecalculateSubscription" : RecalculateSubscription(),
    "AgeSubscription" : AgeSubscription(),
    "RemoveDeadFeeds" : RemoveDeadFeeds(),
    "RecalculateAllPosts" : RecalculateAllPosts(),
    "PurgeFeed" : PurgeFeed(),
    "RecordVote" : RecordVote(),
    "StatsReport" : StatsReport(),
    }
recv_mqueue = ReceivingMessageQueue()
atexit.register(recv_mqueue.remove) # message queue cleanup
import dbimpl # creates the sending message queue in this process

# ----- START

# we need to do this so that we don't hang for too long waiting for feeds
import socket
socket.setdefaulttimeout(20)

stats = StatisticsCollector()

try:
    queue_worker()
except:
    stop = True
    raise
