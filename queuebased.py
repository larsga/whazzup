"""
The purpose of this module is to allow experimentation with a
queue-based approach to maintaining server state. Eventually all the
code here will be folded into feedlib.
"""

import Queue, time, feedlib, sys, traceback

# possible queued tasks:
#   - (re)compute score for a post  OK
#   - resort all posts
#   - check a feed for new posts    OK
#   - go through feed database looking for feeds which need to be rechecked OK
#   - downvote a post
#   - upvote a post

# tasks have an optional time to execute. if the time is not set, the
# task is executed immediately. the time to execute is used as the
# priority. if no tasks in the queue are ready to be performed, the
# worker thread sleeps for a second, then rechecks.

# need to subclass the priority queue so that we can peek into it and
# see whether the first task is ready to be performed.

class PeekablePriorityQueue(Queue.PriorityQueue):

    def has_task_ready(self):
        if not self.queue:
            return False

        (timetorun, task) = self.queue[0]
        return timetorun < time.time()

def queue_worker():
    while True:
        if not queue.has_task_ready():
            time.sleep(1)
            continue

        (timetorun, task) = queue.get()
        print len(queue.queue), task
        try:
            task.perform()
        except:
            print "ERROR:", sys.exc_info()
            traceback.print_tb(sys.exc_info()[2])
        queue.task_done()

class RecalculatePost:

    def __init__(self, post):
        self._post = post

    def perform(self):
        self._post.recalculate()

    def __repr__(self):
        return "[RecalculatePost %s]" % self._post.get_link()
        
class CheckFeed:

    def __init__(self, feed):
        self._feed = feed

    def perform(self):
        new_posts = feedlib.feeddb.read_feed(self._feed.get_url(), self._feed.get_format())
        for new_post in new_posts:
            queue.put((0, RecalculatePost(new_post)))
        self._feed.set_check_task(False)

    def __repr__(self):
        return "[CheckFeed %s]" % self._feed.get_url()
        
class FindFeedsToCheck:

    def perform(self):
        found = False
        for feed in feedlib.feeddb.get_feeds():
            if feed.should_read() and not feed.has_check_task():
                found = True
                queue.put((0, CheckFeed(feed)))
                feed.set_check_task(True)

        queue.put((time.time() + 60, self))

    def __repr__(self):
        return "[FindFeedsToCheck]"

class SortPosts:

    def perform(self):
        feedlib.feeddb.sort()
        queue.put((time.time() + 30, self))

    def __repr__(self):
        return "[SortPosts]"

queue = PeekablePriorityQueue()
queue.put((0, FindFeedsToCheck()))
queue.put((0, SortPosts()))

def start_queue_worker():
    thread = threading.Thread(target = queue_worker, name = "FeedReader")
    thread.start()
    return thread

def recalculate_all_posts():
    # called when a post is voted on, so that the system knows it needs
    # to recalculate all posts.
    # FIXME: can currently create duplicate tasks (and duplicate recalc)
    for post in feedlib.feeddb.get_items():
        queue.put((0, RecalculatePost(post)))

import threading
thread = None
for t in threading.enumerate():
    if t.name == "FeedReader":
        thread = t
if not thread:
    print "Starting thread"
    thread = start_queue_worker()
else:
    print "Thread already running, not starting"
