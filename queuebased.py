"""
The purpose of this module is to allow experimentation with a
queue-based approach to maintaining server state. Eventually all the
code here will be folded into feedlib.
"""

import Queue, time, feedlib, sys

# possible queued tasks:
#   - (re)compute score for a post
#   - resort all posts
#   - check a feed for new posts
#   - go through feed database looking for feeds which need to be rechecked
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
        print task
        try:
            task.perform()
        except:
            print "ERROR:", sys.exc_info()
        queue.task_done()

class CheckFeed:

    def __init__(self, feed):
        self._feed = feed

    def perform(self):
        feedlib.feeddb.read_feed(self._feed.get_url(), self._feed.get_format())
        
class FindFeedsToCheck:

    def perform(self):
        for feed in feedlib.feeddb.get_feeds():
            if feed.should_read():
                queue.put((0, CheckFeed(feed)))

        queue.put((time.time() + 20, self))

queue = PeekablePriorityQueue()
queue.put((0, FindFeedsToCheck()))

queue_worker()
