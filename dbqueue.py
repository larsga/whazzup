
import threading, time
import sysv_ipc
# importing dbimpl further down

# ----- RECEIVING MESSAGE QUEUE

class ReceivingMessageQueue:

    def __init__(self):
        # create queue, and fail if it already exists
        self._mqueue = sysv_ipc.MessageQueue(7321, sysv_ipc.IPC_CREX)

    def get_next_message(self):
        try:
            return self._mqueue.receive(False)[0] # discard type
        except sysv_ipc.BusyError:
            return None # no message available

    def remove(self):
        self._mqueue.remove()

def queue_worker():
    while not stop:
        msg = recv_mqueue.get_next_message()
        if not msg:
            time.sleep(1)
            continue

        tokens = msg.split()
        key = tokens[0]
        apply(msg_dict[key].invoke, tokens[1 : ])

# ----- RECEIVABLE MESSAGES

class FindFeedsToCheck:

    def invoke(self):
        print "Find feeds to check"
        dbimpl.cur.execute("""
        select id from feeds where
          last_read is null or
          last_read < now() + (time_to_wait * interval '1 second')
        """)
        for (id) in dbimpl.cur.fetchall():
            dbimpl.mqueue.send("CheckFeed %s" % id)

class AgePosts:

    def invoke(self):
        print "Age posts" # FIXME: do it

class PurgePosts:

    def invoke(self):
        print "Purge posts" # FIXME: do it

class CheckFeed:

    def invoke(self, feedid):
        feedid = int(feedid)
        print "Check feed", feedid
        
# ----- CRON SERVICE

class CronService:
    """Maintains a set of tasks which can be run periodically, and can
    be polled at intervals to find tasks which need to run."""

    def __init__(self):
        self._tasks = []

    def add_task(self, task):
        self._tasks.append(task)
        
    def run_tasks(self):
        for task in self._tasks:
            if task.is_time_to_run():
                task.run() # this just puts the real task into the queue

class RepeatableTask:
    "Task which can be run periodically."

    def __init__(self, interval):
        "Interval is the time in seconds between runs of the task."
        self._last_run = 0
        self._interval = interval

    def is_time_to_run(self):
        return time.time() - self._last_run > self._interval

    def run(self):
        self._invoke()
        self._last_run = time.time()

    def _invoke(self):
        "Override to provide actual content of task."
        raise NotImplementedError()

def cron_worker():
    while not stop:
        cron.run_tasks()
        time.sleep(1)

def start_cron_worker():
    thread = threading.Thread(target = cron_worker, name = "CronWorker")
    thread.start()
    return thread

# ----- CRON TASKS

class QueueTask(RepeatableTask):

    def __init__(self, message, interval):
        RepeatableTask.__init__(self, interval)
        self._message = message

    def _invoke(self):
        dbimpl.mqueue.send(self._message)

# ----- CLEAN STOPPING

stop = False
import signal
def signal_handler(signal, frame):
    global stop
    print "SIGINT received"
    stop = True
signal.signal(signal.SIGINT, signal_handler)

# ------ SET UP MESSAGING

msg_dict = {
    "FindFeedsToCheck" : FindFeedsToCheck(),
    "AgePosts" : AgePosts(),
    "PurgePosts" : PurgePosts(),
    "CheckFeed" : CheckFeed(),
    }
recv_mqueue = ReceivingMessageQueue()
import dbimpl # this creates the sending message queue in this process

# ----- SET UP CRON
        
cron = CronService()
cron.add_task(QueueTask("FindFeedsToCheck", 600))
cron.add_task(QueueTask("AgePosts", 3600))
cron.add_task(QueueTask("PurgePosts", 86400))
start_cron_worker()

# ----- START

queue_worker()

# ----- SHUTDOWN CLEANUP

recv_mqueue.remove()
