
import threading, time
import dbimpl

# ----- RECEIVING MESSAGE QUEUE

class ReceivingMessageQueue:

    def __init__(self):
        pass # connect to SYSV msg queue etc

    def get_next_message(self):
        return None

def queue_worker():
    while True:
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
        pass # FIXME: do it
        
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
    while True:
        cron.run_tasks()
        time.sleep(1)

def start_cron_worker():
    thread = threading.Thread(target = cron_worker, name = "CronWorker")
    thread.start()
    return thread

# ----- CRON TASKS

class QueueTask(RepeatableTask):

    def __init__(self, interval, message):
        RepeatableTask.__init__(self, interval)
        self._message = message

    def _invoke(self):
        msg_queue.send(message)

# ----- SET UP CRON
        
cron = CronService()
cron.add_task(QueueTask("FindFeedsToCheck", 600))
cron.add_task(QueueTask("AgePosts", 3600))
cron.add_task(QueueTask("PurgePosts", 86400))
#cron_worker()

# ------ SET UP MESSAGING

msg_dict = {
    "FindFeedsToCheck" : FindFeedsToCheck(),
    }
