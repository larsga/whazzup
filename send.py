
import sys
import sysv_ipc
from config import QUEUE_NUMBER

mqueue = sysv_ipc.MessageQueue(QUEUE_NUMBER)
mqueue.send(sys.argv[1])
