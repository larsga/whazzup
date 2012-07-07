
from config import QUEUE_FILE
import sys
import sysv_ipc

no = int(open(QUEUE_FILE).read())
mqueue = sysv_ipc.MessageQueue(no)
mqueue.send(sys.argv[1])
