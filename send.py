
import sys
import sysv_ipc

no = int(open("queue-no.txt").read())
mqueue = sysv_ipc.MessageQueue(no)
mqueue.send(sys.argv[1])
