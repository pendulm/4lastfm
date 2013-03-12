import gevent
import gevent.queue
import time

def worker():
    i = queue.get()
    print "get %d from queue" % i
    time.sleep(0.1)
    queue.put(i)

queue = gevent.queue.Queue()

jobs = [gevent.spawn(worker) for i in range(50)]

for i in range(20):
    queue.put(i)

gevent.joinall(jobs)
    
