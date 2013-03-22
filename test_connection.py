from kazoo.client import KazooClient
import time

#192.168.239.141 2181
from kazoo.client import KazooState

def my_listener(state):
	if state == KazooState.LOST:
		print "session lost"
	elif state == KazooState.SUSPENDED:
		print "session suspended"
	else:
		print "ok here i am"

#zk = KazooClient(hosts='192.168.239.141:2181')
zk = KazooClient(hosts='127.0.0.1:2181', timeout=2.0)

zk.start()

zk.add_listener(my_listener)
print zk.state

print 'listener added'
c = 1
while True:
	print 'get children'
	children = zk.get_children('/')
	print children
	c +=1
	time.sleep(1)
	if c > 10:
		print "stop"
		zk.stop()
	print c, zk.state


