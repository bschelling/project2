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

def node_watcher(event):
	print "got event from node_watcher"



#zk = KazooClient(hosts='192.168.239.141:2181')
zk = KazooClient(hosts='127.0.0.1:2181', timeout=2.0)
zk.add_listener(my_listener)
zk.start()

if zk.exists("/my/favorite") == None:
	zk.ensure_path("/my/favorite")
	print "node created /my/favorite"
else:
	print "/my/favorite was there"
data, stat = zk.get("/my/favorite")
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


if zk.exists("/my/favorite/node") == None:
	zk.create("/my/favorite/node", b"a value")
	print "node created"
else:
	print "/my/favorite/node was there??"

@zk.ChildrenWatch("/my/favorite/node")
def watch_children(children):
	print ">>>>>>>>ChildrenWatch got event"
	print("Children are now: %s" % children)

@zk.DataWatch("/my/favorite/node")
def watch_node(data, stat):
	print ">>>>>>>>>.DataWatch got event"
	print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))


data, stat = zk.get("/my/favorite/node")
print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))

#create a ephemeral node
if zk.exists("/my/favorite/emphnode") == None:
	zk.create("/my/favorite/emphnode", b"a value", ephemeral=True)
	print "ok created  ephemeral node"

else:
	print "shit this node should not exist at all"

#children = zk.get_children("/my/favorite/node", watch=node_watcher)
print "setting some data"
zk.set("/my/favorite/node", b"some data")
#children = zk.get_children("/my/favorite/node", watch=node_watcher)
#print "deleting node"
#zk.delete("/my/favorite/node", recursive=True)

if True:
	transaction = zk.transaction()
	transaction.check('/my/favorite/node', version=10)
	transaction.set_data('/my/favorite/node', b" new value so new so new")
	results = transaction.commit()
print results

data, stat = zk.get("/my/favorite/node")
print data,stat

zk.stop()
print "bye"



