from kazoo.client import KazooClient
import time
import random

#todo use logging - soon

#192.168.239.141 2181
from kazoo.client import KazooState

def my_listener(state):
	if state == KazooState.LOST:
		print "session lost"
	elif state == KazooState.SUSPENDED:
		print "session suspended"
	else:
		print "ok here i am"

def election_child_watcher(event):
	print "election_child_watcher"
	print event

#WTF closure stuff working ?
def prev_path_watcher(event):
	if prev_path == None:
		print "I won"
	else:
		print "setting watch on "+ prev_path


def main():
	zk = KazooClient(hosts='127.0.0.1:2181', timeout=2.0)
	zk.add_listener(my_listener)
	zk.start()

	if zk.exists("/ELECTION") == None:
		zk.ensure_path("/ELECTION")

	c = 1
	node_pathes = []
	while c < 10:
		c += 1
		node_path  = zk.create("/ELECTION/guid-n_", b"a value", ephemeral=True, sequence=True)
		node_pathes.append(node_path)

	my_path = random.choice(node_pathes)
	my_path = my_path.replace("/ELECTION/","")
	#print "print my_path", my_path

	children = zk.get_children("/ELECTION/", watch=election_child_watcher)
	get_next_path = False;

	prev_path = None
	for child_path in sorted(children):
		if child_path == my_path:
			break
		prev_path = child_path

	#I'm the leader
	if prev_path == None:
		print "OK I'm leader don't have to watch"
		return

	#fires twice, once on creation ignore
	@zk.DataWatch("/ELECTION/"+prev_path)
	def watch_node(data, stat):
			#only watch for first change
			if stat.version == 1:
				print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
				print "setting watch on "+ prev_path
				print "my",my_path
	zk.set("/ELECTION/"+prev_path, b"some data")

	print "boom. watch triggered?"
	#time.sleep(10)
	print "bye"

	zk.stop()

if __name__ == '__main__':
	main()