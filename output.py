#!/usr/local/bin/python2.7
from kazoo.client import KazooClient
import time
import random
import logging
import logging.handlers
import sys
import gevent
import zerorpc

from kazoo.client import KazooState

class storageServer(object):
	def __init__(self):
		self.isPrimary = False
		self.addr = "AAAAAA"

	def start(self):
		#todo errorhandling
		self.zk = KazooClient(hosts='127.0.0.1:2181', timeout=2.0)
		self.isPrimary = self.start_election()
		if self.isPrimary:
			my_logger.debug('%s : i am primary  lets go tell everybody', self.addr)
		time.sleep(10)
		self.zk.stop()

	def connection_listener(self,state):
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


	def start_election(self):
		#todo errorhandling what to do if connection is lost
		self.zk.add_listener(self.connection_listener)
		if self.zk.exists("/ELECTION") is None:
			self.zk.ensure_path("/ELECTION")

		c = 1
		node_pathes = []
		while c < 10:
			c += 1
			node_path  = self.zk.create("/ELECTION/guid-n_", b"avalue", ephemeral=True, sequence=True)
			node_pathes.append(node_path)

		#todo get real pathes here
		my_path = random.choice(node_pathes)
		my_path = my_path.replace("/ELECTION/","")
		children = self.zk.get_children("/ELECTION/")

		#get path of previous node, set watch
		prev_path = None
		for child_path in sorted(children):
			if child_path == my_path:
				break
			prev_path = child_path

		#I'm the leader
		if prev_path is None:
			return True

		#fires twice, once on creation ignore
		@self.zk.DataWatch("/ELECTION/"+prev_path)
		def watch_node(data, stat):
				#only watch for first change
				if stat.version == 1:
					print("Version: %s, data: %s" % (stat.version, data.decode("utf-8")))
					print "setting watch on "+ prev_path
					print "my",my_path
				else:
					print "ignored event"

		#test listener
		self.zk.set("/ELECTION/"+prev_path, b"some data")


if __name__ == '__main__':
	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)
	storageserver = storageServer()
	storageserver.start()
