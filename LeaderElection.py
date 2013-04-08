#!/usr/local/bin/python2.7
import traceback
import inspect
from kazoo.client import KazooClient
import time
import random
import logging
import logging.handlers
import sys
import gevent
import zerorpc
import collections
import kazoo
from kazoo.client import KazooState
from kazoo.exceptions import *
import os
from gevent import monkey
from gevent.queue import Queue, Empty
import base64
monkey.patch_all()


class LeaderElection(object):

	def __init__(self, addr, config_file='server_config'):

		self.election_path_prefix ="/MYLEADERELECTION/"

		self.op_log = "operations"+addr[-1:]+".log"
		self.addr = addr
		self.i = 0
		self.is_primary = False
		self.quorum_size = 2

		self.stati = {"Down": 0, "Election": 1, "Reorganisation": 2, "Normal": 3}
		self.status = self.stati["Down"]
		self.servers = []
		self.server_stati = {}
		self.uplist = set()
		self.election_path =""
		self.election_prev =""

		#check if we want to use this http://stackoverflow.com/questions/2970608/what-are-named-tuples-in-python
		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
			if line.startswith("#"):
				continue
			line = line.rstrip()
			if line == addr:
				self.i = i
				connection = self
			else:
				#TODO check for connection down at startup
				connection = zerorpc.Client(timeout=1)
				connection.connect('tcp://' + line)
				my_logger.debug("%s server %s added as up!", self.addr, line)
				#
				self.server_stati[line] = "up"
				self.uplist.add(i)

			self.servers.append(ntconnection(line, connection, i))
			i += 1

		self.status = self.stati["Reorganisation"]


	def start(self):
		#todo add our client list to arguments check performance using timeouts
		self.zk = KazooClient(timeout=0.1)
		self.zk.start()
		self.status = self.stati["Normal"]
		self.start_election()
		my_logger.debug("%s self.is_primary %s", self.addr, self.is_primary)

	def formatExceptionInfo(self, maxTBlevel=5):
		cla, exc, trbk = sys.exc_info()
		excName = cla.__name__
		try:
			excArgs = exc.__dict__["args"]
		except KeyError:
			excArgs = "<no args>"
		excTb = traceback.format_tb(trbk, maxTBlevel)
		return (excName, excArgs, excTb)



	def get_primary_addr(self):
		if self.is_primary:
			return self.addr
		children = self.zk.get_children(self.election_path_prefix)
		if len(children) == 0:
			my_logger.debug("%s somethings very wrong here", self.addr)
			raise SystemError

		primary_path = self.get_sorted_children()[0]
		primary_addr = str(self.zk.get(self.election_path_prefix + primary_path)[0])
		return primary_addr

	def connection_listener(self, state):
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


	def get_sorted_children(self):
		children = self.zk.get_children(self.election_path_prefix)
		# can't just sort directly: the node names are prefixed by uuids
		children.sort(key=lambda c: c[c.find("guid_n") + len("guid_n"):])
		return children

	def get_prev_path(self, my_path):

		prev_path = None
		primary_path = None
		children = self.get_sorted_children()
		primary_path = children[0]
		my_logger.debug("%s sorted children is %s", self.addr, children)
		if primary_path == my_path:
			return prev_path, my_path
		else:
			for child_path in children:
				if child_path == my_path:
					break
				else:
					prev_path = child_path

		return  prev_path, self.election_path_prefix+str(primary_path)

	def primary_exists_watch(self,event):
		my_logger.debug("%s event %s", self.addr, event)

	def watch_node(self, my_path, prev_path):
		@self.zk.DataWatch(self.election_path_prefix + prev_path)
		def watch_node(data, stat):
			try:
				if data is None and stat is None:
					prev_path, primary_path = self.get_prev_path(my_path)
					if prev_path is None:
						my_logger.debug("%s prev_path deleted prev_path %s primary_path %s", self.addr, prev_path, primary_path)
						self.set_primary(True, primary_path)

					else:
						if self.zk.exists(primary_path):
							if self.zk.get(primary_path)[0] != self.addr:
								my_logger.debug("%s %s deleted but still not addmin, my_path %s prev_path %s ", self.addr,
									self.zk.get(primary_path)[0], my_path, prev_path)
								self.watch_node(my_path, prev_path)
							else:
								my_logger.debug("%s ok lets wait node with our address still there %s", self.addr, self.zk.get(primary_path)[0])
								gevent.sleep(1)
						else:
							my_logger.debug("%s primary path %s does not exisit anymore ", self.addr, primary_path)

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, str(self.formatExceptionInfo(Exception)))
				raise SystemError

	def set_primary(self, is_primary, primary_path):
		my_logger.debug("%s PRIMARY  %s set from %s", self.addr, is_primary, inspect.stack()[1][3])
		self.is_primary = is_primary
		if not is_primary:
			self.zk.exists(primary_path, watch = self.primary_exists_watch)

	def start_election(self):
		self.zk.add_listener(self.connection_listener)
		if self.zk.exists(self.election_path_prefix) is None:
			self.zk.ensure_path(self.election_path_prefix)

		my_path = self.zk.create(self.election_path_prefix+"guid_n", self.addr, ephemeral=True, sequence=True)
		my_path = my_path.replace(self.election_path_prefix, "")
		my_logger.debug("%s my path is %s", self.addr, my_path)

		prev_path, primary_path = self.get_prev_path(my_path)
		#put exists watch on the primary!
		if not prev_path is None:
			self.watch_node(my_path, prev_path)
			self.set_primary(False, primary_path)
		else:
			self.set_primary(True,"")


if __name__ == '__main__':
	logger = logging.getLogger('spam_application')
	logger.setLevel(logging.DEBUG)

	addr = sys.argv[1]
	my_logger = logging.getLogger("storagelogger")
	my_logger.setLevel(logging.DEBUG)

	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')

	ch = logging.StreamHandler()
	ch.setFormatter(formatter)
	my_logger.addHandler(ch)

	fh = logging.FileHandler("server" + addr[-1:] + ".log", mode="w")
	fh.setFormatter(formatter)
	my_logger.addHandler(fh)


	storageserver = LeaderElection(addr)
	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()




