#!/usr/local/bin/python2.7
from kazoo.client import KazooClient
import time
import random
import logging
import logging.handlers
import sys
import gevent
import zerorpc
import collections
import os
from kazoo.client import KazooState
from kazoo.exceptions import *


class storageClient(object):

	def __init__(self, max, config_file='server_config'):

		self.election_path_prefix ="/MYLEADERELECTION/"
		self.addr = "127.0.0.1:9000"
		self.max = max
		self.servers = []
		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio status')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
			if line.startswith("#"):
				continue
			line = line.rstrip()
			connection = zerorpc.Client(timeout=10)
			connection.connect('tcp://' + line)
			#todo to we need an uplist here?
			self.servers.append(ntconnection(line,connection,i,"up"))

		self.zk = KazooClient(timeout =1)
		self.zk.start()

	def connection_listener(self, state):
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


	def get_sorted_children(self):
		#check if children really exist
		children = self.zk.get_children(self.election_path_prefix)
		# can't just sort directly: the node names are prefixed by uuids
		children.sort(key=lambda c: c[c.find("guid_n") + len("guid_n"):])
		return children

	def get_primary_addr(self):
		primary_path = self.get_sorted_children()[0]
		primary_addr = str(self.zk.get(self.election_path_prefix + primary_path)[0])
		return primary_addr

	def get_server_by_addr(self, addr):
		for server in self.servers:
			if server.addr == addr :
				return server
		raise SystemError


	def start(self):

		if self.zk.exists(self.election_path_prefix) is None:
			print "no election path found quitting"
		primary_addr = self.get_primary_addr()
		a = time.time()

		max = self.max + 1
		print "my max is " + str(max)

		for i in range(1 , max):
			server = self.get_server_by_addr(primary_addr)
			try:
				#todo read key value from a file
				print "sending request to server", server.addr
				result = server.connection.kv_set("hello"+str(i),"world"+str(i),self.addr)
				print i, server.addr, result
				if result !="commited":
					time.sleep(1)

			except zerorpc.TimeoutExpired:
				time.sleep(2)
				primary_addr = self.get_primary_addr()
				print "changed primary addr ", primary_addr

			print i, "requests sent", time.time() - a

		print "ok counting operations"
		num_lines = sum(1 for line in open('operations1.log'))
		print "commited requests:", num_lines


if __name__ == '__main__':

	max =  int(sys.argv[1])
	addr = "127.0.0.1:9000"
	my_logger = logging.getLogger('clientlogger')
	my_logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch.setFormatter(formatter)
	my_logger.addHandler(ch)

	storageserver = storageClient(max)

	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()









