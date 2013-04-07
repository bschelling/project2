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

class storageClient(object):

	def __init__(self, max, config_file='server_config'):
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

	def start(self):
		max = self.max + 1
		for server in self.servers:

			if server.addr == "127.0.0.1:9001":
				a = time.time()
				for i in range(1,max):
					result = server.connection.kv_set("hello"+str(i),"world"+str(i),self.addr)
					print i, server.addr, result
					if result !="commited":
						dummy = 1
						#time.sleep(1)
						#break
				print i, "requests sent", time.time() - a
				break


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









