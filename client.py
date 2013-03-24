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

class storageClient(object):

	def __init__(self, addr, config_file='server_config'):
		self.addr = addr
		self.servers = []
		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio status')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
			line = line.rstrip()
			connection = zerorpc.Client(timeout=2)
			connection.connect('tcp://' + line)
			#todo to we need an uplist here?
			self.servers.append(ntconnection(line,connection,i,"up"))

	def start(self):
		max = 101
		for server in self.servers:
			if server.addr == "127.0.0.1:9001":
				for i in range(1,max):
					result = server.connection.kv_set("hello","world",self.addr)
					print server.addr, result
				print i, "requests sent"
				break


	def transaction_ack(self,message):
		logging.debug("%s client received", message)


if __name__ == '__main__':
	#addr =  sys.argv[1]
	addr = "127.0.0.1:9000"
	my_logger = logging.getLogger('MyLogger')
	my_logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch.setFormatter(formatter)
	my_logger.addHandler(ch)

	storageserver = storageClient(addr)
	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()









