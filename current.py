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
import leveldb
import kazoo
from kazoo.client import KazooState
import os


class storageServer(object):
	''' primary handling: check primary every time before answer, return primary
		2 times primary true check if started to fast!
		rename transaction to prepare path
	'''
	def __init__(self,  addr, config_file='server_config'):
		self.storage_prefix = "/storageserver/tx"
		self.commit_prefix = "/storageserver/cx"
		self.addr = addr
		#todo have to make this persistent
		self.transaction_id = 0
		self.i = 0
		self.is_primary = False
		self.servers = []
		self.stati = {"Down": 0, "Election": 1, "Reorganisation": 2, "Normal": 3}
		self.t_stati = {"Ready": 0, "Commited": 2, "Rolledback":3}
		self.status = self.stati["Down"]
		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio status')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
			line = line.rstrip()
			if line == addr:
				self.i = i
				connection = self
			else:
				connection = zerorpc.Client(timeout=0.2)
				connection.connect('tcp://' + line)

			#todo to we need an uplist here?
			self.servers.append(ntconnection(line,connection,i,"up"))
			i+=1

		self.status = self.stati["Reorganisation"]
		self.pending_transactions = {}


	def start(self):
		#todo errorhandling here
		self.zk = KazooClient(hosts='127.0.0.1:2181', timeout=1.0)
		self.zk.start()
		self.status = self.stati["Normal"]
		self.start_election()
		my_logger.debug('%s :  primary %s', self.addr, self.is_primary)


	####################################zerorpc functions  ################################
	def primary_commit_watch(self, event):
		my_logger.debug("%s got a commit from %s ", self.addr, event.path)
		#open("commit.log").write(event)
		f = open('commit.log','a')
		f.write(event.path+'\n') # python will convert \n to os.linesep
		f.close()


	def primary_prepare_watch(self, event):
		if event.type == "CHILD":
			try:
				children = self.zk.get_children(event.path)
				#todo check which server is missing if we don't have all
				if len(children) == len(self.servers)-1:
					for child in children:
						my_logger.debug("%s got ack on prepare from  %s", self.addr, event.path+"/"+child)

					commit_path = self.zk.get(event.path)[0]
					#notify backups
					my_logger.debug("%s adding watch to commit_path %s ", self.addr, commit_path)
					#TODO get data from from transcationpath add to commit path
					self.zk.set(commit_path, "hello=world")

				else:
					print "else stuff"
					#reset watch will each child gets notified on delete
					children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
					my_logger.debug("%s  pending %s %s", self.addr, event.type, event.path)

			except Exception, e:
			    my_logger.debug(" %s Couldn't do it: %s", self.addr, e)

	def kv_set(self, key, value, remote_addr):
		if self.is_primary:
			#self.storage_prefix = "/storageserver/tx"
			#todo better way to encode values,
			#todo use patient watch for cleanup?

			commit_path = str(self.zk.create(self.commit_prefix , key +"="+value , acl=None, ephemeral=False, sequence=True, makepath=True))
			transaction_path = self.zk.create(self.storage_prefix, commit_path.encode('UTF-8') , acl=None, ephemeral=False, sequence=True, makepath=True)
			children = self.zk.get_children(commit_path,watch=self.primary_commit_watch)
			children = self.zk.get_children(transaction_path, watch=self.primary_prepare_watch)

			prepare_ack = self.primary_prepare(transaction_path, commit_path, key, value)
			if not prepare_ack:
				my_logger.debug("%s prepare failed")
				raise RuntimeError

			my_logger.debug("%s created transcation path %s", self.addr, transaction_path)
			#check run as greenlet?
			return "ok kv set"




	def primary_prepare(self, transaction_path, commit_path, key, value):
		prepare_ack = False
		for server in self.servers:
			if server.addr != self.addr:
				try:
					prepare_ack  = server.connection.backup_kv_prepare(self.addr, transaction_path, commit_path, key, value)
				except zerorpc.TimeoutExpired:
					my_logger.debug('%s : timeout from %s', self.addr, server.addr)
					prepare_ack = False
					break
		return prepare_ack

	def primary_commit(self, transaction_id):
		commited = False
		for server in self.servers:
			if server.addr != self.addr:
				try:
					commited = server.connection.kv_commit(self.addr, self.transaction_id,  key, value)
				except zerorpc.TimeoutExpired:
					my_logger.debug('%s : timeout from %s', self.addr, server.addr)
					commited = False
					break
		return commited


	####################################zerorpc functions incoming ################################
	def backup_kv_prepare(self, remote_addr, transaction_path, commit_path, key, value):
		if self.is_primary:
			my_logger.debug("%s called backup on primary",self.addr)
			raise SystemError
		else:
			try:
				# create and trigger primary_prepare_watch
				my_logger.debug("%s creating prepare node  adding exists watcher to%s ",self.addr, transaction_path+"/"+self.addr )
				#todo remember key, value pending prepares?
				path = self.zk.create(transaction_path+"/"+self.addr, self.addr, acl=None, ephemeral=False, sequence=True, makepath=True)
				self.zk.get(commit_path, watch = self.backup_kv_commit_watcher)
				return True

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)


	def backup_kv_commit_watcher(self, event):
		try:
		#master changed our node data, we will recreate it
			my_logger.debug("%s my node was changed! event %s", self.addr, event)
			if self.is_primary:
				my_logger.debug("%s called backup on primary",self.addr)
				raise SystemError
			else:
				if event.type == "CHANGED":
					commit_path = str(event.path)
					data = self.zk.get(commit_path)[0]
					key,value = data.split("=")
					my_logger.debug("%s child commited key %s value %s now recreate it bang", self.addr, key, value)
					#create should fire listener on commit for master
					#time.sleep(1)
					self.zk.create(commit_path+"/"+self.addr)

					#db = leveldb.LevelDB('./db'+str(self.i))
					#db.put(key,value)
		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)


	def kv_roll_back(self, remote_addr, transaction_id):
		if self.is_primary:
			return False
		else:
			t_state, key, value =  self.pending_transactions[transaction_id]
			self.pending_transactions[transaction_id] = (self.t_stati["Rollback"],key,value)
			db = leveldb.LevelDB('./db'+str(self.i))
			db.delete(key)


	####################################election  functions ################################
	def connection_listener(self,state):
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


	def cleanup(self):
		self.zk.stop()

	def get_prev_path(self, my_path):
		children = self.zk.get_children("/ELECTION/")
		prev_path = None
		tmp = sorted(children)
		if tmp[0] == my_path:
			return my_path, self.addr
		else:
			for child_path in sorted(children):
				if child_path == my_path:
					break
				prev_path = child_path
			return prev_path, self.zk.get("/ELECTION/"+prev_path)[0]

	def watch_node(self, my_path, prev_path):
		@self.zk.DataWatch("/ELECTION/"+prev_path)
		def watch_node(data, stat):
			if data is None and stat is None:
				prev_path, primary_addr = self.get_prev_path(my_path)
				if prev_path == my_path:
					my_logger.debug("%s %s deleted, i am first child, broadcast", self.addr, primary_addr)
					self.is_primary= True
				else:
					#node still exists but we got the same address, emphemeral node still there
					my_logger.debug("%s %s deleted but still not addmin, my_path %s prev_path %s ", self.addr, primary_addr, my_path, prev_path)
					if primary_addr != self.addr :
						self.watch_node( prev_path )
						self.is_primary = False

	def start_election(self):
		#todo errorhandling what to do if connection is lost
		self.zk.add_listener(self.connection_listener)
		if self.zk.exists("/ELECTION") is None:
			self.zk.ensure_path("/ELECTION")

		my_path  = self.zk.create("/ELECTION/guid-n_", self.addr, ephemeral=True, sequence=True)
		my_path = my_path.replace("/ELECTION/","")

		prev_path, primary_addr = self.get_prev_path(my_path)
		if primary_addr != self.addr:
			self.watch_node(my_path, prev_path)
			self.is_primary = False
		else:
			self.is_primary = True


if __name__ == '__main__':
	#todo use better option parseing
	try:
		os.remove("commit.log")
	except Exception:
		print "file not there"

	addr =  sys.argv[1]
	my_logger = logging.getLogger(__name__)
	my_logger.setLevel(logging.DEBUG)
	ch = logging.StreamHandler()
	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch.setFormatter(formatter)
	my_logger.addHandler(ch)
	storageserver = storageServer(addr)
	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()




