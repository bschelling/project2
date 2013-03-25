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
from gevent.event import AsyncResult
from gevent import monkey
monkey.patch_all()



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
				connection = zerorpc.Client()
				connection.connect('tcp://' + line)

			#todo to we need an uplist here?
			self.servers.append(ntconnection(line,connection,i,"up"))
			i+=1

		self.status = self.stati["Reorganisation"]
		self.pending_transactions = {}
		self.db = leveldb.LevelDB('./db'+str(self.i))


	def start(self):

		self.zk = KazooClient()
		self.zk.start()
		self.status = self.stati["Normal"]
		self.start_election()
		my_logger.debug("%s self.is_primary %s",self.addr, self.is_primary)

	def primary_commit_watch(self, event):
		children = self.zk.get_children(event.path)
		my_logger.debug("%s got a commit from %s children %s", self.addr, event, children)
		try:
			if event.type == "CHILD":
				if len(children) == len(self.servers) - 1:
					my_logger.debug("%s commit successfull children %s", self.addr, len(children) )
					commit_path = str(event.path)
					data = str(self.zk.get(commit_path)[0])
					key , value = data.split("=")
					#todo use base64encode
					op_logger.debug("%s  %s  %s", commit_path, key, value)

					#todo10 maybe nicer way to do this?
					self.g_setter.args = "1"
					self.g_setter.start()
					my_logger.debug("%s spawned setter ", self.addr)
				else:
					children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
			else:
				my_logger.debug("%s reset watch event %s ", self.addr, event)
				children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
				my_logger.debug("%s children %s ", self.addr, children)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_prepare_watch(self, event):
		my_logger.debug(" %s primary_prepare_watch called %s", self.addr, event)
		children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
		try:
			if event.type == "CHILD":
				if len(children) == len(self.servers) - 1:
					#my_logger.debug("%s adding ready node to prepare path %s ", self.addr, event.path)
					self.zk.create(event.path+"/ready", acl=None, ephemeral=True, sequence=False, makepath=False)
				else:
					children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
					#my_logger.debug("%s resetting watch for %s children is  %s", self.addr, event.path, children)

			else:
				my_logger.debug("%s reset watch event %s ", self.addr, event)
				children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
				#my_logger.debug("%s ATTENTION children here? %s ", self.addr, children)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError

	def setter(self, status):
		my_logger.debug("%s setting status for waiter %s ", self.addr,  status)
		self.a.set(status)
		return

	def waiter(self):
		status = self.a.get()
		my_logger.debug("%s returning status  %s ", self.addr,  status)
		return status

	def kv_set(self, key, value, remote_addr):
		#check have to use a here?

		if self.is_primary:
			self.a = AsyncResult()
			#don't start setter only initialize
			self.g_setter = gevent.Greenlet(self.setter)

			my_logger.debug(" %s started handling request from  %s", self.addr, remote_addr)
			commit_path = str(self.zk.create(self.commit_prefix , key +"="+value , acl=None, ephemeral=False, sequence=True, makepath=True))
			prepare_path = self.zk.create(self.storage_prefix, commit_path.encode('UTF-8') , acl=None, ephemeral=False, sequence=True, makepath=True)

			children = self.zk.get_children(commit_path,watch=self.primary_commit_watch)
			children = self.zk.get_children(prepare_path, watch=self.primary_prepare_watch)
			prepare_ack = self.primary_prepare(prepare_path, commit_path, key, value)
			if not prepare_ack:
				my_logger.debug("%s prepare failed")
				raise RuntimeError

			self.g_waiter = gevent.spawn(self.waiter)
			gevent.joinall([
	    		self.g_setter,
				self.g_waiter,
			])
			my_logger.debug("%s result received %s", self.addr, self.g_waiter.value)
			return self.g_waiter.value

		else:
			my_logger.debug("%s sorry not primary", self.addr)
			return False

	def primary_prepare(self, prepare_path, commit_path, key, value):
		prepare_ack = False
		for server in self.servers:
			if server.addr != self.addr:
				try:
					prepare_ack  = server.connection.backup_kv_prepare(self.addr, prepare_path, commit_path, key, value)
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
	def backup_kv_prepare(self, remote_addr, prepare_path, commit_path, key, value):
		if self.is_primary:
			my_logger.debug("%s called backup on primary",self.addr)
			raise SystemError
		else:
			try:
				# create and trigger primary_prepare_watch
				my_logger.debug("%s creating prepare node  adding exists watcher to%s ",self.addr, prepare_path+"/"+self.addr )
				self.zk.exists(prepare_path+"/ready", watch=self.backup_kv_commit_watcher)
				path = self.zk.create(prepare_path+"/"+self.addr, self.addr, acl=None, ephemeral=False, sequence=True, makepath=True)
				return True

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError


	def backup_kv_commit_watcher(self, event):
		#fired on create of ready node on prepare_path from exists watcher in backup_kv_prepare
		try:
			#my_logger.debug("%s event %s", self.addr, event)
			if self.is_primary:
				my_logger.debug("%s called backup on primary",self.addr)
				raise SystemError
			else:
				if event.type == "CREATED":
					#/storageserver/tx0000006618/ready
					ready_path = str(event.path)
					prepare_path = ready_path[0:ready_path.rfind("/")]
					commit_path = str( self.zk.get(prepare_path)[0] )
					data = str(self.zk.get(commit_path)[0])
					key , value = data.split("=")
					self.db.Put(key, value)
					self.zk.create(commit_path+"/"+self.addr)
					my_logger.debug("%s backup commmited key %s value %s ", self.addr, key, value)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


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
				#todo remove this or move to cleanup
		if self.is_primary:
			self.zk.delete(self.storage_prefix, recursive=True)
			self.zk.delete(self.commit_prefix, recursive=True)
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
					if primary_addr != self.addr :
						my_logger.debug("%s %s deleted but still not addmin, my_path %s prev_path %s ", self.addr, primary_addr, my_path, prev_path)
						self.watch_node( prev_path )
						self.is_primary = False
					else:
						#node still exists but we got the same address, emphemeral node still there
						self.is_primary = True

	#todo better use this than set values, check if we have to do
	def set_primary(self, is_primary):
		self.is_primary = is_primary
		if self.is_primary:
			self.zk.delete(self.storage_prefix, recursive=True)
			self.zk.delete(self.commit_prefix, recursive=True)





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
	addr =  sys.argv[1]


	my_logger = logging.getLogger("storagelogger")
	my_logger.setLevel(logging.DEBUG)

	formatter = logging.Formatter('[%(asctime)s] %(message)s %(funcName)s:%(lineno)d')
	ch = logging.StreamHandler()
	ch.setFormatter(formatter)
	my_logger.addHandler(ch)

	fh = logging.FileHandler("server"+addr[-1:]+".log", mode="w")
	fh.setFormatter(formatter)
	my_logger.addHandler(fh)

	op_logger = logging.getLogger("oplogger")
	op_logger.setLevel(logging.DEBUG)
	fop = logging.FileHandler("operations"+addr[-1:]+".log", mode="w")
	fop_formatter = logging.Formatter('[%(asctime)s] %(message)s')
	fop.setFormatter(fop_formatter)
	op_logger.addHandler(fop)



	storageserver = storageServer(addr)
	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()




