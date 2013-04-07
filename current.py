#!/usr/local/bin/python2.7
import traceback
import hashlib
import re
import inspect
import time
import logging
import logging.handlers
import sys
import gevent
import zerorpc
import collections
import leveldb

import kazoo
from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.exceptions import *
import os
from gevent import monkey
from gevent.queue import Queue, Empty
import base64
monkey.patch_all()


class storageServer(object):

	def __init__(self, addr, config_file='server_config'):

		self.op_log = "operations"+addr[-1:]+".log"
		self.election_path_prefix ="/MYLEADERELECTION/"
		self.storage_prefix = "/storageserver/tx"
		self.commit_prefix = "/storageserver/cx"
		self.is_primary = False
		self.primary_path = ""
		self.primary_addr = ""

		self.file_transmitsize = 1024 * 1024  #bytes offset
		self.quorum_size = 1
		self.addr = addr
		self.i = 0

		self.servers = []
		self.server_stati = {}
		self.uplist = set()

		self.stati = {"Down": 0, "Election": 1, "Reorganisation": 2, "Normal": 3}
		self.status = self.stati["Down"]

		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
			line = line.rstrip()
			if line.startswith("#"):
				continue
			if line == addr:
				self.i = i
				connection = self
			else:
				connection = zerorpc.Client(timeout=1,heartbeat=1)
				connection.connect('tcp://' + line)
				my_logger.debug("%s server %s added as up!", self.addr, line)
				self.server_stati[line] = "up"
				self.uplist.add(i)

			self.servers.append(ntconnection(line, connection, i))
			i += 1

		self.status = self.stati["Reorganisation"]
		self.pending_transactions = {}
		self.db = leveldb.LevelDB('./db' + str(self.i))
		#create task queue optional specify max size
		self.tasks = Queue()
		self.wtasks = Queue()
		self.commited ={}


	def start(self):
		self.zk = KazooClient(timeout=1)
		self.zk.start()
		self.status = self.stati["Normal"]
		self.start_election()
		my_logger.debug("%s self.is_primary %s", self.addr, self.is_primary)


	def primary_commit_watch(self, event):
		try:
			try:
				children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
			except NoNodeError:
				return

			if event.type == "DELETED" or event.path in self.commited:
				return

			elif event.type == "CHILD":
				if len(children) >= self.quorum_size:
					self.commited[event.path] =1
					my_logger.debug("%s commit successfull children %s", self.addr, len(children))
					commit_path = str(event.path)
					#TODO please put me into a function!
					data = str(self.zk.get(commit_path)[0])
					key, value = data.split("=")
					key_enc = base64.b64encode( key )
					value_enc = base64.b64encode( value )
					m = hashlib.md5()
					m.update(key_enc  +value_enc)
					f = open(self.op_log,"a")
					f.write(commit_path+" "+key_enc+" "+value_enc+" "+m.hexdigest()+"\n")
					f.close()
					self.tasks.put("commited")

			else:
				children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
				if len(children) >= self.quorum_size:
					return
				my_logger.debug("%s resetted watch children %s ", self.addr, children)

		except Exception, e:
			my_logger.debug(self.formatExceptionInfo())
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_prepare_watch(self, event):
		my_logger.debug(" %s primary_prepare_watch called %s", self.addr, event)
		try:
			if event.type == "DELETED":
				return

			elif event.type == "CHILD":
				if not self.zk.exists(event.path + "/ready") is None:
					return
				children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
				if len(children) == len(self.servers) - 1:
					self.zk.create(event.path + "/ready", acl=None, ephemeral=True, sequence=False, makepath=False)
					self.tasks.put("prepare_ready")
					return
			else:
				children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError

	def status_handler(self):
		task = ""
		try:
			while True:
				task = self.tasks.get(timeout=2) # decrements queue size by 1
				my_logger.debug(" %s got task %s", self.addr, task)
				gevent.sleep(0)
				if task == "commited" or task == "failed":
					break
			return task

		except Empty:
			return task

	def formatExceptionInfo(self, maxTBlevel=5):
		cla, exc, trbk = sys.exc_info()
		excName = cla.__name__
		try:
			excArgs = exc.__dict__["args"]
		except KeyError:
			excArgs = "<no args>"
		excTb = traceback.format_tb(trbk, maxTBlevel)
		return (excName, excArgs, excTb)


	def get_prio_by_addr(self,addr):
		my_logger.debug("%s got addr %s ", self.addr,addr)
		i = -1
		for i, server in enumerate(self.servers):
			if server.addr == addr:
				break
		return i

	def primary_cleanup_check(self,path):
		try:
			children = self.zk.get_children(path)
			sucessfull_childs = set()
			for child in children:
				replica_addr = str(child)
				if replica_addr!="ready" and replica_addr[0:14] != self.addr:
					sindex = self.get_prio_by_addr(str(child))
					if sindex != -1:
						sucessfull_childs.add(sindex)
				self.zk.delete(path+"/"+child)

			self.uplist = sucessfull_childs
			my_logger.debug("%s deleting path %s uplist now is %s ", self.addr, path, self.uplist)
			self.zk.delete(path, recursive=False)

		except Exception, e:
			my_logger.debug(self.formatExceptionInfo())
			my_logger.debug("%s Couldn't do it: %s %s", self.addr, e, KazooException.message)
			raise SystemError

	def primary_cleanup(self, prepare_path, commit_path):

		self.primary_cleanup_check(prepare_path)
		self.primary_cleanup_check(commit_path)

	def primary_transmit_oplog(self, remote_addr, replica_size):

		offset 	= replica_size
		content = []
		fname 	= self.op_log

		if not os.path.isfile(fname):
			return content

		transmitsize = self.file_transmitsize
		fsize = os.path.getsize(fname)
		my_logger.debug("%s syncing to %s offset %s my_size %s replica_size %s",
			self.addr, remote_addr, offset, fsize, replica_size)

		if fsize < offset :
			my_logger.debug("%s argh what happened replica has bigger size %s", self.addr, remote_addr)
			raise SystemError
		elif fsize == offset:
			my_logger.debug("%s sync successfull fsize = replica_size", self.addr)
			transmitsize = -1
		elif fsize - offset < transmitsize:
			#transfer the whole file in one block
			my_logger.debug("%s only small bits missing", self.addr)
			#read until the end of file!
			transmitsize = 0
		elif offset > fsize:
			my_logger.debug("%s offset %s bigger than fsize %s"
				, self.addr, offset, fsize)
			transmitsize = -1
			raise SystemError
		elif offset + transmitsize > fsize:
			transmitsize = 0
			my_logger.debug("%s offset + transmitsize %s bigger than fsize %s ")
		else:
			transmitsize = self.file_transmitsize

		my_logger.debug("%s remaining size of oplog to sync with %s is %s"
			, self.addr, remote_addr,  fsize - replica_size)

		if (fsize - replica_size) < 1024 :
			try:
				if self.zk.exists("/syncpath/"+remote_addr) is None:
					self.zk.create("/syncpath/"+remote_addr, ephemeral=True, makepath=True)
			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError

		if transmitsize != -1:
			if os.path.isfile(fname):
				file = open(fname)
				file.seek(offset)
				content = file.readlines(transmitsize)
			else:
				my_logger.debug("%s file  %s not available", self.addr, fname)

		return content

	def get_connection_by_addr(self, addr):
		for server in self.servers:
			if server.addr == addr :
				return server.connection
		raise SystemError

	def backup_get_oplog(self, primary_addr):

		#todo check my own oplog before just deleting it
		if os.path.isfile(self.op_log):
			os.remove(self.op_log)

		if primary_addr == self.addr:
			my_logger.debug("%s i am primary %s", self.addr, primary_addr)
			#raise SystemError
			return

		my_logger.debug("%s trying to get oplog from %s", self.addr, primary_addr)

		p_connection = zerorpc.Client(timeout=30)
		p_connection.connect('tcp://' + primary_addr)
		retries = 0
		max_retries = 1
		read_offset = 0
		start = time.time()
		size_received = 0
		failcount = 0
		max_failcount = 1

		f = open(self.op_log,"a")
		while True:
			my_logger.debug("%s send size received to server as %s"
				, self.addr, size_received)
			try:
				lines_tupel = p_connection.primary_transmit_oplog(self.addr, size_received )
			except zerorpc.TimeoutExpired:
				return

			if len(lines_tupel) == 0:
				if retries < max_retries:
					retries += 1
					continue
				else:
					break

			for line in lines_tupel:
				tid, key, value, digest = line.rstrip().split(" ")
				md5 = hashlib.md5()
				md5.update(key + value)
				if md5.hexdigest() == digest:
					self.db.Put(base64.b64decode( key ), base64.b64decode( value ))
				else:
					if failcount == 0:
						failcount += 1
						#trigger retransmisson
						size_received = 0
						continue
					else:
						raise SystemError

				#TODO put that in a function!
				f.write(tid+" "+key+" "+value+" "+digest+"\n")
				size_received += len(line)

			read_offset += self.file_transmitsize

		elapsed = time.time() - start
		my_logger.debug("%s size received %s time elapsed %s using transmitsize %s", self.addr
			, size_received, elapsed, self.file_transmitsize)

		#todo check if we really sent the message to the primary, otherwise use address
		#received from replica or find out ourselves and go back to recovery
		right_oplog_version = p_connection.primary_register(size_received, self.addr, self.i)

		if right_oplog_version:
			self.status = self.stati["Normal"]
			my_logger.debug("%s we made it! sync complete in %s", self.addr
				,elapsed)
		else:
			my_logger.debug("%s getting oplog failed, couldn't catch up %s", self.addr, primary_addr)



	def kv_set(self, key, value, remote_addr):

		if self.is_primary:
			#check if a node is syncing on syncpath
			try:
				if not self.zk.exists("/syncpath") is None:
					children = self.zk.get_children("/syncpath")
					my_logger.debug(" %s children on syncpath %s", self.addr, str(children))
					if len(children) > 0:
						return "syncing "+str(children)

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError

			lock = self.zk.Lock("/lockpath", "my-identifier")
			with lock:
				#todo check if we have quorum before checking request?
				# todo keep a lock , check if we should spawn greenlet per request or is this taken care of already?
				if self.status != self.stati["Normal"]:
					return 0

				my_logger.debug(" %s started handling request from  %s", self.addr, remote_addr)
				self.g_status_handler = gevent.Greenlet(self.status_handler)
				self.g_status_handler.start()

				self.tasks.put("handling")

				commit_path = str(
					self.zk.create(self.commit_prefix, key + "=" + value, acl=None, ephemeral=False, sequence=True,
						makepath=True))

				prepare_path = str(
					self.zk.create(self.storage_prefix, commit_path.encode('UTF-8'), acl=None, ephemeral=False,
						sequence=True, makepath=True))

				children = self.zk.get_children(commit_path, watch=self.primary_commit_watch)
				children = self.zk.get_children(prepare_path, watch=self.primary_prepare_watch)

				prepare_acks = self.primary_prepare(prepare_path, commit_path, key, value)
				self.tasks.put("prepareacks" + str(prepare_acks))

				if prepare_acks < self.quorum_size:
					my_logger.debug("%s prepare failed  prepare_acks %s quorumsize %s ", self.addr, prepare_acks,
						self.quorum_size)
					my_logger.debug("%s prepare failed %s %s", self.addr, str(self.server_stati), self.uplist)
					self.tasks.put("failed")
				else:
					#we still have enough supporters, trigger the prepare watch myself
					for addr, status in self.server_stati.iteritems():
						if status == "down":
							#use primary addr here to distinguish
							path = self.zk.create(prepare_path + "/" + self.addr, self.addr, acl=None, ephemeral=True,
								sequence=True, makepath=True)

				#if a client signals us to wait it will signal for the wait handler
				gevent.joinall([
					self.g_status_handler,
				])
				my_logger.debug("%s result received %s", self.addr, self.g_status_handler.value)

			lock.release()
			gevent.spawn_later(2, self.primary_cleanup, prepare_path, commit_path)
			return self.g_status_handler.value

		else:
			my_logger.debug("%s sorry not primary", self.addr)
			return 0

	def primary_prepare(self, prepare_path, commit_path, key, value):
		prepare_acks = 0
		for server in self.servers:
			if server.addr != self.addr and self.server_stati[server.addr] == "up":
				try:
					prepare_ok = server.connection.backup_kv_prepare(self.addr, prepare_path, commit_path, key, value)
					if prepare_ok:
						prepare_acks += 1
				except zerorpc.TimeoutExpired:
					self.server_stati[server.addr] = "down"
					self.uplist.discard(server.prio)
					my_logger.debug('%s : timeout from %s uplist now %s ', self.addr, server.addr, str(self.uplist))

		return prepare_acks


	####################################zerorpc functions incoming ################################
	def backup_kv_prepare(self, remote_addr, prepare_path, commit_path, key, value):
		if self.is_primary:
			my_logger.debug("%s called backup on primary", self.addr)
			raise SystemError

		elif self.status != self.stati["Normal"]:
			raise zerorpc.TimeoutExpired

		else:
			try:
				if not self.zk.exists(prepare_path+"/ready") is None:
					my_logger.debug("%s ready on  prepare_path already set ---------------> STALE ", self.addr)
					return

				if self.zk.exists(prepare_path) is None:
					my_logger.debug("%s this prepare_path %s doesnt exist anymore, returning "
						, self.addr, prepare_path + "/" + self.addr)
					return

				# create and trigger primary_prepare_watch
				# check we coul have gotten an old message here, check if the prepare path still exists!
				my_logger.debug("%s creating prepare node  adding exists watcher to%s ", self.addr,
					prepare_path + "/" + self.addr)

				self.zk.exists(prepare_path + "/ready", watch=self.backup_kv_commit_watcher)
				path = self.zk.create(prepare_path + "/" + self.addr, self.addr, acl=None, ephemeral=True,
					sequence=False, makepath=False)
				return True

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError


	def backup_kv_commit_watcher(self, event):
		#fired on create of ready node on prepare_path from exists watcher in backup_kv_prepare
		try:
			if self.is_primary:
				my_logger.debug("%s called backup on primary", self.addr)
				raise SystemError
			else:
				if event.type == "CREATED":
					ready_path = str(event.path)
					prepare_path = ready_path[0:ready_path.rfind("/")]
					commit_path = str(self.zk.get(prepare_path)[0])
					data = str(self.zk.get(commit_path)[0])
					key, value = data.split("=")
					#TODO write log here!

					self.db.Put(key, value)
					#write log here
					key_enc = base64.b64encode( key )
					value_enc = base64.b64encode( value )
					m = hashlib.md5()
					m.update(key_enc  +value_enc)
					f = open(self.op_log,"a")
					f.write(commit_path+" "+key_enc+" "+value_enc+" "+m.hexdigest()+"\n")
					f.close()

					#if we already commited theres no need to create
					self.zk.create(commit_path + "/" + self.addr)
					my_logger.debug("%s backup commmited key %s value %s ", self.addr, key, value)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_register(self, op_log_size, backup_addr, remote_prio):

		if not os.path.isfile(self.op_log):
			return True
		try:
			if not  self.zk.exists("/syncpath/"+backup_addr) is None:
				self.zk.delete("/syncpath/"+backup_addr)
				my_logger.debug(" %s deleted syncpath for %s ", self.addr, "/syncpath/"+backup_addr)
			else:
				my_logger.debug(" %s where is the fking node...%s ", self.addr, "/syncpath/"+backup_addr)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError

		if op_log_size == os.path.getsize(self.op_log) :
			#todo move this into function and make it atomic?
			self.uplist.add(remote_prio)
			self.server_stati[backup_addr] = "up"
			my_logger.debug(" %s added remote prio %s to uplist, uplist now %s "
				, self.addr, remote_prio, str(self.uplist))
			return True
		else:
			my_logger.debug(" %s size does not match oplogsize %s path size %s "
				, self.addr, op_log_size, os.path.getsize("log5000.txt") )
			return False

	def get_primary_addr(self):

		if self.is_primary:
			return self.addr

		children = self.zk.get_children(self.election_path_prefix)
		if len(children) == 0:
			my_logger.debug("%s somethings very wrong here", self.addr)
			raise SystemError

		primary_path = self.get_sorted_children()[0]
		primary_addr = str(self.zk.get(self.election_path_prefix + primary_path)[0])
		my_logger.debug("%s i am not primary", self.addr)
		return primary_addr

	####################################election  functions ################################
	def connection_listener(self, state):
		#todo got to election!
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


	def get_sorted_children(self):

		children = self.zk.get_children(self.election_path_prefix)
		# can't just sort directly:
		# sort the children by logfilesize nodename format is s[oplogsize]_sequencenumber
		children.sort(key=lambda c: c[c.find("guid_n") + len("guid_n"):])
		return children

	def get_prev_path(self):

		prev_path = None
		primary_path = None
		sorted_children = self.get_sorted_children()
		primary_path = sorted_children[0]
		my_logger.debug("%s sorted children is %s", self.addr, sorted_children)
		my_path = self.election_path.replace(self.election_path_prefix, "")
		if primary_path == my_path:
			return None, self.election_path

		else:
			for child_path in sorted_children:
				if child_path == my_path:
					break
				else:
					prev_path = child_path

		return  str(self.election_path_prefix + prev_path), str(self.election_path_prefix+primary_path)

	#todo since we have to reset the watch every time check if not better use children watch!
	def watch_node(self, prev_path):
		print "setting watch on ", prev_path
		@self.zk.DataWatch( prev_path)
		def watch_node(data, stat):
			try:
				if data is None and stat is None:
					prev_path, primary_path = self.get_prev_path()
					if prev_path is None:
						my_logger.debug("%s prev_path deleted prev_path %s primary_path %s"
							,self.addr, prev_path, primary_path)
						self.set_primary(True, primary_path)
						return

					if not self.zk.exists(primary_path) is None:
						if self.zk.get(primary_path)[0] != self.addr:
							my_logger.debug("%s %s deleted but still not addmin, my_path %s prev_path %s "
								, self.addr,self.zk.get(primary_path)[0], self.election_path, prev_path)
						else:
							my_logger.debug("%s ok lets wait node with our address still there %s"
								, self.addr, self.zk.get(primary_path)[0])
					else:
						my_logger.debug("%s primary path %s does not exisit anymore ", self.addr, primary_path)

					my_logger.debug("%s resetting watch to prev_path %s ", self.addr, prev_path)
					self.watch_node(prev_path)

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, str(self.formatExceptionInfo(Exception)))
				raise SystemError

	def set_primary(self, is_primary, primary_path):

		my_logger.debug(">>>>>>>>>>>>>>>>>>>>>>>>set<<<<<<<<<<<<<<<<<<")
		my_logger.debug("%s PRIMARY  %s set from %s", self.addr, is_primary, inspect.stack()[1][3])
		my_logger.debug("%s self.election_path is %s", self.addr, self.election_path)
		my_logger.debug("%s sorted children is %s", self.addr, self.get_sorted_children())

		if self.primary_path == primary_path:
			my_logger.debug("%s already got path %s as primary_path, ignored ", self.addr, primary_path)
			return
		else:
			my_logger.debug("%s setting my primary to %s with path %s ",self.addr, is_primary, primary_path)
			self.is_primary = is_primary
			self.primary_path = primary_path

		if not is_primary:
			if not self.zk.exists( primary_path ) is None:
				primary_addr = str(self.zk.get(primary_path)[0])
				#stale primary with same address after node restart, ignore
				if primary_addr == self.addr:
					my_logger.debug("%s primary_addr %s same as mine, ignored ",self.addr, primary_addr)
					return

				#check if primary knows he's primary
				primary_conn = self.get_connection_by_addr(addr)
				try:
					r_primary_addr = primary_conn.get_primary_addr()
					my_logger.debug("%s primary_addr %s confirmed primary is %s",self.addr, primary_addr, r_primary_addr )
				except zerorpc.TimeoutExpired:
					#primary not reachable or already new election running, wait
					my_logger.debug("%s primary_addr %s not reachable, timeout ",self.addr, primary_addr )
					return
				if r_primary_addr == primary_addr and r_primary_addr != self.addr:
						my_logger.debug("%s primary_addr %s confirmed getting oplog now!",self.addr, primary_addr)
						self.backup_get_oplog(r_primary_addr)

			else:
				my_logger.debug("%s primary path %s does not exist ",self.addr, primary_path)

		else:
			for server in self.servers:
				if server.addr != self.addr:
					try:
						print "setting primary on ", server.addr
						#todo check if they accept!
						server.connection.set_primary(False, self.election_path)
					except zerorpc.TimeoutExpired:
						print "timeout setting primary on ", server.addr

	def start_election(self):
		'''

		 Election is performed on zookeeper. Each node creates a path on zookeeper
		 the first node on the path wins the election.

		 Each node keeps a watch on its predecessor. Notified about a change it will check if
		 its the first node on the path. The new Primary has to notify each replica. The primary
		 will not do any communication with the replica until it has completely synchronised its
		 operation log.

		 On notification Replicas will start to sync their operation log with the new primary.
		 After successfull synchronisation the replica will register with the primary and take
		 part in the backup process.

		'''

		self.zk.add_listener(self.connection_listener)
		if self.zk.exists(self.election_path_prefix) is None:
			self.zk.ensure_path(self.election_path_prefix)

		self.election_path = self.zk.create(self.election_path_prefix + "guid_n"
			, self.addr, ephemeral=True, sequence=True
		)

		my_logger.debug("%s my path is %s", self.addr, self.election_path)
		prev_path, primary_path = self.get_prev_path()

		if  prev_path is None:
			self.set_primary(True, primary_path)
		else:
			self.watch_node(prev_path)
			self.set_primary(False, primary_path)


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


	storageserver = storageServer(addr)
	s = zerorpc.Server(storageserver)
	s.bind('tcp://' + addr)
	storageserver.start()
	s.run()

