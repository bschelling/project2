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
import shutil
monkey.patch_all()


class storageServer(object):

	def __init__(self, addr, config_file='server_config'):

		self.op_log = "operations"+addr[-1:]+".log"
		self.election_path_prefix ="/MYLEADERELECTION/"
		self.election_path_counter="/MYLEADERELECTION_COUNT/"
		self.election_id = 0
		self.storage_prefix = "/storageserver/tx"
		self.commit_prefix = "/storageserver/cx"

		self.is_primary = False
		self.primary_path = ""
		self.primary_addr = ""

		self.file_transmitsize = 1024  * 1024  #bytes offset
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
				connection = zerorpc.Client(timeout=1)
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


	def start(self, restart=0):

		self.zk = KazooClient(timeout=1)
		self.zk.start()
		self.start_election()
		my_logger.debug("%s self.is_primary %s", self.addr, self.is_primary)


	def write_data(self,commit_path, key, value):
		key_enc = base64.b64encode( key )
		value_enc = base64.b64encode( value )
		m = hashlib.md5()
		m.update(key_enc  +value_enc)
		f = open(self.op_log,"a")
		f.write(commit_path+" "+key_enc+" "+value_enc+" "+m.hexdigest()+"\n")
		f.close()


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
					my_logger.debug("%s commit successfull children %s", self.addr, len(children))
					commit_path = str(event.path)
					data = str(self.zk.get(commit_path)[0])
					key, value = data.split("=")
					self.write_data(commit_path, key, value)
					self.tasks.put("commited")
					self.commited[event.path] =1

			else:
				children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
				if len(children) >= self.quorum_size:
					return
				my_logger.debug("%s resetted watch children %s ", self.addr, children)

		except Exception, e:
			my_logger.debug(self.formatExceptionInfo())
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError

	def oplog_sanity_check(self):

		if not os.path.isfile(self.op_log):
			return 0
		else:
			return os.path.getsize(self.op_log)


	def oplog_sanity_check_todo(self):
		#called on restart of a note, check if my oplog file is sane
		# todo check if start of oplog is the as server has

		f = open(self.op_log)
		is_sane_op_log = True

		#todo check for ascending transaction sequence
		for line in f:
			try:
				tid, key, value, digest = line.rstrip().split(" ")
			except ValueError:
				logging.debug(line)
				op_log_sanity = False
				raise SystemError
				break

			md5 = hashlib.md5()
			md5.update(key + value)
			if md5.hexdigest() == digest:
				continue
			else:
				logging.debug(line)
				op_log_sanity = False
				raise SystemError
				break

		f.close()
		if is_sane_op_log:
			return os.path.getsize(self.op_log)
		else:
			#todo empty local database
			return 0

	def primary_prepare_watch(self, event):
		#my_logger.debug(" %s primary_prepare_watch called %s", self.addr, event)
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
				#my_logger.debug(" %s got task %s", self.addr, task)
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
		#my_logger.debug("%s got addr %s ", self.addr,addr)
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
			#my_logger.debug("%s deleting path %s uplist now is %s ", self.addr, path, self.uplist)
			self.zk.delete(path, recursive=False)

		except Exception, e:
			my_logger.debug(self.formatExceptionInfo())
			#my_logger.debug("%s Couldn't do it: %s %s", self.addr, e, KazooException.message)
			raise SystemError

	def primary_cleanup(self, prepare_path, commit_path):

		self.primary_cleanup_check(prepare_path)
		self.primary_cleanup_check(commit_path)


	#if the replica dies we want the syncpath to go away, so check it here!
	def backup_create_syncpath(self):
		try:
			if self.zk.exists("/syncpath/"+self.addr) is None:
				syncpath = self.zk.create("/syncpath/"+self.addr, ephemeral=True, makepath=True)
			else:
				my_logger.debug(" %s TODO syncpath already exists, check log", self.addr)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_transmit_oplog(self, remote_addr, replica_size):

		offset 	= replica_size
		content = []
		fname 	= self.op_log
		transmitsize = self.file_transmitsize

		if not self.is_primary:
			#todo restart election here??
			print "not primary nothing to transmit"
			return content

		if not os.path.isfile(fname):
			fsize = 0

		else:
			fsize = os.path.getsize(fname)
			my_logger.debug("%s syncing to %s offset %s my_size %s replica_size %s",
			self.addr, remote_addr, offset, fsize, replica_size)

		if fsize < offset :
			self.is_primary = False
			my_logger.debug("%s argh what happened replica has bigger size %s", self.addr, remote_addr)
			gevent.sleep(3)
			self.zk.stop()
			return content

		elif fsize == offset:
			my_logger.debug("%s sync successfull fsize = replica_size", self.addr)
			transmitsize = -1
		elif fsize - offset < transmitsize:
			#transfer the whole file in one block
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

		#todo how soon do we want the syncpath to be created, tradeoff stability/speed
		if (fsize - replica_size) < 1024 * 1024 :
			my_logger.debug("%s only %s bytes missing ", self.addr, str(fsize - offset))
			conn = self.get_connection_by_addr(remote_addr)
			try:
				conn.backup_create_syncpath()
			except zerorpc.TimeoutExpired:
				my_logger.debug("%s timeout on calling backup_create_syncpath remote_addr", self.addr, remote_addr)

		if transmitsize != -1:
			if os.path.isfile(fname):
				file = open(fname)
				file.seek(offset)
				content = file.readlines(transmitsize)
			else:
				my_logger.debug("%s file  %s not available", self.addr, fname)
				raise SystemError

		return content

	def get_connection_by_addr(self, addr):
		for server in self.servers:
			if server.addr == addr :
				print "ok i got an connection here", type(server.connection)
				return server.connection

		raise SystemError

	def backup_get_oplog(self):
		if self.is_primary:
			return

		primary_addr = self.primary_addr
		my_logger.debug("%s trying to get oplog from %s", self.addr, primary_addr)

		p_connection = zerorpc.Client(timeout=30)
		p_connection.connect('tcp://' + primary_addr)
		retries = 0
		max_retries = 2
		start = time.time()
		failcount = 0
		max_failcount = 1

		#check if operationlog is sane or has to be retransmitted

		size_received = 0
		#todo first value has to be the same in both files
		if os.path.isfile(self.op_log):
			size_received = self.oplog_sanity_check()
			if size_received == 0:
				print "<<<<<<<<<<<<<<<<<<<<<<<<<< full sync no valid operations file"
				os.remove(self.op_log)
				f = open(self.op_log+".bak","w")
			else:
				print "<<<<<<<<<<<<<<<<<<<<<<<<<< copy file append", size_received
				shutil.copy2(self.op_log, self.op_log+".bak")
				f = open(self.op_log+".bak","a")
		else:
			f = open(self.op_log+".bak","w")

		while retries < max_retries :

			my_logger.debug("%s send size received to server as %s"
				, self.addr, size_received)
			try:
				lines_tupel = p_connection.primary_transmit_oplog(self.addr, size_received )
			except zerorpc.TimeoutExpired:
				return

			if len(lines_tupel) == 0:
				if retries < max_retries:
					retries += 1
					gevent.sleep(1)

			for line in lines_tupel:
				if self.is_primary or primary_addr != self.primary_addr:
					my_logger.debug("%s primary has changed to self.primary_addr giving up! %s"
									, self.addr, self.primary_addr)
					f.close()
					return
				try:
					tid, key, value, digest = line.rstrip().split(" ")

				except ValueError:
					#TODO
					open("debugx.txt","w").write(str(lines_tupel))
					my_logger.debug("%s parseerror at offset %s", self.addr ,size_received)
					retries += 1
					f.close()
					f = open(self.op_log+".bak","w")
					size_received = 0
					continue

				md5 = hashlib.md5()
				md5.update(key + value)
				if md5.hexdigest() == digest:
					self.db.Put(base64.b64decode( key ), base64.b64decode( value ))
					f.write(tid+" "+key+" "+value+" "+digest+"\n")
					size_received += len(line)
				else:
					if failcount == 0:
						failcount += 1
						#trigger retransmisson
						size_received = 0
						continue
					else:
						raise SystemError

		f.close()
		elapsed = time.time() - start
		my_logger.debug("%s size received %s time elapsed %s using transmitsize %s", self.addr
			, size_received, elapsed, self.file_transmitsize)

		try:
			right_oplog_version = p_connection.primary_register(size_received, self.addr, self.i)

		except zerorpc.TimeoutExpired:
			my_logger.debug("%s could not reach primary after sync %s", self.addr
				,elapsed)
			raise SystemError

		if right_oplog_version:
			my_logger.debug("%s >>>>>>>>>>>>>>>>>>>>>> we made it! sync complete in %s", self.addr
				,elapsed)
			os.rename(self.op_log+".bak", self.op_log)
			self.status = self.stati["Normal"]
			if not self.zk.exists("/syncpath/"+self.addr) is None:
				self.zk.delete("/syncpath/"+self.addr)

		else:
			my_logger.debug("%s getting oplog failed, couldn't catch up %s", self.addr, primary_addr)
			if not self.zk.exists("/syncpath/"+self.addr) is None:
				self.zk.delete("/syncpath/"+self.addr)


	def kv_get(self, key, remote_addr):
		val = self.db.Get(key)
		return val
		#TODO
		#lock = self.zk.Lock("/lockpath", "my-identifier")
		#with lock:
		#	val = self.db.Get(key)
		#	lock.release()


	def kv_set(self, key, value, remote_addr):

		if self.is_primary:
			if  self.status != self.stati["Normal"]:
				return (500, self.status)

			#check if a node is syncing on syncpath
			try:
				if not self.zk.exists("/syncpath") is None:
					children = self.zk.get_children("/syncpath")
					#my_logger.debug(" %s children on syncpath %s", self.addr, str(children))
					if len(children) > 0:
						return "syncing "+str(children)

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError

			print "lock here....."
			#lock = self.zk.Lock("/lockpath", "my-identifier")
			#todo check lock here
			if True:
				if self.status != self.stati["Normal"]:
					lock.release()
					return (500, "my status not normal")

				#my_logger.debug(" %s started handling request from  %s", self.addr, remote_addr)
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


			status = 500
			if self.g_status_handler.value == "commited":
				status = 200

			gevent.spawn_later(2, self.primary_cleanup, prepare_path, commit_path)
			#lock.release()
			return (status, self.g_status_handler.value)


		else:
			my_logger.debug("%s sorry not primary", self.addr)
			status = 400
			return (status, self.get_primary_addr())

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
			#raise SystemError
			return

		elif self.status != self.stati["Normal"]:
			my_logger.debug("%s not normal!", self.addr)
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
					self.write_data(commit_path, key, value)
					#if we already commited theres no need to create
					self.zk.create(commit_path + "/" + self.addr)
					my_logger.debug("%s backup commmited key %s value %s ", self.addr, key, value)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_register(self, op_log_size, backup_addr, remote_prio):
		'''

		 if a node wants to register and we have an operation log, the replica has
		 to proove it already synced the operationlog completely and without errors
		 see backup_get_oplog. op_log_size corresponds to operation log size synced
		 since we don't allow client request as long as the operation log is synced
		 we have to remove the lock on the syncpath if transmission was sucessfull.

		'''
		if not self.is_primary:
			my_logger.debug(" %s sorry not primary ", self.addr)
			return False

		if not os.path.isfile(self.op_log) and op_log_size == 0:
			my_logger.debug(" %s my op_log file is zero! ", self.addr)
			return True

		elif not os.path.isfile(self.op_log) and op_log_size > 0:
			#raise SystemError
			return False

		else:
			local_size = os.path.getsize(self.op_log)
			if op_log_size ==  local_size:
				my_logger.debug(" %s my oplog %s size %s remoteoplog size %s ", self.addr, self.op_log, local_size, op_log_size)
				self.uplist.add(remote_prio)
				self.server_stati[backup_addr] = "up"
				my_logger.debug(" %s added remote prio %s to uplist, uplist now %s "
					, self.addr, remote_prio, str(self.uplist))
				return True

			else:
				my_logger.debug(" %s client oplogsize %s does not match my op_log_size  %s "
					, self.addr, op_log_size, os.path.getsize(self.op_log) )
				#force backup to retry!
				return False


	def get_primary_addr(self):
		my_logger.debug("%s >>>>>>>>>>>>>>>>>>>>got called!",self.addr)
		if self.is_primary:
			my_logger.debug("%s >>>>>>>>>>>>>>>>>>>>returning %s!",self.addr, self.addr)
			return self.addr
		else:
			my_logger.debug("%s >>>>>>>>>>>>>>>>>>>>returning %s!",self.addr, self.primary_addr)
			return self.primary_addr

	####################################election  functions ################################
	def connection_listener(self, state):
		my_logger.debug('%s : >>>>>>>>>>>> connection_listener <<<<<<<<<<< got state %s', self.addr, state)
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
			my_logger.debug('%s : restarting', self.addr)
			self.status = self.stati["Down"]
			self.start(1)

		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
			#will not answer to requests,
			self.status = self.stati["Down"]
		else:
			my_logger.debug('%s : >>>>>>>>>>>> running in state %s', self.addr, state)
			#call election!
			#self.start_election()

	def get_sorted_children(self):
		#check if children really exist
		children = self.zk.get_children(self.election_path_prefix)
		# can't just sort directly: the node names are prefixed by uuids
		children.sort(key=lambda c: c[c.find("guid_n") + len("guid_n"):])
		return children


	def get_primary_path(self):
		primary_path = None
		sorted_children = self.get_sorted_children()
		primary_path = sorted_children[0]
		return self.election_path_prefix+primary_path


	def watch_node(self, primary_path):
		@self.zk.DataWatch( primary_path)
		def watch_node(data, stat):
			try:
				if data is None and stat is None:
					primary_path = self.get_primary_path()

					if primary_path == self.election_path:
						self.election_id = self.increment_election_id()
						self.set_primary(True, primary_path, self.election_id)
						return
					else:
						my_logger.debug("%s resetting watch to primary_path %s my_path %s"
							, self.addr, primary_path, self.election_path)
						self.watch_node(primary_path)

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, str(self.formatExceptionInfo(Exception)))
				raise SystemError


	def set_primary(self, is_primary, primary_path, election_id):
		'''
		 	can be set from election or forced by someone who  won the election
		'''
		try:
			if election_id < self.election_id:
				if is_primary:
					#primary can only be set by ourselve and every call must have higher electionid
					my_logger.debug("%s i am primary and get lower election id!", self.addr)
					raise SystemError

			if self.zk.exists(primary_path) is None:
				my_logger.debug("%s primary path %s does not exist",self.addr, primary_path)
				return

			if primary_path == self.primary_path and  is_primary == self.is_primary:
				print "is_primary is", is_primary
				print "self.is_primary", self.is_primary
				my_logger.debug("%s ignored primary path unchanged election id is %s remote is %s"
					,self.addr, self.election_id, election_id)
				return

			if self.primary_path == primary_path and not is_primary:
				my_logger.debug("%s same primary path returning", self.addr)
				return

			my_logger.debug(" ==============  CHANGEING PRIMARY=============")
			self.election_id = election_id
			self.is_primary = is_primary
			self.primary_path = primary_path
			self.primary_addr = self.zk.get(primary_path)[0]

			my_logger.debug("self.is_primary %s self.election_path %s self.primary_path %s self.primary_addr %s"
				,self.is_primary, self.election_path, self.primary_path,  self.primary_addr)
			my_logger.debug(" ================== PRIMARY CHANGED ==============")

			if not is_primary:
				if self.addr == self.primary_addr:
					my_logger.debug("%s got same primary address, returning", self.addr)
					return
				else:
					gevent.spawn(self.backup_get_oplog)
					self.status = self.stati["Normal"]
					return True

			else:
				self.status = self.stati["Normal"]

		except zerorpc.LostRemote:
			my_logger.debug(" %s lost remote.", self.addr)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, str(self.formatExceptionInfo(Exception)))



	def increment_election_id(self):
		self.zk.create(self.election_path_counter + "election_n"
			, self.addr, ephemeral=False, sequence=True
		)
		election_id = len(self.zk.get_children(self.election_path_counter))
		return election_id

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
		self.status = self.stati["Election"]
		self.is_primary = False
		self.primary_path = ""
		self.primary_addr = ""


		self.zk.add_listener(self.connection_listener)
		if self.zk.exists(self.election_path_prefix) is None:
			self.zk.ensure_path(self.election_path_prefix)

		if self.zk.exists(self.election_path_counter) is None:
			self.zk.ensure_path(self.election_path_counter)

		self.election_id = self.increment_election_id()
		my_logger.debug("%s my election_id is %s", self.addr, self.election_id)

		self.election_path = self.zk.create(self.election_path_prefix + "guid_n"
			, self.addr, ephemeral=True, sequence=True
		)

		my_logger.debug("%s my path is %s", self.addr, self.election_path)

		primary_path = self.get_primary_path()
		if  primary_path == self.election_path:
			self.set_primary(True, primary_path, self.election_id)
		else:
			self.watch_node(primary_path)
			self.set_primary(False, primary_path, self.election_id)


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

