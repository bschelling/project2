#!/usr/local/bin/python2.7
import traceback
import hashlib
import re
import inspect
from kazoo.client import KazooClient
from kazoo.recipe.watchers import PatientChildrenWatch
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
#from kazoo.exceptions import KazooException
#from kazoo.exceptions import NoNodeError
#from kazoo.exceptions import NodeExistsError
#from kazoo.recipe import lock
from kazoo.exceptions import *
import os
from gevent import monkey
from gevent.queue import Queue, Empty
import base64

monkey.patch_all()


class storageServer(object):
	''' primary handling: check primary every time before answer, return primary
		2 times primary true check if started to fast!
		rename transaction to prepare path
	'''

	def __init__(self, addr, config_file='server_config'):

		self.op_log = "operations"+addr[-1:]+".log"

		self.file_transmitsize = 1024 * 1024  #bytes offset
		self.quorum_size = 1
		self.storage_prefix = "/storageserver/tx"
		self.commit_prefix = "/storageserver/cx"
		self.addr = addr
		#todo have to make this persistent
		self.transaction_id = 0
		self.i = 0
		self.is_primary = False
		self.servers = []
		self.server_stati = {}
		self.uplist = set()

		self.stati = {"Down": 0, "Election": 1, "Reorganisation": 2, "Normal": 3}
		self.t_stati = {"Ready": 0, "Commited": 2, "Rolledback": 3}
		self.status = self.stati["Down"]

		#check if we want to use this http://stackoverflow.com/questions/2970608/what-are-named-tuples-in-python
		ntconnection = collections.namedtuple('ntconnection', 'addr connection prio')
		f = open(config_file, 'r')
		i = 0
		for line in f.readlines():
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
		self.pending_transactions = {}
		self.db = leveldb.LevelDB('./db' + str(self.i))
		#create task queue optional specify max size
		self.tasks = Queue()
		self.wtasks = Queue()
		self.commited ={}


	def start(self):
		#todo add our client list to arguments check performance using timeouts
		self.zk = KazooClient()
		self.zk.start()
		self.status = self.stati["Normal"]
		self.start_election()
		my_logger.debug("%s self.is_primary %s", self.addr, self.is_primary)


	# todo delete still triggers watch, data_watch doesn't giv use path, check if we can do without memory,
	# maybe primary should fire watch himself
	def primary_commit_watch(self, event):
		children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
		my_logger.debug("%s >>>>>>>>>>>>>>>>>>> %s <<<<<<<<<<<<<<", self.addr, event)
		my_logger.debug("%s  commit watch on %s children %s", self.addr, event.path, children)
		if event.path in self.commited:
			my_logger.debug("%s already commited %s", self.addr, event.path)
			return
		try:
			if event.type == "DELETED":
				my_logger.debug(" %s primary_commit_watch deleted %s", self.addr, event)
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
					my_logger.debug("%s got a commit event  %s children %s, quorum not reached yet %s quorum_size"
						, self.addr, event, children, self.quorum_size)
				#children = self.zk.get_children(event.path, watch=self.primary_commit_watch)

			elif event.type == "CREATED":
				my_logger.debug("%s got event %s ", self.addr, event)
				if  event.path.find("/ready") != -1:
					my_logger.debug("%s discarding create event  %s ", self.addr, event)
					return

			else:
				#tocheck do we ever reach this
				my_logger.debug("%s reset watch event not child? %s ", self.addr, event)
				children = self.zk.get_children(event.path, watch=self.primary_commit_watch)
				if len(children) >= self.quorum_size:
					my_logger.debug("%s returning, children bigger quorum size  %s ", self.addr, len(children))
					return
				my_logger.debug("%s resetted watch children %s ", self.addr, children)


		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def primary_prepare_watch(self, event):
		my_logger.debug(" %s primary_prepare_watch called %s", self.addr, event)
		try:
			if event.type == "DELETED":
				my_logger.debug(" %s primary_prepare_watch deleted %s", self.addr, event)
				return

			elif event.type == "CHILD":
				#yeah weird but exists is not boolean!
				if not self.zk.exists(event.path + "/ready") is None:
					my_logger.debug("%s ready path already exists", self.addr)
					return

				#reset the watch immediately or we will be missing something!
				children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)
				if len(children) == len(self.servers) - 1:
					my_logger.debug("%s adding ready node to prepare path %s len self.servers-1 %s children is %s "
						, self.addr, event.path, len(self.servers) - 1, children)
					self.zk.create(event.path + "/ready", acl=None, ephemeral=True, sequence=False, makepath=False)
					self.tasks.put("prepare_ready")
					return

				else:
					my_logger.debug("%s prepare waiting  %s children is %s uplist  is %s "
						, self.addr, event.path, children, str(self.uplist))

			#triggered by exists watch ignore
			elif event.type == "CREATED":
				my_logger.debug("%s got event %s ", self.addr, event)
				if  event.path.find("/ready") != -1:
					my_logger.debug("%s discarding create event  %s ", self.addr, event)
					return

			else:
				my_logger.debug("%s ??? reset watch event %s ", self.addr, event)
				children = self.zk.get_children(event.path, watch=self.primary_prepare_watch)


		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	def status_handler(self):
		task = ""
		try:
			while True:
				task = self.tasks.get(timeout=1) # decrements queue size by 1
				my_logger.debug(" %s got task %s", self.addr, task)
				gevent.sleep(0)
				if task == "commited" or task == "failed":
					break
			#ok our workflow is complete
			return task

		except Empty:
			print('Quitting time!')
			#somethings missing check where we left off
			print task
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




	def kv_set_cleanup(self, laststage, prepare_path, commit_path):
		if True:
			return
		#todo remove children that didn't succeed from uplist, this sleep here is not nice, check if we can do better
		gevent.sleep(0.1)
		try:
			my_logger.debug("%s deleting commit path %s and prepare path %s ", self.addr, prepare_path, commit_path)
			#check if this path even exists
			#TODO THAT GOES INTO A FUNCTION
			if not self.zk.exists(prepare_path) is None:
				children = self.zk.get_children(prepare_path)
				for child in children:
					self.zk.delete(prepare_path+"/"+child)
					my_logger.debug("..............node deleted "+prepare_path)
				try:
					self.zk.delete(prepare_path, recursive=False)
				except NoNodeError:
					my_logger.debug("..............NoNodeError"+prepare_path)
					pass
			if not self.zk.exists(commit_path) is None:
				children = self.zk.get_children(commit_path)
				for child in children:
					my_logger.debug("..............node deleted ")
					self.zk.delete(commit_path+"/"+child)

				try:
					self.zk.delete(commit_path, recursive=False)
				except NoNodeError:
					my_logger.debug("..............NoNodeError"+commit_path)
					pass

		except KazooException, e:
			my_logger.debug(self.formatExceptionInfo())
			my_logger.debug("%s Couldn't do it: %s %s", self.addr, e, KazooException.message)
			raise SystemError


		except Exception, e:
			my_logger.debug(self.formatExceptionInfo())
			my_logger.debug("%s Couldn't do it: %s %s", self.addr, e, Exception.message)
			raise SystemError
		return

	#todo add checksum, compression
	def primary_transmit_oplog(self, remote_addr, replica_size):

		offset = replica_size
		content = []
		fname = self.op_log

		if not os.path.isfile(fname):
			return content

		#default transfer blocks
		transmitsize = self.file_transmitsize

		fsize = os.path.getsize(fname)
		my_logger.debug("%s syncing to %s offset %s my_size %s replica_size %s", self.addr, remote_addr, offset, fsize, replica_size)

		if fsize < offset :
			my_logger.debug("%s argh what happened replica has bigger size %s", self.addr, remote_addr)
			my_logger.debug("%s fsize %s replica_size %s",self.addr, fsize, replica_size)

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
			#todo check here!
			raise SystemError

		elif offset + transmitsize > fsize:
			transmitsize = 0
			my_logger.debug("%s offset + transmitsize %s bigger than fsize %s ")
			my_logger.debug("%s new transmitsize set to  %s ", self.addr, transmitsize)

		else:
			transmitsize = self.file_transmitsize

		my_logger.debug("%s remaining size of oplog to sync with %s is %s"
			, self.addr, remote_addr,  fsize - replica_size)

		#if transmitsize is 0 we will read until the end of file!
		if (fsize - replica_size) < 1024 * 1024:
			my_logger.debug("%s replica %s got close to catching up,only %s remaining"
							, self.addr, remote_addr, transmitsize)
			try:
				#todo better keep list in memory?
				if self.zk.exists("/syncpath/"+remote_addr) is None:
					self.zk.create("/syncpath/"+remote_addr, ephemeral=True, makepath=True)
				else:
					my_logger.debug(" %s node %s already exists?", self.addr, "/syncpath/"+remote_addr )

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError

		if transmitsize != -1:
			if os.path.isfile(fname):
				file = open(fname)
				file.seek(offset)
				content = file.readlines(transmitsize)
				if transmitsize == 0:
					open("debug.txt","w").write(str(content))

				my_logger.debug("%s read file with offset %s and transmitsize %s"
					, self.addr, offset, transmitsize, )
			else:
				my_logger.debug("%s file  %s not available", self.addr, fname)

		return content

	def get_connection_by_addr(self, addr):
		for server in self.servers:
			#todo check if primary is up?
			if server.addr == addr :
				return server.connection
		#somebody's asking for a address we don't have
		raise SystemError

	def backup_get_oplog(self, primary_addr):

		#todo check my own oplog before just deleting it
		if os.path.isfile(self.op_log):
			os.remove(self.op_log)

		if primary_addr == self.addr:
			my_logger.debug("%s i am primary %s", self.addr, primary_addr)
			raise SystemError
			return

		my_logger.debug("%s trying to get oplog from %s", self.addr, primary_addr)

		#todo check if file is valid, if yes send offset of my logfile version (if rotated) and offset
		p_connection = zerorpc.Client(timeout=30)
		p_connection.connect('tcp://' + primary_addr)
		retries = 0
		max_retries = 0
		read_offset = 0
		start = time.time()
		size_received = 0


		#todo if oplogs are logrotated, we have to get the list of files first and loop through them here

		f = open(self.op_log,"a")
		while True:
			my_logger.debug("%s send size received to server as %s"
				, self.addr, size_received)

			xstart = time.time()
			lines_tupel = p_connection.primary_transmit_oplog(self.addr, size_received )
			print "elapsed", time.time() - xstart

			#gevent.sleep(1)
			if len(lines_tupel) == 0:
				# retry will give the same line again if nothing changed at the primary
				# since we have only puts and the order of inserts is not changed, np
				if retries < max_retries:
					gevent.sleep(1)
					retries += 1
					continue
				else:
					print "breaking out here."
					break

			for line in lines_tupel:
				#line ="[2013-04-04 21:34:43,258] /storageserver/cx0000012585  hello13   world13 digest"

				tid, key, value, digest = line.rstrip().split(" ")
				md5 = hashlib.md5()
				md5.update(key + value)
				if md5.hexdigest() == digest:
					#todo try catch, put in separate function?
					dummy =1
					self.db.Put(base64.b64decode( key ), base64.b64decode( value ))
				else:
					print line
					print tid, "X"+key+"X", "X"+value+"X", "X"+digest+"X"
					print "Y"+md5.hexdigest()+"Y"
					raise SystemError

				#TODO put that in a function!

				f.write(tid+" "+key+" "+value+" "+digest+"\n")
				#f.write(line)
				#f.close()
				size_received += len(line)

			read_offset += self.file_transmitsize
			#size_received = os.path.getsize("operations" + addr[-1:] + ".log")


		# TRANSMISSON DONE todo do some sanity checks on log here!
		#todo ok one more time and this goes to init
		elapsed = time.time() - start

		#size_received = os.path.getsize("operations" + addr[-1:] + ".log")
		my_logger.debug("%s size received %s time elapsed %s using transmitsize %s", self.addr
			, size_received, elapsed, self.file_transmitsize)

		#todo check if we really sent the message to the primary, otherwise use address
		#received from replica or find out ourselves and go back to recovery

		#todo do verify here check rsync output use short lived connection?
		right_oplog_version = p_connection.primary_register(size_received, self.addr, self.i)

		if right_oplog_version:
			self.status = self.stati["Normal"]
			my_logger.debug("%s we made it! sync complete in %s", self.addr
				,elapsed)

		else:
			my_logger.debug("%s getting oplog failed, couldn't catch up %s", self.addr, primary_addr)


	def get_primary_addr(self):
		children = self.zk.get_children("/ELECTION/")
		#todo check how many children have to be available
		if len(children) == 0:
			my_logger.debug("%s somethings very wrong here", self.addr)
			raise SystemError

		primary_path = sorted(children)[0]
		primary_addr = str(self.zk.get("/ELECTION/" + primary_path)[0])
		return primary_addr


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
				prepare_path = self.zk.create(self.storage_prefix, commit_path.encode('UTF-8'), acl=None,
					ephemeral=False, sequence=True, makepath=True)

				children = self.zk.get_children(commit_path, watch=self.primary_commit_watch)
				if len(children) > 0:
					my_logger.debug("%s somethings very wrong here", self.addr)
					raise SystemError

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
							path = self.zk.create(prepare_path + "/" + self.addr, self.addr, acl=None, ephemeral=False,
								sequence=True, makepath=True)

				#if a client signals us to wait it will signal for the wait handler
				gevent.joinall([
					self.g_status_handler,
				])
				my_logger.debug("%s result received %s", self.addr, self.g_status_handler.value)
				self.kv_set_cleanup(self.g_status_handler.value, prepare_path, commit_path)

			#print lock.contenders()
			lock.release()

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
				path = self.zk.create(prepare_path + "/" + self.addr, self.addr, acl=None, ephemeral=False,
					sequence=True, makepath=False)
				return True

			except Exception, e:
				my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
				raise SystemError


	def backup_kv_commit_watcher(self, event):
		#fired on create of ready node on prepare_path from exists watcher in backup_kv_prepare
		try:
			#my_logger.debug("%s event %s", self.addr, event)
			if self.is_primary:
				my_logger.debug("%s called backup on primary", self.addr)
				raise SystemError
			else:
				if event.type == "CREATED":
					#/storageserver/tx0000006618/ready
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

					self.zk.create(commit_path + "/" + self.addr)
					my_logger.debug("%s backup commmited key %s value %s ", self.addr, key, value)

		except Exception, e:
			my_logger.debug(" %s Couldn't do it: %s", self.addr, e)
			raise SystemError


	####################################election  functions ################################
	def connection_listener(self, state):
		if state == KazooState.LOST:
			my_logger.debug('%s : session lost', self.addr)
		elif state == KazooState.SUSPENDED:
			my_logger.debug('%s : session suspended', self.addr)
		else:
			my_logger.debug('%s : running in state %s', self.addr, state)


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
				else:
					prev_path = child_path
			return prev_path, self.zk.get("/ELECTION/" + tmp[0])[0]

	def watch_node(self, my_path, prev_path):
		@self.zk.DataWatch("/ELECTION/" + prev_path)
		def watch_node(data, stat):
			try:
				if data is None and stat is None:
					prev_path, primary_addr = self.get_prev_path(my_path)
					if prev_path == my_path:
						my_logger.debug("%s %s deleted, i am first child, broadcast", self.addr, primary_addr)
						self.set_primary(True)
					else:
						if primary_addr != self.addr:
							my_logger.debug("%s %s deleted but still not addmin, my_path %s prev_path %s ", self.addr,
								primary_addr, my_path, prev_path)
							self.watch_node(my_path, prev_path)
						#TODO10 check for status change? something happened!!
						#self.set_primary( False )
						else:
							my_logger.debug("%s hust. we got a problem set primary %s", self.addr, primary_addr)
							#node still exists but we got the same address, emphemeral node still there
							self.set_primary(True)

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

		#todo replace check for op_log size with call to verify
		#if op_log_size == os.path.getsize("operations" + addr[-1:] + ".log") :

		if op_log_size == os.path.getsize(self.op_log) :
			#todo move this into function and make it atomic?
			self.uplist.add(remote_prio)
			self.server_stati[backup_addr] = "up"
			my_logger.debug(" %s added remote prio %s to uplist, uplist now %s "
				, self.addr, remote_prio, str(self.uplist))
			return True

		my_logger.debug(" %s size does not match oplogsize %s path size %s "
				, self.addr, op_log_size, os.path.getsize("log5000.txt") )

		return False

	#todo better use this than set values, check if we have to do
	def set_primary(self, is_primary):
		my_logger.debug("%s primary set from %s", self.addr, inspect.stack()[1][3])
		self.is_primary = is_primary
		if  self.is_primary == False:
			primary_addr = self.get_primary_addr()
			if primary_addr != "":
				todo = 1
				#TODO update status to up only after we checked
				#get oplog first and sync!


				self.backup_get_oplog(primary_addr)
			#registered
			#self.backup_register( primary_addr )
			else:
				dummy = 1

	def start_election(self):
		#todo errorhandling what to do if connection is lost
		self.zk.add_listener(self.connection_listener)
		if self.zk.exists("/ELECTION") is None:
			self.zk.ensure_path("/ELECTION")

		my_path = self.zk.create("/ELECTION/" + self.addr, self.addr, ephemeral=True, sequence=True)
		my_path = my_path.replace("/ELECTION/", "")

		prev_path, primary_addr = self.get_prev_path(my_path)
		print prev_path, primary_addr
		if primary_addr != self.addr:
			self.watch_node(my_path, prev_path)
			self.set_primary(False)
		else:
			self.set_primary(True)


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




