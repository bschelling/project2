#!/usr/local/bin/python2.7
import leveldb

def batchTest():
	db = leveldb.LevelDB('./db')
	batch = leveldb.WriteBatch()
	for i in range(1,1000000):
		batch.Put('hello'+str(i), 'world')
	db.Write(batch, sync = True)
	print "put ", i
	print db.Get("hello"+str(i-100))

def dummy():
	# single put
	db = leveldb.LevelDB('./db')
	db.Put('hello', 'world')
	print db.Get('hello')
	# single delete
	#db.Delete('hello')
	print db.Get('hello')



dummy()