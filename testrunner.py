import subprocess
import sys
import time
import os
import signal
import random
from sys import platform as _platform
import socket

def start_server(pidlist, prog, server, logserver, proxy):
	print ">>>>>>>>starting server", prog, server
	print server, logserver
	p = subprocess.Popen(['python',prog+".py", server, logserver, proxy], shell=False,  stderr=None)

	global ps
	ps[server]=p.pid
	pidlist.write(server+"\t"+str(p.pid)+"\n")


def stop_server(server, pid):
	print ">>>>>>>>killing server",pid ,server
	os.kill(pid, signal.SIGINT)


def startprocesses(pidlist, prog, logserver ,proxy):
	progs = ["bully","invitation","bully_async"]

	if prog not in progs:
		print "invalid prog"
		return

	servers = []
	f = open("server_config", 'r')
	for line in f.readlines():
		line = line.rstrip()
		#only start servers on right ip
		if socket.gethostbyname(socket.gethostname()) == line[:line.find(":")]:
			servers.append(line)
		else:
			print "ignoring",line

	for server in servers:
		time.sleep(2)
		start_server(pidlist, prog, server, logserver, proxy)

	#todo stop and start nodes, restart all nodes after certain timeout
	while False:
		time.sleep(30)
		server = random.choice(ps.keys())
		pid = ps[server]
		if pid == 0:
			start_server(prog,server,logserver)
		else:
			global ps
			ps[server] = 0
			stop_server(server,pid)


if __name__ == '__main__':
	print "running on ",socket.gethostbyname(socket.gethostname())
	pidlist = open("running_servers.log","w+")
	ps = {}
	deadservers =[]
	proxy = ""
	logserver = ""
	if len(sys.argv) > 1:
		prog = sys.argv[1]
	else:
		 print "c'mon..."

	if len(sys.argv) > 2:
		logserver = sys.argv[2]
	if len(sys.argv) > 3:
		proxy = sys.argv[3]


	startprocesses(pidlist, prog,logserver,proxy)



