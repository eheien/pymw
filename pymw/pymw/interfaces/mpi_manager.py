#!/usr/bin/env /home/myri-fs/e-heien/bin/python

import os
import time
import sys
import threading
import mpi
import socket
import struct
import subprocess

class SocketTransport:
	def __init__(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.scache = {}

	def send(self, data):
		size = struct.pack("!Q", len(data))
		t_size = struct.calcsize("!Q")
		s_size = 0
		while s_size < t_size:
			p_size = self.socket.send(size[s_size:])
			if p_size == 0:
				raise RuntimeError("Socket connection is broken")
			s_size += p_size

		t_size = len(data)
		s_size = 0
		while s_size < t_size:
			p_size = self.socket.send(data[s_size:])
			if p_size == 0:
				raise RuntimeError("Socket connection is broken")
			s_size += p_size

	def recv(self):
		e_size = struct.calcsize("!Q")
		r_size = 0
		data = ""
		while r_size < e_size:
			msg = self.socket.recv(e_size-r_size)
			if msg == "":
				raise RuntimeError("Socket connection is broken")
			r_size += len(msg)
			data += msg
		e_size = struct.unpack("!Q", data)[0]

		r_size = 0
		data = ""
		while r_size < e_size:
			msg = self.socket.recv(e_size-r_size)
			if msg == "":
				raise RuntimeError("Socket connection is broken")
			r_size += len(msg)
			data += msg
		return data

	def close(self):
		self.socket.close()

	def _connect(self, host, port):
		self.socket.connect((host, port))

#def worker_control_thread(worker_num, task_sem, task_lock, task_list, transport):
#	while True:
#		task_sem.acquire()
#		task_lock.acquire()
#		next_task = task_list.pop()
#		task_lock.release()
#		start_time = time.time()
#		#print "Worker", str(worker_num), "doing task", str(next_task[0])
#		mpi.isend(next_task, worker_num)
#		if not next_task:
#			return
#		finish_notice = mpi.irecv(worker_num)
#		while not finish_notice: pass
#		#print "Control thread", str(worker_num), "got finish notice" 
#		task_lock.acquire()
#		transport.send(next_task[0])
#		task_lock.release()

def worker_control_thread(worker_list, task_sem, task_lock, task_list, transport):
	finish_notice_list = []
	while True:
		if len(worker_list) > 0:
			if task_sem.acquire(blocking=False):
				task_lock.acquire()
				next_task = task_list.pop()
				task_lock.release()
				if not next_task:
					for i in range(1, mpi.size):
						mpi.isend(None, i)
					return
				worker_num = worker_list.pop()
				#print "Worker", str(worker_num), "sent task", str(next_task[0])
				mpi.isend(next_task, worker_num)
				# Handle null tasks
				notice_req = mpi.irecv(worker_num)
				finish_notice_list.append([worker_num, notice_req, time.time(), next_task])
		#print "Checking", str(len(finish_notice_list)), "workers for finish notice"
		for notice_req in finish_notice_list:
			if notice_req[1]:
				finished_task = notice_req[3]
				#print "Worker", str(notice_req[0]), "finished task" , str(finished_task[0]), "in", str(time.time()-notice_req[2]), "seconds"
				transport.send(finished_task[0])
				worker_list.append(notice_req[0])
				finish_notice_list.remove(notice_req)
		#time.sleep(0.1)

def master():
	try:
		transport = SocketTransport()
		transport._connect(sys.argv[1], int(sys.argv[2]))
	except:
		for worker_num in range(0,mpi.size):
			mpi.isend(None, worker_num, tag=0)
		raise

	task_list = []
	task_sem = threading.Semaphore(0)
	task_lock = threading.Lock()
	#my_thread = threading.Thread(target=rank_0_worker, args=())
	#my_thread.start()
	control_thread = threading.Thread(target=worker_control_thread, args=(range(1, mpi.size), task_sem, task_lock, task_list, transport))
	control_thread.start()
	#control_threads = [threading.Thread(target=worker_control_thread, args=(worker_num, task_sem, task_lock, task_list, transport)) for worker_num in range(1, mpi.size)]
    #logging.info("Started"+str(len(control_threads))+"worker threads")
	#print "Started", str(len(control_threads)), "worker threads"
	#for c_thread in control_threads:
	#	c_thread.start()

	while True:
		try:
			cmd = transport.recv()
			task_lock.acquire()
			task_list.append(cmd.split())
			task_lock.release()
			task_sem.release()
		except RuntimeError:
			#for c_thread in control_threads:
			#	task_list.append(None)
			#	task_sem.release()
			#for c_thread in control_threads:
			#	c_thread.join()
			task_list.append(None)
			task_sem.release()
			control_thread.join()
			return

def worker():
	while True:
		msg, status = mpi.recv()
		if msg is None: return
		start_time = time.time()
		retval = subprocess.call(["python", msg[1], msg[2], msg[3]])
		#print "Worker", str(mpi.rank), "took", str(time.time()-start_time), "seconds for task", str(msg[0])
		mpi.send("done", 0)

if mpi.rank == 0: master()
else: worker()

