#!/usr/bin/env python

import os
import time
import sys
import threading
import mpi
import socket
import struct
import subprocess

class SocketTransport():
	def __init__(self):
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.scache = {}

	def send(self, data):
		size = struct.pack("!Q", len(data))
		t_size = struct.calcsize("!Q")
		s_size = 0L
		while s_size < t_size:
			p_size = self.socket.send(size[s_size:])
			if p_size == 0:
				raise RuntimeError("Socket connection is broken")
			s_size += p_size

		t_size = len(data)
		s_size = 0L
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

def worker_control_thread(worker_num, task_sem, task_lock, task_list, transport):
	while True:
		task_sem.acquire()
		task_lock.acquire()
		next_task = task_list.pop()
		task_lock.release()
		mpi.send(next_task, worker_num)
		if not next_task:
			return
		mpi.recv(worker_num)
		transport.send(next_task[0])

def master():
	finished = False
	transport = SocketTransport()
	transport._connect("0.0.0.0", 43194)
	task_list = []
	task_sem = threading.Semaphore(0)
	task_lock = threading.Lock()
	control_threads = [threading.Thread(target=worker_control_thread, args=(worker_num, task_sem, task_lock, task_list, transport)) for worker_num in range(1, mpi.size)]
	print "Started", str(len(control_threads)), "worker threads"
	for c_thread in control_threads:
		c_thread.start()

	while not finished:
		try:
			cmd = transport.recv()
			task_lock.acquire()
			task_list.append(cmd.split())
			task_lock.release()
			task_sem.release()
		except RuntimeError:
			print "Sending quit signal to all workers"
			finished = True
			for c_thread in control_threads:
				task_list.append(None)
				task_sem.release()

def worker():
	finished = False
	while not finished:
		msg, status = mpi.recv()
		if msg is None:
			finished = True
		else:
			retval = subprocess.call(["python", msg[1], msg[2], msg[3]])
			mpi.send("done", 0)

if mpi.rank == 0: master()
else: worker()
