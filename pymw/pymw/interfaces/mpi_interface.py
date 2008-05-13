import subprocess
import threading
import pymw
import sys
import os
import socket
import logging
import struct

class SocketTransport():
	def __init__(self, socket1=None):
		if socket1:
			self.socket = socket1
		else:
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

	def accept(self):
		return csocket

class MPIInterface:
	def __init__(self, num_workers=1, mpirun_loc="mpirun"):
		if num_workers%2: num_workers -= 1
		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.host = socket.gethostbyname(socket.gethostname())
		self.port = 43192
		try:
			self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
			self.socket.bind((self.host, self.port))
			self.socket.listen(1)
		except socket.error:
			logging.error("Cannot create socket with port " + str(self.port)
					+ " (port is already in use)")

		self._mpi_manager_process = subprocess.Popen(args=[mpirun_loc, "-np", str(num_workers), "/home/myri-fs/e-heien/local/bin/pyMPI",
														   "/home/myri-fs/e-heien/osaka/pymw/pymw/pymw/interfaces/mpi_manager.py", self.host, str(self.port)],
														   stdin=None, stdout=None, stderr=None)#stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		# accept connection from manager process
		self.socket.settimeout(18)
		(csocket, address) = self.socket.accept()

		csocket.setblocking(True)
		self.csocket = SocketTransport(csocket)
		self.task_dict = {}
		task_finish_thread = threading.Thread(target=self._get_finished_tasks)
		task_finish_thread.start()
	
	def _save_state(self):
		print "saving state"
	
	def _restore_state(self):
		print "restoring state"
	
	def reserve_worker(self):
		return None
	
	def _get_finished_tasks(self):
		try:
			while True:
				task_name = self.csocket.recv()
				#print "Finished task", repr(task_name)
				task = self.task_dict[task_name]
				task.task_finished()
		except RuntimeError:
			return

	def execute_task(self, task, worker):
		#print "Submitted task", str(task)
		self.task_dict[str(task)] = task
		self.csocket.send(str(task)+" "+task._executable+" "+task._input_arg+" "+task._output_arg)

	def get_status(self):
		return {}

	def _cleanup(self):
		self.csocket.close()
