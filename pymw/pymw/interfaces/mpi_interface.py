import subprocess
import threading
import pymw
import sys
import os
import socket
import logging
import struct

def send(pymw_socket, data):
	size = struct.pack("!Q", len(data))
	pymw_socket.sendall(size)
	pymw_socket.sendall(data)

def recv(pymw_socket):
	e_size = struct.calcsize("!Q")
	r_size = 0
	data = ""
	while r_size < e_size:
		msg = pymw_socket.recv(e_size-r_size)
		if msg == "":
			raise RuntimeError("Socket connection is broken")
		r_size += len(msg)
		data += msg
	e_size = struct.unpack("!Q", data)[0]

	r_size = 0
	data = ""
	while r_size < e_size:
		msg = pymw_socket.recv(e_size-r_size)
		if msg == "":
			raise RuntimeError("Socket connection is broken")
		r_size += len(msg)
		data += msg
	return data

class MPIInterface:
	def __init__(self, num_workers=1, mpirun_loc="mpirun"):
		if num_workers%2: num_workers -= 1

		self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.host = socket.gethostbyname(socket.gethostname())
		self.port = 43192
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.bind((self.host, self.port))
		self.socket.listen(1)

		self._manager_process = subprocess.Popen(args=[mpirun_loc, "-np", str(num_workers),
			"/home/myri-fs/e-heien/local/bin/pyMPI",
			"/home/myri-fs/e-heien/osaka/pymw/pymw/interfaces/mpi_manager.py",
			self.host, str(self.port)],
			stdin=None, stdout=None, stderr=None)
			#stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		# accept connection from manager process
		self.socket.settimeout(30)
		(csocket, address) = self.socket.accept()
		self.csocket = csocket

		self.csocket.setblocking(True)
		#self.worker_count = recv(self.csocket)
		self.task_dict = {}
		self.mpi_error = None
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
				task_name = recv(self.csocket)
				task = self.task_dict[task_name]
				task.task_finished()
		except KeyError, e:
			self.mpi_error = e
			return
		except:
			error_msg = self._manager_process.stderr.readlines()
			full_msg = ""
			for line in error_msg:
				full_msg += line
			self.mpi_error = Exception(full_msg)
			return

	def execute_task(self, task, worker):
		if self.mpi_error:
			task.task_finished(self.mpi_error)
		#try:
		self.task_dict[str(task)] = task
		send(self.csocket, str(task)+" "+task._executable+" "+task._input_arg+" "+task._output_arg)
		#except:
		#	if self._manager_process.stderr:
		#		error_msg = self._manager_process.stderr.readlines()
		#	else:
		#		error_msg = [""]
		#	full_msg = ""
		#	for line in error_msg:
		#		full_msg += line
		#	self.mpi_error = Exception(full_msg)
		#	task.task_finished(self.mpi_error)

	def get_status(self):
		return {}

	def _cleanup(self):
		self.csocket.close()

