#!/usr/bin/env /home/myri-fs/e-heien/bin/python

import sys
import mpi
import socket
import struct
import subprocess

def send(comm_sock, data):
	size = struct.pack("!Q", len(data))
	comm_sock.sendall(size)
	comm_sock.sendall(data)

def recv(comm_sock):
	total_size = struct.calcsize("!Q")
	recv_size = 0L
	data = ""
	try:
		comm_sock.setblocking(0)
		while recv_size < total_size:
			msg = comm_sock.recv(total_size-recv_size)
			if msg == "":
				raise RuntimeError("Socket connection is broken")
			recv_size += len(msg)
			data += msg
	except socket.error:
		return None
	finally:
		comm_sock.setblocking(1)

	total_size = struct.unpack("!Q", data)[0]

	recv_size = 0L
	data = ""
	while recv_size < total_size:
		msg = comm_sock.recv(total_size-recv_size)
		if msg == "":
			raise RuntimeError("Socket connection is broken")
		recv_size += len(msg)
		data += msg
	return data

def master():
	try:
		pymw_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		pymw_socket.connect((sys.argv[1], int(sys.argv[2])))
	except:
		for worker_num in range(1, mpi.size):
			mpi.send(None, worker_num)
		raise

	#my_thread = threading.Thread(target=rank_0_worker, args=())
	#my_thread.start()

	worker_list = range(1, mpi.size)
	#send(pymw_socket, str(mpi.size))
	task_list = []
	finish_notice_list = []
	try:
		while True:
			cmd = recv(pymw_socket)
			if cmd:
				task_list.append(cmd.split())

			# If we have a task and a worker available to do it
			if len(task_list) > 0 and len(worker_list) > 0:
				next_task = task_list.pop()
				worker_num = worker_list.pop()
				#print "Worker", str(worker_num), "sent task", str(next_task[0])
				# Send the task to the worker
				mpi.isend(next_task, worker_num)
				notice_req = mpi.irecv(worker_num)
				finish_notice_list.append([notice_req, worker_num, next_task])
			for notice_req in finish_notice_list:
				if notice_req[0]:
					finished_task = notice_req[2]
					#print "Worker", str(notice_req[1]), "finished task" , str(finished_task[0])
					send(pymw_socket, finished_task[0])
					worker_list.append(notice_req[1])
					finish_notice_list.remove(notice_req)

	except RuntimeError:
		for i in range(1, mpi.size):
			mpi.send(None, i)

def worker():
	while True:
		msg, status = mpi.recv()
		if msg is None: return
		retval = subprocess.call(["python", msg[1], msg[2], msg[3]])
		mpi.send("done", 0)

if mpi.rank == 0: master()
else: worker()

