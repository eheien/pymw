#!/usr/bin/env python
# mpirun -np 4 /home/myri-fs/e-heien/local/bin/pyMPI /home/myri-fs/e-heien/booga.py

import mpi
import os
import time
import threading

def worker_control_thread(worker_num, task_sem, task_lock, task_list):
	while True:
		task_sem.acquire()
		task_lock.acquire()
		next_task = task_list.pop()
		if not next_task:
			task_lock.release()
			return
		task_lock.release()
		mpi.send(next_task, worker_num)
		mpi.recv(worker_num)
		print next_task

def master():
	finished = False
	print mpi.size
	task_list = []
	task_sem = threading.Semaphore(0)
	task_lock = threading.Lock()
	control_threads = [threading.Thread(target=worker_control_thread, args=(worker_num, task_sem, task_lock, task_list)) for worker_num in range(1, mpi.size)]
	for c_thread in control_threads:
		c_thread.start()

	while not finished:
		try:
			cmd = raw_input()
			task_lock.acquire()
			task_list.append(cmd)
			task_lock.release()
			task_sem.release()
		except:
			for i in range(mpi.size):
				mpi.send(None, i)
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
			time.sleep(1)
			mpi.send("booga", 0)

if mpi.rank == 0: master()
else: worker()

