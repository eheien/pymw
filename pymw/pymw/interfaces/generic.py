#!/usr/bin/env python
"""Provide a generic multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "22 February 2009"

import subprocess
import sys
import errno
import time
import threading

class GenericInterface:
	"""Provides a simple generic interface for single machine systems.
	This can take advantage of multicore machines by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		self._num_workers = num_workers
		self._available_worker_list = [worker_num for worker_num in range(num_workers)]
		self._worker_lock = threading.Condition()
		self._python_loc = python_loc
	
	# Wait for a worker to become available
	def get_available_workers(self):
		self._worker_lock.acquire()
		while len(self._available_worker_list) == 0:
			self._worker_lock.wait()
		self._worker_lock.release()
		return list(self._available_worker_list)
	
	def reserve_worker(self, worker):
		self._available_worker_list.remove(worker)
	
	def execute_task(self, task, worker):
		try:
			# Execute the task and deal with error codes
			if sys.platform.startswith("win"): cf=0x08000000
			else: cf=0
			exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg, task._output_arg],
											    	creationflags=cf, stderr=subprocess.PIPE)
			proc_stdout, proc_stderr = exec_process.communicate()   # wait for the process to finish
			retcode = exec_process.returncode
			task_error = None
			if retcode is not 0:
				task_error = Exception("Executable failed with error "+str(retcode), proc_stderr)
				
		except OSError:
			# TODO: check the actual error code
			task_error = Exception("Could not find Python interpreter executable")
		
		# Notify the task of completion
		task.task_finished(task_error)
		
		# Put the worker back on the list
		self._worker_lock.acquire()
		self._available_worker_list.append(worker)
		self._worker_lock.notify()
		self._worker_lock.release()

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-self._available_worker_list.qsize()}
