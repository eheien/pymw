#!/usr/bin/env python
"""Provide a multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "10 April 2008"

import subprocess
import threading
import cPickle
import pymw
import sys

"""On worker restarting:
Multicore systems cannot handle worker restarts - recording PIDs
can result in unspecified behavior (especially if the system is restarted).
The solution is to check for a result on program restart - if it's not there,
delete the directory and start the task anew."""

class _Worker:
	def __init__(self, worker_num, avail_worker_list):
		self._active_task = None
		self._avail_worker_list = avail_worker_list
		self._worker_num = worker_num
		self._error = None

	def worker_record(self):
		if self._active_task: task_name = str(self._active_task)
		else: task_name = None
		return [worker_num, task_name]

	def wait_for_finish(self):
		retcode = self._process.wait()			# wait for the process to finish
		if retcode is not 0:
			self._error = pymw.InterfaceException("Executable failed with error "+str(retcode))
		
		self.cleanup()

	def cleanup(self):
		self._active_task.task_finished(self._error)	# notify the task
		self._active_task = None
		self._avail_worker_list.append(self)	# rejoin the list of available workers

class BaseSystemInterface:
	"""Provides a simple interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		self._num_workers = num_workers
		self._available_worker_list = pymw._SyncList()
		self._worker_list = []
		self._python_loc = python_loc
		for worker_num in range(num_workers):
			new_worker = _Worker(worker_num, self._available_worker_list)
			self._available_worker_list.append(new_worker)
			self._worker_list.append(new_worker)
	
	def _save_state(self):
		return None
	
	def _restore_state(self, old_state):
		return
	
	def reserve_worker(self):
		return self._available_worker_list.wait_pop()
	
	def execute_task(self, task, worker):
		worker._active_task = task
		try:
			if sys.platform.startswith("win"):
				worker._process = subprocess.Popen(args=[self._python_loc,
					task._executable, task._input_arg, task._output_arg],
					creationflags=0x08000000, stderr=subprocess.PIPE)
			else:
				worker._process = subprocess.Popen(args=[self._python_loc,
					task._executable, task._input_arg, task._output_arg], stderr=subprocess.PIPE)
		except OSError:
			# TODO: check the actual error code
			worker._error = pymw.InterfaceException("Could not find python")
			worker.cleanup()
			return
		
		worker_finish_handler = threading.Thread(target=worker.wait_for_finish)
		worker_finish_handler.start()

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-len(self._worker_list)}

