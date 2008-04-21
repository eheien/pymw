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

class BaseSystemInterface:
	"""Provides a simple interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		self._num_workers = num_workers
		self._available_worker_list = pymw._SyncList()
		self._worker_list = []
		self._python_loc = python_loc
		for worker_num in range(num_workers):
			self._available_worker_list.append(worker_num)
			self._worker_list.append(worker_num)
	
	def _save_state(self):
		return None
	
	def _restore_state(self, old_state):
		return
	
	def reserve_worker(self):
		return self._available_worker_list.wait_pop()
	
	def execute_task(self, task, worker):
		try:
			if sys.platform.startswith("win"):
				exec_process = subprocess.Popen(args=[self._python_loc,
					task._executable, task._input_arg, task._output_arg],
					creationflags=0x08000000, stderr=subprocess.PIPE)
			else:
				exec_process = subprocess.Popen(args=[self._python_loc,
					task._executable, task._input_arg, task._output_arg], stderr=subprocess.PIPE)
			
			proc_stdout, proc_stderr = exec_process.communicate()   # wait for the process to finish
			retcode = exec_process.returncode
			task_error = None
			if retcode is not 0:
				task_error = pymw.InterfaceException("Executable failed with error "+str(retcode), proc_stderr)
				
		except OSError:
			# TODO: check the actual error code
			task_error = pymw.InterfaceException("Could not find python")
		
		task.task_finished(task_error)	# notify the task
		self._available_worker_list.append(worker)	# rejoin the list of available workers

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-len(self._worker_list)}

