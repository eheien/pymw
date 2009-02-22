#!/usr/bin/env python
"""Provide a generic multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "22 February 2009"

import subprocess
import sys
import errno
import Queue

class GenericInterface:
	"""Provides a simple generic interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		self._num_workers = num_workers
		self._available_worker_list = Queue.Queue(0)
		self._python_loc = python_loc
		for worker_num in range(num_workers):
			self._available_worker_list.put_nowait(item=worker_num)
	
	def reserve_worker(self):
		return self._available_worker_list.get(block=True)
	
	def execute_task(self, task, worker):
		try:
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
			task_error = Exception("Could not find python")
		
		task.task_finished(task_error)	# notify the task
		self._available_worker_list.put_nowait(item=worker)	# rejoin the list of available workers

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-self._available_worker_list.qsize()}
