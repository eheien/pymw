#!/usr/bin/env python
"""Provide a generic multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "22 February 2009"

import subprocess
import sys
import errno

class GenericInterface:
	"""Provides a simple generic interface for single machine systems.
	This can take advantage of multicore machines by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		"""Interface initialization should start any necessary programs, 
		and create an initial list of workers if appropriate."""
		self._num_workers = num_workers
		self._available_worker_list = [worker_num for worker_num in range(num_workers)]
		self._python_loc = python_loc
	
	def get_available_workers(self):
		"""Return a list of available workers, or [] if there are no available workers."""
		return list(self._available_worker_list)
	
	def reserve_worker(self, worker):
		"""Reserve a given worker such that it will not be returned by get_available_workers."""
		self._available_worker_list.remove(worker)
	
	def worker_finished(self, worker):
		"""Return a given worker to the pool such that it will be returned by get_available_workers."""
		self._available_worker_list.append(worker)
	
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

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-self._available_worker_list.qsize()}
