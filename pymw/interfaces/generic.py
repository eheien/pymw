#!/usr/bin/env python
"""Provide a generic multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "22 February 2009"

import subprocess
import sys
import errno
import tempfile
import shutil

class GenericInterface:
	"""Provides a simple generic interface for single machine systems.
	This can take advantage of multicore machines by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc=sys.executable):
		"""Interface initialization should start any necessary programs, 
		and create an initial list of workers if appropriate."""
		self._num_workers = num_workers
		self._available_worker_list = [worker_num for worker_num in range(num_workers)]
		self._worker_dirs = {}
		for wnum in range(num_workers):
			self._worker_dirs[wnum] = tempfile.mkdtemp()
		self._python_loc = python_loc
	
	def get_available_workers(self):
		"""Return a list of available workers, or [] if there are no available workers."""
		return list(self._available_worker_list)
	
	def reserve_worker(self, worker):
		"""Remove a given worker from the pool of available workers."""
		self._available_worker_list.remove(worker)
	
	def worker_finished(self, worker):
		"""Return a given worker to the pool of available workers."""
		self._available_worker_list.append(worker)
	
	def execute_task(self, task, worker):
		"""Execute the task and deal with error codes"""
		if sys.platform.startswith("win"): cf=0x08000000
		else: cf=0
		
		# Copy any necessary files to the worker directory
		if task._data_file_zip: shutil.copy(task._data_file_zip, self._worker_dirs[worker])
		
		# Execute the task
		exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg, task._output_arg],
												cwd=self._worker_dirs[worker], creationflags=cf, stderr=subprocess.PIPE)
		proc_stdout, proc_stderr = exec_process.communicate()   # wait for the process to finish
		if exec_process.returncode is not 0:
			raise Exception("Executable failed with error "+str(exec_process.returncode)+"\n"+proc_stderr.decode())
		
		task.task_finished()

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": len(self._available_worker_list)}

	def _cleanup(self):
		for wnum in self._worker_dirs:
			shutil.rmtree(path=self._worker_dirs[wnum], ignore_errors=True)
