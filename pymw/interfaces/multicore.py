#!/usr/bin/env python
"""Provide a multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "22 February 2009"

import subprocess
import sys
import ctypes
import os
import signal
import errno
import pickle
import tempfile
import shutil

class Worker:
	"""Represents a worker in the multicore interface.
	Provides functions to kill the worker and clean up any temporary files."""
	def __init__(self):
		self._exec_process = None
		self._worker_dir = tempfile.mkdtemp()
	
	def _kill(self):
		try:
			if self._exec_process:
				if sys.platform.startswith("win"):
					ctypes.windll.kernel32.TerminateProcess(int(self._exec_process._handle), -1)
				else:
					os.kill(self._exec_process.pid, signal.SIGKILL)
		except:
			pass
	
	def _cleanup(self):
		shutil.rmtree(self._worker_dir)

class MulticoreInterface:
	"""Provides a simple interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc=sys.executable):
		self._num_workers = num_workers
		self._available_worker_list = [Worker() for worker_num in range(num_workers)]
		self._worker_list = [worker for worker in self._available_worker_list]
		self._python_loc = python_loc
		self._input_objs = {}
		self._output_objs = {}
		self.pymw_interface_modules = "pickle", "sys"
	
	def get_available_workers(self):
		return list(self._available_worker_list)
	
	def reserve_worker(self, worker):
		self._available_worker_list.remove(worker)
	
	def worker_finished(self, worker):
		self._available_worker_list.append(worker)
	
	def execute_task(self, task, worker):
		if sys.platform.startswith("win"): cf=0x08000000
		else: cf=0
		
		# Copy any necessary files to the worker directory
		if task._data_file_zip: shutil.copy(task._data_file_zip, worker._worker_dir)
		
		# Pickle the input argument and remove it from the list
		input_obj_str = pickle.dumps(self._input_objs[task._input_arg])

		worker._exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg, task._output_arg],
												cwd=worker._worker_dir, creationflags=cf, stdin=subprocess.PIPE,
												stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		# Wait for the process to finish
		proc_stdout, proc_stderr = worker._exec_process.communicate(input_obj_str)
		retcode = worker._exec_process.returncode
		if retcode is 0:
			self._output_objs[task._output_arg] = pickle.loads(proc_stdout)
		else:
			raise Exception("Executable failed with error "+str(retcode)+"\n"+proc_stderr)
		
		worker._exec_process = None
		task.task_finished()	# notify the task

	def _cleanup(self):
		for worker in self._worker_list:
			worker._kill()
			worker._cleanup()
	
	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-len(self._available_worker_list)}
	
	def pymw_master_read(self, loc):
		return self._output_objs[loc]
	
	def pymw_master_write(self, output, loc):
		self._input_objs[loc] = output
	
	def pymw_worker_read(options):
		return pickle.Unpickler(sys.stdin).load()
	
	def pymw_worker_write(output, options):
		if "file_input" in options:
			outfile = open(sys.argv[2], 'w')
			pickle.Pickler(outfile).dump(output[0])
			outfile.close()
			output[0]=None
		print((pickle.dumps(output)))
