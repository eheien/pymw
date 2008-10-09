#!/usr/bin/env python
"""Provide a multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "10 April 2008"

import subprocess
import sys
import ctypes
import os
import signal
import threading
import Queue

"""On worker restarting:
Multicore systems cannot handle worker restarts - recording PIDs
can result in unspecified behavior (especially if the system is restarted).
The solution is to check for a result on program restart - if it's not there,
delete the directory and start the task anew."""

class Worker:
	def __init__(self):
		self._exec_process = None
	
	def _kill(self):
		if self._exec_process:
			if sys.platform.startswith("win"):
				ctypes.windll.kernel32.TerminateProcess(int(self._exec_process._handle), -1)
			else:
				os.kill(self._exec_process.pid, signal.SIGKILL)

class MulticoreInterface:
	"""Provides a simple interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers=1, python_loc="python"):
		self._num_workers = num_workers
		self._available_worker_list = Queue.Queue(0)
		self._worker_list = []
		self._python_loc = python_loc
		for worker_num in range(num_workers):
			w = Worker()
			self._available_worker_list.put_nowait(item=w)
			self._worker_list.append(w)
	
	def reserve_worker(self):
		return self._available_worker_list.get(block=True)
	
	def execute_task(self, task, worker):
		try:
			if sys.platform.startswith("win"): cf=0x08000000
			else: cf=0
			
			worker._exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg,
					task._output_arg], creationflags=cf, stderr=subprocess.PIPE)
			#print self._python_loc , task._executable, task._input_arg,	task._output_arg
			#print "working:",os.getcwd()
			proc_stdout, proc_stderr = worker._exec_process.communicate()   # wait for the process to finish
			retcode = worker._exec_process.returncode
			task_error = None
			if retcode is not 0:
				task_error = Exception("Executable failed with error "+str(retcode), proc_stderr)
				
		except OSError:
			# TODO: check the actual error code
			task_error = Exception("Could not find python")
		
		worker._exec_process = None
		task.task_finished(task_error)	# notify the task
		self._available_worker_list.put_nowait(item=worker)	# rejoin the list of available workers

	def _cleanup(self):
		for worker in self._worker_list:
			worker._kill()
	
	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-len(self._worker_list)}

	def pymw_get_input():
		#print "get_input"
		infile = open(sys.argv[1], 'r')
		obj = cPickle.Unpickler(infile).load()
		infile.close()
		return obj
	
	def pymw_return_output(output):
		#print "return_output"
		outfile = open(sys.argv[2], 'w')
		cPickle.Pickler(outfile).dump(output)
		outfile.close()


