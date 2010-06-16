#!/usr/bin/env python
"""Provide an interface using the multiprocessing module available in Python 2.6.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "28 October 2009"

import multiprocessing
import pickle
import sys
import tempfile
import shutil
import traceback

def worker_main(conn_pipe):
	while True:
		conn_pipe.poll(None)  # Wait for the next task
		args = conn_pipe.recv()
		exec(open(args[0]).read())
		conn_pipe.send("done")

class MultiProcInterface:
	"""Provides a simple generic interface for single machine systems.
	This can take advantage of multicore machines by starting multiple processes."""

	def __init__(self, num_workers=1):
		self._num_workers = num_workers
		self._pipe_list = [multiprocessing.Pipe() for i in range(num_workers)]
		self._worker_list = [multiprocessing.Process(target=worker_main, args=(self._pipe_list[i][1],))
							 for i in range(num_workers)]
		self._available_worker_list = [worker for worker in self._worker_list]
		self._worker_dirs = {}
		self._worker_pipes = {}
		for wnum in range(num_workers):
			worker = self._worker_list[wnum]
			self._worker_pipes[worker] = self._pipe_list[wnum][0]
			self._worker_dirs[worker] = tempfile.mkdtemp()
			worker.start()
	
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
		
		# Copy any necessary files to the worker directory
		if task._data_file_zip: shutil.copy(task._data_file_zip, self._worker_dirs[worker])
		
		# Execute the task
		conn_pipe = self._worker_pipes[worker]
		conn_pipe.send([task._executable, task._input_arg, task._output_arg])
		conn_pipe.poll(None)  # Wait for the response
		result = conn_pipe.recv()
		
		task.task_finished()

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": len(self._available_worker_list)}

	def _cleanup(self):
		for worker in self._worker_list:
			worker.terminate()
			
		for wnum in self._worker_dirs:
			shutil.rmtree(path=self._worker_dirs[wnum], ignore_errors=True)
