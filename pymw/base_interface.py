import subprocess
import threading
import cPickle
import pymw
import sys

"""On worker restarting:
Multicore systems cannot handle worker restarts - recording PIDs can result in unspecified behavior (especially if the system is restarted).  The solution is to check for a result on program restart - if it's not there, delete the directory and start the task anew."""

class _Worker:
	def __init__(self, worker_num, avail_worker_list):
		self._active_task = None
		self._avail_worker_list = avail_worker_list
		self._worker_num = worker_num

	def worker_record(self):
		if self._active_task: task_name = str(self._active_task)
		else: task_name = None
		return [worker_num, task_name]

	def wait_for_finish(self):
		self._process.wait()			# wait for the process to finish
		self._active_task.task_finished()	# notify the task
		self._active_task = None
		self._avail_worker_list.append(self)	# rejoin the list of available workers

class BaseSystemInterface:
	"""Provides a simple interface for single machine systems.
	This can take advantage of multicore by starting multiple processes."""

	def __init__(self, num_workers):
		self._num_workers = num_workers
		self._available_worker_list = pymw._SyncList()
		self._worker_list = []
		for worker_num in range(num_workers):
			new_worker = _Worker(worker_num, self._available_worker_list)
			self._available_worker_list.append(new_worker)
			self._worker_list.append(new_worker)
	
	def _save_state(self):
		print "saving state"
	
	def _restore_state(self, old_state):
		print "restoring state"
	
	def reserve_worker(self):
		return self._available_worker_list.wait_pop()
	
	def execute_task(self, task, worker):
		worker._active_task = task
		if sys.platform.startswith("win"):
			worker._process = subprocess.Popen(args=["python",
				task._executable, task._input_arg, task._output_arg],
				creationflags=0x08000000)
		else:
			worker._process = subprocess.Popen(args=["python",
				task._executable, task._input_arg, task._output_arg])
		worker_finish_handler = threading.Thread(target=worker.wait_for_finish)
		worker_finish_handler.start()

	def get_status(self):
		return {"num_total_workers" : self._num_workers,
			"num_active_workers": self._num_workers-len(self._worker_list)}

