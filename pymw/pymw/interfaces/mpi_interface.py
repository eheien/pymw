import subprocess
import threading
import pymw
import sys
import os
import logging

class MPIInterface:
	def __init__(self, num_workers=4, mpirun_loc="mpirun"):
		# remove duplicate host names (for example, hosts in more than one group)
		self._mpi_manager_process = subprocess.Popen(args=[mpirun_loc, "-np", str(num_workers+1), "/home/myri-fs/e-heien/local/bin/pyMPI", "/home/myri-fs/e-heien/osaka/pymw/pymw/pymw/interfaces/mpi_manager.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		task_finish_thread = threading.Thread(target=self._get_finished_tasks)
		task_finish_thread.start()
		num_workers = int(self._mpi_manager_process.stdout.readline())
		self.task_dict = {}
	
	def _save_state(self):
		print "saving state"
	
	def _restore_state(self):
		print "restoring state"
	
	def reserve_worker(self):
		return None
	
	def _get_finished_tasks(self):
		try:
			while True:
				task_name = self._mpi_manager_process.stdout.readline()
				print "Finished task", task_name
				task = self.task_dict[task_name]
				task.task_finished()
		except:
			pass

	def execute_task(self, task, worker):
		print "Submitted task", str(task)
		self.task_dict[str(task)] = task
		self._mpi_manager_process.stdin.write(str(task))

	def get_status(self):
		return {}


