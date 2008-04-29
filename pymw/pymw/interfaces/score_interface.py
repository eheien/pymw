import subprocess
import threading
import cPickle
import pymw
import sys
import os

class _Worker:
	def __init__(self, host_name):
		self._active_task = None
		self._host_name = host_name
	
class SCoreSystemInterface:
	def __init__(self, num_workers=4):
		# Use the scoregroups and scorehosts commands to find all hosts accessible from this machine
		groups_check = subprocess.Popen(args=["scoregroups"],
			stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		group_list = groups_check.stdout.readline().split()
		host_name_list = []
		for group_name in group_list:
			hosts_check = subprocess.Popen(args=["scorehosts", group_name],
				stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			host_name_list.extend(hosts_check.stdout.readline().split())

		# remove duplicate host names (for example, hosts in more than one group)
		host_set = list(set(host_name_list))
		self._worker_list = [_Worker(host) for host in host_set]
		self._worker_list = self._worker_list[:num_workers]
		self._count = 0
		self._mpi_manager_process = subprocess.Popen(args=["mpirun", "-np", str(num_workers+1), "/home/myri-fs/e-heien/local/bin/pyMPI", "/home/myri-fs/e-heien/osaka/pymw/pymw/pymw/interfaces/mpi_manager.py"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		booga = self._mpi_manager_process.stdout.readline()
		print booga
	
	def _save_state(self):
		print "saving state"
	
	def _restore_state(self):
		print "restoring state"
	
	def reserve_worker(self):
		self._count = (self._count+1)%len(self._worker_list)
		return self._count
		#return self._worker_list[self._count]
	
	def execute_task(self, task, worker):
		if sys.platform.startswith("win"):
			print "SCore not yet supported on Windows"
		else:
			self._mpi_manager_process.stdin.write(str(worker)+str(task))
			#task_process = subprocess.Popen(args=["mpirun", "-np", "1", "-machinefile", hostfilename,
				#"/home/myri-fs/e-heien/local/bin/pyMPI", task._executable, task._input_arg, task._output_arg], stderr=subprocess.PIPE)
			#task_process.wait()
		
		#task.task_finished()

	def get_status(self):
		return {}

