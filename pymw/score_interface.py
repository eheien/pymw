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
	def __init__(self, num_workers):
		# Use the scoregroups and scorehosts commands to find all hosts accessible from this machine
		groups_check = subprocess.Popen(args=["scoregroups"],
			stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		group_list = groups_check.stdout.readline().split()
		host_name_list = []
		for group_name in group_list:
			hosts_check = subprocess.Popen(args=["scorehosts", group_name],
				stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			host_name_list.extend(hosts_check.stdout.readline().split())

		# remove duplicate elements
		host_set = list(set(host_name_list))
		self._worker_list = [_Worker(host) for host in host_set]
		self.count = 0
	
	def _save_state(self):
		print "saving state"
	
	def _restore_state(self):
		print "restoring state"
	
	def reserve_worker(self):
		self.count += 1
		return self._worker_list[self.count]
		#return None #fill this in later
	
	def execute_task(self, task, worker):
		if sys.platform.startswith("win"):
			print "SCore not supported on Windows (yet)"
		else:
			task_process = subprocess.Popen(args=["scrun", "--nodes=1",
				"python", task._executable, task._input_arg, task._output_arg])
			task_process.wait()
		
		task.task_finished()

	def get_status(self):
		return {}#{"num_workers" : self._num_workers}#, "num_active_workers": len(self._active_workers)}

