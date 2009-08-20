#!/usr/bin/env python
"""Provide a interface for simulating master worker computing on a desktop grid based on traces.
Thanks to Derrick Kondo for the idea.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "2 May 2009"

import errno
import random

class SimWorker:
	def __init__(self):
		self._next_avail = 0
		self._executed_task_times = []

class GridSimulatorInterface:
	def __init__(self, trace_files=[]):
		self._num_workers = 10
		self._num_executed_tasks = 0
		self._worker_list = [SimWorker() for worker_num in range(self._num_workers)]
	
	def get_available_workers(self):
		return self._worker_list
	
	def reserve_worker(self, worker):
		min_avail_time = 1E10
		min_worker = None
		for worker in self._worker_list:
			if worker._next_avail < min_avail_time:
				min_avail_time = worker._next_avail
				min_worker = worker
		
		return min_worker
	
	def execute_task(self, task, worker):
		if not worker:
			task.task_finished(Exception("Cannot use NULL worker"))
			return
		task_exec_time = random.uniform(60, 120)
		self._num_executed_tasks += 1
		worker._next_avail += task_exec_time
		worker._executed_task_times.append(task_exec_time)
		task.task_finished(None)	# notify the task

	def get_status(self):
		# Compute time statistics (mean, median, stddev) on tasks submitted to the interface
		total_cpu_time = 0
		cpu_times = []
		for worker in self._worker_list:
			cpu_times.extend(worker._executed_task_times)
		cpu_times.sort()
		total_cpu_time = reduce(lambda x, y: x+y, cpu_times)
		mean_cpu_time = total_cpu_time / len(cpu_times)
		median_cpu_time = cpu_times[len(cpu_times)/2]
		stddev_time = 0
		for task_time in cpu_times:
			stddev_time += pow(mean_cpu_time - task_time, 2)
		stddev_time = pow(stddev_time/len(cpu_times), 0.5)
		
		return {"num_total_workers" : self._num_workers, "num_total_tasks" : self._num_executed_tasks,
			    "cur_sim_time": 0,
			    "total_cpu_time": total_cpu_time, "mean_cpu_time": mean_cpu_time,
			    "median_cpu_time": median_cpu_time, "stddev_time": stddev_time}

	def pymw_master_read(self, loc):
		return None, None, None
	
	def pymw_master_write(self, output, loc):
		return None
	
	def pymw_worker_read(loc):
		return None
	
	def pymw_worker_write(output, loc):
		return None

	def pymw_worker_func(func_name_to_call):
		return None
