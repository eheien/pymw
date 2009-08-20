#!/usr/bin/env python
"""Provide a interface for simulating master worker computing on a desktop grid based on traces.
Thanks to Derrick Kondo for the idea.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "2 May 2009"

import errno

# TODO: have some sort of wraparound for worker availability intervals,
# or cleanly error out for workers that are no longer available

class SimWorker:
	def __init__(self, worker_speed, worker_avail):
		self._speed = worker_speed
		self._avail = worker_avail
		self._avail_ind = 0
		self._sub_avail_time = 0
		self._executed_task_times = []

class GridSimulatorInterface:
	def __init__(self, trace_files=[]):
		self._num_workers = 0
		self._num_executed_tasks = 0
		self._worker_list = []
	
	def generate_workers(self, num_workers, speed_func, avail_func):
		for i in range(num_workers):
			new_worker_speed = speed_func()
			new_worker_avail = avail_func()
			new_worker = SimWorker(new_worker_speed, new_worker_avail)
			self._worker_list.append(new_worker)
			
		self._num_workers += num_workers
			
	def read_workers_from_trace_files(self, trace_files=[]):
		print "not yet implemented"
		
	def get_available_workers(self):
		return self._worker_list
	
	def reserve_worker(self, worker):
		self._worker_list.remove(worker)
	
	def worker_finished(self, worker):
		self._worker_list.append(worker)
        
	def execute_task(self, task, worker):
		if not worker:
			task.task_finished(Exception("Cannot use NULL worker"))
			return
		wall_exec_time = 0
		task_exec_time = task._raw_exec(worker)
		while task_exec_time > 0:
			worker_int_speed = worker._avail[worker._avail_ind][1] * worker._speed
			int_remaining_secs = worker._avail[worker._avail_ind][0] - worker._sub_avail_time
			int_cpu_secs = int_remaining_secs * worker_int_speed
			if int_cpu_secs < task_exec_time:
				task_exec_time -= int_cpu_secs
				wall_exec_time += int_remaining_secs
				worker._avail_ind += 1
				worker._sub_avail_time = 0
			else:
				executed_secs = task_exec_time/worker_int_speed
				task_exec_time = 0
				wall_exec_time += executed_secs
				worker._sub_avail_time += executed_secs
				
		self._num_executed_tasks += 1
		worker._executed_task_times.append(wall_exec_time)
		task.task_finished(None)	# notify the task

	def get_status(self):
		# Compute time statistics (mean, median, stddev) on tasks submitted to the interface
		wall_times = []
		for worker in self._worker_list:
			wall_times.extend(worker._executed_task_times)
		wall_times.sort()
		total_wall_time = reduce(lambda x, y: x+y, wall_times)
		mean_wall_time = total_wall_time / len(wall_times)
		median_wall_time = wall_times[len(wall_times)/2]
		stddev_wall_time = 0
		for task_time in wall_times:
			stddev_wall_time += pow(mean_wall_time - task_time, 2)
		stddev_wall_time = pow(stddev_wall_time/len(wall_times), 0.5)
		
		return {"num_total_workers" : self._num_workers, "num_total_tasks" : self._num_executed_tasks,
			    "cur_sim_time": 0,
			    "total_wall_time": total_wall_time, "mean_wall_time": mean_wall_time,
			    "median_wall_time": median_wall_time, "stddev_wall_time": stddev_wall_time}

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
