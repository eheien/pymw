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
	def __init__(self, worker_name, worker_speed, worker_avail):
		self._name = worker_name
		self._speed = worker_speed
		self._avail = worker_avail
		self._avail_ind = 0
		self._cur_time = 0
		self._sub_avail_time = 0
		self._task_wall_times = []
		self._task_cpu_times = []
	
	# TODO: handle going out of bounds on avail array
	# Simulates the worker performing cpu_secs
	# Returns the actual wall time to complete this
	def run_cpu(self, cpu_secs):
		self._task_cpu_times.append(cpu_secs)
		wall_exec_time = 0
		while cpu_secs > 0:
			# Calculate the speed of this worker during the interval
			worker_int_speed = self._avail[self._avail_ind][1] * self._speed
			# Determine the remaining length of this interval
			int_remaining_secs = self._avail[self._avail_ind][0] - self._sub_avail_time
			# Determine the available CPU seconds in this interval
			int_cpu_secs = int_remaining_secs * worker_int_speed
			# If we won't finish the task in this interval
			if int_cpu_secs < cpu_secs:
				# Move to the next interval
				wall_exec_time += int_remaining_secs
				self._avail_ind += 1
				self._sub_avail_time = 0
				cpu_secs -= int_cpu_secs
			else:
				# Move to the middle of this interval
				executed_secs = cpu_secs/worker_int_speed
				wall_exec_time += executed_secs
				self._sub_avail_time += executed_secs
				cpu_secs = 0
		
		self._cur_time += wall_exec_time
		self._task_wall_times.append(wall_exec_time)
		
		return wall_exec_time
	
	# Advances the wall time of this worker by wall_secs
	# If the worker is not available at the new time,
	# advances the wall time further until the worker is available
	def advance_wall_time(self, wall_secs):
		rem_secs = wall_secs
		# Advance the availablity interval pointer until we've passed wall_secs
		while rem_secs > 0:
			int_remaining_secs = self._avail[self._avail_ind][0] - self._sub_avail_time
			if int_remaining_secs < rem_secs:
				rem_secs -= int_remaining_secs
				self._sub_avail_time = 0
				self._avail_ind += 1
			else:
				self._sub_avail_time += rem_secs
				rem_secs = 0
		
		# Advance until we're in an available state
		additional_secs = 0
		while self._avail[self._avail_ind][1] == 0:
			additional_secs += self._avail[self._avail_ind][0] - self._sub_avail_time
			self._avail_ind += 1
			self._sub_avail_time = 0
		
		# Advance the current simulation time
		self._cur_time += wall_secs + additional_secs
		
	# Test if this worker is available at sim_time
	def past_sim_time(self, sim_time):
		if sim_time >= self._cur_time: return True
		else: return False
		
	def __str__(self):
		return self._name

	def __repr__(self):
		return self._name
	
	def __cmp__(self, other):
		return self._cur_time - other._cur_time

class GridSimulatorInterface:
	def __init__(self, trace_files=[]):
		self._cur_sim_time = 0
		self._num_workers = 0
		self._num_executed_tasks = 0
		self._worker_list = []
		self._waiting_list = []
	
	def generate_workers(self, num_workers, speed_func, avail_func):
		for i in range(num_workers):
			new_worker_speed = speed_func()
			new_worker_avail = avail_func()
			new_worker = SimWorker("W"+str(i), new_worker_speed, new_worker_avail)
			# Advance the new worker to its first available time
			new_worker.advance_wall_time(0)
			# If the new worker isn't available at the start, put it on the waiting list
			if not new_worker.past_sim_time(0):
				self._worker_list.append(new_worker)
			else:
				self._waiting_list.append(new_worker)
			
		self._waiting_list.sort()
		self._num_workers += num_workers
		
	def read_workers_from_trace_files(self, trace_files=[]):
		print "not yet implemented"
	
	# If none of the workers matched the available tasks and there are still workers in the wait queue,
	# advance simulation time and tell PyMW to try again
	def try_avail_check_again(self):
		print "no workers matched"
		if len(self._waiting_list) == 0:
			return False
		
		self._cur_sim_time = self._waiting_list[0]._cur_time
		return True
		
	def get_available_workers(self):
		# Pop workers off the sorted waiting list until cur_sim_time
		while len(self._waiting_list) > 0 and self._waiting_list[0].past_sim_time(self._cur_sim_time):
			self._worker_list.append(self._waiting_list.pop[0])
		
		return self._worker_list
	
	def reserve_worker(self, worker):
		self._worker_list.remove(worker)
	
	def worker_finished(self, worker):
		self._waiting_list.append(worker)
		self._worker_list.append(worker)
	
	def execute_task(self, task, worker):
		if not worker:
			task.task_finished(Exception("Cannot use NULL worker"))
			return
		
		# Get the CPU seconds for the specified task and worker
		task_exec_time = task._raw_exec(worker)
		
		# Run the worker for task_exec_time CPU seconds and get the wall run time
		wall_exec_time = worker.run_cpu(task_exec_time)
		self._waiting_list.append(worker)
		self._waiting_list.sort()
		self._num_executed_tasks += 1
		task.task_finished(None)	# notify the task

	def get_status(self):
		# Compute time statistics (mean, median, stddev) on tasks submitted to the interface
		wall_times = []
		cpu_times = []
		for worker in self._worker_list:
			wall_times.extend(worker._task_wall_times)
			cpu_times.extend(worker._task_cpu_times)
		wall_times.sort()
		cpu_times.sort()
		total_wall_time = reduce(lambda x, y: x+y, wall_times)
		total_cpu_time = reduce(lambda x, y: x+y, cpu_times)
		mean_wall_time = total_wall_time / len(wall_times)
		mean_cpu_time = total_cpu_time / len(cpu_times)
		median_wall_time = wall_times[len(wall_times)/2]
		median_cpu_time = cpu_times[len(cpu_times)/2]
		stddev_wall_time = 0
		stddev_cpu_time = 0
		for wall_time in wall_times:
			stddev_wall_time += pow(mean_wall_time - wall_time, 2)
		stddev_wall_time = pow(stddev_wall_time/len(wall_times), 0.5)
		for cpu_time in cpu_times:
			stddev_cpu_time += pow(mean_cpu_time - cpu_time, 2)
		stddev_cpu_time = pow(stddev_cpu_time/len(cpu_times), 0.5)
		
		worker_sim_times = [worker._cur_time for worker in self._worker_list]
		cur_sim_time = reduce(lambda x, y: max(x, y), worker_sim_times)
		return {"num_total_workers" : self._num_workers, "num_executed_tasks" : self._num_executed_tasks,
			    "cur_sim_time": cur_sim_time,
			    "total_wall_time": total_wall_time, "mean_wall_time": mean_wall_time,
			    "median_wall_time": median_wall_time, "stddev_wall_time": stddev_wall_time,
			    "total_cpu_time": total_cpu_time, "mean_cpu_time": mean_cpu_time,
			    "median_cpu_time": median_cpu_time, "stddev_cpu_time": stddev_cpu_time,
			    }

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
