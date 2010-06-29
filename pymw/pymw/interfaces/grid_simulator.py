#!/usr/bin/env python
"""Provide a interface for simulating master worker computing on a desktop grid based on traces.
Thanks to Derrick Kondo for the idea.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "2 May 2009"

import errno
import heapq
import array

# TODO: have some sort of wraparound for worker availability intervals,
# or cleanly error out for workers that are no longer available

class SimWorker:
	def __init__(self, worker_name, worker_speed, worker_avail_lens, worker_avail_fracs):
		self._name = worker_name
		self._speed = worker_speed
		self._avail_lens = array.ArrayType('f')
		self._avail_fracs = array.ArrayType('f')
		self._avail_lens.fromlist(worker_avail_lens)
		self._avail_fracs.fromlist(worker_avail_fracs)
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
			worker_int_speed = self._avail_fracs[self._avail_ind] * self._speed
			# Determine the remaining length of this interval
			int_remaining_secs = self._avail_lens[self._avail_ind] - self._sub_avail_time
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
	
	# Advances the wall time of this worker by wall_secs
	# If the worker is not available at the new time,
	# advances the wall time further until the worker is available
	def advance_wall_time(self, wall_secs):
		rem_secs = wall_secs
		# Advance the availablity interval pointer until we've passed wall_secs
		while rem_secs > 0:
			int_remaining_secs = self._avail_lens[self._avail_ind] - self._sub_avail_time
			if int_remaining_secs < rem_secs:
				rem_secs -= int_remaining_secs
				self._sub_avail_time = 0
				self._avail_ind += 1
			else:
				self._sub_avail_time += rem_secs
				rem_secs = 0
		
		# Advance until we're in an available state
		additional_secs = 0
		while self._avail_fracs[self._avail_ind] == 0:
			additional_secs += self._avail_lens[self._avail_ind] - self._sub_avail_time
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
		self._num_executed_tasks = 0
		self._worker_list = []
		self._waiting_list = []
	
	def add_worker(self, worker):
		# Advance the new worker to its first available time
		worker.advance_wall_time(0)
		# If the new worker isn't available at the start, put it on the waiting list
		if not worker.past_sim_time(0):
			self._worker_list.append(worker)
		else:
			heapq.heappush(self._waiting_list, worker)
		
	def generate_workers(self, num_workers, speed_func, avail_func):
		for wnum in range(num_workers):
			new_worker_speed = speed_func(wnum)
			new_worker_avail_lens, new_worker_avail_fracs = avail_func(wnum)
			new_worker = SimWorker("W"+str(wnum), new_worker_speed, new_worker_avail_lens, new_worker_avail_fracs)
			self.add_worker(new_worker)
		
	def read_workers_from_fta_tab_files(self, event_trace_file, num_workers=None):
		if event_trace_file:
			worker_dict = {}
			event_trace_file.readline()	  # skip the header line
			for line in event_trace_file:
				split_line = line.split()
				node_id, start_time, stop_time = split_line[2], float(split_line[6]), float(split_line[7])
				if node_id not in worker_dict:
					if num_workers and len(worker_dict) >= num_workers: break
					else: worker_dict[node_id] = []
				worker_dict[node_id].append([start_time, stop_time])
		
		for worker_id in worker_dict:
			avail_lens = []
			avail_fracs = []
			worker_times = worker_dict[worker_id]
			last_interval_end = 0
			for int_time in worker_times:
				interval_length = int_time[0] - start_time
				start_time = int_time[1]
			#print((worker_id, worker_times))
	
	# If none of the workers matched the available tasks and there are still workers in the wait queue,
	# advance simulation time and tell PyMW to try again
	def try_avail_check_again(self):
		if len(self._waiting_list) == 0:
			return False
		
		self._cur_sim_time = self._waiting_list[0]._cur_time
		return True
		
	def get_available_workers(self):
		# Pop workers off the sorted waiting list until cur_sim_time
		while len(self._waiting_list) > 0 and self._waiting_list[0].past_sim_time(self._cur_sim_time):
			self._worker_list.append(heapq.heappop(self._waiting_list))
		
		return self._worker_list
	
	def reserve_worker(self, worker):
		self._worker_list.remove(worker)
	
	def worker_finished(self, worker):
		heapq.heappush(self._waiting_list, worker)
	
	def execute_task(self, task, worker):
		if not worker:
			raise Exception("Cannot use NULL worker")
		
		# Get the CPU seconds for the specified task and worker
		task_exec_time = task._raw_exec(worker)
		
		# Run the worker for task_exec_time CPU seconds
		worker.run_cpu(task_exec_time)
		
		self._num_executed_tasks += 1
		task.task_finished(None)	# notify the task

	# Compute statistics (mean, median, stddev) on values in the array
	def compute_stats(self, times):
		times.sort()
		total_time = 0
		for x in times: total_time += x
		mean_time = total_time / len(times)
		median_time = times[len(times)/2]
		stddev_time = 0
		for time_n in times:
			stddev_time += pow(mean_time - time_n, 2)
		stddev_time = pow(stddev_time/len(times), 0.5)
		return total_time, mean_time, median_time, stddev_time
		
	def get_status(self):
		wall_times = []
		cpu_times = []
		for worker in self._worker_list:
			wall_times.extend(worker._task_wall_times)
			cpu_times.extend(worker._task_cpu_times)
		for worker in self._waiting_list:
			wall_times.extend(worker._task_wall_times)
			cpu_times.extend(worker._task_cpu_times)
		if len(wall_times) > 0:
			total_wall_time, mean_wall_time, median_wall_time, stddev_wall_time = self.compute_stats(wall_times)
			total_cpu_time, mean_cpu_time, median_cpu_time, stddev_cpu_time = self.compute_stats(cpu_times)
		else:
			total_wall_time = mean_wall_time = median_wall_time = stddev_wall_time = 0
			total_cpu_time = mean_cpu_time = median_cpu_time = stddev_cpu_time = 0
		
		worker_sim_times = [worker._cur_time for worker in self._worker_list]
		worker_sim_times.append(0)
		cur_sim_time = max(worker_sim_times)
		num_workers = len(self._worker_list) + len(self._waiting_list)
		return {"num_total_workers" : num_workers, "num_executed_tasks" : self._num_executed_tasks,
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
