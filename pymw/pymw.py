#!/usr/bin/env python
"""Provide a top level interface for master worker computing.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "2 April 2008"

import sys
import threading
import cPickle
import time
import os

"""The state lock ensures that a consistent state snapshot
will be recorded.  Currently, this code operates under the
assumption that there will be at most 2 threads touching
the state - the scheduler and the main thread. (not quite true)"""
state_lock = threading.Lock()

class _SyncList:
	"""Encapsulates a list with atomic operations and semaphore abilities."""
	def __init__(self):
		self._lock = threading.Lock()
		self._sem = threading.Semaphore(0)
		self._list = []
	
	def __len__(self):
		"""Returns the length of the list."""
		self._lock.acquire()
		l_len = len(self._list)
		self._lock.release()
		return l_len
	
	def append(self, item):
		"""Atomically appends an item to the list and increments the semaphore."""
		self._lock.acquire()
		self._list.append(item)
		self._sem.release()
		self._lock.release()
	
	def release(self):
		"""Releases the semaphore without adding an item.
		Used only to wake threads that need to exit.  Other uses can cause undefined behavior."""
		self._sem.release()

	def wait_contains(self, item):
		self._sem.acquire(blocking=True)
		return self.pop()

	def wait_pop(self):
		"""Waits for an item to appear in the list, and pops it off."""
		self._sem.acquire(blocking=True)
		return self.pop()

	def pop(self):
		"""Atomically pops an item off the list or returns None if the list is empty."""
		self._lock.acquire()
		try:
			item = self._list.pop()
			return item
		except:
			return None
		finally:
			self._lock.release()
	
	def contains(self, item):
		"""Checks if the list contains the specified item."""
		self._lock.acquire()
		n = self._list.count(item)
		self._lock.release()
		if n != 0: return True
		else: return False

class TaskException(Exception):
	def __init__(self, value):
		self.param = value
	def __str__(self):
		return repr(self.param)

class InterfaceException(Exception):
	def __init__(self, value):
		self.param = value
	def __str__(self):
		return repr(self.param)

class PyMW_Task:
	"""Represents a task to be executed."""
	def __init__(self, executable, input_data, file_loc="tasks", task_name=None):
		self._finish_event = threading.Event()
		self._executable = executable
		self._input_data = input_data
		self._output_data = None
		if task_name is None:
			self._task_name = str(executable)+"_"+str(input_data)
		else:
			self._task_name = task_name

		# Make the directory for input/output files, if it doesn't already exist
		try:
			os.mkdir(file_loc)
		except OSError, e: 
			#if e.errno <> errno.EEXIST: 
			#	raise
			pass

		# Set the input and output file locations
		self._input_arg = file_loc + "/in_" + self._task_name + ".dat"
		self._output_arg = file_loc + "/out_" + self._task_name + ".dat"

		# Pickle the input data
		input_data_file = open(self._input_arg, 'w')
		cPickle.Pickler(input_data_file).dump(input_data)
		input_data_file.close()

		# Task time bookkeeping
		self._create_time = time.time()
		self._execute_time = 0
		self._finish_time = 0

	def __str__(self):
		return self._task_name
	
	def task_finished(self, error=None):
		"""This must be called by the interface class when the
		task finishes execution."""
		#global state_lock
		#state_lock.acquire()

                if error:
                        self._error = error
                else:
                        self._error = None
                
		try:
			output_data_file = open(self._output_arg, 'r')
			self._output_data = cPickle.Unpickler(output_data_file).load()
			output_data_file.close()
		except:
			pass

		self._finish_time = time.time()
		self._finish_event.set()
		
		#state_lock.release()

	def is_task_finished(self, wait):
		"""Checks if the task is finished, and optionally waits for it to finish."""
		if not self._finish_event.isSet():
			if not wait:
				return False
			self._finish_event.wait()
		return True

	def get_total_time(self):
		"""Get the time from task submission to completion.
		Returns None if task has not finished execution."""
		if self._finish_time != 0:
			return self._finish_time - self._create_time
		else:
			return None

	def get_execution_time(self):
		"""Get the time from start of task execution to completion.
		This may be different from the CPU time.
		Returns None if task has not finished execution."""
		if self._finish_time != 0:
			return self._finish_time - self._execute_time
		else:
			return None

class PyMW_Scheduler:
	"""Takes tasks submitted by user and sends them to the master-worker interface.
	This is done in a separate thread to allow for asynchronous program execution."""
	def __init__(self, task_list, interface):
		self._task_list = task_list
		self._interface = interface
		self._finished = False
		_scheduler_thread = threading.Thread(target=self._scheduler)
		_scheduler_thread.start()
	
	# Note: it is possible for two different Masters to assign tasks to the same worker
	def _scheduler(self):
		"""Waits for submissions to the task list, then submits them to the interface."""
		while not self._finished:
			# Figure out how to fix this
			# Currently, this lock prevents tasks from finishing
			#global state_lock
			#state_lock.acquire()
			next_task = self._task_list.wait_pop() # Wait for a task submission
			if next_task is not None:
				next_task._execute_time = time.time()
				worker = self._interface.reserve_worker()
				self._interface.execute_task(next_task, worker)
			#state_lock.release()

	def _exit(self):
		"""Signals the scheduler thread to exit."""
		self._finished = True
		self._task_list.release()

class PyMW_Master:
	"""Provides functions for users to submit tasks to the underlying interface."""
	def __init__(self, interface, use_state_records=False):
		self._interface = interface
		self._submitted_tasks = _SyncList()
		self._queued_tasks = _SyncList()
		# Try to restore state first, otherwise _restore_state may conflict with the scheduler
		self._use_state_records = use_state_records
		self._restore_state()
		self._scheduler = PyMW_Scheduler(self._queued_tasks, self._interface)
	
	def __del__(self):
		self._scheduler._exit()
	
	def _save_state(self):
		"""Save the current state of the computation to a record file.
		This includes the state of the interface."""
		if not self._use_state_records: return
		
		global state_lock
		state_lock.acquire()
		pymw_state = {}
		
		try:
			interface_state = self._interface._save_state()
			state_file = open("pymw_state_tmp.dat", 'w')
			try:
				cPickle.Pickler(state_file).dump(pymw_state)
				cPickle.Pickler(state_file).dump(interface_state)
			except:
				pass
			state_file.close()
			os.rename("pymw_state_tmp.dat", "pymw_state.dat")
		except:
			pass
		state_lock.release()
		
	def _restore_state(self):
		"""Restore the previous state of a computation from a record file.
		This should only be called at the beginning of a program."""
		if not self._use_state_records: return
		
		try:
			state_file = open("pymw_state.dat", 'r')
			pymw_state = cPickle.Unpickler(state_file).load()
			interface_state = cPickle.Unpickler(state_file).load()
			self._interface._restore_state(interface_state)
		except:
			pass
		
	def submit_task(self, executable, input_data):
		"""Creates and submits a task to the internal list for execution.
		Returns the created task for later use."""
		# TODO: if using restored state, check whether this task has been submitted before
		new_task = PyMW_Task(executable, input_data)
		self._submitted_tasks.append(new_task)
		self._queued_tasks.append(new_task)
		self._save_state()
		return new_task
	
	def get_result(self, task, wait=True):
		"""Gets the result of the executed task.
		If wait is false and the task is not finished, returns None."""
		if not self._submitted_tasks.contains(task):
			raise TaskException("Task has not been submitted")
		
		if not task.is_task_finished(wait):
			return None

		if task._error:
			raise task._error
		
		self._save_state()              # save state whenever a task finishes
		return task._output_data
	
	def get_status(self):
		status = self._interface.get_status()
		status["tasks"] = self._submitted_tasks
		return status

