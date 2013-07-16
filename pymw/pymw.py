#!/usr/bin/env python
"""Provide a top level interface for master worker computing.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "10 April 2008"

import atexit

import pickle
import errno
import logging
import inspect
import os
import signal
import sys
import tempfile
import textwrap
import threading
import time
import traceback
import types
import zipfile
from .interfaces import generic

if sys.version_info[0] > 2:
	from io import StringIO

class PyMW_List:
	"""A class representing a Python list with atomic operation functionality needed for PyMW."""
	
	def __init__(self):
		self._lock = threading.Lock()
		self._add_event = threading.Condition(self._lock)
		self._data = []
	
	def __len__(self):
		return len(self._data)
	
	def get_data(self):
		"""Returns a copy of the internal data list that can be modified."""
		self._lock.acquire()
		copy_list = list(self._data)
		self._lock.release()
		return copy_list
		
	def append(self, item):
		"""Atomically appends an item to the list and notifies any waiting threads."""
		self._add_event.acquire()
		self._data.append(item)
		self._add_event.notifyAll()
		self._add_event.release()

	def pop(self, blocking=False):
		"""Waits for any item to appear in the list, and pops it off."""
		return self.pop_specific([], blocking)

	def pop_specific(self, item_list=[], blocking=False):
		"""Waits for any item from item_list to appear, and pops it off.
		An empty item_list indicates any item is acceptable."""
		item_set = set(item_list)
		self._add_event.acquire()
		while True:
			# Check if any of the current items are acceptable
			# If we have a list of items, choose one from the list
			found_item = None
			if len(item_list) > 0:
				data_set = set(self._data)
				search = item_set & data_set
				if len(search) > 0:
					found_item = list(search)[0]
					self._data.remove(found_item)
			# Otherwise any item is acceptable
			elif len(self._data) > 0:
				found_item = self._data.pop()
			
			if found_item:
				self._add_event.release()
				return found_item
			
			# If we didn't find anything and we should block,
			# wait for a notification from a new item being added
			if blocking:
				self._add_event.wait()
			# If we didn't find anything and we should not block, return None
			else:
				self._add_event.release()
				return None
		
	def contains(self, item):
		"""Checks if the list contains the specified item."""
		self._add_event.acquire()
		n = self._data.count(item)
		self._add_event.release()
		if n != 0: return True
		else: return False

class TaskException(Exception):
	"""Represents an exception caused by a task failure."""
	def __init__(self, value):
		self.param = value
	def __str__(self):
		return repr(self.param)

class InterfaceException(Exception):
	"""Represents an exception caused by an interface failure."""
	def __init__(self, value, detail_str=None):
		self.param = value
		if detail_str:
			self.details = detail_str
		else:
			self.details = ""
	def __str__(self):
		return repr(self.param)+"\n"+repr(self.details)

class PyMW_Task:
	"""Represents a task to be executed."""
	
	TASK_SUBMITTED = "submitted"
	TASK_RUNNING = "running"
	TASK_ERROR = "error"
	TASK_FINISHED = "finished"
	
	def __init__(self, task_name, executable, finished_queue, store_data_func, get_result_func,
				 input_data=None, input_arg=None, output_arg=None, file_loc="tasks",
				 data_file_zip=None, modules_file_zip=None, file_input=False, raw_exec=None):
		# Make sure executable is valid
		if not isinstance(executable, bytes) \
			and not isinstance(executable, types.FunctionType) \
			and not isinstance(executable, str):
			raise TypeError("executable must be a filename or Python function")
		
		self._finished_queue = finished_queue
		self._executable = executable
		self._input_data = input_data
		self._output_data = None
		self._task_name = task_name
		self._get_result_func = get_result_func
		self._store_data_func = store_data_func
		self._file_input = file_input
		self._data_file_zip = data_file_zip
		self._modules_file_zip = modules_file_zip
		self._raw_exec = raw_exec

		# Set the input and output file locations
		if input_arg:
			self._input_arg = input_arg
		else:
			self._input_arg = file_loc + "/in_" + self._task_name + ".dat"
		logging.info("Storing task "+str(self)+" into "+self._input_arg)
		self._store_data_func(input_data, self._input_arg)

		if output_arg:
			self._output_arg = output_arg
		else:
			self._output_arg = file_loc + "/out_" + self._task_name + ".dat"

		# Remove any old output files
		try:
			os.remove(self._output_arg)
		except:
			pass
		
		self._task_state = self.TASK_SUBMITTED
		
		# Task time bookkeeping
		self._times = {"submit_time": time.time(), "execute_time": 0, "finish_time": 0}

	def __str__(self):
		return self._task_name

	def __repr__(self):
		return self._task_name
	
	def _state_data(self):
		return {"task_name": self._task_name, "executable": self._executable,
				"input_arg": self._input_arg, "output_arg": self._output_arg,
				"times": self._times, "state": self._task_state}
	
	def task_finished(self, task_err=None, result=None):
		"""This must be called by the interface class when the
		task finishes execution.  The result of execution should
		be in the file indicated by output_arg."""

		self._error = task_err
		if task_err:
			logging.info("Task "+str(self)+" had an error")
		elif not result:
			try:
				self._output_data, self._stdout, self._stderr = self._get_result_func(self._output_arg)
			except:
				self._output_data = None
				self._error = Exception("Error reading task result "+self._output_arg)
			logging.info("Task "+str(self)+" finished")
		else:
			try:
				self._output_data=[]
				for file in result:
					f=open(file[0],"r")
					self._output_data.append(pickle.loads(f.read()))
			except:
				self._output_data = result
			logging.info("Task "+str(self)+" finished")
		
		self._times["finish_time"] = time.time()
		if self._error: self._task_state = self.TASK_ERROR
		else: self._task_state = self.TASK_FINISHED
		self._finished_queue.append(self)
		try:
			self._worker_finish_func(self._assigned_worker)
		except:
			pass

	def get_total_time(self):
		"""Get the time from task submission to completion.
		Returns None if task has not finished execution."""
		if self._task_state is self.TASK_FINISHED or self._task_state is self.TASK_ERROR:
			return self._times["finish_time"] - self._times["submit_time"]
		else:
			return None

	def get_execution_time(self):
		"""Get the time from start of task execution to completion.
		This may be different from the CPU time.
		Returns None if task has not finished execution."""
		if self._task_state is self.TASK_FINISHED or self._task_state is self.TASK_ERROR:
			return self._times["finish_time"] - self._times["execute_time"]
		else:
			return None

	def get_progress(self):
		"""Get the progress of the task, as represented by a double between 0 and 1."""
		if self._task_state is self.TASK_FINISHED: return 1.0
		elif self._task_state is self.TASK_SUBMITTED: return 0.0
		else: return 0.0
	
	def cleanup(self, delete_files):
		try:
			if delete_files:
				os.remove(self._input_arg)
				os.remove(self._output_arg)
		except OSError:
			pass

		
class PyMW_Scheduler:
	"""Takes tasks submitted by user and sends them to the master-worker interface.
	This is done in a separate thread to allow for asynchronous program execution."""
	def __init__(self, task_queue, interface, task_match_func):
		self._task_queue = task_queue
		self._interface = interface
		self._running = False
		self._interface_worker_lock = threading.Condition()
		if task_match_func: self._task_matcher = task_match_func
		else: self._task_matcher = self._default_task_match_func
	
	def _start_scheduler(self):
		if not self._running:
			logging.info("PyMW_Scheduler started")
			self._running = True
			_scheduler_thread = threading.Thread(target=self._scheduler)
			_scheduler_thread.start()
	
	def _default_task_match_func(self, task_list, worker_list):
		return task_list[0], worker_list[0]
	
	def _worker_finished(self, worker):
		self._interface_worker_lock.acquire()
		try:
			self._interface.worker_finished(worker)
		except:
			pass
		self._interface_worker_lock.notify()
		self._interface_worker_lock.release()
	
	# Returns true if the scheduler should continue running
	def _should_scheduler_run(self):
		return (len(self._task_queue) > 0)
	
	# Get a list of workers available on this interface
	def _get_worker_list(self):
		try:
			worker_list = self._interface.get_available_workers()
			if not type(worker_list)==list: worker_list = [None]
		except:
			worker_list = [None]
		return worker_list
	
	# Match a worker from the list with a task
	# If we couldn't find the task/worker in the list, the task matcher returned an invalid value
	def _match_worker_and_task(self, task_list, worker_list):
		try:
			matched_task, matched_worker = self._task_matcher(task_list, worker_list)
		except:
			matched_worker = worker_list[0]
			matched_task = task_list[0]
		if worker_list.count(matched_worker) == 0: matched_worker = worker_list[0]
		
		return matched_task, matched_worker
	
	# Reserve the worker with the interface and remove the task from the queue
	def _reserve_task_worker(self, matched_task, matched_worker):
		# Remove the task from the queue
		popped_task = self._task_queue.pop_specific(item_list=[matched_task])
		if not popped_task: matched_task = task_list[0]
		
		# Reserve the worker with the interface
		matched_task._assigned_worker = matched_worker
		matched_task._worker_finish_func = self._worker_finished
		try:
			self._interface.reserve_worker(matched_worker)
		except:
			pass
	
	# Lets the interface know that no workers matched, and checks if it should try again immediately
	# Otherwise, it waits until a worker has finished or 1 second has passed (whichever is first)
	def _wait_for_worker(self):
		try:
			if self._interface.try_avail_check_again(): return
		except:
			pass
		self._interface_worker_lock.wait(timeout=1.0)
	
	# Scheduler logic:
	# While there are tasks on the queue
	#	- Get a list of available workers
	#	- If no worker is available
	#		~ try again after a _worker_finished signal or 1 second (whichever is first)
	#	- else (> 0 workers are available)
	#		~ call the task matching function with the list of tasks and list of workers
	#	- If the task matcher doesn't fit any worker with a task
	#		~ try again after a _worker_finished signal or 1 second (whichever is first)
	#	- else (the task matcher gives a match)
	#		~ Remove the task from the list of tasks
	#		~ Reserve the worker with the interface
	#		~ Execute the task on the interface with the given worker
	#		~ When task_finished is called, replace the worker in the interface with _worker_finished
	def _scheduler(self):
		"""Waits for submissions to the task list, then submits them to the interface."""
		# While there are tasks in the queue, assign them to workers
		# NOTE: assumes that only the scheduler thread will remove tasks from the list
		# only the scheduler thread will call reserve_worker, and there is only one scheduler thread
		while self._should_scheduler_run():
			# Hold the interface lock until we have a worker, matched it with a task and
			# reserved it with the interface.  Otherwise we may select the same worker twice
			# or other problems can occur
			
			self._interface_worker_lock.acquire()
			
			# Get a list of available workers and tasks
			# If none are available, then wait a little and try again
			worker_list = self._get_worker_list()
			if len(worker_list) == 0:
				self._wait_for_worker()
				self._interface_worker_lock.release()
				continue
			task_list = self._task_queue.get_data()
			
			# Try to match one of the tasks with one of the workers
			# If no suitable match is found, wait a little and try again
			logging.info("Matching task with a worker")
			matched_task, matched_worker = self._match_worker_and_task(task_list, worker_list)
			if not matched_task:
				self._wait_for_worker()
				self._interface_worker_lock.release()
				continue
			
			# Confirm the match and reserve the task and worker
			self._reserve_task_worker(matched_task, matched_worker)
			self._interface_worker_lock.release()
			
			# Wait until other tasks have been submitted and the thread count decreases,
			# otherwise we might pass the process resource limitations
			while threading.activeCount() > 100:
				time.sleep(0.1)
			
			# Execute the task on the interface with the given worker
			logging.info("Executing task "+str(matched_task))
			task_thread = threading.Thread(target=self._task_executor,
										   args=(self._interface.execute_task, matched_task, matched_worker))
			task_thread.start()
		
		logging.info("PyMW_Scheduler finished")
		self._running = False
	
	# Use this wrapper function to catch any interface exceptions,
	# otherwise we can get hanging threads
	def _task_executor(self, execute_task_func, next_task, worker):
		try:
			next_task._times["execute_time"] = time.time()
			execute_task_func(next_task, worker)
		except Exception as e:
			next_task.task_finished(e)
	
	def _exit(self):
		self._task_queue.append(None)

class PyMW_Master:
	"""Provides functions for users to submit tasks to the underlying interface."""
	def __init__(self, interface=None, loglevel=logging.CRITICAL, delete_files=True, scheduler_func=None):
		logging.basicConfig(level=loglevel, format="%(asctime)s %(levelname)s %(message)s")

		if interface:
			if not hasattr(interface, "execute_task"):
				raise InterfaceException("Interface must have execute_task() function.")
			if not hasattr(interface.execute_task, '__call__'):
				raise InterfaceException("Interface execute_task must be a function.")
			self._interface = interface
		else:
			self._interface = generic.GenericInterface()
		
		self._start_time_str = str(int(time.time()))
		self._submitted_tasks = []
		self._queued_tasks = PyMW_List()
		self._finished_tasks = PyMW_List()
		
		self._delete_files = delete_files
		self._task_dir_name = os.getcwd() + "/tasks"
		self._cur_task_num = 0
		self._function_source = {}
		if sys.version_info[0] > 2:
			self._pymw_interface_modules = "pickle", "sys", "zipfile", "traceback", "io"
		else:
			self._pymw_interface_modules = "pickle", "sys", "zipfile", "traceback","StringIO"
	
		self._data_file_zips = {}
		self._module_zips = {}

		# Make the directory for input/output files, if it doesn't already exist
		try:
			os.mkdir(self._task_dir_name)
		except OSError as e:
			if e.errno != errno.EEXIST: raise

		self._scheduler = PyMW_Scheduler(self._queued_tasks, self._interface, scheduler_func)
		atexit.register(self._cleanup, None, None)
		#signal.signal(signal.SIGKILL, self._cleanup)
	
	def _setup_exec_file(self, file_name, main_func, modules, dep_funcs, file_input, data_file_zip_name):
		"""Sets up a script file for executing a function.  This file
		contains the function source, dependent functions, dependent
		modules and PyMW calls to get the input data and return the
		output data."""
		
		# If the interface doesn't provide methods for communicating with the workers, use default functions
		all_funcs = (main_func,)+dep_funcs
		all_funcs += (self._pymw_worker_manager, self.pymw_emit_result, )
		try:
			all_funcs += (self._interface.pymw_worker_read, self._interface.pymw_worker_write)
		except AttributeError:
			all_funcs += (self.pymw_worker_read, self.pymw_worker_write) 

		try:
			interface_modules = self._interface._pymw_interface_modules
		except AttributeError:
			interface_modules = self._pymw_interface_modules
		
		# Select the function to coordinate task execution on the worker
		try:
			all_funcs += (self._interface.pymw_worker_func,)
		except AttributeError:
			all_funcs += (self.pymw_worker_func,)
		
		# Get the source code for the necessary functions
		func_hash = hash(all_funcs)
		if func_hash not in self._function_source:
			func_sources = [textwrap.dedent(inspect.getsource(func)) for func in all_funcs]
			self._function_source[func_hash] = [main_func.__name__, func_sources, file_name]
		else:
			return

		# Create an archive of required modules
		self._archive_files(modules, True)

		func_data = self._function_source[func_hash]
		func_file = open(file_name, "w")
		# Create the necessary imports and function calls in the worker script
		for module_name in modules+interface_modules:
			func_file.write("import "+module_name+"\n")
		func_file.writelines(func_data[1])
		run_options = {}
		if file_input: run_options["file_input"] = True
		if data_file_zip_name: run_options["arch_file"] = data_file_zip_name
		func_file.write("_pymw_worker_manager("+func_data[0]+", "+repr(run_options)+")\n")
		func_file.close()
		
	def _archive_files(self, data_files, is_modules=False):
		if len(data_files) == 0: return None
		
		file_hash = hash(data_files)
		if is_modules:
			if file_hash in self._module_zips:
				return self._module_zips[file_hash]
		else:
			if file_hash in self._data_file_zips:
				return self._data_file_zips[file_hash]
		
		# TODO: this is insecure, try to use the arch_fd in creating the Zipfile object
		if is_modules: arch_prefix = "modules_"
		else: arch_prefix = "data_"
		arch_fd, arch_file_name = tempfile.mkstemp(suffix=".zip", prefix=arch_prefix, dir=self._task_dir_name)
		os.close(arch_fd)
		
		archive_zip = zipfile.PyZipFile(arch_file_name, mode="w")
		for dfile in data_files:
			ind_file_name = dfile.split("/")[-1]
			if is_modules:
				try:
					archive_zip.writepy(pathname=dfile+".py")
				except IOError:
					logging.info("Couldn't find file for module "+dfile)
			else:
				archive_zip.write(filename=dfile, arcname=ind_file_name)
		archive_zip.close()
		
		if is_modules:
			self._module_zips[file_hash] = arch_file_name
			return self._module_zips[file_hash]
		else:
			self._data_file_zips[file_hash] = arch_file_name
			return self._data_file_zips[file_hash]

	def _check_task_list(self, task_list):
		if len(self._submitted_tasks) <= 0:
			raise TaskException("No tasks have been submitted")
		
		# Check that the task(s) are of type PyMW_Task
		for t in task_list:
			if not isinstance(t, PyMW_Task):
				raise TaskException("Function requires either a task, a list of tasks, or None")
		
		# Check that the task(s) have been submitted before
		submit_intersect = set(self._submitted_tasks) & set(task_list)
		if len(submit_intersect) != len(task_list):
			raise TaskException("Task has not been submitted")
		
	def submit_task(self, executable, input_data=None, modules=(), dep_funcs=(), data_files=(), input_from_file=False):
		"""Creates and submits a task to the internal list for execution.
		Returns the created task for later use.
		executable can be either a filename (Python script) or a function."""
		
		# Check if the executable is a Python function or a script
		if hasattr(executable, '__call__'):
			task_name = str(executable.__name__)+"_"+self._start_time_str+"_"+str(self._cur_task_num)
			exec_file_name = self._task_dir_name+"/"+str(executable.__name__)+"_"+self._start_time_str+".py"
		elif isinstance(executable, str):
			# TODO: test here for existence of script
			task_name = str(executable)+"_"+self._start_time_str+"_"+str(self._cur_task_num)
			exec_file_name = executable+"_"+self._start_time_str+".py"
		else:
			raise TaskException("Executable must be a filename or function")
		
		self._cur_task_num += 1
		
		# Create a zip archive containing the files of data_files
		if len(data_files) > 0:
			zip_arch_file = self._archive_files(data_files, False)
			zip_arch_file_name = zip_arch_file.split("/")[-1]
		else:
			zip_arch_file = None
			zip_arch_file_name = None
		
		# Create a zip archive containing the modules
		if len(data_files) > 0:
			mod_arch_file = self._archive_files(modules, True)
			mod_arch_file_name = zip_arch_file.split("/")[-1]
		else:
			mod_arch_file = None
			mod_arch_file_name = None
		
		# Setup the necessary files
		if hasattr(executable, '__call__'):
			self._setup_exec_file(exec_file_name, executable, modules, dep_funcs, input_from_file, zip_arch_file_name)
		
		try:
			store_func = self._interface.pymw_master_write
			get_result_func = self._interface.pymw_master_read
		except AttributeError:
			store_func = self.pymw_master_write
			get_result_func = self.pymw_master_read
		
		new_task = PyMW_Task(task_name=task_name, executable=exec_file_name,
							 store_data_func=store_func, get_result_func=get_result_func,
							 finished_queue=self._finished_tasks, input_data=input_data,
							 file_loc=self._task_dir_name, data_file_zip=zip_arch_file,
							 modules_file_zip=mod_arch_file, file_input=input_from_file,
							 raw_exec=executable)
		
		self._submitted_tasks.append(new_task)
		self._queued_tasks.append(item=new_task)
		self._scheduler._start_scheduler()
		
		return new_task
		
	def get_result(self, task=None, blocking=True):
		"""Gets the result of the executed task.
		If task is None, return the result of the next finished task.
		If task is a list of tasks, return the result of any task in the list.
		If blocking is false and the task is not finished, returns None."""
		
		if not task:
			task_list = []
		elif type(task)==list:
			task_list = task
		else:
			task_list = [task]
		
		# Check that the task(s) are of type PyMW_Task and have been submitted before
		self._check_task_list(task_list)
		
		my_task = self._finished_tasks.pop_specific(task_list, blocking)
		
		if not my_task:
			return None, None

		if my_task._error:
			raise my_task._error
		
		return my_task, my_task._output_data
	
	def get_progress(self, task):
		if not task:
			task_list = []
		elif type(task)==list:
			task_list = task
		else:
			task_list = [task]
		
		# Check that the task(s) are of type PyMW_Task and have been submitted before
		self._check_task_list(task_list)
		
		task_progress = [task.get_progress() for task in task_list]
		return task_progress
		
	def get_status(self):
		self._scheduler._interface_worker_lock.acquire()
		try:
			status = self._interface.get_status()
		except:
			status = {"interface_status": "error"}
		self._scheduler._interface_worker_lock.release()
		if not type(status)==dict: status = {"interface_status": "error"}
		status["tasks"] = self._submitted_tasks
		return status

	def _cleanup(self, signum, frame):
		self._scheduler._exit()
		
		try:
			self._interface._cleanup()
		except AttributeError:
			pass
		
		for task in self._submitted_tasks:
			task.cleanup(self._delete_files)
		
		if self._delete_files:
			for exec_file in self._function_source:
				try:
					os.remove(self._function_source[exec_file][2])
				except OSError:
					pass
			
			for hash_ind in self._data_file_zips:
				try:
					os.remove(self._data_file_zips[hash_ind])
				except OSError:
					pass
			
			for hash_ind in self._module_zips:
				try:
					os.remove(self._module_zips[hash_ind])
				except OSError:
					pass
		
		try:
			if self._delete_files:
				os.rmdir(self._task_dir_name)
			pass
		except OSError:
			pass

	def pymw_master_read(self, loc):
		infile = open(loc, 'rb')
		obj = pickle.Unpickler(infile).load()
		infile.close()
		return obj
	
	def pymw_master_write(self, output, loc):
		outfile = open(loc, 'wb')
		pickle.Pickler(outfile).dump(output)
		outfile.close()
	
	def pymw_worker_read(options):
		infile = open(sys.argv[1], 'rb')
		obj = pickle.Unpickler(infile).load()
		infile.close()
		return obj

	def pymw_worker_write(output, options):
		outfile = open(sys.argv[2], 'wb')
		pickle.Pickler(outfile).dump(output)
		outfile.close()

	def pymw_set_progress(prog_ratio):
		return
	
	def pymw_emit_result(result):
		global _res_array
		_res_array.append(result)
	
	def _pymw_worker_manager(func_name_to_call, options):
		global _res_array
		_res_array = []
		try:
			# Redirect stdout and stderr
			old_stdout = sys.stdout
			old_stderr = sys.stderr

			if sys.version_info[0] > 2:
				sys.stdout = io.StringIO()
				sys.stderr = io.StringIO()
			else:
				sys.stdout = StringIO.StringIO()
				sys.stderr = StringIO.StringIO()

			# If there is a zip file, unzip the contents
			if "arch_file" in options:
				data_arch = zipfile.PyZipFile(file=options["arch_file"], mode='r')
				archive_files = data_arch.namelist()
				for file_name in archive_files:
					decompressed_file = open(file_name, "wb")
					decompressed_file.write(data_arch.read(file_name))
					decompressed_file.close()
				data_arch.close()
			# Call the worker function
			pymw_worker_func(func_name_to_call, options)
			# Get any stdout/stderr printed during the worker execution
			out_str = sys.stdout.getvalue()
			err_str = sys.stderr.getvalue()
			sys.stdout.close()
			sys.stderr.close()
			# Revert stdout/stderr to originals
			sys.stdout = old_stdout
			sys.stderr = old_stderr
			# The interface is responsible for cleanup, so don't bother deleting the archive files
			# TODO: modify this to deal with other options (multiple results, etc)
			pymw_worker_write([_res_array[0], out_str, err_str], options)
		except Exception as e:
			sys.stdout = old_stdout
			sys.stderr = old_stderr
			traceback.print_exc()
			exit(e)
		
	def pymw_worker_func(func_name_to_call, options):
		# Get the input data
		input_data = pymw_worker_read(options)
		if not input_data: input_data = ()
		# Execute the worker function
		result = func_name_to_call(*input_data)
		# Output the result
		pymw_emit_result(result)

class PyMW_MapReduce:
	def __init__(self, master):
	#def __init__(self, master, exec_map, exec_reduce, num_worker=1, input_data=None, modules=(), dep_funcs=()):
		self._master=master
		self._task_dir_name = "tasks"
		
	def _data_split(self, data, num):
		q1=len(data)//num
		q2=len(data)%num
		res=[]
		p=0
		for i in range(num):
			j=0
			if q2>0:
				j=1
				q2-=1
			res.append(data[p:p+q1+j])
			p=p+q1+j
		return res

		
	def submit_task_mapreduce(self, exec_map, exec_reduce, num_worker=1, input_data=None, modules=(), dep_funcs=(), red_worker=-1, file_input=False):
		task_name = str(exec_map.__name__)+"_"+str(exec_reduce.__name__)+"_MR"
		exec_file_name = self._task_dir_name+"/"+task_name
		
		try:
			store_func = self._master._interface.pymw_master_write
			get_result_func = self._master._interface.pymw_master_read
		except AttributeError:
			store_func = self._master.pymw_master_write
			get_result_func = self._master.pymw_master_read
		
		new_maintask = PyMW_Task(task_name=task_name, executable=task_name,
										   store_data_func=store_func, get_result_func=get_result_func,
										   finished_queue=self._master._finished_tasks, input_data=None,
										   file_loc=self._task_dir_name)
		
		self._master._submitted_tasks.append(new_maintask)
		#start mapreduce_thread 
		thread1 = threading.Thread(target=self.mapreduce_thread, args=(new_maintask, exec_map, exec_reduce, num_worker, input_data, modules, dep_funcs,red_worker,file_input))
		thread1.start()
		
		return new_maintask
		
	def mapreduce_thread(self, new_maintask, exec_map, exec_reduce, num_worker, input_data, modules=(), dep_funcs=(), red_worker=-1, file_input=False):

		split_data=self._data_split(input_data,num_worker)
		
		if file_input:
			size=0
			for i in input_data: size+=os.path.getsize(i[0])
			size_list=[]
			for i in self._data_split(list(range(size)),num_worker): size_list.append(i[-1]+1-i[0])
			size_num=0
			rest=size_list[size_num]
			split_data, data_block = [],[]
			for i in input_data: # for each files
				pos=0
				file_size=os.path.getsize(i[0])
				while pos<file_size:
					if file_size-pos < rest:
						data_block.append([i[0],pos,file_size])
						rest-=file_size-pos
						pos=file_size
					else:
						data_block.append([i[0],pos,pos+rest])
						pos+=rest
						split_data.append(data_block)
						data_block=[]
						size_num+=1
						if size_num!=num_worker : rest=size_list[size_num]
		
		maptasks = []			
		for i in range(num_worker):
			maptasks.append(self._master.submit_task(exec_map , input_data=(split_data[i],), modules=modules, dep_funcs=dep_funcs, input_from_file=file_input))
		
		reducetasks = []
		res_list=[]
		for i in range(len(maptasks)):
			res_task,result = self._master.get_result(maptasks)
			maptasks.remove(res_task)
			if red_worker==-1: # map_num == reduce_num
				reducetasks.append(self._master.submit_task(exec_reduce, input_data=(result,), modules=modules, dep_funcs=dep_funcs, input_from_file=file_input))
			else:
				res_list+=result
		
		if red_worker!=-1: # map_num > reduce_num
			res_split=self._data_split(res_list, red_worker)
			for i in range(red_worker):
				reducetasks.append(self._master.submit_task(exec_reduce, input_data=(res_split[i],), modules=modules, dep_funcs=dep_funcs, input_from_file=file_input))

		result_list = []
		for i in range(len(reducetasks)):
			res_task,result = self._master.get_result(reducetasks)
			reducetasks.remove(res_task)
			result_list.append(result)
		
		new_maintask.task_finished(result=result_list)
