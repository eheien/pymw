#!/usr/bin/env python
"""Provide a top level interface for master worker computing.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "10 April 2008"

import threading
import cPickle
import time
import os
import sys
import types
import atexit
import interfaces.multicore
import logging
import inspect
from pymw_app import *

class PyMW_List:
    def __init__(self):
        self._lock = threading.Lock()
        self._sem = threading.Semaphore(0)
        self._data = []
    
    def append(self, item):
        """Atomically appends an item to the list and increments the semaphore."""
        self._lock.acquire()
        self._data.append(item)
        self._sem.release()
        self._lock.release()

    def pop(self, blocking=False):
        """Waits for an item to appear in the list, and pops it off."""
        if not self._sem.acquire(blocking):
            return None
        self._lock.acquire()
        try:
            next_item = self._data.pop()
        except:
            next_item = None
        self._lock.release()
        return next_item

    def contains(self, item):
        """Checks if the list contains the specified item."""
        self._lock.acquire()
        n = self._data.count(item)
        self._lock.release()
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
    def __init__(self, task_name, executable, finished_queue, input_data=None,
                 input_arg=None, output_arg=None, file_loc="tasks"):
        self._finish_event = threading.Event()
        
        # Make sure executable is valid
        if not isinstance(executable, types.StringType) and not isinstance(executable, types.FunctionType):
            raise TypeError("executable must be a filename or Python function")
        
        self._finished_queue = finished_queue
        self._executable = executable
        self._input_data = input_data
        self._output_data = None
        self._task_name = task_name

        # Set the input and output file locations
        if input_arg:
            self._input_arg = input_arg
        else:
            self._input_arg = file_loc + "/in_" + self._task_name + ".dat"
        
        if output_arg:
            self._output_arg = output_arg
        else:
            self._output_arg = file_loc + "/out_" + self._task_name + ".dat"

        # Pickle the input data
        logging.info("Pickling task "+str(self)+" into file "+self._input_arg)
        input_data_file = open(self._input_arg, 'w')
        cPickle.Pickler(input_data_file).dump(input_data)
        input_data_file.close()

        # Task time bookkeeping
        self._times = {"submit_time": time.time(), "execute_time": 0, "finish_time": 0}

    def __str__(self):
        return self._task_name
    
    def _state_data(self):
        return {"task_name": self._task_name, "executable": self._executable,
                "input_arg": self._input_arg, "output_arg": self._output_arg,
                "times": self._times, "finished": self._finish_event.isSet()}
    
    def task_finished(self, task_err=None):
        """This must be called by the interface class when the
        task finishes execution.  The result of execution should
        be in the file indicated by output_arg."""

        self._error = task_err
        
        try:
            output_data_file = open(self._output_arg, 'r')
            self._output_data = cPickle.Unpickler(output_data_file).load()
            output_data_file.close()
        except OSError:
            pass
        except IOError:
            pass

        logging.info("Task "+str(self)+" finished")
        self._times["finish_time"] = time.time()
        self._finish_event.set()
        self._finished_queue.append(self)

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
        if self._times["finish_time"] != 0:
            return self._times["finish_time"] - self._times["submit_time"]
        else:
            return None

    def get_execution_time(self):
        """Get the time from start of task execution to completion.
        This may be different from the CPU time.
        Returns None if task has not finished execution."""
        if self._times["finish_time"] != 0:
            return self._times["finish_time"] - self._times["execute_time"]
        else:
            return None

    def cleanup(self):
        try:
            os.remove(self._input_arg)
            os.remove(self._output_arg)
        except OSError:
            pass
        
class PyMW_Scheduler:
    """Takes tasks submitted by user and sends them to the master-worker interface.
    This is done in a separate thread to allow for asynchronous program execution."""
    def __init__(self, task_queue, interface):
        self._task_queue = task_queue
        self._interface = interface
        self._running = False
    
    def _start_scheduler(self):
        if not self._running:
            logging.info("PyMW_Scheduler started")
            self._running = True
            _scheduler_thread = threading.Thread(target=self._scheduler)
            _scheduler_thread.start()
    
    def _scheduler(self):
        """Waits for submissions to the task list, then submits them to the interface."""
        next_task = self._task_queue.pop(blocking=False)
        while next_task:
            try:
                worker = self._interface.reserve_worker()
            except AttributeError:
                worker = None
            next_task._times["execute_time"] = time.time()
            logging.info("Executing task "+str(next_task))
            task_thread = threading.Thread(target=self._interface.execute_task,
                                           args=(next_task, worker))
            task_thread.start()
            next_task = self._task_queue.pop(blocking=False)
        logging.info("PyMW_Scheduler finished")
        self._running = False
    
    def _exit(self):
        self._task_queue.append(None)

class PyMW_Master:
    """Provides functions for users to submit tasks to the underlying interface."""
    def __init__(self, interface=None, loglevel=logging.CRITICAL):
        logging.basicConfig(level=loglevel, format="%(asctime)s %(levelname)s %(message)s")

        if interface:
            self._interface = interface
        else:
            self._interface = interfaces.multicore.MulticoreInterface()
        
        self._submitted_tasks = []
        self._queued_tasks = PyMW_List()
        self._finished_tasks = PyMW_List()
        
        self._task_dir_name = "tasks"
        self._cur_task_num = 0
        self._function_source = {}

        # Make the directory for input/output files, if it doesn't already exist
        try:
            os.mkdir(self._task_dir_name)
        except OSError, e:
            #if e.errno <> errno.EEXIST: raise
            pass

        self._scheduler = PyMW_Scheduler(self._queued_tasks, self._interface)
        atexit.register(self._cleanup)
    
    def _setup_exec_file(self, file_name, main_func, modules, dep_funcs):
        """Sets up a script file for executing a function.  This file
        contains the function source, dependent functions, dependent
        modules and PyMW calls to get the input data and return the
        output data."""
        
        all_funcs = (main_func,)+dep_funcs+(pymw_get_input, pymw_return_output)
        func_hash = hash(all_funcs)
        if not self._function_source.has_key(func_hash):
            func_sources = [inspect.getsource(func) for func in all_funcs]
            self._function_source[func_hash] = [main_func.func_name, func_sources, file_name]
        else:
            return
        
        func_data = self._function_source[func_hash]
        func_file = open(file_name, "w")
        for mod in modules+("cPickle", "sys"):
            func_file.write("import "+mod+"\n")
        func_file.writelines(func_data[1])
        func_file.write("\npymw_return_output("+func_data[0]+"(*pymw_get_input()))\n")
        func_file.close()
        
    def submit_task(self, executable, input_data=None, modules=(), dep_funcs=()):
        """Creates and submits a task to the internal list for execution.
        Returns the created task for later use.
        executable can be either a filename (Python script) or a function."""
        
        if callable(executable):
            task_name = str(executable.func_name)+"_"+str(self._cur_task_num)
            exec_file_name = self._task_dir_name+"/"+str(executable.func_name)
            self._setup_exec_file(exec_file_name, executable, modules, dep_funcs)
        elif isinstance(executable, str):
            task_name = str(executable)+"_"+str(self._cur_task_num)
            exec_file_name = executable
        else:
            raise TaskException("executable must be a filename or function")
        
        self._cur_task_num += 1
        
        new_task = PyMW_Task(task_name=task_name, executable=exec_file_name,
                             finished_queue=self._finished_tasks, input_data=input_data,
                             file_loc=self._task_dir_name)
        
        self._submitted_tasks.append(new_task)
        self._queued_tasks.append(item=new_task)
        self._scheduler._start_scheduler()
        
        return new_task
    
    def get_result(self, task=None, blocking=True):
        """Gets the result of the executed task.
        If task is None, return the result of the next finished task.
        If blocking is false and the task is not finished, returns None."""
        if task and not self._submitted_tasks.count(task):
            raise TaskException("Task has not been submitted")
        
        if len(self._submitted_tasks) <= 0:
            raise TaskException("No tasks have been submitted")
        
        if task:
            my_task = task
        else:
            my_task = self._finished_tasks.pop(blocking)
        
        if not my_task:
            return None, None
        
        if not my_task.is_task_finished(blocking):
            return None, None

        if my_task._error:
            raise my_task._error
        
        return my_task, my_task._output_data
    
    def get_status(self):
        status = self._interface.get_status()
        status["tasks"] = self._submitted_tasks
        return status

    def _cleanup(self):
        self._scheduler._exit()
        
        try:
            self._interface._cleanup()
        except AttributeError:
            pass
        
        for task in self._submitted_tasks:
            task.cleanup()
        
        for exec_file in self._function_source:
            os.remove(self._function_source[exec_file][2])
        
        try:
            os.rmdir(self._task_dir_name)
        except OSError:
            pass

