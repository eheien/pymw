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
import errno
import interfaces.multicore
import interfaces.mpi
import interfaces.boinc
import interfaces.condor
import logging
import inspect
import textwrap
import cStringIO

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
    def __init__(self, task_name, executable, finished_queue, store_data_func, get_result_func,
                 input_data=None, input_arg=None, output_arg=None, file_loc="tasks"):
        self._finish_event = threading.Event()
        
        # Make sure executable is valid
        if not isinstance(executable, types.StringType) and not isinstance(executable, types.FunctionType):
            raise TypeError("executable must be a filename or Python function")
        
        self._finished_queue = finished_queue
        self._executable = executable
        self._input_data = input_data
        self._output_data = None
        self._task_name = task_name
        self._get_result_func = get_result_func
        self._store_data_func = store_data_func

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
        if task_err:
            logging.info("Task "+str(self)+" had an error")
        else:
            try:
                self._output_data = self._get_result_func(self._output_arg)
            except:
                self._output_data = None
                self._error = Exception("Error reading task result "+self._output_arg)
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
            pass
        except OSError:
            pass

class PyMW_Task_MapReduce(PyMW_Task):
    """Represents a task to be executed."""
    def __init__(self, task_name, executable, finished_queue, store_data_func, get_result_func,
                 input_data=None, input_arg=None, output_arg=None, file_loc="tasks"):
        self._finish_event = threading.Event()
        
        # Make sure executable is valid
        #if not isinstance(executable, types.StringType) and not isinstance(executable, types.FunctionType):
        #    raise TypeError("executable must be a filename or Python function")
        
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

        # Task time bookkeeping
        self._times = {"submit_time": time.time(), "execute_time": 0, "finish_time": 0}

        
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
            # Wait until other tasks have been submitted and the thread count decreases,
            # otherwise we might exhaust the file descriptors
            while threading.activeCount() > 100: time.sleep(0.1)
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
    def __init__(self, interface=None, loglevel=logging.CRITICAL, delete_files=True):
        logging.basicConfig(level=loglevel, format="%(asctime)s %(levelname)s %(message)s")

        if interface:
            self._interface = interface
        else:
            self._interface = interfaces.multicore.MulticoreInterface()
        
        self._submitted_tasks = []
        self._queued_tasks = PyMW_List()
        self._finished_tasks = PyMW_List()
        
        self._delete_files = delete_files
        self._task_dir_name = "tasks"
        self._cur_task_num = 0
        self._function_source = {}

        # Make the directory for input/output files, if it doesn't already exist
        try:
            os.mkdir(self._task_dir_name)
        except OSError, e:
            if e.errno <> errno.EEXIST: raise

        self._scheduler = PyMW_Scheduler(self._queued_tasks, self._interface)
        atexit.register(self._cleanup)
    
    def _setup_exec_file(self, file_name, main_func, modules, dep_funcs):
        """Sets up a script file for executing a function.  This file
        contains the function source, dependent functions, dependent
        modules and PyMW calls to get the input data and return the
        output data."""
        
        # If the interface doesn't provide methods for communicating with the workers, use default
        try:
            all_funcs = (main_func,)+dep_funcs+(self._interface.pymw_read_location, self._interface.pymw_write_location)
        except AttributeError:
            all_funcs = (main_func,)+dep_funcs+(self.pymw_read_location, self.pymw_write_location) 
        
        # Get the source code for the necessary functions       
        func_hash = hash(all_funcs)
        if not self._function_source.has_key(func_hash):
            func_sources = [textwrap.dedent(inspect.getsource(func)) for func in all_funcs]
            self._function_source[func_hash] = [main_func.func_name, func_sources, file_name]
        else:
            return
        
        func_data = self._function_source[func_hash]
        func_file = open(file_name, "w")
        # TODO: make these interface-dependent
        for mod in modules+("cPickle", "sys", "cStringIO"):
            func_file.write("import "+mod+"\n")
        func_file.writelines(func_data[1])
        if func_data[0]=="<lambda>":
            func_data[0]="finish"
        func_file.write("try:\n")
        func_file.write("\tinput_data = pymw_read_location(None, sys.argv[1])\n")
        func_file.write("\tif not input_data: input_data = ()\n")
        func_file.write("\tpymw_write_location(None, "+func_data[0]+
                        "(*input_data), sys.argv[2])\n")
        func_file.write("except Exception, e:\n")
        func_file.write("\texit(e)\n")
        func_file.close()
        
    def submit_task(self, executable, input_data=None, modules=(), dep_funcs=()):
        """Creates and submits a task to the internal list for execution.
        Returns the created task for later use.
        executable can be either a filename (Python script) or a function."""
        
        # Check if the executable is a Python function or a script
        if callable(executable):
            task_name = str(executable.func_name)+"_"+str(self._cur_task_num)
            exec_file_name = self._task_dir_name+"/"+str(executable.func_name)
            self._setup_exec_file(exec_file_name, executable, modules, dep_funcs)
        elif isinstance(executable, str):
            # TODO: test here for existence of script
            task_name = str(executable)+"_"+str(self._cur_task_num)
            exec_file_name = executable
        else:
            raise TaskException("Executable must be a filename or function")
        
        self._cur_task_num += 1
        
        try:
            store_func = self._interface.pymw_write_location
            get_result_func = self._interface.pymw_read_location
        except AttributeError:
            store_func = self.pymw_write_location
            get_result_func = self.pymw_read_location
        
        new_task = PyMW_Task(task_name=task_name, executable=exec_file_name,
                             store_data_func=store_func, get_result_func=get_result_func,
                             finished_queue=self._finished_tasks, input_data=input_data,
                             file_loc=self._task_dir_name)
        
        self._submitted_tasks.append(new_task)
        self._queued_tasks.append(item=new_task)
        self._scheduler._start_scheduler()
        
        return new_task
    
    def submit_task_mapreduce(self, exec_map, exec_reduce, num_worker=1, input_data=None, modules=(), dep_funcs=()):
        
        task_name = str(exec_map.func_name)+"_"+str(exec_reduce.func_name)+"_MR"
        exec_file_name = self._task_dir_name+"/"+task_name
        
        new_maintask = PyMW_Task_MapReduce(task_name=task_name, executable=None,
                                           finished_queue=self._finished_tasks, input_data=input_data,
                                           file_loc=self._task_dir_name)
        
        self._submitted_tasks.append(new_maintask)
        #start mapreduce_thread 
        thread1 = threading.Thread(target=self.mapreduce_thread, args=(new_maintask, exec_map, exec_reduce, num_worker, input_data, modules, dep_funcs))
        thread1.start()
        
        return new_maintask
    
    def submit_task_mapreduce2(self, maintask, executable, input_data=None, modules=(), dep_funcs=()):
        
        task_name = str(maintask)
        exec_file_name = self._task_dir_name+"/"+"finish"
        self._setup_exec_file(exec_file_name, executable, modules, dep_funcs)
        
        new_task = PyMW_Task(task_name=task_name, executable=exec_file_name,
                             finished_queue=self._finished_tasks, input_data=input_data,
                             file_loc=self._task_dir_name, interface=self._interface)
        
        self._submitted_tasks.append(new_task)
        self._queued_tasks.append(item=new_task)
        self._scheduler._start_scheduler()
        
        return new_task
    
    def mapreduce_thread(self, new_maintask, exec_map, exec_reduce, num_worker, input_data, modules=(), dep_funcs=()):
        
        per = len(input_data) / num_worker
        if len(input_data) % num_worker != 0:
            per += 1
        
        split_data = []
        for i in range(num_worker):
            split_data.append(input_data[i*per:(i+1)*per])
        logging.debug("mapreduce_splitdata:"+str(split_data))
        
        maptasks = []            
        for i in range(num_worker):
            maptasks.append(self.submit_task(exec_map , input_data=(split_data[i],), modules=modules, dep_funcs=dep_funcs))
        
        reducetasks = []
        for task in maptasks:
            res_task,result = self.get_result(task)
            logging.debug("map_return:"+str(res_task)+" "+str(result))
            reducetasks.append(self.submit_task(exec_reduce, input_data=(result,), modules=modules, dep_funcs=dep_funcs))

        last_input = []
        for task in reducetasks:
            res_task,result = self.get_result(task)
            logging.debug("reduce_return:"+str(res_task)+" "+str(result))
            last_input.append(result)
        
        finish = lambda x:x
        
        #lasttask = self.submit_task(finish, input_data=(last_input,), modules=modules, dep_funcs=dep_funcs)
        lasttask = self.submit_task_mapreduce2(new_maintask, finish, input_data=(last_input,), modules=modules, dep_funcs=dep_funcs)
        lasttask.is_task_finished(wait=True)
        
        new_maintask.task_finished()
            
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
        try:
            status = self._interface.get_status()
        except AttributeError:
            status = {}
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
            pass
        
        try:
            os.rmdir(self._task_dir_name)
            pass
        except OSError:
            pass

    def pymw_read_location(selfobj, loc):
        infile = open(loc, 'r')
        obj = cPickle.Unpickler(infile).load()
        infile.close()
        return obj
    
    def pymw_write_location(selfobj, output, loc):
        outfile = open(loc, 'w')
        cPickle.Pickler(outfile).dump(output)
        outfile.close()
