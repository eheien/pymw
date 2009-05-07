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
import logging
import inspect
import textwrap
import interfaces.generic
import interfaces.multicore
import interfaces.mpi
import interfaces.condor
import interfaces.boinc

class PyMW_List:
    def __init__(self):
        self._lock = threading.Lock()
        self._add_event = threading.Condition(self._lock)
        self._data = []
    
    def append(self, item):
        """Atomically appends an item to the list and notifies listeners."""
        self._add_event.acquire()
        self._data.append(item)
        self._add_event.notifyAll()
        self._add_event.release()

    def pop(self, blocking=False):
        """Waits for any item to appear in the list, and pops it off."""
        return self.pop_specific([], blocking)

    def pop_specific(self, item_list=[], blocking=False):
        """Waits for any item from item_list to appear, and pops it off.
        An empty list indicates any item is acceptable."""
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
            # If we didn't find anything and we should not block,
            # return None
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
    def __init__(self, task_name, executable, finished_queue, store_data_func, get_result_func,
                 input_data=None, input_arg=None, output_arg=None, file_loc="tasks", file_input=False):
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
        self._file_input = file_input

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
    
    def task_finished(self, task_err=None, result=None):
        """This must be called by the interface class when the
        task finishes execution.  The result of execution should
        be in the file indicated by output_arg."""

        self._error = task_err
        if task_err:
            logging.info("Task "+str(self)+" had an error")
        elif result==None:
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
                    self._output_data.append(cPickle.loads(f.read()))
            except:
                self._output_data = result

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
            # otherwise we might pass the process resource limitations
            while threading.activeCount() > 100: time.sleep(0.1)
            logging.info("Executing task "+str(next_task))
            task_thread = threading.Thread(target=self._task_executor,
                                           args=(self._interface.execute_task, next_task, worker))
            task_thread.start()
            next_task = self._task_queue.pop(blocking=False)
        
        logging.info("PyMW_Scheduler finished")
        self._running = False
    
    # Use this wrapper function to catch any interface exceptions,
    # otherwise we get hanging threads
    def _task_executor(self, execute_task_func, next_task, worker):
        try:
            next_task._times["execute_time"] = time.time()
            execute_task_func(next_task, worker)
        except Exception, e:
            next_task.task_finished(e)
    
    def _exit(self):
        self._task_queue.append(None)

class PyMW_Master:
    """Provides functions for users to submit tasks to the underlying interface."""
    def __init__(self, interface=None, loglevel=logging.CRITICAL, delete_files=True, file_input=False):
        logging.basicConfig(level=loglevel, format="%(asctime)s %(levelname)s %(message)s")

        if interface:
            self._interface = interface
        else:
            self._interface = interfaces.generic.GenericInterface()
        
        self._submitted_tasks = []
        self._queued_tasks = PyMW_List()
        self._finished_tasks = PyMW_List()
        
        self._delete_files = delete_files
        self._task_dir_name = "tasks"
        self._cur_task_num = 0
        self._function_source = {}
        self._file_input = file_input
        self.pymw_interface_modules = "cPickle", "sys", "cStringIO"

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
        
        # If the interface doesn't provide methods for communicating with the workers, use default functions
        all_funcs = (main_func,)+dep_funcs
        try:
            all_funcs += (self._interface.pymw_worker_read, self._interface.pymw_worker_write)
        except AttributeError:
            all_funcs += (self.pymw_worker_read, self.pymw_worker_write) 

        try:
            interface_modules = self._interface.pymw_interface_modules
        except AttributeError:
            interface_modules = self.pymw_interface_modules
        
        # Select the function to coordinate task execution on the worker
        try:
            all_funcs += (self._interface.pymw_worker_func,)
        except AttributeError:
            all_funcs += (self.pymw_worker_func,)
        
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
        for module_name in modules+interface_modules:
            func_file.write("import "+module_name+"\n")
        func_file.writelines(func_data[1])
        if self._file_input==True:
            func_file.write("pymw_worker_func("+func_data[0]+", True"+")\n")
        else:
            func_file.write("pymw_worker_func("+func_data[0]+")\n")
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
            store_func = self._interface.pymw_master_write
            get_result_func = self._interface.pymw_master_read
        except AttributeError:
            store_func = self.pymw_master_write
            get_result_func = self.pymw_master_read
        
        new_task = PyMW_Task(task_name=task_name, executable=exec_file_name,
                             store_data_func=store_func, get_result_func=get_result_func,
                             finished_queue=self._finished_tasks, input_data=input_data,
                             file_loc=self._task_dir_name, file_input=self._file_input)
        
        self._submitted_tasks.append(new_task)
        self._queued_tasks.append(item=new_task)
        self._scheduler._start_scheduler()
        
        return new_task

    def get_result(self, task=None, blocking=True):
        """Gets the result of the executed task.
        If task is None, return the result of the next finished task.
        If task is a list of tasks, return the result of any task in the list.
        If blocking is false and the task is not finished, returns None."""
        
        if len(self._submitted_tasks) <= 0:
            raise TaskException("No tasks have been submitted")
        
        if not task:
            task_list = []
        elif type(task)==list:
            task_list = task
        else:
            task_list = [task]
        
        # Check that the task(s) are of type PyMW_Task
        for t in task_list:
            if not isinstance(t, PyMW_Task):
                raise TaskException("get_result accepts either a task, a list of tasks, or None")
        
        # Check that the task(s) have been submitted before
        submit_intersect = set(self._submitted_tasks) & set(task_list)
        if len(submit_intersect) != len(task_list):
            raise TaskException("Task has not been submitted")
        
        my_task = self._finished_tasks.pop_specific(task_list, blocking)
        
        if not my_task:
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
            task.cleanup(self._delete_files)
        
        for exec_file in self._function_source:
            if self._delete_files:
                try:
                    os.remove(self._function_source[exec_file][2])
                except OSError:
                    pass
            pass
        
        try:
            if self._delete_files:
                os.rmdir(self._task_dir_name)
            pass
        except OSError:
            pass

    def pymw_master_read(self, loc):
        infile = open(loc, 'r')
        obj = cPickle.Unpickler(infile).load()
        infile.close()
        return obj
    
    def pymw_master_write(self, output, loc):
        outfile = open(loc, 'w')
        cPickle.Pickler(outfile).dump(output)
        outfile.close()
    
    def pymw_worker_read(loc):
        infile = open(loc, 'r')
        obj = cPickle.Unpickler(infile).load()
        infile.close()
        return obj

    def pymw_worker_write(output, loc):
        outfile = open(loc, 'w')
        cPickle.Pickler(outfile).dump(output)
        outfile.close()

    def pymw_worker_func(func_name_to_call):
        try:
            # Redirect stdout and stderr
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = cStringIO.StringIO()
            sys.stderr = cStringIO.StringIO()
            # Get the input data
            input_data = pymw_worker_read(sys.argv[1])
            if not input_data: input_data = ()
            # Execute the worker function
            result = func_name_to_call(*input_data)
            # Get any stdout/stderr printed during the worker execution
            out_str = sys.stdout.getvalue()
            err_str = sys.stderr.getvalue()
            sys.stdout.close()
            sys.stderr.close()
            # Revert stdout/stderr to originals
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            pymw_worker_write([result, out_str, err_str], sys.argv[2])
        except Exception, e:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            exit(e)

class PyMW_MapReduce:
    def __init__(self, master):
    #def __init__(self, master, exec_map, exec_reduce, num_worker=1, input_data=None, modules=(), dep_funcs=()):
        self._master=master
        self._task_dir_name = "tasks"
        
    def submit_task_mapreduce(self, exec_map, exec_reduce, num_worker=1, input_data=None, modules=(), dep_funcs=()):
        task_name = str(exec_map.func_name)+"_"+str(exec_reduce.func_name)+"_MR"
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
        thread1 = threading.Thread(target=self.mapreduce_thread, args=(new_maintask, exec_map, exec_reduce, num_worker, input_data, modules, dep_funcs))
        thread1.start()
        
        return new_maintask
        
    def mapreduce_thread(self, new_maintask, exec_map, exec_reduce, num_worker, input_data, modules=(), dep_funcs=()):
        
        per = len(input_data) / num_worker
        
        split_data = []
        mod=len(input_data) - per * num_worker
        p=0
        for i in range(num_worker):
            j=0
            if mod>0:
                j=1
                mod-=1
            split_data.append(input_data[p:p+per+j])
            p=p+per+j
        
        maptasks = []            
        for i in range(num_worker):
            maptasks.append(self._master.submit_task(exec_map , input_data=(split_data[i],), modules=modules, dep_funcs=dep_funcs))
        
        reducetasks = []
        for i in xrange(len(maptasks)):
            res_task,result = self._master.get_result(maptasks)
            maptasks.remove(res_task)
            reducetasks.append(self._master.submit_task(exec_reduce, input_data=(result,), modules=modules, dep_funcs=dep_funcs))

        result_list = []
        for i in xrange(len(reducetasks)):
            res_task,result = self._master.get_result(reducetasks)
            reducetasks.remove(res_task)
            result_list.append(result)
        
        new_maintask.task_finished(result=result_list)
