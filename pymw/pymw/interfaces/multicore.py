#!/usr/bin/env python
"""Provide a multicore interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "22 February 2009"

import subprocess
import sys
import ctypes
import os
import signal
import errno
import threading
import Queue
import cPickle
import cStringIO

"""On worker restarting:
Multicore systems cannot handle worker restarts - recording PIDs
can result in unspecified behavior (especially if the system is restarted).
The solution is to check for a result on program restart - if it's not there,
delete the directory and start the task anew."""

class Worker:
    def __init__(self):
        self._exec_process = None
    
    def _kill(self):
        try:
            if self._exec_process:
                if sys.platform.startswith("win"):
                    ctypes.windll.kernel32.TerminateProcess(int(self._exec_process._handle), -1)
                else:
                    os.kill(self._exec_process.pid, signal.SIGKILL)
        except:
            pass

class MulticoreInterface:
    """Provides a simple interface for single machine systems.
    This can take advantage of multicore by starting multiple processes."""

    def __init__(self, num_workers=1, python_loc="python"):
        self._num_workers = num_workers
        self._available_worker_list = []
        self._worker_list = []
        self._python_loc = python_loc
        self._input_objs = {}
        self._output_objs = {}
        self._execute_time=[]
        self.pymw_interface_modules = "cPickle", "sys", "cStringIO"
        for worker_num in range(num_workers):
            w = Worker()
            self._available_worker_list.append(w)
            self._worker_list.append(w)
    
    def get_available_workers(self):
        return list(self._available_worker_list)
    
    def reserve_worker(self, worker):
        self._available_worker_list.remove(worker)
        
    def execute_task(self, task, worker):
        if sys.platform.startswith("win"): cf=0x08000000
        else: cf=0
        
        # Pickle the input argument and remove it from the list
        input_obj_str = cPickle.dumps(self._input_objs[task._input_arg])

        try:
            worker._exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg, task._output_arg],
                                                    creationflags=cf, stdin=subprocess.PIPE,
                                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Wait for the process to finish
            proc_stdout, proc_stderr = worker._exec_process.communicate(input_obj_str)
            retcode = worker._exec_process.returncode
            if retcode is 0:
                self._output_objs[task._output_arg] = cPickle.loads(proc_stdout)
                if task._file_input==True:
                    self._output_objs[task._output_arg][0]=[task._output_arg]
            	task_error = None
            else:
                task_error = Exception("Executable failed with error "+str(retcode), proc_stderr)
                
        except OSError, e:
            if e.errno == errno.EEXIST:
            	task_error = Exception("Could not find python")
            else:
        		raise
        
        worker._exec_process = None
        task.task_finished(task_error)    # notify the task
        self._available_worker_list.append(item=worker)    # rejoin the list of available workers

    def _cleanup(self):
        for worker in self._worker_list:
            worker._kill()
    
    def get_status(self):
        return {"num_total_workers" : self._num_workers,
            "num_active_workers": self._num_workers-len(self._worker_list)}
    
    def pymw_master_read(self, loc):
        return self._output_objs[loc]
    
    def pymw_master_write(self, output, loc):
    	self._input_objs[loc] = output
    
    def pymw_worker_read(loc):
        return cPickle.Unpickler(sys.stdin).load()
    
    def pymw_worker_write(output, loc, file_input):
        if file_input==True:
            outfile = open(loc, 'w')
            cPickle.Pickler(outfile).dump(output[0])
            outfile.close()
        print cPickle.dumps(output)

    def pymw_worker_func(func_name_to_call, file_input=False): #_0
        try:
            # Redirect stdout and stderr
            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout, sys.stderr = cStringIO.StringIO(), cStringIO.StringIO()
            # Get the input data
            input_data = pymw_worker_read(0)
            if not input_data: input_data = ()
            # Execute the worker function
            if file_input==True:
                try:
                    f=open(input_data[0][0],"r")
                    filedata=cPickle.loads(f.read())
                except:
                    filedata=[]
                    for i in input_data[0]:
                        num=0
                        for j in xrange(len(i)/3):
                            f=open(i[num+0],"r")
                            f.seek(i[num+1])
                            filedata.append(f.read(i[num+2]-i[num+1]))
                            num+=3
                result = func_name_to_call(filedata)
            else:
                result = func_name_to_call(*input_data)
            # Get any stdout/stderr printed during the worker execution
            out_str, err_str = sys.stdout.getvalue(), sys.stderr.getvalue()
            sys.stdout.close()
            sys.stderr.close()
            # Revert stdout/stderr to originals
            sys.stdout, sys.stderr = old_stdout, old_stderr
            pymw_worker_write([result, out_str, err_str], sys.argv[2], file_input)
        except Exception, e:
            sys.stdout = old_stdout
            sys.stderr = old_stderr
            exit(e)