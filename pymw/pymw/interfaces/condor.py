#!/usr/bin/env python
"""Provide a Condor interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ics.es.osaka-u.ac.jp>"
__date__ = "06 February 2009"

import subprocess
import os
import time
import sys
import cPickle
import threading

# TODO: The second two arguments aren't necessary, but I need them to
# TODO: prevent the worker from crashing.  Fix it.
CONDOR_TEMPLATE = """Universe = vanilla
InitialDir = <INITIAL_DIR/>
Executable = <PYTHON_LOC/>
Error = <PYMW_ERROR/>
Log = <PYMW_LOG/>
Input = <PYMW_INPUT_FILE/>
Output = <PYMW_OUTPUT_FILE/>
Arguments = <PYMW_EXEC_NAME/> <PYMW_INPUT_FILE/> <PYMW_OUTPUT_FILE/>
ShouldTransferFiles = YES
WhenToTransferOutput = ON_EXIT
TransferInputFiles = <PYMW_EXEC_FILE/>
Queue"""

class CondorInterface:
    """Provides a simple interface for desktop grids running Condor."""
    def __init__(self, python_loc="", condor_submit_loc=""):
        if sys.platform.startswith("win"):
            if python_loc != "": self._python_loc = python_loc
            else: self._python_loc = "python.exe"
            if condor_submit_loc != "": self._condor_submit_loc = condor_submit_loc
            else: self._condor_submit_loc = "condor_submit.exe"
        else:
            if python_loc != "": self._python_loc = python_loc
            else: self._python_loc = "/usr/local/bin/python"
            if condor_submit_loc != "": self._condor_submit_loc = condor_submit_loc
            else: self._condor_submit_loc = "condor_submit"
        self._task_list = []
        self._task_list_lock = threading.Lock()
        self._scan_finished_tasks = True
        self._task_finish_thread = threading.Thread(target=self._get_finished_tasks)
        self._task_finish_thread.start()
        
    def _get_finished_tasks(self):
        while self._scan_finished_tasks:
            self._task_list_lock.acquire()
            for task in self._task_list:
                # Check for the output file
                # TODO: also check for an error file
                if os.access(task[0]._output_arg, os.F_OK):
                    # Delete log, error and submission files
                    os.remove(task[1])
                    os.remove(task[2])
                    os.remove(task[3])
                    task[0].task_finished(None)    # notify the task
                    self._task_list.remove(task)
            
#            err_file = open(err_file_name,"r")
#            if err_file:
#                err_output = err_file.read()
#                err_file.close()
#            else: err_output = ""
#            if err_output != "" :
#                task_error = Exception("Executable failed with error:\n"+err_output)
            self._task_list_lock.release()
            time.sleep(0.5)
        print "exiting task scan"
        
    def reserve_worker(self):
        return None
    
    def execute_task(self, task, worker):
        # Create a template for this task
        condor_template = CONDOR_TEMPLATE
        condor_template = condor_template.replace("<PYTHON_LOC/>", self._python_loc)
        condor_template = condor_template.replace("<INITIAL_DIR/>", os.getcwd())
        condor_template = condor_template.replace("<PYMW_EXEC_FILE/>", task._executable)
        condor_template = condor_template.replace("<PYMW_INPUT_FILE/>", task._input_arg)
        condor_template = condor_template.replace("<PYMW_OUTPUT_FILE/>", task._output_arg)
        condor_template = condor_template.replace("<PYMW_EXEC_NAME/>", task._executable.split('/')[1])
        err_file_name = "tasks/"+task._task_name+".err"
        condor_template = condor_template.replace("<PYMW_ERROR/>", err_file_name)
        log_file_name = "tasks/"+task._task_name+".log"
        condor_template = condor_template.replace("<PYMW_LOG/>", log_file_name)
        
        # Write the template to a file
        submit_file_name = "tasks/"+str(task._task_name)+"_condor"
        f = open(submit_file_name,"w")
        f.write(condor_template)
        f.close()
        
        if sys.platform.startswith("win"): cf=0x08000000
        else: cf=0
        
        try:
            # Submit the template file through condor_submit
            submit_process = subprocess.Popen(args=[self._condor_submit_loc, submit_file_name],
                                    creationflags=cf, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Wait for the process to finish
            proc_stdout, proc_stderr = submit_process.communicate()
            
            # TODO: check stdout for problems
            if proc_stderr != "":
                task.task_finished(Exception("condor_submit failed with error:\n"+proc_stderr))
                return
            
            self._task_list_lock.acquire()
            self._task_list.append([task, err_file_name, log_file_name, submit_file_name])
            self._task_list_lock.release()
            
        except OSError:
            # TODO: check the actual error code
            task.task_finished(Exception("Could not find condor_submit"))
    
    def _cleanup(self):
        self._scan_finished_tasks = False
    
    def pymw_read_location(selfobj, loc):
        if not selfobj:
            obj = cPickle.Unpickler(sys.stdin).load()
            return obj
        else:
            infile = open(loc, 'r')
            obj = cPickle.Unpickler(infile).load()
            infile.close()
            return obj
    
    def pymw_write_location(selfobj, output, loc):
        if not selfobj:
            print cPickle.dumps(output)
        else:
            outfile = open(loc, 'w')
            cPickle.Pickler(outfile).dump(output)
            outfile.close()
