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
    def __init__(self, python_loc="C:/python25/python.exe", condor_bin_loc="C:/condor/bin/"):
        self._python_loc = python_loc
        self._condor_bin_loc = condor_bin_loc
    
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
            cout = subprocess.Popen(args=[self._condor_bin_loc+"condor_submit.exe", submit_file_name],
                                    creationflags=cf, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proc_stdout, proc_stderr = cout.communicate()   # wait for the process to finish
            #print proc_stdout
            #print proc_stderr
            # TODO: check stdout, stderr for problems

            # Wait until the output file is generated
            # TODO: also check for an error file
            # TODO: put this check in a separate thread
            while os.access(task._output_arg, os.F_OK) == False:
                time.sleep(0.1)
            
            task_error = None
            err_file = open(err_file_name,"r")
            if err_file: err_output = err_file.read()
            else: err_output = ""
            if err_output != "" :
                task_error = Exception("Executable failed with error:\n"+err_output)
            err_file.close()
        
        except OSError:
            # TODO: check the actual error code
            task_error = Exception("Could not find condor_submit")
        
        # Delete log, error and submission files
        #os.remove(err_file_name)
        #os.remove(log_file_name)
        #os.remove(submit_file_name)
        task.task_finished(task_error)    # notify the task

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
