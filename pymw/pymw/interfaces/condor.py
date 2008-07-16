#!/usr/bin/env python
"""Provide a Condor interface for master worker computing with PyMW.
"""

__author__ = "Takata Yusuke <y-takata@ics.es.osaka-u.ac.jp>"
__date__ = "15 July 2008"

import subprocess
import sys
import ctypes
import os
import re
import signal
import threading
import Queue

# ERIC: Python won't always be in the same place.
# ERIC: You should change C:\python25\python.exe to a tag like <PY_LOC/>
# ERIC: and replace it like the other tags
# ERIC: Do the same for Error and Log
SD_TEMPLATE = """Universe = vanilla
Executable = C:\python25\python.exe
Error = C:/condor/bin/pytest.err
Log = C:/condor/bin/pytest.log
Arguments = <PYMW_EXEC/> <PYMW_INPUT/> <PYMW_OUTPUT/>
InitialDir = <PYMW_DIR/>
Queue"""


#class Worker:
#    def __init__(self):
#        self._exec_process = None
#    
#    def _kill(self):
#        if self._exec_process:
#            if sys.platform.startswith("win"):
#                ctypes.windll.kernel32.TerminateProcess(int(self._exec_process._handle), -1)
#            else:
#                os.kill(self._exec_process.pid, signal.SIGKILL)

class CondorInterface:
    """Provides a simple interface for single machine systems.
    This can take advantage of multicore by starting multiple processes."""

# ERIC: project_home is for the BOINC interface, you can remove it.
# ERIC: Instead, have a "python_loc" input, which defaults to C:\python25\python.exe
    def __init__(self, project_home):
        self._project_home = project_home
        #self._num_workers = num_workers
        #self._available_worker_list = Queue.Queue(0)
        #self._worker_list = []
        #self._python_loc = python_loc
        self._project_templates = project_home + "/templates/"
        self._condor_sd_template = SD_TEMPLATE.split('\n')
        #for worker_num in range(num_workers):
        #    w = Worker()
        #    self._available_worker_list.put_nowait(item=w)
        #    self._worker_list.append(w)
    
# ERIC: Right now, we don't keep track of workers, so this can return None
    def reserve_worker(self):
        return self._available_worker_list.get(block=True)
    
    def execute_task(self, task, worker):
        #in_file = task._input_arg.rpartition('/')[2]
        #out_file = task._output_arg.rpartition('/')[2]
        
        try:
            if sys.platform.startswith("win"): cf=0x08000000
            else: cf=0
            #print task._input_arg
            # Create XML template for the wu
            #wu_template = "pymw_wu_" + str(task._task_name) + ".xml"
            #dest = self._project_templates + sd_template
            condor_sd_template = list(self._condor_sd_template)
# ERIC: Add replacement for other tags (Python location, error file, log file)
# ERIC: Each task should have a unique log and error file
            for i in range(len(condor_sd_template)):
                if re.search("<PYMW_DIR/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_DIR/>", os.getcwd())
                if re.search("<PYMW_EXEC/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_EXEC/>", os.getcwd() +"/"+ task._executable)
                    #condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_EXEC/>", task._executable)
                if re.search("<PYMW_INPUT/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_INPUT/>", os.getcwd() +"/"+ task._input_arg)
                    #condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_INPUT/>", task._input_arg)
                    #print "input", task._input_arg
                if re.search("<PYMW_OUTPUT/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_OUTPUT/>", os.getcwd() +"/"+ task._output_arg)
                    #condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_OUTPUT/>", task._output_arg)
                    
                #if re.search("<PYMW_OUTPUT/>", boinc_wu_template[i]):
                #    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_OUTPUT/>", task._executable + " " + in_file + " " + out_file)
            #dest = self._project_templates
# ERIC: Each task should have a unique submit file.  This should
# ERIC: be in the same place as the input and output files
            dest = "C:/condor/pytest.txt"
            #open(dest, "w").writelines(condor_sd_template)
            f=open(dest,"w")
            for ff in condor_sd_template:
                f.write(ff+"\n")
            f.close()
            #worker._exec_process = subprocess.Popen(args=[self._python_loc, task._executable, task._input_arg,
            #        task._output_arg], creationflags=cf, stderr=subprocess.PIPE)
            #worker._exec_process = subprocess.Popen(args=["C:\condor\bin\condor_submit", dest],
            #                                         creationflags=cf, stderr=subprocess.PIPE)
# ERIC: Please add a variable to the interface to let users specify the location of condor
            cmd = "C:/condor/bin/condor_submit "+dest
# ERIC: os.system works well for running condor_submit, except that it prints
# ERIC: Condor messages.  Try to use subprocess.Popen so you can control whether
# ERIC: the Condor status messages appear
            os.system(cmd)
            
            #proc_stdout, proc_stderr = worker._exec_process.communicate()   # wait for the process to finish
            #retcode = worker._exec_process.returncode
# ERIC: Eventually, you should check the error file here for problems, and create an Exception
# ERIC: if Condor couldn't finish the task
            task_error = None
            #if retcode is not 0:
            #    task_error = Exception("Executable failed with error "+str(retcode), proc_stderr)
                
        except OSError:
            # TODO: check the actual error code
            task_error = Exception("Could not find python")
        
        #worker._exec_process = None
# ERIC: When the task finishes, delete the error, log and submit files
        task.task_finished(task_error)    # notify the task
        #self._available_worker_list.put_nowait(item=worker)    # rejoin the list of available workers

#    def _cleanup(self):
#        for worker in self._worker_list:
#            worker._kill()
#
# ERIC: You could return the result of "condor_status.exe" here
#    def get_status(self):
#        return {"num_total_workers" : self._num_workers,
#            "num_active_workers": self._num_workers-len(self._worker_list)}

