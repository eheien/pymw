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
Executable = <PYTHON_LOC/>
Error = <PYMW_ERROR/>
Log = <PYMW_LOG/>
Arguments = <PYMW_EXEC/> <PYMW_INPUT/> <PYMW_OUTPUT/>
InitialDir = <PYMW_DIR/>
Queue"""

#Executable = C:\python25\python.exe

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
        self._python_loc = "C:/python25/python.exe"
        self._condor_sd_template = SD_TEMPLATE.split('\n')
        self._condor_loc = "C:/condor/bin/"
        #self._num_workers = num_workers
        #self._available_worker_list = Queue.Queue(0)
        #self._worker_list = []
        #for worker_num in range(num_workers):
        #    w = Worker()
        #    self._available_worker_list.put_nowait(item=w)
        #    self._worker_list.append(w)
    
# ERIC: Right now, we don't keep track of workers, so this can return None
    def reserve_worker(self):
        #return self._available_worker_list.get(block=True)
        return None
    
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
                if re.search("<PYTHON_LOC/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYTHON_LOC/>", self._python_loc)
                if re.search("<PYMW_DIR/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_DIR/>", os.getcwd())
                if re.search("<PYMW_EXEC/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_EXEC/>", os.getcwd() +"/"+ task._executable)
                if re.search("<PYMW_INPUT/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_INPUT/>", os.getcwd() +"/"+ task._input_arg)
                if re.search("<PYMW_OUTPUT/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_OUTPUT/>", os.getcwd() +"/"+ task._output_arg)
                if re.search("<PYMW_ERROR/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_ERROR/>", os.getcwd()+"/tasks/pymw.err")
                if re.search("<PYMW_LOG/>", condor_sd_template[i]):
                    condor_sd_template[i] = condor_sd_template[i].replace("<PYMW_LOG/>", os.getcwd()+"/tasks/pymw.log")

            #dest = self._project_templates
# ERIC: Each task should have a unique submit file.  This should
# ERIC: be in the same place as the input and output files
            dest = "tasks/pymw_condor"
            #dest = "C:/condor/pytest.txt"
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
            #cmd = self._condor_loc +" " + dest
# ERIC: os.system works well for running condor_submit, except that it prints
# ERIC: Condor messages.  Try to use subprocess.Popen so you can control whether
# ERIC: the Condor status messages appear
            #os.system(cmd)
            #cout = subprocess.Popen(args=[self._condor_loc, dest], creationflags=cf, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.readlines()
            cout = subprocess.Popen(args=[self._condor_loc+"condor_submit.exe", dest], creationflags=cf, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print cout.poll()
            print subprocess.PIPE
            #cout.wait()
            #print subprocess.PIPE
            #for str in cout:
            #    print str.rstrip()
            #while cout.poll() == None:
            #    pass
            #print cout.poll()
            proc_stdout, proc_stderr = cout.communicate()   # wait for the process to finish

            print os.path.exists("tasks/pymw.err")
            while os.access("tasks/pymw.err", os.F_OK) == False:
                print os.access("tasks/pymw.err", os.F_OK)
                pass
            
            #retcode = worker._exec_process.returncode
# ERIC: Eventually, you should check the error file here for problems, and create an Exception
# ERIC: if Condor couldn't finish the task
            task_error = None
            err = open("tasks/pymw.err","r")
            if err.read() != "" :
                #task_error = Exception("Executable failed with error "+str(retcode), proc_stderr)
                task_error = Exception("Executable failed with error ")
            err.close()
                
        except OSError:
            # TODO: check the actual error code
            task_error = Exception("Could not find python")
        
        #worker._exec_process = None
# ERIC: When the task finishes, delete the error, log and submit files
        os.remove("tasks/pymw.err")
        os.remove("tasks/pymw.log")
        os.remove("tasks/pymw_condor")
        task.task_finished(task_error)    # notify the task
        #self._available_worker_list.put_nowait(item=worker)    # rejoin the list of available workers

#    def _cleanup(self):
#        for worker in self._worker_list:
#            worker._kill()
#
# ERIC: You could return the result of "condor_status.exe" here
    def get_status(self):
        #return {"num_total_workers" : self._num_workers,
        #    "num_active_workers": self._num_workers-len(self._worker_list)}
        return subprocess.Popen(args=[self._condor_loc+"condor_status.exe"], creationflags=cf)

