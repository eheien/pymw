#!/usr/bin/env python
"""Provide a BOINC interface for master worker computing with PyMW.
"""

__author__ = "Adam Kornafeld <kadam@sztaki.hu>"
__date__ = "10 April 2008"

import pymw
import threading
import shutil
import os
import re
import time
import logging

INPUT_TEMPLATE = """\
<file_info>
    <number>0</number>
</file_info>
<file_info>
    <number>1</number>
</file_info>
<workunit>
    <file_ref>
        <file_number>0<file_number>
	<open_name><PYMW_EXECUTABLE/></open_name>
        <copy_file/>
    </file_ref>
    <file_ref>
	<file_number>1<file_number>
        <open_name><PYMW_INPUT/></open_name>
	<copy_file/>
    </file_ref>
    <command_line><PYMW_CMDLINE/></command_line>
    <min_quorum>1</min_quorum>
    <target_nresults>1</target_nresults>
</workunit>
"""

OUTPUT_TEMPLATE = """\
<file_info>
    <name><OUTFILE_0/></name>
    <generated_locally/>
    <upload_when_present/>
    <max_nbytes>32768</max_nbytes>
    <url><UPLOAD_URL/></url>
</file_info>
<result>
    <file_ref>
        <file_name><OUTFILE_0/></file_name>
	<open_name><PYMW_OUTPUT/></open_name>
	<copy_file/>
    </file_ref>
</result>
"""

class _ResultHandler(threading.Thread):
    def __init__(self, task, cwd, sleeptime = 10):
        threading.Thread.__init__(self)
        self._task = task
        self._sleeptime = sleeptime
	self._cwd = cwd

    def run(self):
        while 1:
            if os.path.isfile(os.path.join(self._cwd, self._task._output_arg)):
                self._task.task_finished()
        	break
	    logging.debug("Waiting for result, sleeping for " + str(self._sleeptime) + " seconds...")
	    time.sleep(self._sleeptime)

class BOINCInterface:
    def __init__(self, project_home):
        self._project_home = project_home
        self._project_download = project_home + "/download/"
        self._project_templates = project_home + "/templates/"
        self._boinc_in_template = INPUT_TEMPLATE
        self._boinc_out_template = OUTPUT_TEMPLATE
        self._cwd = os.getcwd()
    
    def reserve_worker(self):
        return None
    
    def execute_task(self, task, worker):
	# Check if project_home dir is known
	if self._project_home == '':
	    logging.critical("Missing BOINC project home directory")
	    task_error = Exception("Missing BOINC project home directory (-p switch)")
	    task.task_finished(task_error)
	    return None
	
        in_file = task._input_arg.rpartition('/')[2]
        out_file = task._output_arg.rpartition('/')[2]
	task_exe = task._executable.rpartition('/')[2]

	# Block concurent threads until changing directories
	logging.debug("Locking thread")
	lock = threading.Lock()
	lock.acquire()

        # Get input destination
        os.chdir(self._project_home)
        cmd = self._project_home + "/bin/dir_hier_path " + in_file
        in_dest = os.popen(cmd, "r").read().strip()
	in_dest_dir = in_dest.rpartition('/')[0]
        cmd = self._project_home + "/bin/dir_hier_path " + task_exe
        exe_dest = os.popen(cmd, "r").read().strip()
	exe_dest_dir = exe_dest.rpartition('/')[0]
	os.chdir(self._cwd)
    
        # Copy input files to download dir
        if not os.path.isfile(exe_dest):
	    while(1):
		logging.debug("Waiting for task exe to become ready")
		if os.path.isfile(task._executable):
		    break
            shutil.copyfile(task._executable, exe_dest)
	while(1):
	    logging.debug("Waiting for input to become ready...")
	    if os.path.isfile(task._input_arg):
		break
	shutil.copyfile(task._input_arg, in_dest)
	    
        # Create input XML template
        in_template = "pymw_in_" + str(task._task_name) + ".xml"
        dest = self._project_templates + in_template
	boinc_in_template = self._boinc_in_template
        boinc_in_template = boinc_in_template.replace("<PYMW_EXECUTABLE/>", task_exe)
        boinc_in_template = boinc_in_template.replace("<PYMW_INPUT/>", in_file)
        boinc_in_template = boinc_in_template.replace("<PYMW_CMDLINE/>", task_exe + " " + in_file + " " + out_file)
        open(dest, "w").writelines(boinc_in_template)
        
        # Create output XML template
        out_template = "pymw_out_" + str(task._task_name) + ".xml"
        dest = self._project_templates + out_template
        boinc_out_template = self._boinc_out_template
        boinc_out_template = boinc_out_template.replace("<PYMW_OUTPUT/>", out_file)
        open(dest, "w").writelines(boinc_out_template)
	
        # Call create_work
        cmd =  "create_work -appname pymw -wu_name pymw_" +  str(task._task_name)
        cmd += " -wu_template templates/" +  in_template
        cmd += " -result_template templates/" + out_template 
        cmd +=  " " + task_exe + " "  + in_file
	
        os.chdir(self._project_home)
	os.system(cmd)
        os.chdir(self._cwd)
	logging.debug("CWD returned to " + self._cwd)
	
	# Release lock	
	lock.release()
	logging.debug("Releasing thread lock")
        
        # Wait for the results
        tasks = []
        result = _ResultHandler(task, self._cwd, 10)
        tasks.append(result)
        result.start()        
        for result in tasks:
            result.join()
	
    def _cleanup(self):
	    return None
