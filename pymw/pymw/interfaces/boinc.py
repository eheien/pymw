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

WU_TEMPLATE = """
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

RESULT_TEMPLATE = """
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
    def __init__(self, task, sleeptime = 10):
        threading.Thread.__init__(self)
        self._task = task
        self._sleeptime = sleeptime

    def run(self):
        while 1:
            time.sleep(self._sleeptime)
            if os.path.exists(self._task._output_arg):
                self._task.task_finished()
            break

class BOINCInterface:
    def __init__(self, project_home):
        self._project_home = project_home
        self._project_download = project_home + "/download/"
        self._project_templates = project_home + "/templates/"
        self._boinc_wu_template = RESULT_TEMPLATE.split('\n')
        self._boinc_result_template = WU_TEMPLATE.split('\n')
        self._cwd = os.getcwd()
    
    def reserve_worker(self):
        return None
    
    def execute_task(self, task, worker):
        in_file = task._input_arg.rpartition('/')[2]
        out_file = task._output_arg.rpartition('/')[2]
	
        # Get input destination
        cmd = self._project_home + "/bin/dir_hier_path " + in_file
        in_path = os.popen(cmd, "r").read().strip()
        cmd = self._project_home + "/bin/dir_hier_path " + task._executable
        exe_path = os.popen(cmd, "r").read().strip()
    
        # Copy input files to download dir
        try:
            if not os.path.exists(exe_path):
                shutil.copyfile(task._executable, exe_path)
            shutil.copyfile(task._input_arg, in_path)
            time.sleep(0.5)
        except IOError:
            print "Oops! Permission denied..."
            return
            
        # Create XML template for the wu
        wu_template = "pymw_wu_" + str(task._task_name) + ".xml"
        dest = self._project_templates + wu_template
        boinc_wu_template = list(self._boinc_wu_template)
        for i in range(len(boinc_wu_template)):
            if re.search("<PYMW_EXECUTABLE/>", boinc_wu_template[i]):
                boinc_wu_template[i] = boinc_wu_template[i].replace("<PYMW_EXECUTABLE/>", task._executable)
            if re.search("<PYMW_INPUT/>", boinc_wu_template[i]):
                boinc_wu_template[i] = boinc_wu_template[i].replace("<PYMW_INPUT/>", in_file)
            if re.search("<PYMW_CMDLINE/>", boinc_wu_template[i]):
                boinc_wu_template[i] = boinc_wu_template[i].replace("<PYMW_CMDLINE/>", task._executable + " " + in_file + " " + out_file)
        open(dest, "w").writelines(boinc_wu_template)
        
        # Create XML template for the result
        result_template = "pymw_result_" + str(task._task_name) + ".xml"
        dest = self._project_templates + result_template
        boinc_result_template = list(self._boinc_result_template)
        for i in range(len(boinc_result_template)):
            if re.search("<PYMW_OUTPUT/>", boinc_result_template[i]):
                boinc_result_template[i] = boinc_result_template[i].replace("<PYMW_OUTPUT/>", out_file)
        open(dest, "w").writelines(boinc_result_template)
	
        # Call create_work
        cmd =  "create_work -appname pymw -wu_name pymw_" +  str(task._task_name)
        cmd += " -wu_template templates/" +  wu_template
        cmd += " -result_template templates/" + result_template 
        cmd +=  " " + task._executable + " "  + in_file
        os.chdir(self._project_home)
        os.system(cmd)
        os.chdir(self._cwd)
        
        # Wait for the results
        tasks = []
        result = _ResultHandler(task, 10)
        tasks.append(result)
        result.start()        
        for result in tasks:
            result.join()
