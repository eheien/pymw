import pymw
import shutil
import os
import re

class _Worker:
    def __init__(self):
	self._active_task = None

    def worker_record(self):
	if self._active_task: task_name = str(self._active_task)
	else: task_name = None

class BOINCInterface:
    def __init__(self, project_home):
	self._project_home = project_home
	self._project_download = project_home + "/download/"
	self._project_upload = project_home + "/upload/"
	self._project_templates = project_home + "/templates/"
	
    def reserve_worker(self):
	return None
    
    def execute_task(self, task, worker):
        in_file = task._input_arg.rpartition('/')[2]
	out_file = task._output_arg.rpartition('/')[2]
	# get input destination
        cmd = self._project_home + "/bin/dir_hier_path " + in_file
        in_path = os.popen(cmd, "r").read().strip()
	cmd = self._project_home + "/bin/dir_hier_path " + task._executable
	exe_path = os.popen(cmd, "r").read().strip()
	
	# copy input files to download dir
	try:
	    shutil.copyfile(task._executable, exe_path)
	    shutil.copyfile(task._input_arg, in_path)
	except IOError:
	    print "Oops! Permission denied..."
	    return
	    
	# create XML template for the wu
	wu_template = "pymw_wu_" + str(task._input_data) + ".xml"
	dest = self._project_templates + wu_template
	ln = open("boinc_wu_template.xml").readlines()
	for i in range(len(ln)):
	    if re.search("<PYMW_EXECUTABLE>", ln[i]):
		ln[i] = ln[i].replace("<PYMW_EXECUTABLE>", task._executable)
	    if re.search("<PYMW_INPUT>", ln[i]):
		ln[i] = ln[i].replace("<PYMW_INPUT>", in_file)
	    if re.search("<PYMW_CMDLINE>", ln[i]):
		ln[i] = ln[i].replace("<PYMW_CMDLINE>", in_file + " " + out_file)

	open(dest, "w").writelines(ln)
	# create XML template for the result
	result_template = "pymw_result_" + str(task._input_data) + ".xml"
	dest = self._project_templates + result_template
	ln = open("boinc_result_template.xml").readlines()
	for i in range(len(ln)):
	    if re.search("<PYMW_OUTPUT>", ln[i]):
		ln[i] = ln[i].replace("<PYMW_OUTPUT>", out_file)
	open(dest, "w").writelines(ln)
	
	# call create_work
	cmd =  "create_work -appname pymw -wu_name pymw_" +  str(task._input_data)
	cmd += " -wu_template templates/" +  wu_template
	cmd += " -result_template templates/" + result_template 
	cmd +=  " " + task._executable + " "  + in_file
	cwd = os.getcwd()
	os.chdir(self._project_home)
	os.system(cmd)
	os.chdir(cwd)
