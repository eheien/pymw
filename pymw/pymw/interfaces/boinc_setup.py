"""Support functions for the BOINC interface.

This module provides supporting functions for creating
the PyMW application in BOINC, as well as various helper
methods. It requires a path to the BOINC project directory.
"""

__author__ = 'Jeremy Cowles <jeremy.cowles@gmail.com>'
__date__ = '9 June 2009'

import os, logging, sys, stat, shutil


# Platform strings, must adhere to BOINC main program naming spec
LINUX_APP_NAME = "pymw_1.03_i686-pc-linux-gnu"
WIN_APP_NAME  = "pymw_1.03_windows_intelx86.exe"
APPLE_APP_NAME = "pymw_1.03_i686-apple-darwin"
PYMW_APP_NAMES = [LINUX_APP_NAME, WIN_APP_NAME, APPLE_APP_NAME,]

# Worker script (the main BOINC program)
POSIX_WORKER = """\
#!/bin/sh
`python $1 $2 $3`
"""

# Windows worker is compiled because batch files are not
# supported as a BOINC main program
WINDOWS_WORKER = "pymw/interfaces/pymw_run.exe"

FILE_REF = """\
<copy_file/>
"""

# This file's presence in the BOINC project directory
# indicates that the daemons are stopped or stopping 
STOP_TRIGGER = "stop_daemons"

configxml, projectxml = None, None
def boinc_import_hack(proj_path):
	"""Imports the BOINC support code

	This is a hack because the actual path is specified at runtime
	and so cannot be imported when the module loads. This works
	around that issue by creating global vars for the namespaces
	and then populating them once the path is known.
	"""
	global configxml, projectxml
	if configxml != None or projectxml != None:
		return
	
	bin_path = os.path.join(proj_path, "py")
	if not bin_path in sys.path:
		sys.path.append(bin_path)
	
	# BOINC will search for config files using these vars
	os.environ['BOINC_PROJECT_DIR'] = proj_path
	
	from Boinc import configxml, projectxml

def get_winworker_path():
	"""Gets the path to the WINDOWS_WORKER
	
	Returns the expected path or None if there was a
	problem determining the expected path.
	
	This function assumes that pymw is on the current
	python path. Note that the path returned is the 
	*expected* path and may not actually exist.
	"""
	paths = [p for p in sys.path[1:] if 'pymw' in p]
	if len(paths) > 0: 
		path = os.path.join(paths[0], WINDOWS_WORKER)
		return path

	paths = [p for p in sys.path[1:] if 'site-packages' in p]
	if len(paths) > 0:
		for cur_path in paths:
			print (("found site-packages", cur_path))
			path = os.path.join(cur_path, WINDOWS_WORKER)
			print (path)
			if os.path.exists(path):
				return path

	return None
 

def install_pymw(project_path, custom_app_dir=None, task_path="tasks"):
	"""Installs a default app named "pymw" into BOINC and
	sets up the assimilator to look in the current directory
	to pick up workunits. In addition to setting up the 
	application, it will also check to see if the project
	is running, it not, it will attempt to start it.
	"""
	logging.debug("---------------------------------")
	logging.debug("Installing PyMW BOINC application")
	logging.debug("---------------------------------")

	# load BOINC internal Python code
	boinc_import_hack(project_path)

	config = setup_config(os.path.abspath(task_path))
	setup_project()
	
	install_apps(config, custom_app_dir)
	
	check_daemons(project_path)
	
	logging.debug("---------------------")
	logging.debug("PyMW setup successful")
	logging.debug("---------------------")

def check_daemons(project_path):
	"""Checks for STOP_TRIGGER, if present calls bin/start
	If the daemons are currently running, stops and restarts them
	"""
	stopped = os.path.exists(os.path.join(project_path, STOP_TRIGGER))
	
	logging.debug("Checking status of BOINC daemons")
	
	if stopped:
		# try to start the daemons
		logging.debug("Current status: STOPPED")
		logging.debug("Attempting to start BOINC daemons...")
		os.system(os.path.join(project_path, "bin", "start"))
	else:
		logging.debug("Current status: RUNNING")
		#logging.debug("Attempting to stop BOINC daemons...")
		#os.system(os.path.join(project_path, "bin", "stop"))
	logging.debug("BOINC daemons running")

def setup_config(task_path):
	"""Adds appropriate daemons to the BOINC config.xml file.
	"""
	logging.info("Adding PyMW Assimilator daemon to config.xml")
	config = configxml.ConfigFile().read()
	
	# Append new instance of pymw_assimilator to config.xml
	asm = "pymw_assimilator.py -d 3 -app pymw -pymw_dir " + task_path
	add_daemon(config, asm, "pymw_assimilator")

	# add a file deleter, ignoring batches
	asm = "file_deleter -d 3 -dont_delete_batches"
	add_daemon(config, asm, "file_deleter")
	
	# add a default validator
	asm = "sample_trivial_validator -d 3 -app pymw"
	add_daemon(config, asm, "sample_trivial_validator")

	# add a feeder and a transitioner
	asm = "feeder -d 3 -random_order"
	add_daemon(config, asm, "feeder")
	asm = "transitioner -d 3"
	add_daemon(config, asm, "transitioner")
	
	return config

def add_daemon(config, command, remove):
	"""Adds a Daemon to the given configuration
	
	If a daemon is found starting with the "remove" parameter that daemon is
	removed from the configuration before adding the given command.
	"""
	# first try to remove it
	for daemon in [d for d in config.daemons if d.cmd.startswith(remove)]:
		config.daemons.remove_node(daemon)
		logging.debug("Removing existing daemon: %s" % daemon.cmd)

	# now add in the command
	config.daemons.make_node_and_append("daemon").cmd = command
	logging.debug("Appending daemon: %s" % command)
	config.write()
	
def setup_project():
	"""Adds the PyMW application to the project xml file
	"""
	# Append new instance of pymw worker to project.xml
	project = projectxml.ProjectFile().read()
	for element in project.elements:
		if element.name == 'pymw':
			logging.info("PyMW client application is already present in " + \
						 "project.xml")
			return
	
	project.elements.make_node_and_append("app").name = "pymw"
	project.write()
	for element in project.elements:
		if element.name == "pymw":
			element.user_friendly_name = "PyMW - Master Worker " + \
										 "Computing in Python"
			project.write()
	return project

def install_apps(config, custom_app_dir=None):
	"""Installs and registers the worker applications into ths apps directory 
	"""
	# Install worker applications
	app_dir = os.path.join(config.config.app_dir, "pymw")
	if not os.path.isdir(app_dir):
		os.mkdir(app_dir)
	
	if custom_app_dir:
		install_custom_app(app_dir, custom_app_dir, LINUX_APP_NAME, "Linux")
		install_custom_app(app_dir, custom_app_dir, APPLE_APP_NAME, "Apple")
		install_custom_app(app_dir, custom_app_dir, WIN_APP_NAME, "Windows")
	else:
		install_posix(app_dir, LINUX_APP_NAME, POSIX_WORKER, "Linux")
		install_posix(app_dir, APPLE_APP_NAME, POSIX_WORKER, "Apple")
		install_windows(app_dir)
	
	# Call update_versions
	project_home = config.config.app_dir.rpartition('/')[0]
	
	os.system("cd %s; ./bin/xadd" % project_home)
	os.system("cd %s; ./bin/update_versions --force --sign" % project_home)

def file_exists(path, name, data=None):
	"""Checks to see if a file exists, if so, prints a message if
	the Name parameter is not None. 
	
	If the data parameter is not None, the file is opened and the
	data parameter is written to it. Return True if the file exists, 
	False otherwise.
	"""
	if os.path.exists(path):
		if name != None:
			logging.debug("%s already installed, skipping file")
		return True
	if data == None:
		return False
	path_file = open(path, "w")
	try:
		path_file.writelines(data)
	finally:
		path_file.close()
		
	return False

def install_custom_app(app_dir, custom_app_dir, app_name, app_platform):
	"""Installs a custom application as the worker
	
	Note that this function assumes that the custom_app_dir is setup exactly
	as BOINC needs it, with .file_ref_info files already in place and
	executables named according to what PyMW needs to run.
	"""
	logging.info("setting up client application for " + app_platform + \
				 " platform")
	target_dir = os.path.join(app_dir, app_name)

	if not os.path.exists(target_dir):
		os.mkdir(target_dir)
	
	source_dir = os.path.join(custom_app_dir, app_platform.lower())
	found = False

	for src_file in os.listdir(source_dir):
		if src_file == app_name:
			found = True
			break
	   
	if not found:
		raise RuntimeError("The specified required name was not found \n" + \
						   "app_name: " + app_name + "\n" \
						   "source_dir: " + source_dir + "\n" \
						   "app_platform: " + app_platform + "\n")

	for src_file in os.listdir(source_dir):
		dest_path = os.path.join(target_dir, src_file)
		source_path = os.path.join(source_dir, src_file)
		if os.path.isdir(source_path):
			continue
		if file_exists(src_file, src_file):
			continue
		shutil.copy2(source_path, dest_path)
		#os.chmod(dest_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
	
	logging.info("client application for " + app_platform + \
				 " platform set up successfully")	 


def install_windows(app_dir):
	"""Installs the windows application by copying:
	 - [Windows-worker].exe
	 - [Windows-worker].exe.file_ref_info
	into the apps directory of the BOINC project
	"""
	# windows_intelx86
	logging.info("setting up client application for Windows platform")
	win_dir = os.path.join(app_dir, WIN_APP_NAME)
	win_exe = os.path.join(win_dir, WIN_APP_NAME)
	win_exe_ref = os.path.join(win_dir, WIN_APP_NAME + ".file_ref_info")
	
	workerpath = get_winworker_path()
	if not workerpath or not os.path.exists(workerpath): 
		logging.critical("Unable to locate the windows worker executable, " + \
						 "windows clients will be disabled")
		if workerpath:
			logging.critical("The path returned was: " + workerpath)
		return
	
	if not os.path.exists(win_dir):
		os.mkdir(win_dir)
	if not file_exists(win_exe, "Windows main-app"):
		shutil.copy(workerpath, win_exe)
	
	file_exists(win_exe_ref, None, FILE_REF)
	
	os.chmod(win_exe, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
	logging.info("Client application for Windows platform set up successfully")
	
def install_posix(app_dir, app_name, worker, friendly_name):
	"""Installs a generic posix shell script by coping:
	 - the-script
	 - the-script.file_ref_info
	into the apps directory of the BOINC project. 
	"""
	logging.info("setting up client application for " + friendly_name + \
				 " platform")
	target_dir = os.path.join(app_dir, app_name)
	target_exe = os.path.join(target_dir, app_name)
	target_exe_ref = os.path.join(target_dir, app_name + ".file_ref_info")

	if not os.path.exists(target_dir):
		os.mkdir(target_dir)
	file_exists(target_exe, friendly_name, worker)
	file_exists(target_exe_ref, None, FILE_REF)

	os.chmod(target_exe, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
	logging.info("client application for " + friendly_name + \
				 " platform set up successfully")	 

