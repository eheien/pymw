"""Support functions for the BOINC interface.

This module provides supporting functions for creating
the PyMW application in BOINC, as well as various helper
methods. It requires a path to the BOINC project directory.
"""

__author__ = 'Jeremy Cowles <jeremy.cowles@gmail.com>'
__date__ = '9 June 2009'

import os, logging, sys, stat, shutil


# Platform strings, must adhere to BOINC main program naming spec
i686_pc_linux_gnu = "pymw_1.00_i686-pc-linux-gnu"
windows_intelx86  = "pymw_1.00_windows_intelx86.exe"
i686_apple_darwin = "pymw_1.00_i686-apple-darwin"

# Worker script (the main BOINC program)
POSIX_WORKER = """\
#!/bin/sh
`python $1 $2 $3`
`touch boinc_finish_called`
"""

# PyMW application, helper script, shipped with BOINC program
PYMW_APP = """\
import sys
import cPickle

def pymw_get_input():
    infile = open(sys.argv[1], 'r')
    obj = cPickle.Unpickler(infile).load()
    infile.close()
    return obj

def pymw_return_output(output):
    outfile = open(sys.argv[2], 'w')
    cPickle.Pickler(outfile).dump(output)
    outfile.close()
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

configxml,projectxml = None,None
def boinc_import_hack(proj_path):
    """Imports the BOINC support code

    This is a hack because the actual path is specified at runtime
    and so cannot be imported when the module loads. This works
    around that issue by creating global vars for the namespaces
    and then populating them once the path is known.
    """
    global configxml,projectxml
    
    bin_path = os.path.join(proj_path, "bin")
    if not bin_path in sys.path:
        sys.path.append(bin_path)
    
    # BOINC will search for config files using these vars
    os.environ['BOINC_CONFIG_XML'] = os.path.join(proj_path, 'config.xml')
    os.environ['BOINC_PROJECT_XML'] = os.path.join(proj_path, 'project.xml')
    os.environ['BOINC_RUN_STATE_XML'] = os.path.join(proj_path, 'run_state.xml')
    
    import configxml,projectxml

def get_winworker_path():
    """Gets the path to the WINDOWS_WORKER
    
    Returns the expected path or None if there was a
    problem determining the expected path.
    
    This function assumes that pymw is on the current
    python path. Note that the path returned is the 
    *expected* path and may not actually exist.
    """
    paths = [p for p in sys.path[1:] if 'pymw' in p]
    if len(paths) == 0: return None
    path = os.path.join(paths[0], WINDOWS_WORKER)
    
    return path

def install_pymw(project_path):
    logging.debug("---------------------------------")
    logging.debug("Installing PyMW BOINC application")
    logging.debug("---------------------------------")

    # load BOINC internal Python code
    boinc_import_hack(project_path)

    config = setup_config(os.path.join(sys.path[0], "tasks"))
    project = setup_project()
    install_apps(config)
    check_daemons(project_path)
    
    logging.debug("---------------------")
    logging.debug("PyMW setup successful")
    logging.debug("---------------------")

def check_daemons(project_path):
    stopped = os.path.exists(os.path.join(project_path, STOP_TRIGGER))
    
    if stopped:
        # try to start the daemons
        logging.debug("BOINC daemon status: STOPPED")
        logging.debug("Attempting to start BOINC daemons...")
        os.system(os.path.join(project_path, "bin", "start"))
    else:
        logging.debug("BOINC daemon status: RUNNING")

def setup_config(task_path):
    logging.info("writing pymw_assimilator in config.xml")
    config = configxml.ConfigFile().read()
    
    # Remove old instances of pymw_assimilator
    for daemon in config.daemons:
        if daemon.cmd[0:16] == 'pymw_assimilator':
            config.daemons.remove_node(daemon)
    
    # Append new instance of pymw_assimilator to config.xml
    config.daemons.make_node_and_append("daemon").cmd = "pymw_assimilator.py -d 3 -app pymw -pymw_dir " + task_path
    config.write()
    return config

def setup_project():
    # Append new instance of pymw worker to project.xml
    project = projectxml.ProjectFile().read()
    found = False
    for element in project.elements:
        if element.name == 'pymw':
            logging.info("PyMW client applications are already installed")
            return
    
    project.elements.make_node_and_append("app").name = "pymw"
    project.write()
    for element in project.elements:
        if element.name == "pymw":
            element.user_friendly_name = "PyMW - Master Worker Computing in Python"
            project.write()
    return project

def install_apps(config):
    # Install worker applications
    app_dir = os.path.join(config.config.app_dir, "pymw")
    if os.path.isdir(app_dir):
        logging.info("PyMW client applications are already installed")
        return

    os.mkdir(app_dir)
    install_posix(app_dir, i686_pc_linux_gnu, POSIX_WORKER, "Linux")
    install_posix(app_dir, i686_apple_darwin, POSIX_WORKER, "Apple")
    install_windows(app_dir, windows_intelx86)
    
    # Call update_versions
    project_home = config.config.app_dir.rpartition('/')[0]
    os.chdir(project_home)
    os.system("xadd")
    os.system("update_versions --force --sign")
        
def install_windows(app_dir, app_name):
    # windows_intelx86
    logging.info("setting up client application for Windows platform")
    win_dir = os.path.join(app_dir, windows_intelx86)
    win_exe = os.path.join(win_dir, windows_intelx86)
    workerpath = get_winworker_path()
    if not workerpath or not os.path.exists(workerpath): 
        logging.critical("Unable to locate the windows worker executable, windows clients will be disabled")
        logging.critical("The path returned was: " + workerpath)
        return
    os.mkdir(win_dir)
    shutil.copy(workerpath, win_exe)
    f = open(os.path.join(win_dir, windows_intelx86 + ".file_ref_info"), "w").writelines(FILE_REF)
    open(os.path.join(win_dir, "pymw_app.py"), "w").writelines(PYMW_APP)
    open(os.path.join(win_dir, "pymw_app.py.file_ref_info"), "w").writelines(FILE_REF)
    os.chmod(win_exe, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
    logging.info("client application for Windows platform set up successfully")
    
def install_posix(app_dir, app_name, worker, friendly_name):
    logging.info("setting up client application for " + friendly_name + " platform")
    target_dir = os.path.join(app_dir, app_name)
    target_exe = os.path.join(target_dir, app_name)
    os.mkdir(target_dir)
    open(target_exe, "w").writelines(worker)
    open(os.path.join(target_dir, app_name + ".file_ref_info"), "w").writelines(FILE_REF)
    open(os.path.join(target_dir, "pymw_app.py"), "w").writelines(PYMW_APP)
    open(os.path.join(target_dir, "pymw_app.py.file_ref_info"), "w").writelines(FILE_REF)
    os.chmod(target_exe, stat.S_IRWXU | stat.S_IRGRP | stat.S_IROTH)
    logging.info("client application for " + friendly_name + " platform set up successfully")     

