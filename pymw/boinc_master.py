from app_types import *
from pymw import *
from boinc_interface import *

interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
pymw_master = PyMW_Master(interface)

task = pymw_master.submit_task('worker.py', Input(1))

print "OK"

del pymw_master
