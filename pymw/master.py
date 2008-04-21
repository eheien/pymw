#!/usr/bin/env python

from pymw import *
from base_interface import *
from score_interface import *
from boinc_interface import *
from time import *

# Initialize PyMW and the interface
interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

total = 0
start = time()
tasks = [pymw_master.submit_task('worker.py', i) for i in range(1,10)]
for task in tasks:
	task, res = pymw_master.get_result(task)
	total = total + res
end = time()

print "The answer is", total
print "Total time:", str(end-start)

pymw_master.cleanup()
