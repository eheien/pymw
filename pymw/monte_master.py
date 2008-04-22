from app_types import *
from pymw import *
from base_interface import *
from boinc_interface import *
from score_interface import *
from math import *
from random import *
import time

interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

start = time.time()

tests_per_task = 100000
num_tasks = 10
tasks = [pymw_master.submit_task('monte_worker.py', input_data=[random(), tests_per_task]) for i in range(num_tasks)]

num_hits = 0
num_tests = 0

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	num_hits += res[0]
	num_tests += res[1]
end = time.time()

pi_estimate = 4 * float(num_hits)/num_tests
print pi_estimate, pi_estimate-pi
print "Total time:", str(end-start)
