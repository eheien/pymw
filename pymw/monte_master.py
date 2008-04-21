from app_types import *
from pymw import *
from base_interface import *
from boinc_interface import *
from score_interface import *
from math import *
from random import *
import time

def sqr_filter(x):
	return sqrt(x-1) == floor(sqrt(x-1))

interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

start = time.time()

num_total_tests = 1000000
num_hits = 0
num_tests = 0
tasks = [pymw_master.submit_task('monte_worker.py', [random(), num_total_tests/4]) for i in range(4)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	num_hits += res[0]
	num_tests += res[1]
end = time.time()

pi_estimate = 4 * float(num_hits)/num_tests
print pi_estimate, pi_estimate-pi
print "Total time:", str(end-start)

pymw_master.cleanup()
