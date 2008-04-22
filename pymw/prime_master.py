from app_types import *
from pymw import *
from base_interface import *
from boinc_interface import *
from score_interface import *
from math import *
import time

def sqr_filter(x):
	return sqrt(x-1) == floor(sqrt(x-1))

interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

max_val = 50000
num_tasks = 1
task_size = max_val/num_tasks

start = time.time()

primes = []

in_data = [[task_size*i, task_size*(i+1)] for i in range(num_tasks)]
tasks = [pymw_master.submit_task('prime_worker.py', input_data=data) for data in in_data]

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res)
end = time.time()

sqr_primes = filter(sqr_filter, primes)
print sqr_primes

print "Total time:", str(end-start)
