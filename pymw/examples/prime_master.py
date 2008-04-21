from app_types import *
from pymw import *
from base_interface import *
from math import *
import time

def mersenne_filter(x):
	return sqrt(x-1) == floor(sqrt(x-1))

base_interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
pymw_master = PyMW_Master(interface=base_interface)

max_val = 50000
num_tasks = 2
task_size = max_val/num_tasks

start = time.time()

tasks = []
primes = []
for i in range(num_tasks):
	tasks.append(pymw_master.submit_task('prime_worker.py', Input([task_size*i, task_size*(i+1)])))

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res.value)
end = time.time()

mersennes = filter(mersenne_filter, primes)
print mersennes

print "Total time:", str(end-start)

pymw_master.cleanup()
