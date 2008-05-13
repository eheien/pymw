from pymw import *
import pymw.interfaces.mpi_interface
#import pymw.interfaces.base_interface
from math import *
from random import *
import time
import sys

nw = int(sys.argv[1])

start = time.time()

interface = pymw.interfaces.mpi_interface.MPIInterface(num_workers=nw)
#interface = pymw.interfaces.base_interface.BaseSystemInterface(num_workers=nw-1)
pymw_master = pymw.pymw.PyMW_Master(interface=interface)

post_init = time.time()

num_tasks = (nw-1)
#tests_per_task = 500000000/num_tasks
tests_per_task = 1000000000/num_tasks
#print "Tests per task", str(tests_per_task)
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
print "Number of Workers:", str(nw), "Non-init time:", str(end-post_init), "Total time:", str(end-start)
exit()

