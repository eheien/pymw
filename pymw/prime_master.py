from pymw import *
from math import *
import pymw.interfaces.mpi_interface
import pymw.interfaces.base_interface
import time
import sys

n_workers = int(sys.argv[1])

init_start = time.time()
#interface = pymw.interfaces.base_interface.BaseSystemInterface(num_workers=n_workers)
#interface = pymw.interfaces.boinc_interface.BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
interface = pymw.interfaces.mpi_interface.MPIInterface(num_workers=n_workers)
pymw_master = pymw.pymw.PyMW_Master(interface=interface)

max_val = 10000
task_size = 5
num_tasks = max_val/task_size

start = time.time()

primes = []

in_data = [[i, i+task_size] for i in range(1, max_val+1, task_size)]
tasks = [pymw_master.submit_task('prime_worker.py', input_data=data) for data in in_data]

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res)
end = time.time()

print primes[-5:]

print "Number of workers:", str(n_workers), "Non-init time:", str(end-start), "Total time:", str(end-init_start)
exit()

