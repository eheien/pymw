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

num_tasks = 100
tasks = [pymw_master.submit_task('null_worker.py', input_data=i) for i in range(num_tasks)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)

end = time.time()

print "Number of workers:", str(nw)
print "Number of tasks:", str(num_tasks)
print "Non-init time:", str(end-post_init)
print "Total time:", str(end-start)
print "Non-init time per task:", str((end-post_init)/num_tasks)
print "Total time per task:", str((end-start)/num_tasks)

exit()

