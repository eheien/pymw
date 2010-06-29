#!/usr/bin/env python

from pymw import *
from pymw import interfaces
import time
from optparse import OptionParser

def null_worker(in_data):
	return in_data

parser = OptionParser(usage="usage: %prog")

parser.add_option("-t", "--num_tasks", dest="n_tasks", default="5", 
				help="number of tasks", metavar="N")

parser.add_option("-s", "--task_size", dest="task_size", default="1", 
				help="task data size (kilobytes)", metavar="N")

options, args = interfaces.parse_options(parser)
 

n_workers, n_tasks, task_size = int(options.n_workers), int(options.n_tasks), int(options.task_size)

start_time = time.time()

interface_obj = interfaces.get_interface(options)
pymw_master = pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()
tasks = [pymw_master.submit_task(null_worker, input_data=(list(range(task_size*256)),)) for i in range(n_tasks)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)

end_time = time.time()
total_io = 2*task_size*n_tasks/1024.0

print(("Number of workers:", str(n_workers)))
print(("Number of tasks:", str(n_tasks)))
print(("Size per task:", str(task_size), "kilobytes"))
print(("Initialization time:", str(post_init_time-start_time)))
print(("Calculation time:", str(end_time-post_init_time)))
print(("Total time:", str(end_time-start_time)))
print(("Calculation time per task:", str((end_time-post_init_time)/n_tasks)))
print(("Total time per task:", str((end_time-start_time)/n_tasks)))
print(("Total I/O:", str(total_io), "megabytes"))
print(("I/O rate:", str(total_io/(end_time-post_init_time)), "MB/sec"))
