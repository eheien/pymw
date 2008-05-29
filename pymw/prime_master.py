#!/usr/bin/env python

import pymw
import pymw.interfaces.multicore
import pymw.interfaces.mpi
import pymw.interfaces.boinc
import time
from optparse import OptionParser

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", default="multicore", help="specify the interface (multicore/mpi/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="n_workers", default="4", help="number of workers", metavar="N")
parser.add_option("-r", "--min_val", dest="min_val", default="1", help="minimum value to check", metavar="N")
parser.add_option("-s", "--max_val", dest="max_val", default="100000", help="maximum value to check", metavar="N")
parser.add_option("-p", "--project_home", dest="p_home", default="", help="directory of the project (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

n_workers, min_val, max_val = int(options.n_workers), int(options.min_val), int(options.max_val)

start_time = time.time()

if options.interface == "multicore":
	interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=n_workers)
	num_tasks = n_workers*3
elif options.interface == "mpi":
	interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=n_workers)
	num_tasks = (n_workers-1)*3
elif options.interface == "boinc":
	interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
	num_tasks = n_workers*3
else:
	print "Interface", options.interface, "unknown."
	exit()

pymw_master = pymw.pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

task_size = (max_val-min_val)/float(num_tasks)
start_val = min_val
in_data = []
for i in range(num_tasks-1):
	next_val = start_val + task_size
	in_data.append([int(start_val), int(next_val-1)])
	start_val = next_val
in_data.append([int(start_val), max_val])

tasks = [pymw_master.submit_task('prime_worker.py', input_data=data) for data in in_data]

primes = []

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res)

end_time = time.time()

print primes[-5:]

print "Number of workers:", str(n_workers)
print "Calculation time:", str(end_time-start_time)
print "Total time:", str(end_time-start_time)
