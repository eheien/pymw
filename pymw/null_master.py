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
parser.add_option("-t", "--num_tasks", dest="n_tasks", default="10", help="number of tasks", metavar="N")
parser.add_option("-p", "--project_home", dest="p_home", default="", help="directory of the project (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

n_workers, n_tasks = int(options.n_workers), int(options.n_tasks)

start_time = time.time()

if options.interface == "multicore":
	interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=n_workers)
elif options.interface == "mpi":
	interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=n_workers)
elif options.interface == "boinc":
	interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
else:
	print "Interface", options.interface, "unknown."
	exit()

pymw_master = pymw.pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()
tasks = [pymw_master.submit_task('null_worker.py', input_data=i) for i in range(n_tasks)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)

end_time = time.time()

print "Number of workers:", str(n_workers)
print "Number of tasks:", str(n_tasks)
print "Initialization time:", str(post_init_time-start_time)
print "Calculation time:", str(end_time-post_init_time)
print "Total time:", str(end_time-start_time)
print "Calculation time per task:", str((end_time-post_init_time)/n_tasks)
print "Total time per task:", str((end_time-start_time)/n_tasks)
