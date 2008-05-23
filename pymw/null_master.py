#!/usr/bin/env python
# null_master.py <interface> <num_workers/project_url> <num_tasks>

import pymw
import pymw.interfaces.multicore
import pymw.interfaces.mpi
import pymw.interfaces.boinc
import time
from optparse import OptionParser

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", help="specify the interface (multicore/mpi/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="num_w", help="number of workers", metavar="N")
parser.add_option("-t", "--num_tasks", dest="num_t", help="number of tasks", metavar="N")
parser.add_option("-p", "--project_home", dest="p_home", help="directory of the project (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

num_w, num_t = int(options.num_w), int(options.num_t)

start_time = time.time()

if options.interface == "boinc":
	interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
elif options.interface == "mpi":
	interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=num_w)
elif options.interface == "multicore":
	interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=num_w)
else:
	print "Interface", options.interface, "unknown."
	exit()

pymw_master = pymw.pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()
tasks = [pymw_master.submit_task('null_worker.py', input_data=i) for i in range(num_t)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)

end_time = time.time()

print "Number of workers:", str(num_w)
print "Number of tasks:", str(num_t)
print "Initialization time:", str(post_init_time-start_time)
print "Calculation time:", str(end_time-post_init_time)
print "Total time:", str(end_time-start_time)
print "Calculation time per task:", str((end_time-post_init_time)/num_t)
print "Total time per task:", str((end_time-start_time)/num_t)
