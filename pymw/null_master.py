#!/usr/bin/env python
# null_master.py <interface> <num_workers/project_url> <num_tasks>

import pymw
import pymw.interfaces.multicore_interface
import pymw.interfaces.mpi_interface
import pymw.interfaces.boinc_interface
import time

start_time = time.time()
interface = sys.argv[1]
num_w = int(sys.argv[2])

if interface == "boinc":
	interface_obj = pymw.interfaces.boinc_interface.BOINCInterface(project_home=sys.argv[2])
elif interface == "mpi":
	interface_obj = pymw.interfaces.mpi_interface.MPIInterface(num_workers=num_w)
elif interface == "multicore":
	interface_obj = pymw.interfaces.multicore_interface.MulticoreInterface(num_workers=num_w)
else:
	print "Interface", interface, "unknown."
	exit()

pymw_master = pymw.pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()
num_tasks = int(sys.argv[3])
tasks = [pymw_master.submit_task('null_worker.py', input_data=i) for i in range(num_tasks)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)

end_time = time.time()

print "Number of workers:", str(num_w)
print "Number of tasks:", str(num_tasks)
print "Non-init time:", str(end_time-post_init_time)
print "Total time:", str(end_time-start_time)
print "Non-init time per task:", str((end_time-post_init_time)/num_tasks)
print "Total time per task:", str((end_time-start_time)/num_tasks)
