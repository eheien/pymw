#!/usr/bin/env python

from pymw import *
#import pymw.interfaces.multicore
#import pymw.interfaces.mpi
#import pymw.interfaces.boinc
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
elif options.interface == "mpi":
	interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=n_workers)
elif options.interface == "boinc":
	interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
else:
	print "Interface", options.interface, "unknown."
	exit()

pymw_master = pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

input_dict = {}
num_inds = 10
gene_len = 10
input_dict["num_inds"] = num_inds
input_dict["gene_len"] = gene_len
input_dict["mut_rate"] = 1./gene_len
input_dict["num_gens"] = 50
input_dict["cross_rate"] = 0.7

gene_pool = [[randint(0,1) for i in range(gene_len)] for n in range(n_workers*num_inds)]

num_active_tasks = 0
max_fitness = 0
task_num = 0
while max_fitness < gene_len:
	tasks = []
	for q in range(n_workers):
		input_dict["ind_set"] = sample(gene_pool, num_inds)
		tasks.append(pymw_master.submit_task('ga_worker.py', input_data=input_dict, new_task_name="ga"+str(task_num)))
		task_num += 1

	gene_pool = []
	for task in tasks:
		res_task, res = pymw_master.get_result(task)
		print res[2]
		max_fitness = max(max_fitness, res[2])
		gene_pool.extend(res[4])

end_time = time.time()

print "Number of workers:", str(n_workers)
print "Calculation time:", str(end_time-start_time)
print "Total time:", str(end_time-start_time)
