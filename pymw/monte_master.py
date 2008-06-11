#!/usr/bin/env python

from pymw import *
import pymw.interfaces.multicore
import pymw.interfaces.mpi
import pymw.interfaces.boinc
import time
import random
import math
from optparse import OptionParser

def throw_dart():
    pt = math.pow(random.random(),2) + math.pow(random.random(),2)
    if pt <= 1: return 1
    else: return 0

def monte_pi(rand_seed, num_tests):
	random.seed(rand_seed)
	num_hits = 0
	
	for i in xrange(num_tests):
	    num_hits += throw_dart()
	    
	return [num_hits, num_tests]

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", default="multicore", help="specify the interface (multicore/mpi/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="n_workers", default="4", help="number of workers", metavar="N")
parser.add_option("-t", "--num_tests", dest="n_tests", default="1000000", help="number of Monte Carlo tests to perform", metavar="N")
parser.add_option("-p", "--project_home", dest="p_home", default="", help="directory of the project (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

n_workers, n_tests = int(options.n_workers), int(options.n_tests)

start_time = time.time()

if options.interface == "multicore":
	interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=n_workers)
	num_tasks = n_workers
elif options.interface == "mpi":
	interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=n_workers)
	num_tasks = n_workers-1
elif options.interface == "boinc":
	interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
	num_tasks = n_workers
else:
	print "Interface", options.interface, "unknown."
	exit()

pymw_master = pymw.pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

tests_per_task = n_tests/num_tasks
tasks = [pymw_master.submit_task(monte_pi,
								 input_data=(random.random(),tests_per_task,),
								 modules=("random", "math"),
								 dep_funcs=(throw_dart,))
    for i in range(num_tasks)]

num_hits = 0
num_tests = 0

for task in tasks:
	res_task, result = pymw_master.get_result(task)
	num_hits += result[0]
	num_tests += result[1]

end_time = time.time()

pi_estimate = 4 * float(num_hits)/num_tests
print pi_estimate, pi_estimate-math.pi
print "Number of Workers:", str(n_workers)
print "Calculation time:", str(end_time-post_init_time)
print "Total time:", str(end_time-start_time)
