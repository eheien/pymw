#!/usr/bin/env python

from pymw import pymw
from pymw import interfaces
import time
import random
import math
from optparse import OptionParser
import sys

def throw_dart():
	pt = math.pow(random.random(),2) + math.pow(random.random(),2)
	if pt <= 1: return 1
	else: return 0

def monte_pi(rand_seed, num_tests):
	random.seed(rand_seed)
	num_hits = 0
	
	for i in range(int(num_tests)):
		num_hits += throw_dart()
		
	return [num_hits, num_tests]

parser = OptionParser(usage="usage: %prog")
parser.add_option("-t", "--num_tests", dest="n_tests", default="1000000", 
					help="number of Monte Carlo tests to perform", metavar="N")
options,args = interfaces.parse_options(parser)

n_workers, n_tests = int(options.n_workers), int(options.n_tests)

start_time = time.time()

# get an interface object based on command-line options 
interface_obj = interfaces.get_interface(options)

num_tasks = n_workers
pymw_master = pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

tests_per_task = n_tests/num_tasks
tasks = [pymw_master.submit_task(monte_pi,
								 input_data=(random.random(),tests_per_task,),
								 modules=("random", "math"),
								 dep_funcs=(throw_dart,))
	for i in range(num_tasks)]

num_hits = 0
num_tests = 0

for i in range(num_tasks):
	res_task, result = pymw_master.get_result()
	num_hits += result[0]
	num_tests += result[1]

end_time = time.time()

pi_estimate = 4.0 * float(num_hits)/num_tests
print(("Estimate of pi:", pi_estimate))
print(("Estimate error:", abs(pi_estimate-math.pi)))
print(("Number of Tasks:", str(num_tasks)))
print(("Tests per Task:", str(tests_per_task)))
print(("Calculation time:", str(end_time-post_init_time)))
print(("Total time:", str(end_time-start_time)))
