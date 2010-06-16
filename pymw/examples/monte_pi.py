#!/usr/bin/env python

from pymw import pymw
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
    
    for i in range(num_tests):
        num_hits += throw_dart()
        
    return [num_hits, num_tests]

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", default="generic", help="specify the interface (generic/multicore/mpi/ganga/condor/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="n_workers", default="4", help="number of workers", metavar="N")
parser.add_option("-t", "--num_tests", dest="n_tests", default="1000000", help="number of Monte Carlo tests to perform", metavar="N")
parser.add_option("-g", "--ganga_loc", dest="g_loc", default="~/Ganga/bin/ganga", help="directory of GANGA executable (GANGA interface)", metavar="FILE")
parser.add_option("-p", "--project_home", dest="p_home", default="", help="directory of the project (BOINC interface)", metavar="DIR")
parser.add_option("-c", "--app_path", dest="custom_app_dir", default="", help="directory of a custom worker application (BOINC interface)", metavar="DIR")
parser.add_option("-a", "--app_args", dest="custom_app_args", default="", help="arguments for a custom worker application (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

n_workers, n_tests = int(options.n_workers), int(options.n_tests)

start_time = time.time()

if options.interface == "generic":
    interface_obj = pymw.interfaces.generic.GenericInterface(num_workers=n_workers)
elif options.interface == "multicore":
    interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=n_workers)
elif options.interface == "multiproc":
    interface_obj = pymw.interfaces.multiproc.MultiProcInterface(num_workers=n_workers)
elif options.interface == "mpi":
    interface_obj = pymw.interfaces.mpi.MPIInterface(num_workers=n_workers)
elif options.interface == "condor":
    interface_obj = pymw.interfaces.condor.CondorInterface()
elif options.interface == "ganga":
    interface_obj = pymw.interfaces.ganga.GANGAInterface(ganga_loc=options.g_loc)
elif options.interface == "boinc":
    interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home,\
                                                         custom_app_dir=options.custom_app_dir,\
                                                         custom_args=[options.custom_app_args])
else:
    print(("Interface", options.interface, "unknown."))
    exit()

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
print(("Calculation time:", str(end_time-post_init_time)))
print(("Total time:", str(end_time-start_time)))
