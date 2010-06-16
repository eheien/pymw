#!/usr/bin/env python

from pymw import *
import time
from optparse import OptionParser

def count_num_strs(search_str):
    char_count = 0
    fp = open("stdio.h", "r")
    for line in fp:
        char_count += line.count(search_str)
    fp.close()
        
    return char_count

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", default="generic", help="specify the interface (generic/multicore/mpi/ganga/condor/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="n_workers", default="4", help="number of workers", metavar="N")
parser.add_option("-t", "--num_tests", dest="n_tests", default="1000000", help="number of Monte Carlo tests to perform", metavar="N")
parser.add_option("-g", "--ganga_loc", dest="g_loc", default="~/Ganga/bin/ganga", help="directory of GANGA executable (GANGA interface)", metavar="FILE")
parser.add_option("-p", "--project_home", dest="p_home", default="", help="directory of the project (BOINC interface)", metavar="DIR")
options, args = parser.parse_args()

n_workers, n_tests = int(options.n_workers), int(options.n_tests)

start_time = time.time()

if options.interface == "generic":
    interface_obj = pymw.interfaces.generic.GenericInterface(num_workers=n_workers)
elif options.interface == "multicore":
    interface_obj = pymw.interfaces.multicore.MulticoreInterface(num_workers=n_workers)
elif options.interface == "ganga":
    interface_obj = pymw.interfaces.ganga.GANGAInterface(ganga_loc=options.g_loc)
elif options.interface == "boinc":
    interface_obj = pymw.interfaces.boinc.BOINCInterface(project_home=options.p_home)
else:
    print(("Interface", options.interface, "unknown."))
    exit()

num_tasks = n_workers
pymw_master = pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

tests_per_task = n_tests/num_tasks
search_strs = ["a", "b", "c", "d", "e", "f"]
tasks = [pymw_master.submit_task(count_num_strs,
                                 input_data=(srch_str,),
                                 data_files=("/usr/include/stdio.h",),
                                 )
    for srch_str in search_strs]

num_hits = 0
num_tests = 0

for i in range(len(tasks)):
    res_task, result = pymw_master.get_result(tasks[i])
    print(("Counted", result, "instances of", search_strs[i]))

end_time = time.time()

print(("Calculation time:", str(end_time-post_init_time)))
print(("Total time:", str(end_time-start_time)))
