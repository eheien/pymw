#!/usr/bin/env python

from pymw import *
import time
import random
import decimal
from optparse import OptionParser

def sd_vals(i):
    s = decimal.Decimal(0)
    n = decimal.Decimal(i - 1)
    d = decimal.Decimal(n)
    while d % 2 == 0:
        s += 1
        d = d /2
    return s, d

# Miller-Rabin primality test
def prime_test(n):
    s, d = sd_vals(n)
    k = 50
    if n == 1: return False
    #num_samples = min(n-2, 50)
    #if n-2 < 1000: a_vals = sample(range(1, n), n-2) # get k random values (no repeats)
    for i in range(k):
        rand_val = decimal.Decimal(str(random.random()))*decimal.Decimal(n-1)
        a = rand_val.to_integral()
        if a < 1: a = 1
        if a > n-1: a = n-1
        p = pow(decimal.Decimal(a), d, n) # p = a^d % n
        if p != 1:
            maybe_prime = False
            for r in xrange(s):
                q = pow(a, pow(2,r)*d, n) # q = a^(d*2^r) % n
                if q == n-1:
                    maybe_prime = True
            if not maybe_prime: return False

    return True

def prime_range_check(lower_bound, upper_bound):
	vals = [pow(decimal.Decimal(i), decimal.Decimal(2))+1 for i in range(lower_bound, upper_bound)]
	odd_vals = filter(lambda(x): (x % 2) != 0, vals)
	
	primes = filter(prime_test, odd_vals)
	return primes

parser = OptionParser(usage="usage: %prog")
parser.add_option("-i", "--interface", dest="interface", default="multicore", help="specify the interface (multicore/mpi/boinc)", metavar="INTERFACE")
parser.add_option("-n", "--num_workers", dest="n_workers", default="4", help="number of workers", metavar="N")
parser.add_option("-r", "--min_val", dest="min_val", default="1", help="minimum value to check", metavar="N")
parser.add_option("-s", "--max_val", dest="max_val", default="10000", help="maximum value to check", metavar="N")
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

tasks = [pymw_master.submit_task(prime_range_check,
								 input_data=data,
								 modules=("random", "decimal"),
								 dep_funcs=(sd_vals, prime_test))
        for data in in_data]

primes = []

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res)

end_time = time.time()

print primes[-5:]

print "Number of workers:", str(n_workers)
print "Calculation time:", str(end_time-start_time)
print "Total time:", str(end_time-start_time)
