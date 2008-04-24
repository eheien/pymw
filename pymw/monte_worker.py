from pymw_app import *
from random import *
from math import *

def throw_dart():
    pt = pow(random(),2) + pow(random(),2)
    if pt <= 1: return 1
    else: return 0

input = pymw_get_input()
rand_seed = input[0]
num_tests = input[1]

seed(rand_seed)
num_hits = 0

for i in xrange(num_tests):
    num_hits += throw_dart()

pymw_return_output([num_hits, num_tests])
