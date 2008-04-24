from pymw_app import *
from math import *

def non_prime_filter(n):
    return lambda x: (x % n) != 0 or x == n

input = pymw_get_input()
lower_bound = input[0]
upper_bound = input[1]

current_set = range(lower_bound, upper_bound)
max_val = int(sqrt(upper_bound)+1)

for i in xrange(2, max_val):
    current_set = filter(non_prime_filter(i), current_set)

pymw_return_output(current_set)

