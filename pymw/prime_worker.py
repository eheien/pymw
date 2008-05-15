from pymw.pymw_app import *
from random import *
from decimal import *

def sd_vals(i):
    s = Decimal(0)
    n = Decimal(i - 1)
    d = Decimal(n)
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
        rand_val = Decimal(str(random()))*Decimal(n-1)
        a = rand_val.to_integral()
        if a < 1: a = 1
        if a > n-1: a = n-1
        p = pow(Decimal(a), d, n) # p = a^d % n
        if p != 1:
            maybe_prime = False
            for r in xrange(s):
                q = pow(a, pow(2,r)*d, n) # q = a^(d*2^r) % n
                if q == n-1:
                    maybe_prime = True
            if not maybe_prime: return False

    return True

input = pymw_get_input()
lower_bound = input[0]
upper_bound = input[1]

vals = [pow(Decimal(i), Decimal(2))+1 for i in range(lower_bound, upper_bound)]

odd_vals = filter(lambda(x): (x % 2) != 0, vals)

primes = filter(prime_test, odd_vals)

pymw_return_output(primes)

