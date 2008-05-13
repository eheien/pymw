from pymw.pymw_app import *
from math import *
from random import *

def sd_vals(i):
    s = 0
    n = i - 1
    d = n
    while d % 2 == 0:
        s += 1
        d = d /2
    return [s, d]

# Miller-Rabin primality test
def prime_test(n):
    sdvals = sd_vals(n)
    s = sdvals[0]
    d = sdvals[1]
    #a_vals = sample(xrange(1, n-1), 50) # get k random values (no repeats)
    #for a in a_vals:
    for k in range(50):
        a = randint(1, n-1)
        p = a
        for i in xrange(int(d)-1):
            p = (p * a) % n
        if p != 1:
            maybe_prime = False
            for r in xrange(s):
                q = a
                for i in xrange(int(pow(2,r)*d)-1):
                    q = (q * a) % n
                if q == n-1:
                    maybe_prime = True
            if not maybe_prime: return False

    return True

input = pymw_get_input()
lower_bound = input[0]
upper_bound = input[1]

vals = [pow(i, 2)+1 for i in range(lower_bound, upper_bound)]

odd_vals = filter(lambda(x): (x % 2) != 0, vals)

primes = filter(prime_test, vals)

pymw_return_output(odd_vals)

