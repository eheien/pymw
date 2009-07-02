================
Example Programs
================
To try out the sample applications, run the example/*.py files.
For example, running the Monte Carlo pi calculation program results in::

	user% python monte_pi.py -t 1000
	3.148 0.00640734641021
	Number of Workers: 4
	Calculation time: 0.470422029495
	Total time: 0.471640825272

Use the -h option to see a list of options for each program.

genetic_alg.py
	(NOT FINSIHED) A genetic algorithm program.
monte_pi.py
	Monte Carlo style program which estimates the value of pi by randomly selecting points in a square.
null_test.py
	A program which performs no computation, but transfers data between the master and workers. This is useful to time pickling and data transmission for a given environment.
prime_finder.py
	Parameter sweep program which uses the Miller-Rabin primality test to find prime numbers.
worker_sim.py
	Demonstrates use of the simulation interface.
