#!/usr/bin/env python

from pymw import *
from time import *

# Initialize PyMW using the default interface
pymw_master = pymw.PyMW_Master()

total = 0
start = time()
tasks = [pymw_master.submit_task('worker.py', input_data=i) for i in range(10)]
for task in tasks:
	task, res = pymw_master.get_result(task)
	total = total + res
end = time()

print "The answer is", total
print "Total time:", str(end-start)
