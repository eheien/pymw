#!/usr/bin/env python

from pymw import *
from pymw import interfaces
import time
from optparse import OptionParser

def count_num_strs(search_str):
	char_count = 0
	fp = open("stdio.h", "r")
	for line in fp:
		char_count += line.count(search_str)
	fp.close()
		
	return char_count

options, args = interfaces.parse_options()
start_time = time.time()
interface_obj = interfaces.get_interface(options)
pymw_master = pymw.PyMW_Master(interface=interface_obj)

post_init_time = time.time()

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
