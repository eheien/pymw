from app_types import *
from pymw import *

def make_calls():
	for i in range(10):
		pymw_worker_call(interface, 'worker.py', Input(i))

def handle_result(output):
	global total
	total += output.value

interface = BaseSystemInterface()
total = 0

pymw_master_call(interface, make_calls, handle_result)

print "The answer is", total

pymw_cleanup(interface)

