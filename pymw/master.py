from app_types import *
from pymw import *
import time

def setup_workers():
	for i in range(100):
		pymw_worker_call(interface, 'worker.py', Input(i))

def handle_result(output):
	global total
	total += output.value

interface = BaseSystemInterface()
total = 0

start = time.time()
pymw_master_call(interface, setup_workers, handle_result, 8)
end = time.time()

print "The answer is", total
print "Total time:", str(end-start)

pymw_cleanup(interface)

