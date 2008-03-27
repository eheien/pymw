from app_types import *
from pymw import *
from base_interface import *
import time

def handle_result(output):
	global total
	total += output.value

interface = BaseSystemInterface(num_workers=4)
pymw = PyMW_Master(interface)

total = 0

start = time.time()
tasks = [pymw.submit_task('worker.py', Input(i)) for i in range(10)]
for task in tasks:
	total += pymw.wait_for_task_finish(task).value
end = time.time()

print "The answer is", total
print "Total time:", str(end-start)

print pymw.get_status()
#pymw.pymw_cleanup(interface)

