from app_types import *
from pymw import *
from boinc_interface import *
import time

interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
pymw_master = PyMW_Master(interface)

total = 0

start = time.time()
tasks = [pymw_master.submit_task('worker.py', Input(i)) for i in range(10)]
for task in tasks:
    total += pymw_master.get_result(task).value
end = time.time()

print "The answer is", total
print "Total time: ", str(end-start)

pymw_master.cleanup()
