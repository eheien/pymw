from pymw import *
from math import *
import time

for n_workers in [1, 2, 3, 4, 6, 8, 12, 16]:
        interface = pymw.interfaces.base_interface.BaseSystemInterface(num_workers=n_workers)
        #interface = pymw.interfaces.boinc_interface.BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
        #interface = pymw.interfaces.score_interface.SCoreSystemInterface(num_workers=4)
        pymw_master = pymw.PyMW_Master(interface=interface)

        max_val = 1000
        task_size = 5
        num_tasks = max_val/task_size

        start = time.time()

        primes = []

        in_data = [[(task_size*i)+1, task_size*(i+1)] for i in range(num_tasks)]
        tasks = [pymw_master.submit_task('prime_worker.py', input_data=data) for data in in_data]

        for task in tasks:
                res_task, res = pymw_master.get_result(task)
                primes.extend(res)
        end = time.time()

        #print primes

        print "Total time:", str(end-start)
