from app_types import *
from pymw import *
from base_interface import *
from boinc_interface import *
from score_interface import *
import time

interface = BaseSystemInterface(num_workers=4, python_loc="/usr/local/bin/python")
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

input_dict = {}
input_dict["num_inds"] = 50
input_dict["gene_len"] = 100
input_dict["mut_rate"] = 1./100  # 1/gene_len
input_dict["num_gens"] = 100
input_dict["cross_rate"] = 0.7

ind_set = [[randint(0,1) for i in range(gene_len)] for n in range(num_inds)]

start = time.time()

tasks = [pymw_master.submit_task('ga_worker.py', input_dict) for i in range(num_tasks)]

for task in tasks:
	res_task, res = pymw_master.get_result(task)
	primes.extend(res)
end = time.time()

print "Total time:", str(end-start)

pymw_master.cleanup()
