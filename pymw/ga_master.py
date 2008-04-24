from pymw import *
from base_interface import *
from boinc_interface import *
from score_interface import *
from random import *
import time

n_workers = 4
interface = BaseSystemInterface(num_workers=n_workers)
#interface = BOINCInterface(project_home="/var/lib/boinc/szdgr/project")
#interface = SCoreSystemInterface(num_workers=4)
pymw_master = PyMW_Master(interface=interface)

input_dict = {}
num_inds = 10
gene_len = 10
input_dict["num_inds"] = num_inds
input_dict["gene_len"] = gene_len
input_dict["mut_rate"] = 1./gene_len
input_dict["num_gens"] = 50
input_dict["cross_rate"] = 0.7

start = time.time()

gene_pool = [[randint(0,1) for i in range(gene_len)] for n in range(n_workers*num_inds)]

num_active_tasks = 0
max_fitness = 0
task_num = 0
while max_fitness < gene_len:
	tasks = []
	for q in range(n_workers):
		input_dict["ind_set"] = sample(gene_pool, num_inds)
		tasks.append(pymw_master.submit_task('ga_worker.py', input_data=input_dict, new_task_name="ga"+str(task_num)))
		task_num += 1

	gene_pool = []
	for task in tasks:
		res_task, res = pymw_master.get_result(task)
		print res[2]
		max_fitness = max(max_fitness, res[2])
		gene_pool.extend(res[4])

end = time.time()

print "Total time:", str(end-start)
