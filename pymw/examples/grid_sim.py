#!/usr/bin/env python

from pymw import *

def task_run_estimate(speed):
    return 100

interface_obj = pymw.interfaces.grid_simulator.GridSimulatorInterface()
pymw_master = pymw.PyMW_Master(interface=interface_obj)

tasks = [pymw_master.submit_task(task_run_estimate) for i in range(100)]
for task in tasks:
    res_task, res = pymw_master.get_result(task)
    
print interface_obj.get_status()
