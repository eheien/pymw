#!/usr/bin/env python
"""Provide an MPI interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <e-heien@ist.osaka-u.ac.jp>"
__date__ = "10 April 2008"

import sys
from mpi4py import MPI
# NOTE: this interface currently has a limit of 100 processes b/c of the PyMW thread limiter.
# This can be fixed if mpi4py IProbe gets fixed

class MPIInterface:
    def __init__(self, num_workers=1):
        self._child_comm = MPI.COMM_SELF.Spawn(sys.executable,
                                   args=['pymw/interfaces/mpi_manager.py'],
                                   maxprocs=num_workers)

        self._num_workers = self._child_comm.Get_remote_size()
        self._available_worker_list = [i for i in range(self._num_workers)]
    
    def get_available_workers(self):
        return list(self._available_worker_list)
    
    def reserve_worker(self, worker):
        self._available_worker_list.remove(worker)
    
    def worker_finished(self, worker):
        self._available_worker_list.append(worker)

    def execute_task(self, task, worker):
        cmd = [task._executable, task._input_arg, task._output_arg]
        self._child_comm.send(cmd, dest=worker, tag=0)
        print "Master sent cmd to", worker
        res = self._child_comm.recv(source=worker, tag=1)
        print "Master got response", res, "from", worker
        task.task_finished()
    
    def _cleanup(self):
        for worker in range(self._num_workers):
            self._child_comm.send(None, dest=worker, tag=0)
        self._child_comm.Disconnect()
    
    def get_status(self):
        return {}
