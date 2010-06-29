#!/usr/bin/env python
"""Provide an MPI interface for master worker computing with PyMW.
"""

__author__ = "Eric Heien <pymw@heien.org>"
__date__ = "10 April 2008"

import sys
import tempfile
import textwrap
import shutil
import os
import inspect

try:
	from mpi4py import MPI
except ImportError:
	MPI = None

# NOTE: this interface currently has a limit of 100 processes b/c of the PyMW thread limiter.
# This can be fixed if mpi4py IProbe gets fixed

def worker_func():
	# Figure out who the parent is and who the worker is
	parent_comm = MPI.Comm.Get_parent()
	rank = parent_comm.Get_rank()
	# Create a directory for temp files
	worker_temp_dir = tempfile.mkdtemp()
	# Go around in an infinite loop
	while True:
		# Get the next command from the master
		msg = parent_comm.recv(source=0, tag=0)
		# If it's a null command, then it's time to quit
		if msg is None:
			parent_comm.Disconnect()
			break
		# Execute the script and get the output
		exec_process = subprocess.Popen(args=[sys.executable, msg[0], msg[1], msg[2]],
										cwd=worker_temp_dir,
										stderr=subprocess.PIPE)
		proc_stdout, proc_stderr = exec_process.communicate()
		# Send the return code and stderr (exception data) back to the master
		send_result = parent_comm.send([rank, exec_process.returncode, proc_stderr], dest=0, tag=1)
	# Delete the worker temp directory
	shutil.rmtree(path=worker_temp_dir, ignore_errors=True)

class MPIInterface:
	def __init__(self, num_workers=1):
		if MPI is None:
			raise Exception("PyMW MPI interface requires mpi4py to be installed. Please install mpi4py and try again.")
		# TODO: Write the worker function to a temp file and run MPI with this file
		self._worker_func_fd, self._worker_func_filename = tempfile.mkstemp(suffix="py")
		self._worker_func_file = os.fdopen(self._worker_func_fd, "w")
		self._worker_func_file.write("from mpi4py import MPI\n")
		self._worker_func_file.write("import tempfile\n")
		self._worker_func_file.write("import shutil\n")
		self._worker_func_file.write("import sys\n")
		self._worker_func_file.write("import subprocess\n")
		worker_func_source = textwrap.dedent(inspect.getsource(worker_func))
		self._worker_func_file.write(worker_func_source)
		self._worker_func_file.write("worker_func()\n")
		self._worker_func_file.close()
		
		self._child_comm = MPI.COMM_SELF.Spawn(sys.executable,
												args=[self._worker_func_filename],
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
		res = self._child_comm.recv(source=worker, tag=1)
		if res[1] is not 0:
			raise Exception(res[2])
		task.task_finished()
	
	def _cleanup(self):
		for worker in range(self._num_workers):
			self._child_comm.send(None, dest=worker, tag=0)
		self._child_comm.Disconnect()
		shutil.rmtree(path=self._worker_func_filename, ignore_errors=True)
	
	def get_status(self):
		return {}
