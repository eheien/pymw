from pymw import *
from pymw import interfaces
import unittest
import sys
import threading
import os
import signal
import tempfile
import logging

# TODO: add test for sending archives of files
# TODO: add test for sending modules

pymw_interface = None

# Null function to test standard operation
def null_worker(in_data):
	return in_data

# Function to test exception handling
def err_worker():
	return 0/0

# Function to test capture of stdout/stderr from workers
def print_worker():
	# TODO: replace these with print statements
	sys.stdout.write("stdout test")
	#print("stdout test")
	sys.stderr.write("stderr test")
	#print("stderr test", end=' ', file=sys.stderr)
	
# Function to test map section of map-reduce code
def square(list1):
	for i in range(len(list1)):
		list1[i] *= list1[i]
	return list1

# Function to test reduce section of map-reduce code
def plus(list2):
	return sum(list2)

def check_files(file_list):
	for fname in file_list:
		fp = open(fname, "r")
		data = fp.readlines()
		fp.close()
		if data[0].count("booga") == 0:
			return False
	
	return True

# Kills all children processes in case of a stuck test
def killAll():
	print()
	print("ERROR: Test failed to finish after 10 seconds, aborting.")
	print("WARNING: there may be unfinished child processes.")
	pgid = os.getpgid(0)
	os.killpg(pgid, signal.SIGKILL)
	os.abort()

# Simulates exceptions happening in various places in an interface
class BadInterface:
	def get_available_workers(self):
		if self.worker_err == 1:
			return 5
	
	def reserve_worker(self, worker):
		if self.worker_err == 1:
			raise Exception()
	
	def worker_finished(self, worker):
		if self.worker_err == 1:
			raise Exception()
	
	def execute_task(self, task, worker):
		if self.execute_err == 1:
			raise Exception("execution error")
		
		task.task_finished(Exception("no problem"))

	def get_status(self):
		if self.status_err == 1:
			return 1
		elif self.status_err == 2:
			raise Exception()

class TestBadInterface(unittest.TestCase):
	def setUp(self):
		self.bad_int = BadInterface()
		self.bad_int.status_err = 0
		self.bad_int.execute_err = 0
		self.bad_int.worker_err = 0
		self.pymw_master = pymw.PyMW_Master(interface=self.bad_int)
		self._kill_timer = threading.Timer(10, killAll)
		self._kill_timer.start()

	def tearDown(self):
		self._kill_timer.cancel()
	
	def testBadStatusReturn(self):
		"""Checking that a bad return from interface get_status() is properly handled"""
		self.bad_int.status_err = 1
		my_status = self.pymw_master.get_status()
		self.assert_("interface_status" in my_status)
		self.assert_(my_status["interface_status"] == "error")
		
	def testBadStatusException(self):
		"""Checking that an exception raised by get_status() is properly handled"""
		self.bad_int.status_err = 2
		my_status = self.pymw_master.get_status()
		self.assert_("interface_status" in my_status)
		self.assert_(my_status["interface_status"] == "error")
		
	def testBadTaskExecution(self):
		"""Checking that an exception raised by execute_task() is properly handled"""
		self.bad_int.execute_err = 1
		task = self.pymw_master.submit_task(null_worker)
		try:
			self.pymw_master.get_result(task)
		except Exception as e:
			self.assert_(e.args[0].count("execution error")>0)
		
	def testBadWorkerFuncs(self):
		"""Checking that failing worker reservation functions are properly handled"""
		self.bad_int.worker_err = 1
		task = self.pymw_master.submit_task(null_worker)
		try:
			my_task, my_val = self.pymw_master.get_result(task)
		except Exception as e:
			self.assert_(e.args[0].count("no problem")>0)

# TODO: add test case for killing workers
class TestPyMW(unittest.TestCase):
	def setUp(self):
		if pymw_interface:
			self.pymw_master = pymw.PyMW_Master(interface=pymw_interface())
			#self.pymw_master = pymw.PyMW_Master(interface=pymw_interface(pymw_interface_args))
		else:
			self.pymw_master = pymw.PyMW_Master()
		self._kill_timer = threading.Timer(10, killAll)
		self._kill_timer.start()

	def tearDown(self):
		self._kill_timer.cancel()
		
	def testGetResultNoSubmit(self):
		"""Checking that getting the result of a non-submitted task returns an error"""
		task = self.pymw_master.submit_task(executable=null_worker, input_data=(1,))
		self.assertRaises(pymw.TaskException, self.pymw_master.get_result, 1234)
		self.assertRaises(pymw.TaskException, self.pymw_master.get_result, [1, 2, 3, 4])
		my_task, next_val = self.pymw_master.get_result()
	
	def testGetAnyResultNoSubmit(self):
		"""Checking that getting a result with no submitted tasks returns an error"""
		self.assertRaises(pymw.TaskException, self.pymw_master.get_result)

	def testBadExecutable(self):
		"""Checking that giving a non-existent worker executable returns an error"""
		bad_task = self.pymw_master.submit_task(executable='dead_parrot')
		self.assertRaises(Exception, self.pymw_master.get_result, bad_task)
		self.assertRaises(Exception, self.pymw_master.submit_task, executable=2)

	def testBadExecutableType(self):
		"""Checking that giving a bad executable type returns an error"""
		self.assertRaises(pymw.TaskException, self.pymw_master.submit_task, executable=1)

	def testBadPython(self):
		"""Checking that using an invalid Python location returns an error"""
		interface = interfaces.generic.GenericInterface(python_loc="/usr/local/dead_parrot/python")
		pymw_master = pymw.PyMW_Master(interface)
		task = pymw_master.submit_task(executable=null_worker, input_data=(1,))
		self.assertRaises(Exception, pymw_master.get_result, task)

	def testProgramError(self):
		"""Checking that exceptions from the worker get passed back correctly"""
		task = self.pymw_master.submit_task(err_worker)
		if sys.version_info[0] < 3:
			try:
				self.pymw_master.get_result(task)
			except Exception as e:
				self.assert_(e.args[0].count("integer division or modulo by zero")>0)
		else:
			try:
				self.pymw_master.get_result(task)
			except Exception as e:
				self.assert_(e.args[0].count("int division or modulo by zero")>0)
			
	
	def testStdoutStderr(self):
		"""Checking that stdout and stderr are correctly routed from the workers"""
		task = self.pymw_master.submit_task(print_worker, modules=("sys",))
		my_task, res = self.pymw_master.get_result(task)
		self.assert_(my_task._stdout == "stdout test")
		self.assert_(my_task._stderr == "stderr test")
	
	def testMapReduce(self):
		"""Test standard operation of MapReduce class"""
		num_tasks = 10
		actual_total = 2870
		pymw_mapreduce=pymw.PyMW_MapReduce(self.pymw_master)
		task_MR = pymw_mapreduce.submit_task_mapreduce(square, plus, num_tasks, input_data=list(range(1,21)), modules=(), dep_funcs=())
		my_task, result = self.pymw_master.get_result(task_MR)
		self.assert_(sum(result) == actual_total)
		
	def testSendFiles(self):
		"""Test the packaging and sending of auxiliary data files."""
		num_files = 10
		file_list = [tempfile.mkstemp() for i in range(num_files)]
		
		name_list = []
		path_list = tuple([file[1] for file in file_list])
		for file in file_list:
			os.write(file[0], b"booga")
			os.close(file[0])
			name_list.append(file[1].split("/")[-1])
		
		task = self.pymw_master.submit_task(check_files, input_data=(name_list,), data_files=path_list)
		my_task, res = self.pymw_master.get_result(task)
		for file in file_list: os.remove(file[1])
		
		self.assert_(res)
		
	def testStandardOperation(self):
		"""Test standard operation with null worker program"""
		num_tasks = 10
		actual_total = num_tasks*(num_tasks-1)/2

		# Submit 3 sets of tasks
		tasks1 = [self.pymw_master.submit_task(null_worker, input_data=(i,)) for i in range(num_tasks)]
		tasks2 = [self.pymw_master.submit_task(null_worker, input_data=(i,)) for i in range(num_tasks)]
		tasks3 = [self.pymw_master.submit_task(null_worker, input_data=(i,)) for i in range(num_tasks)]
		
		# Test get_result for task-by-task result retrieval
		pymw_total = 0
		for task in tasks1:
			my_task, next_val = self.pymw_master.get_result(task)
			self.assert_(my_task.get_total_time() >= 0)
			self.assert_(my_task.get_execution_time() >= 0)
			pymw_total += next_val
		self.assert_(pymw_total == actual_total)
		
		# Test get_result for list based result retrieval
		pymw_total = 0
		for tnum in range(num_tasks):
			my_task, next_val = self.pymw_master.get_result(tasks2)
			self.assert_(my_task.get_total_time() >= 0)
			self.assert_(my_task.get_execution_time() >= 0)
			pymw_total += next_val
		self.assert_(pymw_total == actual_total)

		# Test get_result for arbitrary result retrieval
		pymw_total = 0
		for tnum in range(num_tasks):
			my_task, next_val = self.pymw_master.get_result()
			self.assert_(my_task.get_total_time() >= 0)
			self.assert_(my_task.get_execution_time() >= 0)
			pymw_total += next_val
		self.assert_(pymw_total == actual_total)

if __name__ == '__main__':
	if "--help" in sys.argv or "-h" in sys.argv:
		print("--help: display this help message")
		print("--num_workers n: Set the number of workers to use in the test.")
		print("--boinc project_home: Run tests on BOINC interface.")
		print("--condor: Run tests on Condor interface.")
		print("--ganga: Run tests on GANGA interface.")
		print("--grid_simulator: Run tests on grid simulation interface.")
		print("--mpi: Run tests on MPI interface.")
		print("--multicore: Run tests on multicore interface.")
		exit(0)

	if len(sys.argv) == 1 or "--generic" in sys.argv:
		print("|--------------------------------------|")
		print("| Running test with generic interface. |")
		print("|--------------------------------------|")
		pymw_interface = interfaces.generic.GenericInterface
		pymw_interface_args = ("")
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if len(sys.argv) == 1 or "--generic" in sys.argv:
		print("|----------------------------------|")
		print("| Running test with bad interface. |")
		print("|----------------------------------|")
		interface_suite = unittest.TestLoader().loadTestsFromTestCase(TestBadInterface)
		unittest.TextTestRunner(verbosity=2).run(interface_suite)

	# NOTE: some of these may fail because the tests will time out after 10 seconds.
	# NOTE: perhaps we should make the timeout variable?
	if "--boinc" in sys.argv:
		print("|------------------------------------|")
		print("| Running test with BOINC interface. |")
		print("|------------------------------------|")
		pymw_interface = interfaces.boinc.BOINCInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if "--condor" in sys.argv:
		print("|-------------------------------------|")
		print("| Running test with Condor interface. |")
		print("|-------------------------------------|")
		pymw_interface = interfaces.condor.CondorInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if "--ganga" in sys.argv:
		print("|------------------------------------|")
		print("| Running test with GANGA interface. |")
		print("|------------------------------------|")
		pymw_interface = interfaces.ganga.GANGAInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if "--grid_simulator" in sys.argv:
		print("|---------------------------------------------|")
		print("| Running test with grid simulator interface. |")
		print("|---------------------------------------------|")
		pymw_interface = interfaces.grid_simulator.GridSimulatorInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if "--mpi" in sys.argv:
		print("|----------------------------------|")
		print("| Running test with MPI interface. |")
		print("|----------------------------------|")
		pymw_interface = interfaces.mpi.MPIInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

	if "--multicore" in sys.argv:
		print("|----------------------------------------|")
		print("| Running test with MultiCore interface. |")
		print("|----------------------------------------|")
		pymw_interface = interfaces.multicore.MulticoreInterface
		pymw_interface_args = ()
		pymw_suite = unittest.TestLoader().loadTestsFromTestCase(TestPyMW)
		unittest.TextTestRunner(verbosity=2).run(pymw_suite)

