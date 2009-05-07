from pymw import *
import unittest
import sys

# Null function to test standard operation
def null_worker(in_data):
    return in_data

# Function to test exception handling
def err_worker():
    return 0/0

# Function to test capture of stdout/stderr from workers
def print_worker():
    print "stdout test",
    print >> sys.stderr, "stderr test",
    
def square(list1):
    for i in range(len(list1)):
        list1[i] *= list1[i]
    return list1

def plus(list2):
    return sum(list2)

class TestPyMW(unittest.TestCase):
    def setUp(self):
        self.pymw_master = pymw.PyMW_Master()
    
    # Tests that getting the result of a non-submitted task returns an error
    def testGetResultNoSubmit(self):
        task = self.pymw_master.submit_task(executable=null_worker, input_data=(1,))
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result, 1234)
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result, [1, 2, 3, 4])
        my_task, next_val = self.pymw_master.get_result()
    
    # Tests that getting a result with no submitted tasks returns an error
    def testGetAnyResultNoSubmit(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result)

    # Tests that giving a non-existent worker executable returns an error
    def testBadExecutable(self):
        bad_task = self.pymw_master.submit_task(executable='dead_parrot')
        self.assertRaises(Exception, self.pymw_master.get_result, bad_task)
        self.assertRaises(Exception, self.pymw_master.submit_task, executable=2)

    # Tests that giving a bad executable type returns an error
    def testBadExecutableType(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.submit_task, executable=1)

    # Tests that using an invalid Python location returns an error
    def testBadPython(self):
        interface = pymw.interfaces.generic.GenericInterface(python_loc="/usr/local/dead_parrot/python")
        pymw_master = pymw.PyMW_Master(interface)
        task = pymw_master.submit_task(executable=null_worker, input_data=(1,))
        self.assertRaises(Exception, pymw_master.get_result, task)

    # Tests that exceptions from the worker get passed back correctly
    def testProgramError(self):
        task = self.pymw_master.submit_task(err_worker)
        try:
            self.pymw_master.get_result(task)
        except Exception, e:
            self.assert_(e[1].count("integer division or modulo by zero")>0)
    
    # Tests that stdout and stderr are correctly routed from the workers
    def testStdoutStderr(self):
        task = self.pymw_master.submit_task(print_worker, modules=("sys",))
        my_task, res = self.pymw_master.get_result(task)
        self.assert_(my_task._stdout == "stdout test")
        self.assert_(my_task._stderr == "stderr test")
    
    # TODO: add test case for killing workers
    
    # Tests standard operation with null worker program
    def testStandardOperation(self):
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
        
        # Test get_result for list based retrieval
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
        
        # MapReduce test
        actual_total = 2870
        pymw_mapreduce=pymw.PyMW_MapReduce(self.pymw_master)
        task_MR = pymw_mapreduce.submit_task_mapreduce(square, plus, num_tasks, input_data=range(1,21), modules=(), dep_funcs=())
        my_task, result = self.pymw_master.get_result(task_MR)
        self.assert_(sum(result) == actual_total)

if __name__ == '__main__':
        unittest.main()
