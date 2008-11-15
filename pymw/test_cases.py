from pymw import *
import unittest
    
# Null function to test standard operation
def null_worker(in_data):
    return in_data

# Function to test exception handling
def err_worker():
    return 0/0

class TestPyMW(unittest.TestCase):
    def setUp(self):
        self.pymw_master = pymw.PyMW_Master()
    
    # Tests that getting the result of a non-submitted task returns an error
    def testGetResultNoSubmit(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result, 1234)
    
    # Tests that getting a result with no submitted tasks returns an error
    def testGetAnyResultNoSubmit(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result)

    # Tests that giving a non-existent worker executable returns an error
    def testBadExecutable(self):
        bad_task = self.pymw_master.submit_task(executable='dead_parrot', input_data=None)
        self.assertRaises(Exception, self.pymw_master.get_result, bad_task)

    # Tests that giving a bad executable type returns an error
    def testBadExecutableType(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.submit_task, executable=1)

    # Tests that using an invalid Python location returns an error
    def testBadPython(self):
        interface = pymw.interfaces.multicore.MulticoreInterface(python_loc="/usr/local/dead_parrot/python")
        pymw_master = pymw.PyMW_Master(interface)
        task = pymw_master.submit_task(executable='null_worker.py', input_data=1)
        self.assertRaises(Exception, pymw_master.get_result, task)

    # Tests that exceptions from the worker get passed back correctly
    def testProgramError(self):
        task = self.pymw_master.submit_task(err_worker)
        try:
            self.pymw_master.get_result(task)
        except Exception, e:
            self.assert_(e[1].count("integer division or modulo by zero")>0)
        
    # TODO: add test case for killing workers
    
    # Tests standard operation with null worker program
    def testStandardOperation(self):
        num_tasks = 10
        pymw_total = 0
        actual_total = 0
        tasks = [self.pymw_master.submit_task(null_worker, input_data=(i,)) for i in range(num_tasks)]
        for task in tasks:
            my_task, next_val = self.pymw_master.get_result(task)
            pymw_total += next_val
        for i in range(num_tasks):
            actual_total += i
        
        self.assert_(pymw_total == actual_total)

if __name__ == '__main__':
        unittest.main()
