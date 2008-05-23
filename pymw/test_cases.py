import pymw
import pymw.interfaces
import pymw.interfaces.multicore_interface
import unittest

class TestPyMW(unittest.TestCase):
    def setUp(self):
        self.pymw_master = pymw.pymw.PyMW_Master()
    
    # Tests that getting the result of a non-submitted task returns an error
    def testGetResultNoSubmit(self):
        self.assertRaises(pymw.pymw.TaskException, self.pymw_master.get_result, 1234)
    
    # Tests that getting a result with no submitted tasks returns an error
    def testGetAnyResultNoSubmit(self):
        self.assertRaises(pymw.pymw.TaskException, self.pymw_master.get_result)

    # Tests that giving a non-existent worker executable returns an error
    def testBadExecutable(self):
        bad_task = self.pymw_master.submit_task(executable='dead_parrot', input_data=None)
        self.assertRaises(pymw.pymw.InterfaceException, self.pymw_master.get_result, bad_task)

    # Tests that giving a bad executable type returns an error
    def testBadExecutableType(self):
        self.assertRaises(TypeError, self.pymw_master.submit_task, executable=1)

    # Tests that using an invalid Python location returns an error
    def testBadPython(self):
        interface = pymw.interfaces.multicore_interface.MulticoreInterface(python_loc="/usr/local/dead_parrot/python")
        pymw_master = pymw.pymw.PyMW_Master(interface)
        task = pymw_master.submit_task(executable='null_worker.py', input_data=1)
        self.assertRaises(pymw.pymw.InterfaceException, pymw_master.get_result, task)

    # TODO: add test case for killing workers
    
    # Tests standard operation with null worker program
    def testStandardOperation(self):
        num_tasks = 10
        pymw_total = 0
        actual_total = 0
        tasks = [self.pymw_master.submit_task('null_worker.py', i) for i in range(num_tasks)]
        for task in tasks:
            my_task, next_val = self.pymw_master.get_result(task)
            pymw_total += next_val
        for i in range(num_tasks):
            actual_total += i
        
        self.assert_(pymw_total == actual_total)

#class TestPyMWStateSaveRestore(unittest.TestCase):
#    def setUp(self):
#        self.interface = pymw.interfaces.multicore_interface.MulticoreInterface()
#        self.pymw_master = pymw.pymw.PyMW_Master(self.interface, use_state_records=True)
#    
#    def testGetResults(self):
#        pymw_total = 0
#        for task in self.pymw_master._submitted_tasks:
#            my_task, next_val = self.pymw_master.get_result(task)
#            pymw_total += next_val
#        for i in range(self.num_tasks):
#            actual_total += i*i
#        self.assertEqual(actual_total, pymw_total)
#        self.pymw_master.cleanup()
#    
#    def testMakeTasks(self):
#        self.num_tasks = 10
#        tasks = [self.pymw_master.submit_task('worker.py', i) for i in range(self.num_tasks)]

if __name__ == '__main__':
        unittest.main()
