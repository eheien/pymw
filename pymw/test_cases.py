import pymw
import base_interface
import unittest

class TestPyMW(unittest.TestCase):
    def setUp(self):
        self.interface = base_interface.BaseSystemInterface()
        self.pymw_master = pymw.PyMW_Master(self.interface)

    def tearDown(self):
        self.pymw_master.cleanup()
    
    def testGetResultNoSubmit(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result, 1234)
    
    def testGetAnyResultNoSubmit(self):
        self.assertRaises(pymw.TaskException, self.pymw_master.get_result)

    def testBadExecutable(self):
        bad_task = self.pymw_master.submit_task(executable='dead_parrot', input_data=None)
        self.assertRaises(pymw.InterfaceException, self.pymw_master.get_result, bad_task)

    def testBadPython(self):
        interface = base_interface.BaseSystemInterface(python_loc="/usr/local/dead_parrot/python")
        pymw_master = pymw.PyMW_Master(interface)
        task = pymw_master.submit_task(executable='worker.py', input_data=1)
        self.assertRaises(pymw.InterfaceException, pymw_master.get_result, task)
        pymw_master.cleanup()

    def testStandardOperation(self):
        pymw_total = 0
        actual_total = 0
        num_tasks = 10
        tasks = [self.pymw_master.submit_task('worker.py', i) for i in range(1,num_tasks)]
        for task in tasks:
            pymw_total += self.pymw_master.get_result(task).value
        for i in range(num_tasks):
            actual_total += i*i
        self.assertEqual(actual_total, pymw_total)

class TestPyMWStateSaveRestore(unittest.TestCase):
    def setUp(self):
        self.interface = base_interface.BaseSystemInterface(python_loc="/usr/local/bin/python")
        self.pymw_master = pymw.PyMW_Master(self.interface, use_state_records=True)
    
    def testGetResults(self):
        pymw_total = 0
        for task in self.pymw_master._submitted_tasks:
            pymw_total += self.pymw_master.get_result(task).value
        for i in range(self.num_tasks):
            actual_total += i*i
        self.assertEqual(actual_total, pymw_total)
        self.pymw_master.cleanup()
    
    def testMakeTasks(self):
        self.num_tasks = 10
        tasks = [self.pymw_master.submit_task('worker.py', i) for i in range(1, self.num_tasks)]
    
if __name__ == '__main__':
        unittest.main()
