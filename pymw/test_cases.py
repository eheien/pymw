import pymw
import base_interface
import app_types
import unittest

class TestPyMW(unittest.TestCase):
        def setUp(self):
                self.interface = base_interface.BaseSystemInterface()
                self.pymw_master = pymw.PyMW_Master(self.interface)

        def testGetResultNoSubmit(self):
                self.assertRaises(pymw.TaskException,
                                  self.pymw_master.get_result,1234)

        def testBadExecutable(self):
                bad_task = self.pymw_master.submit_task(executable='dead_parrot', input_data=None)
                self.assertRaises(pymw.InterfaceException,
                                  self.pymw_master.get_result,bad_task)

        def testStandardOperation(self):
                pymw_total = 0
                actual_total = 0
                num_tasks = 10
                tasks = [self.pymw_master.submit_task('worker.py', app_types.Input(i))
                         for i in range(num_tasks)]
                for task in tasks:
                        pymw_total += self.pymw_master.get_result(task).value
                for i in range(num_tasks):
                        actual_total += i*i
                self.assertEqual(actual_total, pymw_total)

if __name__ == '__main__':
        unittest.main()
