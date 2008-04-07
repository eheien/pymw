import pymw
import base_interface
import unittest

class TestPyMW(unittest.TestCase):
        def setUp(self):
                self.interface = base_interface.BaseSystemInterface()
                self.pymw_master = pymw.PyMW_Master(self.interface)

        def testGetResultNoSubmit(self):
                self.assertRaises(pymw.TaskException,
                                  self.pymw_master.get_result,1234)

if __name__ == '__main__':
        unittest.main()
