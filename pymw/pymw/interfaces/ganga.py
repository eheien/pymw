#!/usr/bin/env python
"""Provide a GANGA interface for master worker computing with PyMW.
"""

__author__ = "Wayne San <waynesan@twgrid.org>"
__date__ = "6 May 2010"

import subprocess
import os
import time
import sys
import shutil
import pickle
import threading

GANGA_TEMPLATE = """j = Job(name = 'PyMW Worker')
j.backend = %(GANGA_BKN)s
j.application = Executable()
j.application.exe = File('%(PYTHON_LOC)s')
j.application.args = ['%(PYMW_EXEC_NAME)s', '%(PYMW_INPUT_NAME)s', '%(PYMW_OUTPUT_NAME)s']
j.inputsandbox = ['%(PYMW_EXEC_FILE)s', '%(PYMW_INPUT_FILE)s']
j.outputsandbox = ['%(PYMW_OUTPUT_NAME)s']
j.submit()
print j.outputdir"""

class GANGAInterface:
	"""Provides a simple interface for GANGA."""
	def __init__(self, python_loc="", ganga_loc="", ganga_bkn=""):
		if sys.platform.startswith("win"):
			raise Exception("This interface is not support Windows platform.")
		else:
			if python_loc != "": self._python_loc = python_loc
			else: self._python_loc = sys.executable # "/usr/local/bin/python"
			if ganga_loc != "": self._ganga_loc = ganga_loc
			else: self._ganga_loc = "~/Ganga/bin/ganga"
			if ganga_bkn != "": self._ganga_bkn = ganga_bkn
			else: self._ganga_bkn = "Local()"
		self._task_list = []
		self._task_list_lock = threading.Lock()
		self._result_checker_running = False
		self.pymw_interface_modules = "pickle", "sys", "traceback", "cStringIO"
		
	def _get_finished_tasks(self):
		while True:
			self._task_list_lock.acquire()
			try:
				try:
					for entry in self._task_list:
						task, output_dir, submit_file_name = entry
						out_file = output_dir + os.path.basename(task._output_arg)
						#sys.stderr.write("Output File: %s\n" % out_file)
						# Check for the output files
						# TODO: also check for an error file
						if os.path.isfile(out_file):
							shutil.copy(out_file, task._output_arg)
							task.task_finished()
							self._task_list.remove(entry)
					if len(self._task_list) == 0:
						self._result_checker_running = False
						return
				except Exception as data:
					# just in case a higher-level process is hiding exceptions
					# log any exception that occures and then re-raise it
					print(("GANGAInterface._get_finished_tasks failed: %s" % data))
					self._result_checker_running = False
					raise
				#end try
			finally:
				self._task_list_lock.release()
			#end try
			time.sleep(0.5)
		#end while
	
	def execute_task(self, task, worker):
		# Create a template for this task
		ganga_template = GANGA_TEMPLATE % { "GANGA_BKN": self._ganga_bkn,
											"PYTHON_LOC": self._python_loc,
											"PYMW_EXEC_FILE": task._executable,
											"PYMW_EXEC_NAME": os.path.basename(task._executable),
											"PYMW_INPUT_FILE": task._input_arg,
											"PYMW_INPUT_NAME": os.path.basename(task._input_arg),
											"PYMW_OUTPUT_NAME": os.path.basename(task._output_arg) }
		
		# Write the template to a file
		submit_file_name = os.path.join(os.path.dirname(task._input_arg), task._task_name + "_ganga")
		submit_file = open(submit_file_name,"w")
		submit_file.write(ganga_template)
		submit_file.close()
		
		if sys.platform.startswith("win"): cf=0x08000000
		else: cf=0
		
		# Submit the template file through ganga 
		submit_process = subprocess.Popen(args=[self._ganga_loc, "--quiet", submit_file_name],
								creationflags=cf, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		# Wait for the process to finish
		proc_stdout, proc_stderr = submit_process.communicate()
		
		# TODO: check stdout for problems
		if proc_stderr != "" and proc_stderr.find("Error"):
			raise Exception("Excuting ganga failed with error:\n%s" % proc_stderr)
		
		self._task_list_lock.acquire()
		# TODO: filter stdout for output_dir
		self._task_list.append([task, proc_stdout.strip(), submit_file_name])
		self._task_list_lock.release()
		
		if not self._result_checker_running:
			self._result_checker_running = True
			self._task_finish_thread = threading.Thread(target=self._get_finished_tasks)
			self._task_finish_thread.start()
	
	def _cleanup(self):
		self._scan_finished_tasks = False
