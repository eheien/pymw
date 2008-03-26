import thread
import tempfile
import shutil
import os
#import boinc

#static jobID

def executor(task_list):
	for task in task_list:
		#print "Executing", task[0], task[1], task[2]
		shutil.copyfile(task[1], "input")
		execfile(task[0])
		shutil.move("output", task[2])
		f = open(task[2]+"_done", "w")
		f.close()
		os.remove("input")

class BaseSystemInterface:
	def __init__(self):
		self.task_list = []

	def submit_task(self, executable, input_file, output_file):
		self.task_list.append([executable, input_file, output_file])
		
	def get_unique_file_name(self, dirname):
		uniq_file, file_name = tempfile.mkstemp(dir=dirname)
		os.close(uniq_file)
		return file_name

	def start_execution(self):
		self.unfinished_task_list = self.task_list[:]
		thread.start_new_thread(executor, (self.task_list,))

	def execution_finished(self):
		num_tasks = len(self.task_list)
		num_unfinished = len(self.unfinished_task_list)
		#print str(num_tasks-num_unfinished) + " of " + str(num_tasks) + " tasks are done"
		if num_unfinished > 0:
			return False
		else:
			return True

	def get_next_finished_task(self):
		for task in self.unfinished_task_list:
			try:
				f = open(task[2]+"_done",'r')
				f.close()
				result_file = open(task[2])
				self.unfinished_task_list.remove(task)
				return result_file
			except:
				a = 1
		return None

	def cleanup(self):
		for task in self.task_list:
			os.remove(task[1])
			os.remove(task[2])
			os.remove(task[2]+"_done")

