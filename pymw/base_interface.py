import threading
import tempfile
import shutil
import os
#import boinc

#static jobID

def executor(task_list, task_finish_notify, finished_tasks):
	for task in task_list:
		#print "Executing", task[0], task[1], task[2]
		shutil.copyfile(task[1], "input")
		execfile(task[0])
		shutil.move("output", task[2])
		os.remove("input")
		
		task_finish_notify.acquire()
		finished_tasks.append(task)
		task_finish_notify.release()
		
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
		self.task_finish_notify = threading.Lock()
		self.finished_tasks = []
		self.executor_thread = threading.Thread(target=executor, args=(self.task_list,self.task_finish_notify, self.finished_tasks))
		self.executor_thread.run()

	def execution_finished(self):
		if len(self.unfinished_task_list) > 0: return False
		return True

	def get_next_finished_task(self):
		self.task_finish_notify.acquire()
		if len(self.finished_tasks) <= 0:
			self.task_finish_notify.release()
			return None
		
		task = self.finished_tasks.pop()
		result_file = open(task[2])
		self.unfinished_task_list.remove(task)
		self.task_finish_notify.release()
		return result_file

	def cleanup(self):
		for task in self.task_list:
			os.remove(task[1])
			os.remove(task[2])

