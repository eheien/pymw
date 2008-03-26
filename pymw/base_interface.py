import threading
import tempfile
import shutil
import subprocess
import os
import sys
import time
#import boinc

#static jobID

class Task:
	def __init__(self, executable, input_name, output_name):
		self.executable_name = executable
		self.input_file_name = input_name
		self.output_file_name = output_name

def wait_for_worker_to_finish(worker_processes, task_finish_notify, finished_tasks):
	while True:
		for process in worker_processes:
			res = process[0].poll()
			if res is not None:
				worker_processes.remove(process)
				task_finish_notify.acquire()
				finished_tasks.append(process[1])
				task_finish_notify.release()
				return

def executor(task_list, task_finish_notify, finished_tasks, num_workers):
	worker_processes = []
	for task in task_list:
		if len(worker_processes) >= num_workers:
			wait_for_worker_to_finish(worker_processes, task_finish_notify, finished_tasks)
		args = "python \""+task.executable_name+"\" \""+task.input_file_name+"\" \""+task.output_file_name+"\""
		#print args
		worker = subprocess.Popen(args, creationflags=0x08000000)#, stdout=subprocess.PIPE)
		worker_processes.append([worker, task])

	while len(worker_processes) > 0:
		wait_for_worker_to_finish(worker_processes, task_finish_notify, finished_tasks)
		
class BaseSystemInterface:
	def __init__(self):
		self.task_list = []

	def submit_task(self, task):
		self.task_list.append(task)
		
	def get_unique_file_name(self, dirname):
		uniq_file, file_name = tempfile.mkstemp(dir=dirname)
		os.close(uniq_file)
		return file_name

	def start_execution(self, num_workers):
		self.unfinished_task_list = self.task_list[:]
		self.task_finish_notify = threading.Lock()
		self.finished_tasks = []
		self.executor_thread = threading.Thread(target=executor, args=(self.task_list,self.task_finish_notify, self.finished_tasks, num_workers))
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
		result_file = open(task.output_file_name)
		self.unfinished_task_list.remove(task)
		self.task_finish_notify.release()
		return result_file

	def get_status(self):
		return {"status" : "no status"}
	
	def cleanup(self):
		for task in self.task_list:
			os.remove(task.input_file_name)
			os.remove(task.output_file_name)

