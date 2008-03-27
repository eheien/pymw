from pickle import *
from base_interface import *
import sys
import threading
#import boinc

class _SyncQueue:
	def __init__(self):
		self.lock = threading.Lock()
		self.queue = []
	
	def __len__(self):
		self.lock.acquire()
		q_len = len(self.queue)
		self.lock.release()
		return q_len
	
	def append(self, item):
		self.lock.acquire()
		self.queue.append(item)
		self.lock.release()
	
	def pop(self):
		self.lock.acquire()
		try:
			item = self.queue.pop()
			return item
		finally:
			self.lock.release()
		return None
	
	def remove(self, item):
		self.lock.acquire()
		self.queue.remove(item)
		self.lock.release()

	def contains(self, item):
		self.lock.acquire()
		n = self.queue.count(item)
		self.lock.release()
		if n != 0: return True
		else: return False

class _Task:
	def __init__(self, executable, input_data, task_finish_event, finished_list):
		self.executable = executable
		self.input_data = input_data
		self.output_data = None
		self.task_finish_event = task_finish_event
		self.finished_list = finished_list
	
	def task_finished(self):
		self.finished_list.append(self)
		self.task_finish_event.set()
	
class PyMW_Master:
	def __init__(self, interface):
		self.interface = interface
		self._exit = False
		self._submitted_tasks = _SyncQueue()
		self._queued_tasks = _SyncQueue()
		self._finished_tasks = _SyncQueue()
		self._task_queue_sem = threading.Semaphore(0)
		self._task_finish_event = threading.Event()
		scheduler_thread = threading.Thread(target=self._scheduler)
		scheduler_thread.start()
	
	def __del__(self):
		self._exit = True
	
	# Note: it is possible for two different Masters to assign tasks to the same worker
	def _scheduler(self):
		while not self._exit:
			self._task_queue_sem.acquire(blocking=True)
			next_task = self._queued_tasks.pop()
			worker = self.interface.reserve_worker()
			self.interface.execute_task(next_task, worker)
		# Kill tasks that are still executing?
	
	def submit_task(self, executable, input_data):
		new_task = _Task(executable, input_data, self._task_finish_event, self._finished_tasks)
		self._submitted_tasks.append(new_task)
		self._queued_tasks.append(new_task)
		self._task_queue_sem.release()
		return new_task
	
	def wait_for_task_finish(self, task):
		if not self._submitted_tasks.contains(task):
			return None
		
		while True:
			self._task_finish_event.clear()
			if self._finished_tasks.contains(task):
				return task.output_data
			self._task_finish_event.wait()
	
	def get_status(self):
		return self.interface.get_status()
	
def pymw_get_input():
	obj = Unpickler(sys.stdin).load()
	return obj

def pymw_return_output(output):
	Pickler(sys.stdout).dump(output)

