import sys
import threading
import cPickle
import time

class _SyncQueue:
	def __init__(self):
		self._lock = threading.Lock()
		self._queue = []
	
	def __len__(self):
		self._lock.acquire()
		q_len = len(self.queue)
		self._lock.release()
		return q_len
	
	def append(self, item):
		self._lock.acquire()
		self._queue.append(item)
		self._lock.release()
	
	def pop(self):
		self._lock.acquire()
		try:
			item = self._queue.pop()
			return item
		except:
			return None
		finally:
			self._lock.release()
	
	def remove(self, item):
		self._lock.acquire()
		self._queue.remove(item)
		self._lock.release()

	def contains(self, item):
		self._lock.acquire()
		n = self._queue.count(item)
		self._lock.release()
		if n != 0: return True
		else: return False

class _Task:
	def __init__(self, executable, input_data, task_finish_event, finished_list):
		self.executable = executable
		self.input_data = input_data
		self.output_data = None
		self._create_time = time.time()
		self._execute_time = 0
		self._finish_time = 0
		self._task_finish_event = task_finish_event
		self._finished_list = finished_list
	
	def task_finished(self):
		self._finish_time = time.time()
		self._finished_list.append(self)
		self._task_finish_event.set()

	def get_total_time(self):
		if self._finish_time != 0:
			return self._finish_time - self._create_time
		else:
			return None

	def get_execution_time(self):
		if self._finish_time != 0:
			return self._finish_time - self._execute_time
		else:
			return None

class Scheduler:
	def __init__(self, queued_tasks, task_queue_sem, interface):
		self._finished = False
		self._queued_tasks = queued_tasks
		self._task_queue_sem = task_queue_sem
		self.interface = interface
		_scheduler_thread = threading.Thread(target=self._scheduler)
		_scheduler_thread.start()
	
	# Note: it is possible for two different Masters to assign tasks to the same worker
	def _scheduler(self):
		while True:
			self._task_queue_sem.acquire(blocking=True)
			if self._finished: return
			next_task = self._queued_tasks.pop()
			next_task._execute_time = time.time()
			worker = self.interface.reserve_worker()
			self.interface.execute_task(next_task, worker)

	def _exit(self):
		self._finished = True
		self._task_queue_sem.release()

class PyMW_Master:
	def __init__(self, interface):
		self.interface = interface
		self._exit = False
		self._submitted_tasks = _SyncQueue()
		self._queued_tasks = _SyncQueue()
		self._finished_tasks = _SyncQueue()
		self._task_queue_sem = threading.Semaphore(0)
		self._task_finish_event = threading.Event()
		self._scheduler = Scheduler(self._queued_tasks, self._task_queue_sem, self.interface)
	
	def __del__(self):
		self._scheduler._exit()
	
	def _save_state(self):
		print "save state"
		self.interface._save_state()
		
	def _restore_state(self):
		print "restore state"
		self.interface._restore_state()
		
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
		status = self.interface.get_status()
		status["tasks"] = self._submitted_tasks
		return status

