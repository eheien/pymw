import subprocess
import threading
import cPickle
import pymw

class _Worker:
	def __init__(self, finish_sem):
		self._active_task = None
		self._finish_sem = finish_sem
	
	def wait_for_finish(self):
		self._process.wait()
		self._active_task.output_data = cPickle.Unpickler(self._process.stdout).load()
		self._active_task.task_finished()
		self._finish_sem.release()

class BaseSystemInterface:
	def __init__(self, num_workers):
		self._worker_sem = threading.Semaphore(num_workers)
	
	def reserve_worker(self):
		self._worker_sem.acquire(blocking=True)
		new_worker = _Worker(self._worker_sem)
		return new_worker
	
	def execute_task(self, task, worker):
		worker._active_task = task
		worker._process = subprocess.Popen(args=["/usr/local/bin/python", task.executable],
										  stdin=subprocess.PIPE, stdout=subprocess.PIPE)#, creationflags=0x08000000)
		cPickle.Pickler(worker._process.stdin).dump(task.input_data)
		worker._process.stdin.close()
		worker_thread = threading.Thread(target=worker.wait_for_finish)
		worker_thread.start()

	def get_status(self):
		return {}#{"num_workers" : self._num_workers}#, "num_active_workers": len(self._active_workers)}
