==============
PyMW Internals
==============

This section describes the internal operation of PyMW and the interfaces. It is intended primarily for developers.

PyMW consists of 4 layers of abstraction.
The top layer represents the user, who calls PyMW to perform a computation.
The second layer represents the master, instantiated as a PyMW_Master object. The user interacts with this layer through the PyMW_Master object.
The third layer represents the interface between the master and the underlying workers. The user does not need to interact with this layer.
The fourth layer represents the underlying computation and communication hardware. This will only be touched by the interface layer.

-----------
PyMW Master
-----------
The PyMW_Master object is a Python class used to submit computational tasks, get results of submitted tasks, and check task status.  The key functions for interacting with the PyMW_Master are:

.. function:: submit_task(executable, input_data=None, modules=(), dep_funcs=(), data_files=())

Creates and submits a task to the interface associated with this PyMW_Master.  The task is specified by executable and can be a Python function or Python script.  The input_data is a tuple of arguments passed to the executable.  The modules, dep_funcs and data_files allow the user to specify additional modules, functions and data files to be packaged with the task.

.. function:: get_result(task=None, blocking=True)

Gets the result of a task submitted to PyMW_Master.  If the task is None, this function will return any completed task.  If blocking is true, this function will wait until a task is completed before returning, otherwise it will return None if no task is completed.  Exceptions caused by executing the task will be raised when this function is called.

--------------
PyMW Interface
--------------
A PyMW interface is a Python class with a required set of member functions.  These functions perform task and worker related activities relevant to the target environment of the interface. Other functions may be implemented to improve functionality, but are not required.

There is only one required PyMW interface class functions:

.. function:: execute_task(task, worker)

Executes the specified task on the interface using the provided worker.
When the task has completed, the interface must call task.task_finished().
If this function raises an exception, the task will be marked as erroneous and the exception returned to the user through get_result().

The remaining functions are optional.  These may be used to improve functionality of the interface in regards to worker management.

.. function:: get_available_workers()

Returns a list of workers available for use with this interface.
If this function is not defined in the interface or an exception is raised, a value of None will be used for all workers.

.. function:: reserve_worker(worker)

Reserves the specified worker.
If this function is not defined in the interface, there will be no effect. Exceptions raised by this function are ignored.

.. function:: worker_finished(worker)

Notifies the interface that the specified worker has completed the computation.
If this function is not defined in the interface, there will be no effect. Exceptions raised by this function are ignored.

.. function:: get_status()

Returns a dictionary containing interface specific status information. Raising an exception or returning a non-dictionary object is treated the same as returning an empty dictionary.

.. function:: _cleanup()

Called by PyMW at the end of the program. Any interface specific cleanup should be performed here.
