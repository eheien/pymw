==============
PyMW Internals
==============

This section describes the internal operation of PyMW and the interfaces. It is intended primarily for developers.

PyMW consists of 4 layers of abstraction as shown in Figure xxx.
The top layer represents the user, who calls PyMW to perform a computation.
The second layer represents the master, instantiated as a PyMW_Master object. The user interacts with this layer through the PyMW_Master object.
The third layer represents the interface between the master and the underlying workers. The user does not need to interact with this layer.
The fourth layer represents the underlying computation and communication hardware. This will only be touched by the interface layer.

-----------
PyMW Master
-----------
The PyMW_Master object is a Python class used to submit computational tasks, get results of submitted tasks, and check task status.

^^^^^^^^^^^^^^^^^^^^^^^^
Writing a PyMW Interface
^^^^^^^^^^^^^^^^^^^^^^^^
A PyMW interface is a Python class with a required set of member functions. Other functions may be implemented to improve functionality, but are not required.

The PyMW interface class functions are:

(required)

.. function:: execute_task(task, worker)
Executes the specified task on the interface using the provided worker.
When the task has completed, the interface must call task.task_finished() in this function or another (see later discussion).
If this function raises an exception, the task will be marked as erroneous and the exception returned to the user through get_result().

(optional)

.. function:: get_available_workers()
Returns a list of workers available for use with this interface.
If this function is not defined in the interface, a value of None will be used for all workers.

.. function:: reserve_worker(worker)
Reserves the specified worker.
If this function is not defined in the interface, there will be no effect. If this function raises an exception, ***.

.. function:: worker_finished(worker)
Notifies the interface that the specified worker has completed the computation.
If this function is not defined in the interface, there will be no effect. If this function raises an exception, ***.

.. function:: get_status()
Returns a dictionary containing interface specific status information. Raising an exception or returning a non-dictionary object is treated the same as returning an empty dictionary.

.. function:: _cleanup()
Called by PyMW at the end of the program. Any interface specific cleanup should be performed here.
