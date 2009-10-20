===========================
Writing a Program with PyMW
===========================

The general idea behind using PyMW can be described in 3 steps.

1. Create a master and associated interface.
2. Submit tasks to the master. These tasks will be computed on the interface using the workers.
3. Get the results of the computation.

First the program must import the PyMW module::
	
	from pymw import pymw

Next, write the function you want to run in parallel.  For example::

	def square(x): return x*x

Next, create an interface object and register it with a PyMW_Master object. These will provide the functions needed to submit tasks and get results. A list of interfaces available in PyMW is available in Section x.::

	pymw_interface = pymw.interfaces.generic.GenericInterface()
	pymw_master = pymw.PyMW_Master(pymw_interface)

Submit some tasks to the master.  These will return PyMW_Task objects you can later use to refer to the task.::

	my_tasks = [pymw_master.submit_task(square, (i,)) for i in range(10)]

Get the result from the master.  This will return the result of the computation, as well as the associated PyMW_Task.::

	for task in my_tasks:
		task_obj, result = pymw_master.get_result(task)
		print result

