===========================
Writing a Program with PyMW
===========================

The general idea behind using PyMW is very simple and can be described in 3 steps.

1. Create a master and associated interface.
2. Submit tasks to the master. These tasks will be computed on the interface.
3. Get the results of the computation.

First the program must import the PyMW module::
	
	import pymw

Next, write the function you want to run in parallel.  For example::

	(something).

Next, create an interface object and PyMW_Master object. These will provide the functions needed to submit tasks and get results. A list of interfaces available in PyMW 0.x is available in Section x.::

	pymw_interface = GenericInterface()
	pymw_master = PyMW_Master(pymw_interface)

Submit a task to the master.  This will return a PyMW_Task object you can later use to refer to the task.::

	my_task = pymw_master.submit_task()

Get the result from the master.  This will return the result of the computation, as well as the associated PyMW_Task.::

	final_result, the_task = pymw_master.get_result(my_task)
