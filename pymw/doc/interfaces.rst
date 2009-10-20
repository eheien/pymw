===============
PyMW Interfaces
===============

PyMW provides different interfaces for a variety of computation environments.

^^^^^^^
Generic
^^^^^^^
Uses multiple processes spawned with subprocess.Popen() on a single machine to perform tasks.  This interface requires no special setup, and works with Python 2.5 and above.

^^^
MPI
^^^
Uses Python module pyMPI to execute tasks on MPI enabled clusters.  This interface requires `pyMPI <http://pympi.sourceforge.net/>`_ to be installed.

^^^^^^
Condor
^^^^^^
Uses the `Condor desktop grid system <http://www.cs.wisc.edu/condor/>`_ to execute tasks.  Currently only works with Windows.

^^^^^^^^^^^^^^
Grid Simulator
^^^^^^^^^^^^^^
The grid simulator interface is different from other interfaces in that it is not meant to perform actual tasks. Instead, the function used as the task is called to determine how long a task will take to execute on a given worker.  This interface can be used to test different scheduling algorithms before deploying a system.  It requires no special setup.

^^^^^
BOINC
^^^^^
Uses the `BOINC volunteer computing system <http://boinc.berkeley.edu/>`_ to
perform tasks. To create a BOINC application using PyMW one must:

* Setup a BOINC server
* Create a new project using the tools/create_project script
* Pass the path to the new project as the BOINC interface's "project_home"
  parameter
* Optionally, pass the path to the custom worker app (such as PyBOINC, discussed
  below) and any arguments needed to run it. PyBOINC requires one argument,
  the name of the zipped Python library to use for execution without the
  platform name (i.e. 1.00_python26.zip)

Without using a custom worker app, such as
`PyBOINC <http://bitbucket.org/jeremycowles/pyboinc>`_, Python is required to be
installed on the client machines and must be available on the PATH environment
variable (i.e. typing "python" will run the interpreter from any directory).
However, by using PyBOINC, no interpreter is needed on the client machine and
no assumptions are made about the PATH evnironment variable.

Creating a server is relatively simple using the VM Image and is a good way to
get up and running quickly with BOINC. After you have setup
a BOINC VM, the Python Apps page in the BOINC wiki provides a step-by-step
walkthrough of how to run PyMW apps using the VM.

.. seealso::

   http://boinc.berkeley.edu/trac/wiki/QuickStart
      BOINC Quick Start

   http://boinc.berkeley.edu/trac/wiki/VmServer
      BOINC Virtual Machine Image
      
   http://boinc.berkeley.edu/trac/wiki/PythonApps
       Using PyMW with BOINC

Automatic Project Setup
"""""""""""""""""""""""
Upon every execution of a PyMW application, the BOINC interface will perform the
following tasks to ensure that the BOINC application is setup properly:

* Checks that the proper BOINC daemons are installed (assimilator, validator,
  transitioner, file_deleter and feeder)
* Checks the Project for an existing installation of a PyMW BOINC application,
  if it does not exist, one is created
* If no custom worker app is specified, default workers that simply launch
  "Python [script] [input] [output]" are installed for Windows, Linux and Mac
* If a custom worker is specified, it's files are copied from <workerpath>/linux
  /windows /apple into the appropriate BOINC app directories (more on this below)
* Recycles the daemon processes by calling `bin/stop` followed by `bin/start`
* Creates a new batch ID based on the current UNIX time, all BOINC work units
  created are assigned this ID
* Upon completion, clears the batch ID so that associated work units and files
  can be deleted from BOINC

Customizing BOINC Job Creation
""""""""""""""""""""""""""""""
When creating BOINC work units, it is assumed that you want to generate two
initial results per work unit, the maximum output size is 65,536 bytes or less
and you will not be using BOINC replication. These are all valid settings for
creating an initial project, however, you may find that they do not suite your
needs fully.

To customize these settings, you can call the following function on the BOINC
interface object::

   set_boinc_args(target_nresults=2, min_quorum=1, max_nbytes=65536)

The "target_nresults" and "min_quorum" are used when creating the BOINC input
template and "max_nbytes" is used for the output template. In addition to these
settings, the entire BOINC template is embedded in the pymw/interfaces/boinc.py
source file and can be fully customized by simply adding code to the templates
as needed.

.. seealso::

   http://boinc.berkeley.edu/trac/wiki/WorkGeneration
      BOINC Work Generation

   http://boinc.berkeley.edu/trac/wiki/XmlFormat
      BOINC XML Formats
      
   http://boinc.berkeley.edu/trac/wiki/JobIn
      BOINC Jobs

Custom Worker Applications
""""""""""""""""""""""""""
Any application can be used with PyMW as a worker application, so if you have a
custom Python interpreter that you would like to send, you can setup the
following directory structure and pass it to PyMW::

    ~/myapp/linux/pymw_1.03_i686-pc-linux-gnu
    ~/myapp/windows/pymw_1.03_windows_intelx86.exe
    ~/myapp/apple/pymw_1.03_i686-apple-darwin

*Note: The application is not required to reside in your home directory, this is just
used to illustrate this example.*

Notice that each executable has a very particular name; you must rename your
applications executable to match this pattern. When executing your applicaiton,
send that path along with any additional execution arguments you wish to pass
into your app::

   $ python monte_pi.py -p <boinc_proj_path> -c ~/myapp -a <additional_args>

Here is an example using PyBOINC, assuming it's been extracted to your home
directory::

   $ python monte_pi.py -p <boinc_proj_path> -c ~/pyboinc/python26 -a 1.00_python26.zip

In addition to renaming your executable, there are several limitations imposed
by BOINC. First, sub-directories are not allowed. To work around this issue,
it is suggested that you zip the directory structure you need and then unzip it
before execution.

Also, BOINC has no concept of file mutability. This means that if you change the
contents of a file, it must have a new name. For this reason, it is strongly
suggested that you version all files sent with your application. For example,
the PyBOINC Python libraries all contain the string "_1.00_", which allows you
to increment the version number if you happen to change the contents of the zip.

Security Concerns
"""""""""""""""""
PyMW allows arbitrary execution of unsigned Python code on compute nodes, which is not typical of large BOINC projects. For a large-scale public project, PyMW scripts must be digitally signed on a remote machine (signing on the BOINC server is equally insecure). Unsigned executables should never be sent as part of work units on a public project.
