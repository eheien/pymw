__all__ = ["generic", "boinc", "condor", "ganga", "mpi", "multicore"]

from .generic import *

import sys

from optparse import OptionParser
for interface in __all__:
	from pymw.interfaces import interface

def parse_options(parser=None, args=None):
	"""Parses the standard options associated with a PyMW application.
	Additional options will be returned for additional parsing.
	Returns options, args
	"""
	if not parser:
		parser = OptionParser(usage="usage: %prog")
	if not args:
		args = sys.argv[1:]

	parser.add_option("-i", "--interface", dest="interface", default="generic", 
			help="specify the interface (generic/multicore/mpi/condor/boinc)", 
			metavar="INTERFACE")

	parser.add_option("-n", "--num_workers", dest="n_workers", default="4", 
			help="number of workers", metavar="N")

	parser.add_option("-g", "--ganga_loc", dest="g_loc", default="~/Ganga/bin/ganga", 
			help="directory of GANGA executable (GANGA interface)", metavar="FILE")

	parser.add_option("-p", "--project_home", dest="p_home", default="", 
			help="directory of the project (BOINC interface)", metavar="DIR")

	parser.add_option("-c", "--app_path", dest="custom_app_dir", default="", 
			help="directory of a custom worker application (BOINC interface)", 
			metavar="DIR")

	parser.add_option("-a", "--app_args", dest="custom_app_args", default="", 
			help="arguments for a custom worker application (BOINC interface)", 
			metavar="DIR")

	return parser.parse_args(args)

def get_interface(options):
	"""Returns a PyMW interface instance specifed in the options or the generic 
	interface if none was specified.
	"""
	n_workers = int(options.n_workers)

	if options.interface == "generic":
		interface_obj = generic.GenericInterface(num_workers=n_workers)
	elif options.interface == "multicore":
		interface_obj = multicore.MulticoreInterface(num_workers=n_workers)
	elif options.interface == "mpi":
		interface_obj = mpi.MPIInterface(num_workers=n_workers)
	elif options.interface == "condor":
		interface_obj = condor.CondorInterface()
	elif options.interface == "ganga":
		interface_obj = interfaces.ganga.GANGAInterface(ganga_loc=options.g_loc)
	elif options.interface == "boinc":
		interface_obj = boinc.BOINCInterface(project_home=options.p_home,\
											 custom_app_dir=options.custom_app_dir,\
											 custom_args=[options.custom_app_args])
	else:
		print(("Interface", options.interface, "unknown."))
		exit()

	return interface_obj

