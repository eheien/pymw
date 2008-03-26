from pickle import *
from base_interface import *
import time
import sys
#import boinc

#static jobID

# For BOINC: create a uniquely-named file x in the download hierarchy, file name should contain batch ID
def pymw_worker_call(interface, worker_executable, input_data):
	try:
		os.mkdir("input_files")
		os.mkdir("output_files")
	except OSError: pass
	
	input_file_name = interface.get_unique_file_name("input_files/")
	output_file_name = interface.get_unique_file_name("output_files/")
	input_file = open(input_file_name, 'w')
	Pickler(input_file).dump(input_data)
	input_file.close()
	task = Task(worker_executable, input_file_name, output_file_name)
	interface.submit_task(task)

def pymw_master_call(interface, setup_workers, handle_result, num_workers=1):
	#read jobID from pyboinc_checkpoint
	if True: #none
		#create a batch record; jobID = its ID
		setup_workers()
		#write jobID to checkpoint file
	#move all files from old/ to new/
	interface.start_execution(num_workers)
	while not interface.execution_finished():
		task_result = interface.get_next_finished_task()
		if task_result is not None:
			output = Unpickler(task_result).load()
			task_result.close()
			handle_result(output)
			#move x to old/
		else:
			time.sleep(1)

def pymw_status(interface):
	return interface.get_status()

def pymw_cleanup(interface):
	interface.cleanup()
	try:
		os.rmdir("input_files")
		os.rmdir("output_files")
	except: pass

def pymw_get_input():
	#boinc.boinc_resolve_filename("input", infile)
	input_file_name = sys.argv[1]
	infile = open(input_file_name, 'r')
	obj = Unpickler(infile).load()
	infile.close()
	return obj

def pymw_return_output(output):
	#boinc.boinc_resolve_filename("output", outfile)
	output_file_name = sys.argv[2]
	outfile = open(output_file_name, 'w')
	Pickler(outfile).dump(output)
	outfile.close()

