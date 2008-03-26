from pickle import *
from base_interface import *
import time
#import boinc

#static jobID

# For BOINC: create a uniquely-named file x in the download hierarchy, file name should contain batch ID
def pymw_worker_call(interface, worker_executable, input):
	try:
		os.mkdir("input_files")
		os.mkdir("output_files")
	except OSError:
		pass
	
	input_file_name = interface.get_unique_file_name("input_files/")
	output_file_name = interface.get_unique_file_name("output_files/")
	input_file = open(input_file_name, 'w')
	Pickler(input_file).dump(input)
	input_file.close()
	interface.submit_task(worker_executable, input_file_name, output_file_name)

def pymw_master_call(interface, make_calls, handle_result):
	#read jobID from pyboinc_checkpoint
	if True: #none
		#create a batch record; jobID = its ID
		make_calls()
		#write jobID to checkpoint file
	#move all files from old/ to new/
	interface.start_execution()
	while not interface.execution_finished():
		task_result = interface.get_next_finished_task()
		if task_result is not None:
			output = Unpickler(task_result).load()
			task_result.close()
			handle_result(output)
			#move x to old/
		else:
			time.sleep(1)

def pymw_cleanup(interface):
	interface.cleanup()
	os.rmdir("input_files")
	os.rmdir("output_files")

def pymw_get_input():
	#boinc.boinc_resolve_filename("input", infile)
	infile = open("input", 'r')
	obj = Unpickler(infile).load()
	infile.close()
	return obj

def pymw_return_output(output):
	#boinc.boinc_resolve_filename("output", outfile)
	outfile = open("output", 'w')
	Pickler(outfile).dump(output)
	outfile.close()

