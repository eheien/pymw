#!/usr/bin/env python
"""Provide a BOINC interface for master worker computing with PyMW.
"""

import threading, shutil, os, sys
import time, calendar
import logging
from . import boinc_setup


__author__ = ["Adam Kornafeld <kadam@sztaki.hu>",
			  "Jeremy Cowles <jeremy.cowles@gmail.com", ]
__date__ = "10 April 2008"


INPUT_ZIP_INFO = """<file_info>
	<number>2</number>
</file_info>"""

INPUT_ZIP_REF = """	<file_ref>
		<file_number>2<file_number>
		<open_name>$PYMW_ZIP</open_name>
		<copy_file/>
	</file_ref>
"""

INPUT_TEMPLATE = """\
<file_info>
	<number>0</number>
</file_info>
<file_info>
	<number>1</number>
</file_info>
$INPUT_ZIP_INFO
<workunit>
	<file_ref>
		<file_number>0<file_number>
		<open_name>$PYMW_EXECUTABLE</open_name>
		<copy_file/>
	</file_ref>
	<file_ref>
		<file_number>1<file_number>
		<open_name>$PYMW_INPUT</open_name>
		<copy_file/>
	</file_ref>
	$INPUT_ZIP_REF
	<command_line>$PYMW_CMDLINE</command_line>
	<min_quorum>$MIN_QUORUM</min_quorum>
	<target_nresults>$TARGET_NRESULTS</target_nresults>
</workunit>
"""


OUTPUT_TEMPLATE = """\
<file_info>
	<name><OUTFILE_0/></name>
	<generated_locally/>
	<upload_when_present/>
	<max_nbytes>$MAX_NBYTES</max_nbytes>
	<url><UPLOAD_URL/></url>
</file_info>
<result>
	<file_ref>
		<file_name><OUTFILE_0/></file_name>
		<open_name>$PYMW_OUTPUT</open_name>
		<copy_file/>
	</file_ref>
</result>
"""

lock = threading.Lock()

class BOINCInterface:
	def __init__(self, project_home, custom_app_dir=None, custom_args=[], task_path="tasks"):
		self._max_nbytes = 65536
		self._target_nresults = 2
		self._min_quorum = 1
		self._project_home = project_home
		self._custom_args = custom_args
		self._project_download = project_home + "/download/"
		self._project_templates = project_home + "/templates/"
		self._boinc_in_template = INPUT_TEMPLATE
		self._boinc_out_template = OUTPUT_TEMPLATE
		self._cwd = os.getcwd()
		self._batch_id = str(self._get_unix_timestamp())
		
		# task reclamation support
		self._task_list = []
		self._task_list_lock = threading.Lock()
		self._result_checker_running = False
		self._task_finish_thread = None
		
		# auto-magical BOINC installation script
		boinc_setup.install_pymw(project_home, custom_app_dir, task_path)

	def _get_unix_timestamp(self):
		return calendar.timegm(time.gmtime())

	def _get_finished_tasks(self):
		"""Task reclamation thread.
		
		When tasks are completed by BOINC, the assimilator will
		drop files back into the output directory for PyMW to reclaim,
		This thread finds those files and marks the respective task
		as completed.		
		"""
		
		while True:
			self._task_list_lock.acquire()
			try:
				try:
					for entry in self._task_list:
						task, out_file = entry
						# Check for the output files
						if os.path.isfile(out_file):
							task.task_finished()
							self._task_list.remove(entry)
						elif os.path.isfile(out_file + ".error"):
							# error results come back with a ".error" extension
							f_obj = open(out_file + ".error")
							try:
								error_message = "".join(f_obj.readlines())
							finally:
								f_obj.close()
							
							os.remove(out_file + ".error")
							task.task_finished(task_err=\
											   Exception("BOINC computation"\
														 + " failed:\n " + \
														 error_message))
							self._task_list.remove(entry)
					if len(self._task_list) == 0:
						self._result_checker_running = False
						return
				except Exception as data:
					# just in case a higher-level process is hiding exceptions
					# log any exception that occures and then re-raise it
					logging.critical("BOINCInterface._get_finished_tasks" + \
									 " error [but stil running]: %s" % data)
					#self._result_checker_running = False
					#raise
			finally:
				self._task_list_lock.release()
			
			time.sleep(0.2)
	
	def _queue_task(self, task, output_file):
		"""Thread-safe addition of a task,output tuple to the task list.
		
		Appends a task,output tuple to the task list and then
		attempts to start the result checker thread if not already running.
		"""
		# force an absolute path to prevent CWD bugs
		output_file = os.path.abspath(output_file)
		self._task_list_lock.acquire()
		try:
			self._task_list.append((task, output_file))
		finally:
			self._task_list_lock.release()
		
		if not self._result_checker_running:
			self._result_checker_running = True
			self._task_finish_thread = threading.Thread(target=\
													self._get_finished_tasks)
			self._task_finish_thread.start()

	def _project_path_exists(self):
		return self._project_home != '' and os.path.exists(self._project_home)
	
	def set_boinc_args(self, target_nresults=2, min_quorum=1, max_nbytes=65536):
		self._target_nresults = target_nresults
		self._min_quorum = min_quorum
		self._max_nbytes = max_nbytes
		
	def execute_task(self, task, worker):
		"""Executes a task on the given worker
		"""
		# Check if project_home dir is known
		if not self._project_path_exists():
			logging.critical("Missing BOINC project home directory")
			raise Exception("Missing BOINC project home directory (-p switch)")

		bid = "_b" + self._batch_id
		new_exe = task._executable 
		task_exe = os.path.basename(new_exe)

		in_file = os.path.basename(task._input_arg)
		
		task._output_arg = task._output_arg.replace(".dat", bid + ".dat")
		out_file = os.path.basename(task._output_arg)
		
		
		if task._data_file_zip:
			zip_file = os.path.basename(task._data_file_zip)
		else:
			zip_file = None

		# Block concurrent threads until changing directories
		lock.acquire()
		try:
			logging.debug("Locking thread")
			logging.debug("input: %s, output: %s, task: %s" \
						  % (in_file, out_file, task_exe,))
			
			cmd = "cd " + self._project_home
			cmd += ";./bin/dir_hier_path " + in_file
			p = os.popen(cmd, "r")
			try:
				in_dest = p.read().strip()
			finally:
				p.close()
			
			in_dest_dir = os.path.dirname(in_dest)
			if os.path.exists(in_dest):
				os.remove(in_dest)
			
			if zip_file:
				cmd = "cd " + self._project_home
				cmd += ";./bin/dir_hier_path " + zip_file
				p = os.popen(cmd, "r")
				try:
					zip_dest = p.read().strip()
				finally:
					p.close()
				zip_dest_dir = os.path.dirname(zip_dest)
				if os.path.exists(zip_dest):
					os.remove(zip_dest)
			
			cmd = "cd " + self._project_home
			cmd += ";./bin/dir_hier_path " + task_exe
			p = os.popen(cmd, "r")
			try:
				exe_dest = p.read().strip()
			finally:
				p.close()
			
			exe_dest_dir = os.path.dirname(exe_dest)
			if os.path.exists(exe_dest): os.remove(exe_dest)
			
			# Copy input files to download dir
			while(not os.path.isfile(task._executable)):
				logging.debug("Waiting for task exe to become ready")
			shutil.copyfile(task._executable, exe_dest)

			while(not os.path.isfile(task._input_arg)):
				logging.debug("Waiting for input to become ready...")
			shutil.copyfile(task._input_arg, in_dest)
			if zip_file: shutil.copyfile(task._data_file_zip, zip_dest)
				
			# Create input XML template
			in_template_name = "pymw_in_" + str(task._task_name) + ".xml"
			dest = self._project_templates + in_template_name
			boinc_in_template = self._get_input_template(task_exe,
													zip_file,
													in_file,
													out_file)
			
			logging.debug("writing in xml template: %s" % dest)
			
			f_obj = open(dest, "w")
			try:
				f_obj.writelines(boinc_in_template)
			finally:
				f_obj.close()
			
			# Create output XML template
			out_template = "pymw_out_" + str(task._task_name) + ".xml"
			dest = self._project_templates + out_template
			boinc_out_template = self._get_ouput_template(out_file)
			logging.debug("writing out xml template: %s" % dest)
			
			f = open(dest, "w")
			try:
				f.writelines(boinc_out_template)
			finally:
				f.close()
			
			# Call create_work
			cmd =  "cd " + self._project_home 
			cmd += "; ./bin/create_work -appname pymw -wu_name"
			cmd += " pymw_" + str(task._task_name) 
			cmd += " -wu_template templates/" +  in_template_name
			cmd += " -result_template templates/" + out_template
			cmd += " -batch " + self._batch_id 
			cmd += " " + task_exe + " " + in_file
			
			if zip_file:
				cmd += " " + zip_file
			
			os.system(cmd)
		finally:
			# Release lock		
			lock.release()
			logging.debug("Releasing thread lock")
		
		# Add the task to the current task reclamation queue
		pickup_file = os.path.join(os.path.dirname(task._output_arg), out_file)
		self._queue_task(task, pickup_file)
	
	def _get_ouput_template(self, out_file):
		"""Returns a populated output BOINC template
		"""
		outtempl = self._boinc_out_template
		outtempl = outtempl.replace("$PYMW_OUTPUT", out_file)
		outtempl = outtempl.replace("$MAX_NBYTES", str(self._max_nbytes))
		return outtempl
	
	def _get_input_template(self, task_exe, zip_file, in_file, out_file):
		"""Returns a populated input BOINC template
		"""
		intmp = self._boinc_in_template
		intmp = intmp.replace("$PYMW_EXECUTABLE", task_exe)
		intmp = intmp.replace("$PYMW_INPUT", in_file)
		intmp = intmp.replace("$MIN_QUORUM", str(self._min_quorum))
		intmp = intmp.replace("$TARGET_NRESULTS", str(self._target_nresults))
		
		if zip_file:
			intmp = intmp.replace("$INPUT_ZIP_INFO", INPUT_ZIP_INFO)
			intmp = intmp.replace("$INPUT_ZIP_REF", INPUT_ZIP_REF)
			intmp = intmp.replace("$PYMW_ZIP", zip_file)
		else:
			intmp = intmp.replace("$INPUT_ZIP_INFO", "")
			intmp = intmp.replace("$INPUT_ZIP_REF", "")
		
		if self._custom_args:
			cust_args = " \"" + " ".join(self._custom_args) + "\" "
		else:
			cust_args = ""
			
		cmdline = task_exe + cust_args + in_file + " " + out_file
		intmp = intmp.replace("$PYMW_CMDLINE", cmdline)
		
		return intmp
	
	def _cleanup(self):
		if self._result_checker_running:
			self._task_list_lock.acquire()
			try: self._task_list = []
			finally: self._task_list_lock.release()
		
		# now release the batch so it can be deleted
		if not self._project_path_exists():
			logging.critical("Missing BOINC project home directory")
			logging.critical("Unable to cleanup batch: " + self._batch_id)
			return None

		logging.debug("Zeroing batch in BOINC db where batch = " \
					  + self._batch_id)
		mgr = Manager(self._project_home)
		mgr.zero_batch(self._batch_id)
		
	def pymw_worker_func(func_name_to_call, options):
		# Get the input data
		input_data = pymw_worker_read(options)
		if not input_data: input_data = ()
		# Execute the worker function
		result = func_name_to_call(*input_data)
		# Output the result
		pymw_emit_result(result)
		open("boinc_finish_called", "w").close()
		

class Manager():
	# This file's presence in the BOINC project directory
	# indicates that the daemons are stopped or stopping 
	STOP_TRIGGER = "stop_daemons"

	def __init__(self, proj_path):
		self.project_path = proj_path
		self.Boinc = None
		self._import_hack()
	
	def _import_hack(self):
		"""Imports the BOINC support code
	
		This is a hack because the actual path is specified at runtime
		and so cannot be imported when the module loads. This works
		around that issue by creating global vars for the namespaces
		and then populating them once the path is known.
		"""
		
		bin_path = os.path.join(self.project_path, "py")
		if not bin_path in sys.path:
			sys.path.append(bin_path)
		
		# BOINC will search for config files using these vars
		os.environ['BOINC_PROJECT_DIR'] = self.project_path
		mods = ['configxml',
				'db_base',
				'boinc_db',
				'database',
				'projectxml',]
		self.Boinc = __import__("Boinc", fromlist=mods)
	
	def _bin_run(self, boinc_app_name):
		return os.system(os.path.join(self.project_path, "bin", boinc_app_name))
	
	def is_running(self):
		return not os.path.exists(os.path.join(self.project_path, \
											   self.STOP_TRIGGER))
	
	def zero_batch(self, batch_id, cancel_workunits=False):
		self.Boinc.database.connect()
		
		try:
			unsent = self.Boinc.boinc_db.RESULT_SERVER_STATE_UNSENT
			over = self.Boinc.boinc_db.RESULT_SERVER_STATE_OVER
			didnt_need = self.Boinc.boinc_db.RESULT_OUTCOME_DIDNT_NEED
			units = self.Boinc.database.Workunits.find(batch=batch_id)
			for wu in units:
				wu.batch = 0
				if cancel_workunits:
					wu.error_mask |= self.Boinc.boinc_db.WU_ERROR_CANCELED
					results = self.Boinc.database.Results.find(workunit=wu)
					for result in results:
						if result.server_status == unsent:
							result.server_status = over
							result.outcome = didnt_need
							result.commit()
							
				wu.commit()
		finally:
			self.Boinc.database.close()
	
	def delete_batch(self, batch_id):
		self.zero_batch(batch_id, cancel_workunits=True)
		self._bin_run("file_deleter -d 3 -dont_delete_batches")
	
	def get_boinc_lib(self):
		return self.Boinc
