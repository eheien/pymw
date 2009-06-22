#!/usr/bin/env python
"""Provide a BOINC interface for master worker computing with PyMW.
"""

import threading,shutil,os,sys,re
import time,calendar
import logging
import boinc_setup


__author__ = "Adam Kornafeld <kadam@sztaki.hu>"
__date__ = "10 April 2008"


INPUT_TEMPLATE = """\
<file_info>
    <number>0</number>
</file_info>
<file_info>
    <number>1</number>
</file_info>
<workunit>
    <file_ref>
        <file_number>0<file_number>
        <open_name><PYMW_EXECUTABLE/></open_name>
        <copy_file/>
    </file_ref>
    <file_ref>
        <file_number>1<file_number>
        <open_name><PYMW_INPUT/></open_name>
        <copy_file/>
    </file_ref>
    <command_line><PYMW_CMDLINE/></command_line>
    <min_quorum>1</min_quorum>
    <target_nresults>1</target_nresults>
</workunit>
"""

OUTPUT_TEMPLATE = """\
<file_info>
    <name><OUTFILE_0/></name>
    <generated_locally/>
    <upload_when_present/>
    <max_nbytes>32768</max_nbytes>
    <url><UPLOAD_URL/></url>
</file_info>
<result>
    <file_ref>
        <file_name><OUTFILE_0/></file_name>
        <open_name><PYMW_OUTPUT/></open_name>
        <copy_file/>
    </file_ref>
</result>
"""

lock = threading.Lock()
#logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")

class BOINCInterface:
    def __init__(self, project_home):
        self._project_home = project_home
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
        boinc_setup.install_pymw(project_home)

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
                        task,out_file = entry
                        # Check for the output files
                        if os.path.isfile(out_file):
                            task.task_finished()
                            self._task_list.remove(entry)
                        elif os.path.isfile(out_file + ".error"):
                            # error results come back with a ".error" extension
                            f = open(out_file + ".error")
                            try: error_message = "\n".join(f.readlines())
                            finally: f.close()
                            task.task_finished(task_err=Exception("BOINC computation failed:\n " + error_message))
                            self._task_list.remove(entry)
                    if len(self._task_list) == 0:
                        self._result_checker_running = False
                        return
                except Exception,data:
                    logging.critical("_get_finished_tasks crashed: %s" % data)
                    self._result_checker_running = False
                    raise
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
        try: self._task_list.append((task,output_file))
        finally: self._task_list_lock.release()
        
        if not self._result_checker_running:
            self._result_checker_running = True
            self._task_finish_thread = threading.Thread(target=self._get_finished_tasks)
            self._task_finish_thread.start()

    def _project_path_exists(self):
        return self._project_home != '' and os.path.exists(self._project_home)
    
    def execute_task(self, task, worker):
        global lock
        
        # Check if project_home dir is known
        if not self._project_path_exists():
            logging.critical("Missing BOINC project home directory")
            raise Exception("Missing BOINC project home directory (-p switch)")
        
        in_file = os.path.basename(task._input_arg)
        out_file = os.path.basename(task._output_arg)
        task_exe = os.path.basename(task._executable)

        # Block concurrent threads until changing directories
        lock.acquire()
        try:
            logging.debug("Locking thread")
            logging.debug("input: %s, output: %s, task: %s" % (in_file, out_file, task_exe,))
    
            cmd = "cd " + self._project_home + ";./bin/dir_hier_path " + in_file
            try:
                p = os.popen(cmd, "r")
                try: in_dest = p.read().strip()
                finally: p.close()
            except Exception,error_args:
                logging.critical("error reading in_dest: %s" % error_args )
                raise
            
            #logging.debug("found in_dest: %s" % in_dest)
            in_dest_dir = os.path.dirname(in_dest)
            cmd = "cd " + self._project_home + ";./bin/dir_hier_path " + task_exe
    
            p = os.popen(cmd, "r")
            try: exe_dest = p.read().strip()
            finally: p.close()
            
            exe_dest_dir = os.path.dirname(exe_dest)
        
            # Copy input files to download dir
            if not os.path.isfile(exe_dest):
                while(1):
                    logging.debug("Waiting for task exe to become ready")
                    if os.path.isfile(task._executable):
                        break
                shutil.copyfile(task._executable, exe_dest)
            while(1):
                logging.debug("Waiting for input to become ready...")
                if os.path.isfile(task._input_arg):
                    break
                
            shutil.copyfile(task._input_arg, in_dest)
                
            # Create input XML template
            in_template = "pymw_in_" + str(task._task_name) + ".xml"
            dest = self._project_templates + in_template
            boinc_in_template = self._boinc_in_template
            boinc_in_template = boinc_in_template.replace("<PYMW_EXECUTABLE/>", task_exe)
            boinc_in_template = boinc_in_template.replace("<PYMW_INPUT/>", in_file)
            boinc_in_template = boinc_in_template.replace("<PYMW_CMDLINE/>", task_exe + " " + in_file + " " + out_file)
            logging.debug("writing in xml template: %s" % dest)
            
            f = open(dest, "w")
            try: f.writelines(boinc_in_template)
            finally: f.close()
            
            # Create output XML template
            out_template = "pymw_out_" + str(task._task_name) + ".xml"
            dest = self._project_templates + out_template
            boinc_out_template = self._boinc_out_template
            boinc_out_template = boinc_out_template.replace("<PYMW_OUTPUT/>", out_file)
            logging.debug("writing out xml template: %s" % dest)
            
            f = open(dest, "w")
            try: f.writelines(boinc_out_template)
            finally: f.close()
            
            # Call create_work
            cmd =  "cd " + self._project_home 
            cmd += "; ./bin/create_work -appname pymw -wu_name pymw_" +  str(task._task_name) + "_b" + self._batch_id
            cmd += " -wu_template templates/" +  in_template
            cmd += " -result_template templates/" + out_template
            cmd += " -batch " + self._batch_id 
            cmd += " " + task_exe + " "  + in_file
            os.system(cmd)
        finally:
            # Release lock        
            lock.release()
            logging.debug("Releasing thread lock")
        
        # Add the task to the current task reclamation queue
        self._queue_task(task, task._output_arg)
        
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

        # setup access to the BOINC database support
        bin_path = os.path.join(self._project_home, "py")
        if not bin_path in sys.path:
            sys.path.append(bin_path)

        # BOINC will search for config files using this var
        os.environ['BOINC_PROJECT_DIR'] = self._project_home
        
        # load the BOINC database module
        from Boinc import database
        
        # query the database for the batch and zero out every WU
        database.connect()
        logging.debug("Zeroing batch in BOINC db where batch = " + self._batch_id)

        try:
            units = database.Workunits.find(batch=self._batch_id)
            for wu in units:
                wu.batch = 0
                wu.commit()
        finally:
            database.close()
