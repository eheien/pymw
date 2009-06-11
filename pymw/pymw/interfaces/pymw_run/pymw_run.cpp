// ---------------------------------------------------//
// pymw_run.exe
//
// Author: Jeremy Cowles <jeremy.cowles@gmail.com>
// Date: 10 June 2009
//
// Description:
// ------------
// This program is used by BOINC to start the Python
// interpreter and uses with the following command:
//
//   Python <pymw_script> <input_data> <output_data>
//
// when computation completes, the following is run:
//
//   cmd /C type nul > boinc_finish_called
//
// This executable is distributed by the BOINC server
// and will have the following name at runtime:
//
//    pymw_1.00_windows_intelx86.exe
//
// the version and architecture are subject to change
// ---------------------------------------------------//

#include "stdafx.h"

int run_proc(wchar_t* cmd);

int _tmain(int argc, _TCHAR* argv[])
{
	if (argc != 4){
		printf("Error: expected 3 args");
		return 1;
	}

	wchar_t* exe = new wchar_t[256];
	
	// build the python command line
	_tcscpy_s(exe,256,_T("python.exe "));
	_tcscat_s(exe,256,argv[1]);
	_tcscat_s(exe,256,_T(" "));
	_tcscat_s(exe,256,argv[2]);
	_tcscat_s(exe,256,_T(" "));
	_tcscat_s(exe,256,argv[3]);

	// save the result incase the interpreter exits abnormally
	int result = run_proc(exe);

	// touch the boinc_finish_called file
	_tcscpy_s(exe,256,_T("cmd.exe /C type nul > boinc_finish_called"));
	run_proc(exe);

	// cleanup and exit
	delete [] exe;
	if (result != 0) return 1;
	return 0;
}

// wrapper function for CreateProcess()
// cmd *must* be a mutable string pointer or sporatic errors will occure
int run_proc(wchar_t* cmd){
    STARTUPINFO si = { sizeof(si) };
	PROCESS_INFORMATION pi;

	if(CreateProcess(0, cmd, 0, 0, FALSE, 0, 0, 0, &si, &pi))
	{
	   // wait for process to finish
	   WaitForSingleObject(pi.hProcess, INFINITE);

	   // clean up handles
	   CloseHandle(pi.hProcess);
	   CloseHandle(pi.hThread);
	}else{
		return 1;
	}

	return 0;
}