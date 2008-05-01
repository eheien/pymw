#!/usr/bin/env python
# mpirun -np 4 /home/myri-fs/e-heien/local/bin/pyMPI /home/myri-fs/e-heien/booga.py

import mpi
import os

finished = False

if mpi.rank == 0:
	print mpi.size-1
	while not finished:
		try:
			cmd = raw_input().split()
			target = int(cmd[0])%(mpi.size-1)+1
			msg = cmd[1]
			mpi.send(msg, target)
		except:
			for i in range(mpi.size):
				mpi.send(None, i)
			finished = True
else:
	while not finished:
		msg, status = mpi.recv()
		if msg is None:
			finished = True
		else:
			print mpi.rank, msg

