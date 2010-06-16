from mpi4py import MPI
import subprocess
import pickle

def worker():
    parent_comm = MPI.Comm.Get_parent()
    rank = parent_comm.Get_rank()
    while True:
        msg = parent_comm.recv(source=0, tag=0)
        print((rank, "received command", msg))
        if msg is None:
            parent_comm.Disconnect()
            return
        #retval = subprocess.call(["python", msg[0], msg[1], msg[2]])
        outfile = open(msg[2], 'w')
        pickle.Pickler(outfile).dump([[31.4, 100], "", ""])
        outfile.close()
        print((rank, "trying send, result:", parent_comm.send(rank, dest=0, tag=1)))
        print((rank, "done sending result for task", msg[2]))

worker()
