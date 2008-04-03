import sys
import cPickle

class Input:
    def __init__(self, arg):
        self.value = arg

    def __str__(self):
        return str(self.value)

class Output:
    def __init__(self, arg):
        self.value = arg

    def __str__(self):
        return str(self.value)

def pymw_get_input():
    infile = open(sys.argv[1], 'r')
    obj = cPickle.Unpickler(infile).load()
    infile.close()
    return obj

def pymw_return_output(output):
    outfile = open(sys.argv[2], 'w')
    cPickle.Pickler(outfile).dump(output)
    outfile.close()

