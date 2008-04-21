from app_types import *
from math import *
from random import *

def fitness_func(ind):
    return reduce(lambda x, y: x+y, ind)

def mutate(ind, mut_rate):
    for i in range(len(ind)):
        if random() < mut_rate:
            ind[i] = 1-ind[i]

def crossover(ind1, ind2):
    cpt = randint(0,len(ind1))
    new_ind1 = ind1[:cpt]+ind2[cpt:]
    new_ind2 = ind2[:cpt]+ind1[cpt:]
    
    return new_ind1, new_ind2

num_inds = 5
gene_len = 10

ind_set = [[randint(0,1) for i in range(gene_len)] for n in range(num_inds)]
print ind_set
mut_rate = 1./gene_len
num_gens = 10
frac_cross = 0.5

for gen in range(num_gens):
    fitness = [fitness_func(ind) for ind in ind_set]
    sum_fit = reduce(lambda x, y: x+y, fitness)
    num_crossovers = int(frac_cross*num_inds/2)
    new_ind_set = []
    for xover in range(num_crossovers):
        parents = sample(ind_set,2)
        child1, child2 = crossover(parents[0], parents[1])
        new_ind_set.append(child1)
        new_ind_set.append(child2)
    
    new_ind_set.extend(sample(ind_set, len(ind_set)-num_crossovers))
    ind_set = new_ind_set
    print ind_set
    for ind in ind_set:
        mutate(ind, mut_rate)
            
    #print fitness

#input = pymw_get_input()
#output = Output(current_set)
#pymw_return_output(output)

