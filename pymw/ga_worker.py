from app_types import *
from math import *
from random import *

def fitness_func(ind):
    return reduce(lambda x, y: x+y, ind)

def mutate(ind, mut_rate):
    for i in range(len(ind)):
        if random() < mut_rate:
            ind[i] = 1-ind[i]

def select(fitness, ind_set):
    total_fit = reduce(lambda x, y: x+y, fitness)
    spin_val = random()*total_fit
    ind = 0
    while spin_val >= 0:
        spin_val -= fitness[ind]
        ind += 1
    return ind_set[ind-1]

def crossover(ind1, ind2):
    cpt = randint(0,len(ind1))
    new_ind1 = ind1[:cpt]+ind2[cpt:]
    new_ind2 = ind2[:cpt]+ind1[cpt:]
    
    return new_ind1, new_ind2

input = pymw_get_input()
num_inds = input["num_inds"]
gene_len = input["gene_len"]
mut_rate = input["mut_rate"]
num_gens = input["num_gens"]
cross_rate = input["cross_rate"]
ind_set = input["ind_set"]

for gen in range(num_gens):
    fitness = [fitness_func(ind) for ind in ind_set]
    total_fit = reduce(lambda x, y: x+y, fitness)
    print total_fit
    new_ind_set = []
    num_crossovers = num_inds*cross_rate
    for i in range(num_inds/2):
        parent1 = select(fitness, ind_set)
        parent2 = select(fitness, ind_set)
        if random() < cross_rate:
            child1, child2 = crossover(parent1, parent2)
            mutate(child1, mut_rate)
            mutate(child2, mut_rate)
            new_ind_set.append(child1)
            new_ind_set.append(child2)
        else:
            new_ind_set.append(parent1)
            new_ind_set.append(parent2)
    ind_set = new_ind_set
    #print ind_set
            
    #print fitness

#output = Output(current_set)
#pymw_return_output(output)

