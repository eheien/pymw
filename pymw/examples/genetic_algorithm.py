#!/usr/bin/env python

from pymw import pymw
from pymw import interfaces 
import time
from optparse import OptionParser
import random
import logging

def fitness_func(ind):
    return reduce(lambda x, y: x+y, ind)

def mutate(ind, mut_rate):
    for i in range(len(ind)):
        if random.random() < mut_rate:
            ind[i] = 1-ind[i]

def select(fitness, ind_set):
    total_fit = reduce(lambda x, y: x+y, fitness)
    spin_val = random.random()*total_fit
    ind = 0
    while spin_val >= 0:
        spin_val -= fitness[ind]
        ind += 1
    return ind_set[ind-1]

def crossover(ind1, ind2):
    cpt = random.randint(0,len(ind1))
    new_ind1 = ind1[:cpt]+ind2[cpt:]
    new_ind2 = ind2[:cpt]+ind1[cpt:]
    
    return new_ind1, new_ind2

parser = OptionParser(usage="usage: %prog")

parser.add_option("-m", "--num_individuals", dest="n_inds", default="20", 
                help="total number of individuals for genetic algorithm")

parser.add_option("-l", "--gene_length", dest="g_len", default="20", 
                help="gene length of each individual")

parser.add_option("-t", "--total_generations", dest="total_gens", default="10",
                help="total number of generations to run genetic algorithm")

options, args = interfaces.parse_options(parser) 

n_workers, num_inds, gene_len = int(options.n_workers), int(options.n_inds), int(options.g_len)
total_gens = int(options.total_gens)

start_time = time.time()

interface_obj = interfaces.get_interface(options)

pymw_master = pymw.PyMW_Master(interface=interface_obj)
post_init_time = time.time()

mut_rate = 1./gene_len
cross_rate = 0.7
max_fitness = 0

# Create an initial random gene pool
gene_pool = [[random.randint(0,1) for i in range(gene_len)] for n in range(num_inds)]

for gen_count in range(total_gens):
    # Submit tasks to evaluate each individuals fitness
    cur_generation_tasks = [pymw_master.submit_task(fitness_func, input_data=(individual,))
                            for individual in gene_pool]

    # Get the results
    fitness = []
    for task in cur_generation_tasks:
        result_task, result = pymw_master.get_result(task)
        fitness.append(result)
        
    max_fitness = reduce(lambda x, y: max(x,y), fitness)
    sum_fitness = reduce(lambda x, y: x+y, fitness)
    print(("Generation", gen_count, "max fitness", max_fitness, "average fitness", sum_fitness/num_inds))
    new_gene_pool = []
    for i in range(num_inds/2):
        parent1 = select(fitness, gene_pool)
        parent2 = select(fitness, gene_pool)
        if random.random() < cross_rate:
            child1, child2 = crossover(parent1, parent2)
            mutate(child1, mut_rate)
            mutate(child2, mut_rate)
            new_gene_pool.append(child1)
            new_gene_pool.append(child2)
        else:
            new_gene_pool.append(parent1)
            new_gene_pool.append(parent2)
    gene_pool = new_gene_pool

fitness = [fitness_func(ind) for ind in gene_pool]
avg_fit = reduce(lambda x, y: x+y, fitness)/len(gene_pool)

end_time = time.time()

print(("Number of individuals:", str(num_inds)))
print(("Best individual:", str(max_fitness),"/", str(gene_len)))
print(("Average individual:", str(avg_fit),"/", str(gene_len)))
print(("Number of workers:", str(n_workers)))
print(("Calculation time:", str(end_time-start_time)))
print(("Total time:", str(end_time-start_time)))
