#!/usr/bin/env python

import h5py
import pandas as pd
import numpy
import time

h5file = 'west.h5'

transactions = []

#@profile
def main():
    with h5py.File(h5file, "r+") as f:
        for i in range(150,165):
            print("iteration", i)
            if transactions:
                arr_t = numpy.array((transactions), dtype=int)
                arr_i = numpy.array((transactions), dtype=int)[:,0]
                to_delete = i-2
                if to_delete in arr_i:
                    to_delete_index = arr_i == to_delete
                    limit = numpy.where(to_delete_index == True)[0][-1]
                    del transactions[0:limit+1]
    
            path = "iterations/iter_" + str(i).zfill(8)
            nextpath = "iterations/iter_" + str(i+1).zfill(8)
            pcoords = f[path]['pcoord'][:,-1] 
            parents = f[path]['seg_index']['parent_id']
            nextparents = f[nextpath]['seg_index']['parent_id']
            wtgraph = f[path]['wtgraph']
            nextwtgraph = f[nextpath]['wtgraph']
            wtg_np = f[path]['seg_index']['wtg_n_parents']
            wtg_off = f[path]['seg_index']['wtg_offset']
            nextwtg_np = f[nextpath]['seg_index']['wtg_n_parents']
            nextwtg_off = f[nextpath]['seg_index']['wtg_offset']
            weights = f[path]['seg_index']['weight']
            nextweights = f[nextpath]['seg_index']['weight']
            auxcoords = f[path]['auxdata/c14n34'][:,-1]
            max_weight = weights.max()
            w = numpy.where(weights == max_weight)[0][0]
            next_parent_add = numpy.where(nextparents == w)[0][0]
            
            # Find the walkers that entered the alternate product state.        
            aux_mask = numpy.where(auxcoords < 1.7)
            diff = numpy.setdiff1d(nextwtgraph, nextparents)
            merged = []
            
            for k in numpy.arange(0,nextweights.shape[0]):
                off = nextwtg_off[k]
                np = off + nextwtg_np[k]
                merged.append(nextwtgraph[off:np])
            
            merge_ids = []
            transaction_weights = []
            diff_where = []
    
            for l in aux_mask[0]:
                if l in nextparents:
                    next_seg = numpy.where(nextparents==l)[0]
                    merge_ids.append(next_seg)
                    transaction_weights.append(weights[l])
                else:
                    for num, m in enumerate(merged):
                        if l in m:
                            ndiff = numpy.setdiff1d(m,diff)
                            if ndiff.shape[0] > 1: 
                                if ndiff[0] in nextparents:
                                    ndiff = ndiff[0]
                                    continue
                                else:
                                    ndiff = ndiff[1]
                            next_seg = numpy.where(nextparents==ndiff)[0]
                            merge_ids.append(next_seg)
                            transaction_weights.append(weights[l])
                        else:
                            continue
    
            iteration = i
            iteration_plus_one = i+1
            for num, p in enumerate(merge_ids):
                next_parent_subtract = p
                if len(next_parent_subtract) > 1:
                    num_split = len(next_parent_subtract)
                    split_weight = transaction_weights[num]/num_split
                    for q in next_parent_subtract:
                        #print(iteration, p, "xxx", "[", q, "]")
                        transactions.append([iteration_plus_one, q, -1, split_weight])
                        transactions.append([iteration_plus_one, next_parent_add, 1, split_weight])
                else:
                    #print(iteration, p, "-->", "[", next_parent_subtract[0], "]")
                    transactions.append([iteration_plus_one, next_parent_subtract[0], -1, transaction_weights[num]])
                    transactions.append([iteration_plus_one, next_parent_add, 1, transaction_weights[num]])
                  
            # Now to reset and do forward propagation. This must be done on-the-fly.   
    
            merge_ids = []
            transaction_weights = []
                
            if not transactions:
                continue
            arr_t = numpy.array((transactions), dtype=int)
            arr_i = numpy.array((transactions), dtype=int)[:,0]
            if i not in arr_i:
                continue
    
            for j in arr_t[arr_i == i]:
    
                if j[1] in nextparents:
                    next_seg = numpy.where(nextparents==j[1])[0]
                    merge_ids.append(next_seg)
                    transaction_weights.append(j[3])
                else:
                    for num, m in enumerate(merged):
                        if j[1] in m:
                            ndiff = numpy.setdiff1d(m,diff)
                            if ndiff.shape[0] > 1: 
                                if ndiff[0] in nextparents:
                                    ndiff = ndiff[0]
                                    continue
                                else:
                                    ndiff = ndiff[1]
                            next_seg = numpy.where(nextparents==ndiff)[0]
                            merge_ids.append(next_seg)
                            transaction_weights.append(j[3])
                        else:
                            continue
                                
            iteration = i
            iteration_plus_one = i+1
            for num, p in enumerate(merge_ids):
                next_parent_subtract = p
                if len(next_parent_subtract) > 1:
                    num_split = len(next_parent_subtract)
                    split_weight = transaction_weights[num]/num_split
                    for q in next_parent_subtract:
                        #print(iteration, "FP", p, "xxx", "[", q, "]")
                        transactions.append([iteration_plus_one, q, -1, split_weight])
                        transactions.append([iteration_plus_one, next_parent_add, 1, split_weight])
                else:
                    #print(iteration, "FP", p, "-->", "[", next_parent_subtract[0], "]")
                    transactions.append([iteration_plus_one, next_parent_subtract[0], -1, transaction_weights[num]])
                    transactions.append([iteration_plus_one, next_parent_add, 1, transaction_weights[num]])   
    
            b = 0      
            if not transactions:
                continue
            arr_t = numpy.array((transactions), dtype=int)
            arr_i = numpy.array((transactions), dtype=int)[:,0]
    
            if i not in arr_i:
                continue
    ##############################           
            for r in arr_t[arr_i == i]:
                segment = r[1]
                new_weight = weights[segment] + r[2]*r[3]
                weights[segment] = new_weight
                b = 1
    ##############################
            if b == 1:
                weights /= (weights.sum())
                f[path]['seg_index']['weight',:] = weights
            else:
                continue
main()
