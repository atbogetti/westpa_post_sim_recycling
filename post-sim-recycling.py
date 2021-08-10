#!/usr/bin/env python

import h5py
import pandas as pd
import numpy
import time
import os
import sys
from tqdm import tqdm

#os.system("cp west_bak.h5 west.h5")
h5file = 'west.h5'
fi = 2
li = 1500
transactions = []
print_switch = False

#@profile
def main():
    with h5py.File(h5file, "r+") as f:
        for i in range(fi,li):
            print("iteration", i)
            time1 = time.time()

            # Delete transaction information from iteration x-2
            if transactions:
                arr_t = numpy.array(transactions)
                arr_i = arr_t[:,0]
                to_delete = i-2
                if to_delete in arr_i:
                    to_delete_index = arr_i == to_delete
                    limit = numpy.where(to_delete_index == True)[0][-1]
                    del transactions[0:limit+1]
    
            # Set some necessary variables with hdf5 file data
            path = "iterations/iter_" + str(i).zfill(8)
            seg_index_path = "iterations/iter_" + str(i).zfill(8) + "/seg_index"
            if i == 1:
                prevpath == path
            else:
                prevpath = "iterations/iter_" + str(i-1).zfill(8)
            nextpath = "iterations/iter_" + str(i+1).zfill(8)
            prev_pcoords = f[prevpath]['pcoord'][:,-1] 
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
            auxcoords1 = f[path]['auxdata/c5n34'][:,-1]
            pauxcoords1 = f[prevpath]['auxdata/c5n34'][:,-1]
            auxcoords2 = f[path]['auxdata/c7n34'][:,-1]
            pauxcoords2 = f[prevpath]['auxdata/c7n34'][:,-1]
            auxcoords3 = f[path]['auxdata/c9n34'][:,-1]
            pauxcoords3 = f[prevpath]['auxdata/c9n34'][:,-1]
            auxcoords4 = f[path]['auxdata/c12n34'][:,-1]
            pauxcoords4 = f[prevpath]['auxdata/c12n34'][:,-1]
            auxcoords5 = f[path]['auxdata/c14n34'][:,-1]
            pauxcoords5 = f[prevpath]['auxdata/c14n34'][:,-1]
            auxcoords6 = f[path]['auxdata/c16n34'][:,-1]
            pauxcoords6 = f[prevpath]['auxdata/c16n34'][:,-1]
            auxcoords7 = f[path]['auxdata/c21n34'][:,-1]
            pauxcoords7 = f[prevpath]['auxdata/c21n34'][:,-1]
            auxcoords8 = f[path]['auxdata/c23n34'][:,-1]
            pauxcoords8 = f[prevpath]['auxdata/c23n34'][:,-1]
            auxcoords9 = f[path]['auxdata/c25n34'][:,-1]
            pauxcoords9 = f[prevpath]['auxdata/c25n34'][:,-1]

            # Find the walkers that entered the alternate product state.        
            aux_mask1 = numpy.where(auxcoords1 < 1.7)[0]
            if aux_mask1.size > 0:
                for y, idx in enumerate(aux_mask1):
                    parent = parents[y]
                    if pauxcoords1[parent] < 1.7:
                        aux_mask1 = numpy.delete(aux_mask1, numpy.where(aux_mask1 == idx)[0])
            aux_mask2 = numpy.where(auxcoords2 < 1.7)[0]
            if aux_mask2.size > 0:
                for y, idx in enumerate(aux_mask2):
                    parent = parents[y]
                    if pauxcoords2[parent] < 1.7:
                        aux_mask2 = numpy.delete(aux_mask2, numpy.where(aux_mask2 == idx)[0])
            aux_mask3 = numpy.where(auxcoords3 < 1.7)[0]
            if aux_mask3.size > 0:
                for y, idx in enumerate(aux_mask3):
                    parent = parents[y]
                    if pauxcoords3[parent] < 1.7:
                        aux_mask3 = numpy.delete(aux_mask3, numpy.where(aux_mask3 == idx)[0])
            aux_mask4 = numpy.where(auxcoords4 < 1.7)[0]
            if aux_mask4.size > 0:
                for y, idx in enumerate(aux_mask4):
                    parent = parents[y]
                    if pauxcoords4[parent] < 1.7:
                        aux_mask4 = numpy.delete(aux_mask4, numpy.where(aux_mask4 == idx)[0])
            aux_mask5 = numpy.where(auxcoords5 < 1.7)[0]
            if aux_mask5.size > 0:
                for y, idx in enumerate(aux_mask5):
                    parent = parents[y]
                    if pauxcoords5[parent] < 1.7:
                        aux_mask5 = numpy.delete(aux_mask5, numpy.where(aux_mask5 == idx)[0])
            aux_mask6 = numpy.where(auxcoords6 < 1.7)[0]
            if aux_mask6.size > 0:
                for y, idx in enumerate(aux_mask6):
                    parent = parents[y]
                    if pauxcoords6[parent] < 1.7:
                        aux_mask6 = numpy.delete(aux_mask6, numpy.where(aux_mask6 == idx)[0])
            aux_mask7 = numpy.where(auxcoords7 < 1.7)[0]
            if aux_mask7.size > 0:
                for y, idx in enumerate(aux_mask7):
                    parent = parents[y]
                    if pauxcoords7[parent] < 1.7:
                        aux_mask7 = numpy.delete(aux_mask7, numpy.where(aux_mask7 == idx)[0])
            aux_mask8 = numpy.where(auxcoords8 < 1.7)[0]
            if aux_mask8.size > 0:
                for y, idx in enumerate(aux_mask8):
                    parent = parents[y]
                    if pauxcoords8[parent] < 1.7:
                        aux_mask8 = numpy.delete(aux_mask8, numpy.where(aux_mask8 == idx)[0])
            aux_mask9 = numpy.where(auxcoords9 < 1.7)[0]
            if aux_mask9.size > 0:
                for y, idx in enumerate(aux_mask9):
                    parent = parents[y]
                    if pauxcoords9[parent] < 1.7:
                        aux_mask9 = numpy.delete(aux_mask9, numpy.where(aux_mask9 == idx)[0])
            aux_mask = numpy.concatenate((aux_mask1, 
                                          aux_mask2,
                                          aux_mask3,
                                          aux_mask4,
                                          aux_mask5,
                                          aux_mask6,
                                          aux_mask7,
                                          aux_mask8,
                                          aux_mask9))
            diff = numpy.setdiff1d(nextwtgraph, nextparents)
            merged = []
            
            # Build a list of all walkers that were involved in merge events
            for k in numpy.arange(0,nextweights.shape[0]):
                off = nextwtg_off[k]
                np = off + nextwtg_np[k]
                gotmerged = nextwtgraph[off:np]
                if gotmerged.shape[0] > 1:
                    merged.append(gotmerged)
            #print(merged)
            

            merge_ids = []
            merge_ids_add = []
            transaction_weights = []
            transaction_weights_add = []
            diff_where = []
   
            if aux_mask.shape[0] > 0:
                bstates = numpy.where(numpy.logical_and(pcoords[:,0]>18, pcoords[:,0]<22))[0][0]
                bstate_weights = weights[bstates]
                max_weight = bstate_weights.max()
                w = numpy.where(weights == max_weight)[0][0]
#                next_parent_add = numpy.where(nextparents == w)[0][0]

                if w in nextparents:
                    next_seg = numpy.where(nextparents==w)[0]
                    merge_ids_add.append(next_seg)
                    transaction_weights_add.append(weights[w])
                    if print_switch:
                        print(i, w, "---", next_seg)
                # The not so easy case where merging occurred
                else:
                    for num, m in enumerate(merged):
                        if w in m:
                            ndiff = numpy.setdiff1d(m,diff)
                            if ndiff.shape[0] > 1: 
                                if ndiff[0] in nextparents:
                                    ndiff = ndiff[0]
                                    continue
                                else:
                                    ndiff = ndiff[1]
                            if ndiff.size == 0:
                                next_seg = numpy.where(nextparents<0)[0]
                            else:
                                next_seg = numpy.where(nextparents==ndiff)[0]
                            merge_ids_add.append(next_seg)
                            transaction_weights_add.append(weights[w])
                            if print_switch:
                                print(i, w, ndiff, "-->", next_seg)
                        else:
                            continue

            # Figure out which child resulted from the segment
            for l in aux_mask:
                # The easiest case with no merging
                if l in nextparents:
                    next_seg = numpy.where(nextparents==l)[0]
                    merge_ids.append(next_seg)
                    transaction_weights.append(weights[l])
                    if print_switch:
                        print(i, l, "---", next_seg)
                # The not so easy case where merging occurred
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
                            if ndiff.size == 0:
                                next_seg = numpy.where(nextparents<0)[0]
                            else:
                                next_seg = numpy.where(nextparents==ndiff)[0]
                            merge_ids.append(next_seg)
                            transaction_weights.append(weights[l])
                            if print_switch:
                                print(i, l, ndiff, "-->", next_seg)
                        else:
                            continue
    
            # The following adds the transaction to a list that affects the x+1 iteration
            iteration = i
            iteration_plus_one = i+1
            for num, p in enumerate(merge_ids):
                next_parent_subtract = p
                # This is to account for splitting
                if len(next_parent_subtract) > 1:
                    num_split = len(next_parent_subtract)
                    split_weight = transaction_weights[num]/num_split
                    for q in next_parent_subtract:
                        transaction_array1 = numpy.array([iteration_plus_one, q, -1, split_weight])
                        transactions.append(transaction_array1)
                else:
                    transaction_array1 = numpy.array([iteration_plus_one, next_parent_subtract[0], -1, transaction_weights[num]])
                    transactions.append(transaction_array1)

            for num, p in enumerate(merge_ids_add):
                next_parent_add = p
                # This is to account for splitting
                if len(next_parent_add) > 1:
                    num_split = len(next_parent_add)
                    split_weight = transaction_weights_add[num]/num_split
                    for q in next_parent_add:
                        transaction_array2 = numpy.array([iteration_plus_one, q, 1, split_weight])
                        transactions.append(transaction_array2)
                # Or if no splitting occurred
                else:
                    transaction_array2 = numpy.array([iteration_plus_one, next_parent_add[0], 1, transaction_weights_add[num]])
                    transactions.append(transaction_array2)
                  
            # Now to do forward propagation.   
    
            merge_ids = []
            transaction_weights = []
            unique_transactions = []

            if not transactions:
                continue
            
            arr_t = numpy.array(transactions)
            arr_i = arr_t[:,0]
            if i not in arr_i:
                continue

            # This condenses the transaction list which makes the following much faster
            this_iter = arr_t[arr_i == i]
            arr_s = this_iter[:,1] 
            for u in numpy.unique(arr_s):
                unique_positions = this_iter[arr_s == u]
                total_weight = (unique_positions[:,2]*unique_positions[:,3]).sum()
                unique_array = numpy.array([int(i), int(u), int(1), total_weight])
                unique_transactions.append(unique_array)

            # Loop through the condensed list and propagate, similar to the merge block above
            for j in unique_transactions:
                if j[1] in nextparents:
                    next_seg = numpy.where(nextparents==j[1])[0]
                    merge_ids.append(next_seg)
                    transaction_weights.append(j[2]*j[3])
                    if print_switch:
                        print("FP", i, int(j[1]), "---", next_seg)
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
                            if ndiff.size == 0:
                                next_seg = numpy.where(nextparents<0)[0]
                            else:
                                next_seg = numpy.where(nextparents==ndiff)[0]
                            merge_ids.append(next_seg)
                            transaction_weights.append(j[2]*j[3])
                            if print_switch:
                                print("FP", i, int(j[1]), ndiff, "-->", next_seg)
                        else:
                            continue
                                
            # Add to transaction list
            iteration = i
            iteration_plus_one = i+1
            for num, p in enumerate(merge_ids):
                next_parent_subtract = p
                if len(next_parent_subtract) > 1:
                    num_split = len(next_parent_subtract)
                    split_weight = transaction_weights[num]/num_split
                    for q in next_parent_subtract:
                        transaction_array1 = numpy.array([iteration_plus_one, q, -1, split_weight])
                        transactions.append(transaction_array1)

                else:
                    transaction_array1 = numpy.array([iteration_plus_one, next_parent_subtract[0], -1, transaction_weights[num]])
                    transactions.append(transaction_array1)
 
            b = 0      
            if not transactions:
                continue

            arr_t = numpy.array(transactions)
            arr_i = arr_t[:,0]
 
            if i not in arr_i:
                continue
         
            # Modify the hdf5 file weights
            for r in arr_t[arr_i == i]:
                segment = int(r[1])
                new_weight = weights[segment] + r[2]*r[3]
                weights[segment] = new_weight
                b = 1
            
            if b == 1:
                weights /= (weights.sum())
                group = f[seg_index_path]
                group['weight', ...] = weights
            else:
                continue
            time2 = time.time()
            #print(time2-time1)
main()
