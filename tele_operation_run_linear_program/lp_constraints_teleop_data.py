from cvxopt import matrix, solvers, spmatrix
from cvxopt.modeling import variable, op, dot
from cvxopt import matrix, solvers
from scipy.stats import norm
import numpy as np
import math
import sys
from multiprocessing import pool
from functools import partial
import pickle
from collections import OrderedDict
import gc
from prepare_data import create_C_lambda_probs, create_C_lambda, create_W, get_workers, create_pjt


def get_index_vals_i(C_lambda_probs, T, C, size_i, size_j, parms):
    import time

    tt1 = time.time()

    i = parms[0]
    t = parms[1]
    J = []
    TIME = []
    T_TAG = []
    vals = []
    parms_list = []

    for t_arr in range(t + 1):
        for t_tag in range(t_arr, t_arr + C):
            if t_tag > t:
                continue
            for j in range(size_j):

                sample = C_lambda_probs[i][j][t - t_tag]
                if sample > 0:  # only append variables of possible prob
                    J.append(j)
                    TIME.append(t_arr)
                    T_TAG.append(t_tag)
                    vals.append(sample)
                    parms_list.append(parms)

    tt2 = time.time() - tt1

    ret_val = {}

    for parm, j, t, t_tag, val in zip(parms_list, J, TIME, T_TAG, vals):
        if parm not in ret_val:
            ret_val[parm] = {}

        if t not in ret_val[parm].keys():
            ret_val[parm][t] = {}
        if t_tag not in ret_val[parm][t].keys():
            ret_val[parm][t][t_tag] = {}
        if j not in ret_val[parm][t][t_tag].keys():
            ret_val[parm][t][t_tag][j] = {}
        ret_val[parm][t][t_tag][j] = val

    return ret_val


def get_index_vals_j(g, T, C, size_i, size_j, parms):
    j = parms[0]
    t = parms[1]
    I = []
    T_TAG = []
    vals = []
    parms_list = []

    for t_tag in range(t, t + C):
        for i in range(size_i):
            I.append(i)
            T_TAG.append(t_tag)
            vals.append(1.0)
            parms_list.append(parms)

    return parms_list, I, T_TAG, vals


def create_variables(size_j, size_i, C, t, ln=0):
    variables = OrderedDict()
    ind = ln
    for t_tag in range(t, t + C):
        for i in range(size_i):
            for j in range(size_j):
                variables[(t, t_tag, i, j)] = ind
                ind += 1

    return ind, variables


def create_constraint_2(variables, b_2, parms):
    column = []
    coefl = []

    key, val = parms[0], parms[1]

    t = key[1]
    j = key[0]
    consts_vars = []
    for t_tag, val2 in val.items():
        for i, coef in val2.items():
            column.append(variables[(t, t_tag, i, j)])
            coefl.append(coef)

    
    return coefl, column, b_2[key]


def create_constraint_3(variables, b_3, parms):
    column = []
    coefl = []

    key, val = parms[0], parms[1]
    t = key[1]
    i = key[0]
    consts_vars = []
    for t, val1 in val.items():
        for t_tag, val2 in val1.items():
            for j, coef in val2.items():
                coef = float(coef)
                column.append(variables[(t, t_tag, i, j)])
                coefl.append(coef)

    return coefl, column, b_3[key]


def create_constraint_4_a(parms):
    key = parms[0]
    val = parms[1]
    column = [val]
    coef = [-1.0]

    return coef, column, 0.0


def create_constraint_4_b(parms):
    key = parms[0]
    val = parms[1]
    column = [val]
    coef = [1.0]

    return coef, column, 1.0


def update_big_dict2(big_dict, parms, I, T_TAG, vals):
    for parm, i, t_tag, val in zip(parms, I, T_TAG, vals):
        big_dict[parm][t_tag][i] = val


def update_big_dict3(big_dict, parms, J, TIME, T_TAG, vals):
    for parm, j, t, t_tag, val in zip(parms, J, TIME, T_TAG, vals):
        if t not in big_dict[parm].keys():
            big_dict[parm][t] = {}
        if t_tag not in big_dict[parm][t].keys():
            big_dict[parm][t][t_tag] = {}
        if j not in big_dict[parm][t][t_tag].keys():
            big_dict[parm][t][t_tag][j] = {}
        big_dict[parm][t][t_tag][j] = val


def update_big_dict(big_dict, parms, I, J, TIME, T_TAG, vals):
    for parm, i, j, t, t_tag, val in zip(parms, I, J, TIME, T_TAG, vals):
        big_dict[parm][t][t_tag][i][j] = val


def main_code(T,C,num_of_workers):
    load = False
    save = False

    fname = 'normal_and_pjt'
    T = T + C
    C = C +1

    alpha = 0.5
    workers = get_workers()[:num_of_workers] # no more than 11
    W = create_W(workers, C)

    LHS = len(workers)

    pjt = create_pjt(T-C+1 , C-1)
    RHS = len(pjt[0])
    C_lambda_dict = create_C_lambda(workers)
    C_lambda_probs = create_C_lambda_probs(LHS, RHS, T-C+1, C-1, C_lambda_dict)

    print("Start with initializing LP", flush=True)
    size_i = LHS
    size_j = RHS
    b_3 = {}
    b_2 = {}
    for j in range(size_j):
        for t in range(T):
            b_2[(j, t)] = float(pjt[t][j])

    for i in range(size_i):
        for t in range(T):
            b_3[(i, t)] = 1.0

    parms_i, parms_j = [], []
    for t in range(T):
        for i in range(size_i):
            parms_i.append((i, t))
        for j in range(size_j):
            parms_j.append((j, t))
    print("done init parms", flush=True)

    big_dict_const2 = {parm: {int(parm[1]) + t_tag: {i: 1.0 for i in range(size_i)} for t_tag in
                              range(C)} for parm in parms_j}
    print("done create dict2", flush=True)
    big_dict_const3 = {parm: {} for parm in parms_i}
    print("done init dict3", flush=True)

    k_i = partial(get_index_vals_i, C_lambda_probs, T, C, size_i, size_j)
    k_j = partial(get_index_vals_j, C_lambda_probs, T, C, size_i, size_j)
    if load:
        with open('dictionaries.pickle', 'rb') as handle:
            dictionaries = pickle.load(handle)
    else:
        with pool.Pool(70) as p:
            dictionaries = list(p.map(k_i, parms_i))

    if save:
        with open('results/dictionaries.pickle', 'wb') as handle:
            pickle.dump(dictionaries, handle, protocol=pickle.HIGHEST_PROTOCOL)

    print("done update dict3 as lists", flush=True)

    sd = {key: val for d in dictionaries for key, val in d.items()}
    big_dict_const3.update(sd)

    print("done update dict3 as total", flush=True)

    variables = OrderedDict()
    ln = 0
    for item in range(T):
        ln, dct = create_variables(size_j, size_i, C, item, ln)
        variables.update(dct)
        print("created variables for t {}".format(item), flush=True)

    print("done variables", flush=True)

    k_conatraint2 = partial(create_constraint_2, variables, b_2)
    k_conatraint3 = partial(create_constraint_3, variables, b_3)

    constraints2 = []
    print("Starting for constraint 2 with {} entities ".format(len(big_dict_const2.items())), flush=True)

    cnt = 0
    for item in big_dict_const2.items():
        cnt += 1
        constraints2.append(k_conatraint2(item))
        if cnt % 1000 == 0:
            print("const 2 cnt is {}".format(cnt), flush=True)

    print("done conastraint2", flush=True)

    print("Starting for constraint 3 with {} entities ".format(len(big_dict_const3.items())), flush=True)

    constraints3 = []
    cnt = 0
    for item in big_dict_const3.items():
        cnt += 1
        constraints3.append(k_conatraint3(item))
        if cnt % 1000 == 0:
            print("const 3 cnt is {}".format(cnt), flush=True)

    constraints4a = []
    print("done conastraint3", flush=True)

    del big_dict_const3
    del big_dict_const2

    gc.collect()
    print("Starting for constraint 4a with {} entities".format(len(variables.items())), flush=True)
    cnt = 0
    for item in variables.items():
        cnt += 1
        constraints4a.append(create_constraint_4_a(item))
        if cnt % 500000 == 0:
            print("const 4a cnt is {}".format(cnt), flush=True)
    print("done conastraint4a", flush=True)

    constraints4b = []
    print("done conastraint4a", flush=True)
    print("Starting for constraint 4b with {} entities".format(len(variables.items())), flush=True)

    cnt = 0
    for item in variables.items():
        cnt += 1
        constraints4b.append(create_constraint_4_b(item))

        if cnt % 500000 == 0:
            print("const 4b cnt is {}".format(cnt), flush=True)
    print("done conastraint4b", flush=True)

    print("generating Matrix A", flush=True)
    from itertools import chain

    constraints_list = constraints2 + constraints3 + constraints4a + constraints4b

    R = []
    C = []
    V = []

    b = []
    row = 0
    print("generating constraints list with {} constraints".format(len(constraints_list)), flush=True)
    step = len(constraints_list) // 100
    cnt = 0

    for item in constraints_list:
        Rr = []
        Cr = []
        Vr = []

        for coef, column in zip(item[0], item[1]):
            Rr.append(row)
            Vr.append(coef)
            Cr.append(column)

        R.append(Rr)
        C.append(Cr)
        V.append(Vr)
        b.append(item[2])
        row += 1
        if row % step == 0:
            print("so far generated {} constraints".format(row), flush=True)

    del constraints_list
    gc.collect()
    print("building A and b", flush=True)
    with open("results/vals_" + fname + ".csv", "w+") as f:
        np.array([item for sublist in V for item in sublist]).tofile(f, sep=",")

    with open("results/I_" + fname + ".csv", "w+") as f:
        np.array([item for sublist in R for item in sublist]).tofile(f, sep=",")

    with open("results/J_" + fname + ".csv", "w+") as f:
        np.array([item for sublist in C for item in sublist]).tofile(f, sep=",")

    with open("results/b_" + fname + ".csv", "w+") as f:
        np.array(b).tofile(f, sep=",")

    print("done build constraints list", flush=True)
    c1 = 0
    c2 = 0
    obj_ls = []

    print("NUMBER OF VARIABLES IS ", len(variables.items()), flush=True)

    C = []
    column = []

    for name, var in variables.items():
        weight = W[name[2]][name[3]][name[1]-name[0]] + 1
        if weight > 0:
            c1 += 1
            if c1 % 1000 == 0:
                print("c1 weight ", c1, flush=True)
            C.append( -1 * weight)
            column.append(var)
        else:
            c2 += 1
            if c2 % 1000 == 0:
                print("c2 weight ", c2, flush=True)
            C.append(0)

    print("Buidling Objective matrix", flush=True)

    with open("results/c_" + fname + ".csv", "w+") as f:
        np.array(C).tofile(f, sep=",")

    import pickle

    dct = {v: k for k, v in variables.items()}

    with open("results/dict_" + fname + ".csv", "wb") as f:
        pickle.dump(dct, f, protocol=pickle.HIGHEST_PROTOCOL)

    print("Done Building objective, time to run solver", flush=True)

if __name__ == '__main__':
    main_code(3600, 10, 3)
