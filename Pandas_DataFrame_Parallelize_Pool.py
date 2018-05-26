#!/usr/bin/env python2
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

import multiprocessing, collections
from multiprocessing import Pool
from functools import partial

num_cores = multiprocessing.cpu_count() - 1
num_partitions = num_cores * 2

def multiply_columns(data, col_out, col_from, func):
    """
    The service function
    """

    data_eval = pd.DataFrame(data[col_from].apply(func).tolist(), index=data.index)#

    if col_out is not None:
        if isinstance(col_out, collections.Iterable):
            length_col_out = len(col_out)
        else:
            length_col_out, col_out = 1, [col_out]

        for i in range(length_col_out):
            if data_eval.shape[1] == 0:
                print ('='*15, )
                print (length_col_out, data_eval.shape)
                print ('='*15, )
            else:
                data[col_out[i]] = data_eval.iloc[:, i % data_eval.shape[1]]

        del length_col_out

    return data


def parallelize_dataframe_Pool(df
                          , col_out, col_from, func = lambda x: len(x)
                          , num_partitions = 10 #number of partitions to split dataframe
                          , num_cores = 4 #number of cores on your machine
                          , kwargs = ()
                          ):
    ''' The wraper
    Return type: pandas.DataFrame
    
    Parameters
    ----------
    df : pandas.DataFrame
        df in
    col_out : (list, str)
        List of names columns out.
        Only the first N = len(col_out) columns of results will be returned,
        regardless of the dimension of the results of the computation of the function
    col_from : str
        Name the column in.
    function : callable
        A function with signature function(x1, **kwargs) that apply
    num_partitions : int
        number of partitions to split dataframe
    num_cores : int
        number of cores on your machine
        
    '''

    df_split = np.array_split(df, num_partitions)
    pool = Pool(num_cores)
    
    df = pd.concat(pool.map(partial(multiply_columns, col_out=col_out, col_from=col_from\
                                    , func=partial(func, **kwargs)), df_split))

    pool.close()
    pool.join()

    return df

df_test = pd.DataFrame(range(10), columns=['val'])
print (df_test.head())

def funct_test(x, nb_iteration=3):
    return [x*i for i in range(nb_iteration)]

kwargs = {'nb_iteration':3}
df_test = parallelize_dataframe_Pool(df_test, ['ret_{0}'.format(x) for x in range(kwargs['nb_iteration'])], 'val', funct_test
                        , num_cores = min(df_test.shape[0], num_cores)
                        , num_partitions = min(df_test.shape[0], num_partitions)
                        , kwargs=kwargs)


print (df_test.head())
