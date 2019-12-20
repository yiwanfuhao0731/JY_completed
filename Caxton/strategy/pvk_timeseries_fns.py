import pandas as pd
import numpy as np
import itertools


# compute returns
def returns_series(price_series, return_type, return_period=1):
    if return_type == 'not':
        price_series = price_series.apply(np.log)
    elif return_type.lower() == 'ed dv01':
        price_series = 10000 - price_series * 100
    elif return_type.lower() == 'dv01':
        price_series = price_series * 100
    elif return_type.lower() == 'bp dv01':
        None
    else:
        raise ValueError('return type \'{}\' not recognized'.format(return_type))

    return price_series.fillna(method='ffill').diff(periods=return_period).apply(
        lambda x: x / np.sqrt(return_period))


def align(pd_objects, join='outer', axis=0):
    """apply align on all combinations of the list of pd objects"""
    for (i, j) in itertools.combinations(range(len(pd_objects)), 2):
        (pd_objects[i], pd_objects[j]) = pd_objects[i].align(pd_objects[j], join, axis)

    return tuple(pd_objects)


def meanreversion_line(srs, periods=range(1, 31), ret_type='log'):
    """Mean reversion line (compare realized vol at different intervals)"""

    if ret_type not in ('log', 'arith', 'pct'):
        raise ValueError(f'Return type must be log, arith, pct (given type \'{ret_type}\' not recognized')

    std = pd.Series(index=periods)

    for per in periods:
        if ret_type == 'arith':
            rets = srs.diff(per)
        elif ret_type == 'log':
            rets = srs.apply(np.log).diff(per)
        elif ret_type == 'pct':
            rets = srs / srs.shift(per) - 1
        else:
            raise ValueError(f'Uncaught error. Return type must be log, arith, pct (given type \'{ret_type}\' not ' +
                             'recognized')

        std[per] = np.std(rets) * np.sqrt(252 / per)

    return std

print ('hello world')
list_of_pd = []
for i in range(3):
    data = np.random.rand(10,1)
    pd0 = pd.DataFrame(index = [i for i in range(data.shape[0])],data=data)
    list_of_pd.append(pd0)
#print (list_of_pd)
a_pd = align(list_of_pd)
print (a_pd)