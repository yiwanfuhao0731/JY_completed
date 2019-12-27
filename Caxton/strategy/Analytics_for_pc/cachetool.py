from datetime import datetime,timedelta
import os
import pickle
from strategy.Analytics_for_pc.wfcreate import swf

def cached(key_list,cache):
    '''
    this decorator is used to check if the list of keys are already in the dictionary. if yes, the function need not to be re-run, to save some time
    :param key_list: a list of keys.
    :param : memorised dictionary used in cache
    :return:
    '''
    def decorator(func):
        def wrapper(*args,**kwargs):
            if len([i for i in key_list if i not in cache.keys()])<0.01:
                return
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator

def initialise_cache_swf(cache,hours=12):
    if os.path.exists(cache):
        if os.path.getmtime(cache) > datetime.timestamp(datetime.now() - timedelta(hours=hours)):
            with open(cache, 'rb') as handle:
                local_db = pickle.load(handle)
            return local_db
        else:
            return swf()
    else:
        return swf()