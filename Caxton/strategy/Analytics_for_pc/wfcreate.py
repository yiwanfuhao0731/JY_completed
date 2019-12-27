import collections
import pandas as pd
import os
import numpy as np
from functools import reduce
import Analytics.series_utils as s_util
SU = s_util.date_utils_collection()
import panormus.data.bo.econ as econ
conn = econ.get_instance_using_credentials()
from datetime import datetime,timedelta
import pickle

TEMP_DIR = os.environ['TEMP']

class swf:
    def __init__(self, local_sample=('1943-01-01', '2025-01-01'),short_name=None):
        #check if it has been run before already:
        #if not short_name is None:
            #self.temp_pickle_dir = os.path.join(TEMP_DIR, short_name+'.pickle')
        wf = collections.OrderedDict()
        self.mysmpl = local_sample
        self.genr_empty_df()
        self.genr_empty_df_m()
        self.genr_empty_df_q()
        self.df = collections.OrderedDict()
        self.alpha = collections.OrderedDict()
        self.combo_indicator = collections.OrderedDict()

    def importts(self, dir, filetype='csv', repeat=True,to_freq='bday',date_parse=False,date_format='%d/%m/%Y',customColName=None,force_override = True):
        if filetype == 'csv':
            df_raw = pd.read_csv(dir, header=0, index_col=0)
            if date_parse:
                df_raw.index = pd.to_datetime(df_raw.index, format=date_format)

            if len(df_raw.index) < 0.001:
                _, tail = os.path.split(dir)
                new_name = tail.split('.')[0]
                if not force_override:
                    while new_name in self.df.keys():
                        new_name = new_name + '_new'
                self.df[new_name] = self.empty_df

            else:
                df_raw.index = pd.to_datetime(df_raw.index)
                if not (customColName is None):
                    df_raw.columns = customColName
                for s in df_raw.columns.tolist():
                    this_df = df_raw[[s]]
                    # should use conversion down to bday here!!!
                    if to_freq=='bday':
                        this_df = SU.conversion_to_bDay(this_df)
                        this_df = pd.merge(self.empty_df, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    elif to_freq=='M':
                        this_df = SU.conversion_down_to_m(this_df)
                        this_df = pd.merge(self.empty_df_m, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    elif to_freq=='Q':
                        this_df = SU.conversion_to_q(this_df)
                        this_df = pd.merge(self.empty_df_q, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    else:
                        print ('Frequency does not identify')
                        raise ValueError
                    if repeat:
                        first_idx = this_df.first_valid_index()
                        last_idx = this_df.last_valid_index()
                        this_df = SU.repeat_value(this_df,first_idx,last_idx)
                    new_name = s
                    if not force_override:
                        while new_name in self.df.keys():
                            new_name = new_name + '_new'
                    self.df[new_name] = this_df
        elif filetype == 'EconDB':
            if not isinstance(dir,list):
                print ('Sorry, the database ticker should be a list! ')
            df_raw = conn.get(dir)

            if len(df_raw.index) < 0.001:
                print ('Sorry, the ticker is not found in EconDB')
                raise ValueError

            else:
                df_raw.index = pd.to_datetime(df_raw.index)
                for s in df_raw.columns.tolist():
                    this_df = df_raw[[s]]
                    # should use conversion down to bday here!!!
                    if to_freq=='bday':
                        this_df = SU.conversion_to_bDay(this_df)
                        this_df = pd.merge(self.empty_df, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    elif to_freq=='M':
                        this_df = SU.conversion_down_to_m(this_df)
                        this_df = pd.merge(self.empty_df_m, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    elif to_freq=='Q':
                        this_df = SU.conversion_to_q(this_df)
                        this_df = pd.merge(self.empty_df_q, this_df, left_index=True, right_index=True, how='left')
                        this_df = this_df[[s]]
                    else:
                        print ('Frequency does not identify')
                        raise ValueError
                    if repeat:
                        first_idx = this_df.first_valid_index()
                        last_idx = this_df.last_valid_index()
                        this_df = SU.repeat_value(this_df,first_idx,last_idx)
                    new_name = s
                    if not force_override:
                        while new_name in self.df.keys():
                            new_name = new_name + '_new'
                    self.df[new_name] = this_df

    def genr_empty_df(self):
        date_rng = pd.date_range(start=self.mysmpl[0], end=self.mysmpl[1], freq='B')
        self.empty_df = pd.DataFrame(index=date_rng, columns=[0])

    def genr_empty_df_m(self):
        date_rng = SU.empty_M_df()
        self.empty_df_m = pd.DataFrame(index=pd.to_datetime(date_rng['Date']), columns=[0])
        self.empty_df_m = SU.conversion_to_FOM(self.empty_df_m.loc[self.mysmpl[0]:self.mysmpl[1],:])

    def genr_empty_df_q(self):
        date_rng = SU.empty_Q_df()
        self.empty_df_q = pd.DataFrame(index=pd.to_datetime(date_rng['Date']), columns=[0])
        self.empty_df_q = SU.conversion_to_FOM(self.empty_df_q.loc[self.mysmpl[0]:self.mysmpl[1], :])
        self.empty_df_q.loc[:,:] = 0

    def genr(self, name, const=0):
        df = self.empty_df.copy()
        new_name = name
        while new_name in self.df.keys():
            new_name = new_name + '_new'
        df.columns = [new_name]
        df = const
        self.df[new_name] = df
        return df

    def pool_genr(self, pool, poolname, prefix='', suffix='', const=np.nan):
        df = self.empty_df.copy()
        for iso in pool:
            col_name = prefix + iso + suffix
            df[col_name] = const
            df.drop(0, inplace=True)
        while poolname in self.df.keys():
            poolname = poolname + '_new'
        self.df[poolname] = df
        return self.df[poolname]

    def pool_makegroup(self, pool, poolname, prefix='', suffix=''):
        for iso in pool:
            key = prefix + iso + suffix
            if not key in self.df.keys():
                print('Sorry, can not make group if the series is not in the wf: ', key)
                raise ValueError
        dfs = [self.df[prefix + iso + suffix] for iso in pool]
        if len(dfs) == 1:
            df_pool = dfs[0]
        else:
            df_pool = reduce(lambda df1, df2: pd.merge(df1, df2, left_index=True, right_index=True, how='outer'), dfs)
        while poolname in self.df.keys():
            poolname = poolname + '_new'
        self.df[poolname] = df_pool
        return self.df[poolname]

    def update_df(self, df_name, new_df,to_freq='bday',repeat='True'):
        new_df.index = pd.to_datetime(new_df.index)
        # should use conversion down to bday here!!!
        if to_freq == 'bday':
            new_df = SU.conversion_to_bDay(new_df)
            new_df = pd.merge(self.empty_df, new_df, left_index=True, right_index=True, how='left')
            new_df = new_df.drop([0], axis=1)
        elif to_freq == 'M':
            new_df = SU.conversion_down_to_m(new_df)
            new_df = pd.merge(self.empty_df_m, new_df, left_index=True, right_index=True, how='left')
            new_df = new_df.drop([0], axis=1)

        elif to_freq == 'Q':
            new_df = SU.conversion_to_q(new_df)
            new_df = pd.merge(self.empty_df_q, new_df, left_index=True, right_index=True, how='left')
            new_df = new_df.drop([0], axis=1)
        else:
            print('Frequency does not identify')
            raise ValueError
        if repeat:
            for s in new_df.columns:
                last_idx = new_df.loc[:, [s]].last_valid_index()
                new_df.loc[new_df.index <= last_idx, s].fillna(method='ffill', inplace=True)

        self.df[df_name] = new_df

    def add_df(self, df_name, new_df, repeat=True,to_freq='bday',force_override = True):
        if not force_override:
            while df_name in self.df.keys():
                df_name = df_name + '_new'
            new_df.index = pd.to_datetime(new_df.index)
        else:
            if df_name in self.df.keys():
                print (df_name, ' is already in the df keys')
                del self.df[df_name]

        # should use conversion down to bday here!!!
        if to_freq == 'bday':
            new_df = SU.conversion_to_bDay(new_df)
            new_df = pd.merge(self.empty_df, new_df, left_index=True, right_index=True, how='left')
            new_df = new_df.drop([0],axis=1)
        elif to_freq == 'M':
            new_df = SU.conversion_down_to_m(new_df)
            new_df = pd.merge(self.empty_df_m, new_df, left_index=True, right_index=True, how='left')
            new_df = new_df.drop([0],axis=1)

        elif to_freq == 'Q':
            new_df = SU.conversion_to_q(new_df)
            new_df= pd.merge(self.empty_df_q, new_df, left_index=True, right_index=True, how='left')
            new_df= new_df.drop([0],axis=1)
        else:
            print('Frequency does not identify')
            raise ValueError
        if repeat:
            for s in new_df.columns:
                last_idx = new_df.loc[:, [s]].last_valid_index()
                new_df.loc[new_df.index <= last_idx, s].fillna(method='ffill', inplace=True)

        self.df[df_name] = new_df

    def create_indicator_group_folder(self, indicator_dir):
        """
        Tearsheet is saved to the reporting folder
        """
        destination = indicator_dir

        self.create_folder(destination)
        assert os.path.isdir(destination)
        assert os.path.exists(destination)

    def create_folder(self, path):
        "Creates all folders necessary to create the given path."
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except IOError as io:
            print("Cannot create dir %s" % path)
            raise io

def initialise_wf(cache,hours=12):
    if os.path.exists(cache):
        if os.path.getmtime(cache) > datetime.timestamp(datetime.now() - timedelta(hours=hours)):
            with open(cache, 'rb') as handle:
                local_db = pickle.load(handle)
            return local_db
        else:
            return swf()
    else:
        return swf()

def use_cached(key_list,cache):
    '''
    this decorator is used to check if the list of keys are already in the dictionary. if yes, the function need not to be re-run, to save some time
    :param key_list: a list of keys, i.e.['Forward_sum_Z','credit_ngdp_12_z']
    :param use_dict: the dictionary used in cache
    :return:
    '''
    def decorator(func):
        def wrapper(*args,**kwargs):
            if len([i for i in key_list if i not in cache.df.keys()])<0.01:
                return
            else:
                return func(*args, **kwargs)
        return wrapper
    return decorator


    # check if the key are all in the dictionary, if yes, return, else, run the function
    print ('pass this function!')
    pass

class indicator_tree:
    # a indicator tree is a tree class that put signal node into parent-child type structure
    # signal node is a node object that contain 3 things: name, collection of z_score, collection of raw_data
    def __init__(self):
        pass

    def get_df(self):
        pass

    def print_all_sub(self):
        pass

    def nodes(self):
        pass

class signal_node:
    def __init__(self):
        pass

    def signal_name(self):
        pass

 