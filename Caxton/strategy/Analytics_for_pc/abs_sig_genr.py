import os
import sys
import pandas as pd

import numpy as np

import collections

import panormus.data.bo.econ as econ

import Analytics.series_utils as s_util

from Analytics.download_util import Downloader as Downloader

import Analytics.finance as fin_lib

import Analytics.scoring_methods as smc

import os

import pickle

from datetime import datetime, timedelta

import Analytics.wfcreate as wf

from panormus.data import haver

import backtesting_utils.post_signal_genr as post_signal_genr

import backtesting_utils.chart as TCT

import json

# visualisation packages

import math

import importlib

import glob

import matplotlib.dates as mdates

import matplotlib.pyplot as plt

from matplotlib.backends.backend_pdf import PdfPages

import collections

from textwrap import wrap

# part of the utilities

dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')

dateparse2 = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')

conn = econ.get_instance_using_credentials()

WKDIR = os.path.dirname(os.path.realpath(__file__))

PROJ_DIR = os.path.join(WKDIR, "..")

TEMP_DIR = os.environ['TEMP']

TEMP_RAW_PICKLE = os.path.join(TEMP_DIR, 'TEMP_JY_local_db.pickle')


class abstract_sig_genr:

    def __init__(self, *args, **kwargs):

        self.local_db = self.read_pickle(*args, **kwargs)
        self.cache_db = self.read_pickle(*args, **kwargs)

        self.SU = s_util.date_utils_collection()

        self.SM = smc.scoring_method_collection()

    def conn_get_clean(self, ticker):

        df_raw = conn.get(ticker)

        df_list = []

        if len(df_raw.columns) < 0.01:
            df_list.append(df_raw)

        for col in df_raw.columns:

            df = df_raw[[col]]

            if len(df.index) > 0.01:

                test_index = [d.date() for d in df.loc[:'2019-8-19', :].index[-1000:]]

                if len(test_index) != len(set(test_index)):
                    new_index = [d.replace(hour=0, minute=0, second=0) for d in df.index]

                    df.index = new_index

                    df.dropna(inplace=True)

                    df = df[~df.index.duplicated(keep='last')]

            df_list.append(df)

        return pd.concat(df_list, axis=1)

    def read_pickle(self, *args, **kwargs):

        '''

        :return: read pickle

        '''

        if kwargs.get('use_cached') in [False]:
            return dict()

        if os.path.exists(TEMP_RAW_PICKLE):

            if os.path.getmtime(TEMP_RAW_PICKLE) > datetime.timestamp(datetime.now() - timedelta(hours=5)):

                with open(TEMP_RAW_PICKLE, 'rb') as handle:

                    self.local_db = pickle.load(handle)

                return self.local_db

            else:

                return dict()

        else:

            return dict()

    def check_in_use(self):

        # TODO: this file is to check if the pdf charts file is in use

        '''

        :return: check if in use

        '''

        pass

    # TODO: try to use the suffix-name in the file , as the key to the data set, must be much easier to

    def download_data_2(self, MASTER_INPUT_DIR, Short_Name):

        '''

        downloading method allows for more sources (EconDB or csv), return a dictionary

        :return: an ordered dictionary in the format like: {iso1:{'data':[df1,df2],'des':[des1,des2]}}

        '''

        result_dict = collections.OrderedDict()

        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)

        import_df.replace(np.nan, 'invalid_name', inplace=True)

        iso_list = import_df['ISO'].values.tolist()

        for iso in set(iso_list):
            result_dict[iso] = {}

            result_dict[iso]['data'] = []

            result_dict[iso]['des'] = []

        # start download the data

        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

        for i in range(number_of_series_col):

            i = str(int(i) + int(1))

            series_col = ('').join(['series', i])

            load_fn_col = ('').join(['load_fn', i])

            location_col = ('').join(['location', i])

            ser_list = import_df[series_col].values.tolist()

            load_fn_list = import_df[load_fn_col].values.tolist()

            location_list = import_df[location_col].values.tolist()

            for k, iso in enumerate(iso_list):

                if ser_list[k] == 'invalid_name':
                    continue

                if load_fn_list[k] == 'EconDB':
                    des = len(result_dict[iso]['des'])

                    result_dict[iso]['des'].append(des)

                    data = self.conn_get_clean([ser_list[k]])

                    print(iso, ser_list[k], ' is done!')

                    result_dict[iso]['data'].append(data)

                if load_fn_list[k] == 'csv':
                    print('Sorry, currently does not support csv import, to be added...')

        return result_dict

    def download_data_3(self, MASTER_INPUT_DIR, Short_Name, export_backup=None):

        '''

        downloading method allows for more sources (EconDB, csv or haver), return a dictionary

        the returned data structure is different, as {'AUS_SER1':df_AUS_SER1, 'AUS_SER2':df_AUS_SER2 ...}, so that you can use wildcard to iterate through

        :return: an ordered dictionary in the format like: {iso1:{'data':[df1,df2],'des':[des1,des2]}}

        :param MASTER_INPUT_DIR: excel spread sheet of data info

        :param Short_Name: excel tac name

        :param export_backup: export a backup to json file so that it is readable by git

        :return: dict of data

        '''

        print('start downloading data...')

        result_dict = collections.OrderedDict()

        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)

        if not export_backup in [None]:
            self.dump_param_to_txt(import_df, export_backup)

        import_df.replace(np.nan, 'invalid_name', inplace=True)

        iso_list = import_df['ISO'].values.tolist()

        # start download the data

        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

        for i in range(number_of_series_col):

            i = str(int(i) + int(1))

            series_col = ('').join(['series', i])

            load_fn_col = ('').join(['load_fn', i])

            location_col = ('').join(['location', i])

            suffix_col = ('').join(['in_file_suf_fix', i])

            ser_list = import_df[series_col].values.tolist()

            load_fn_list = import_df[load_fn_col].values.tolist()

            location_list = import_df[location_col].values.tolist()

            suffix_list = import_df[suffix_col].values.tolist()

            for k, iso in enumerate(iso_list):

                if ser_list[k] == 'invalid_name':
                    result_dict[suffix_list[k]] = pd.DataFrame()

                    continue

                if 'panel' in ser_list[k]:

                    assert location_list[k] != 'invalid_name', 'sorry, the loc is invalid'

                    try:

                        result_dict[suffix_list[k]] = pd.read_csv(location_list[k], index_col=0, header=0)

                    except:

                        loc = os.path.join(PROJ_DIR, location_list[k])

                        result_dict[suffix_list[k]] = pd.read_csv(loc, index_col=0, header=0)

                    continue

                if load_fn_list[k] == 'EconDB':

                    if ser_list[k] not in self.local_db.keys():

                        df = self.conn_get_clean([ser_list[k]])

                        result_dict[suffix_list[k]] = df

                        self.local_db[ser_list[k]] = df

                    else:

                        result_dict[suffix_list[k]] = self.local_db[ser_list[k]]

                    continue

                if load_fn_list[k] == 'csv':

                    assert location_list[k] != 'invalid_name', 'sorry, the loc is invalid'

                    if not os.path.isfile(location_list[k]):

                        try:

                            loc = os.path.join(PROJ_DIR, location_list[k])

                            df = pd.read_csv(loc, index_col=0, header=0)

                        except:

                            result_dict[suffix_list[k]] = pd.DataFrame()

                            continue

                    else:

                        df = pd.read_csv(location_list[k], index_col=0, header=0)

                        df.index = pd.to_datetime(df.index)

                    if len(df.index) < 0.001 or (ser_list[k] not in df.columns.tolist()):
                        result_dict[suffix_list[k]] = pd.DataFrame()

                        continue

                    df.index = pd.to_datetime(df.index)

                    result_dict[suffix_list[k]] = df.loc[:, [ser_list[k]]]

                    continue

                if load_fn_list[k] == 'haver':

                    if ser_list[k] not in self.local_db.keys():

                        # print (ser_list[k])

                        try:

                            df = haver.get_data([ser_list[k]])

                        except:

                            print(ser_list[k])

                        # print (df)

                        try:

                            df.index = df.index.to_timestamp()

                        except:

                            print(df)

                            raise ValueError

                        result_dict[suffix_list[k]] = df

                        self.local_db[ser_list[k]] = df

                    else:

                        result_dict[suffix_list[k]] = self.local_db[ser_list[k]]

                    continue

        # dump the updated file to the pickle

        with open(TEMP_RAW_PICKLE, 'wb') as handle:

            pickle.dump(self.local_db, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print('done downloading data...')

        return result_dict

    def download_data_3_1(self, MASTER_INPUT_DIR, Short_Name, export_backup=None):

        '''

        adding actual release date field to the download

        '''

        print('start downloading data...')

        result_dict = collections.OrderedDict()

        actual_release_dict = collections.OrderedDict()

        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)

        if not export_backup in [None]:
            self.dump_param_to_txt(import_df, export_backup)

        import_df.replace(np.nan, 'invalid_name', inplace=True)

        iso_list = import_df['ISO'].values.tolist()

        # start download the data

        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

        for i in range(number_of_series_col):

            i = str(int(i) + int(1))

            series_col = ('').join(['series', i])

            load_fn_col = ('').join(['load_fn', i])

            location_col = ('').join(['location', i])

            suffix_col = ('').join(['in_file_suf_fix', i])

            ser_list = import_df[series_col].values.tolist()

            load_fn_list = import_df[load_fn_col].values.tolist()

            location_list = import_df[location_col].values.tolist()

            suffix_list = import_df[suffix_col].values.tolist()

            for k, iso in enumerate(iso_list):

                if ser_list[k] == 'invalid_name':
                    result_dict[suffix_list[k]] = pd.DataFrame()

                    continue

                if 'panel' in ser_list[k]:

                    assert location_list[k] != 'invalid_name', 'sorry, the loc is invalid'

                    try:

                        result_dict[suffix_list[k]] = pd.read_csv(location_list[k], index_col=0, header=0)

                    except:

                        loc = os.path.join(PROJ_DIR, location_list[k])

                        result_dict[suffix_list[k]] = pd.read_csv(loc, index_col=0, header=0)

                    continue

                if load_fn_list[k] == 'EconDB':

                    if ser_list[k] not in self.local_db.keys():

                        df = self.conn_get_clean([ser_list[k]])

                        result_dict[suffix_list[k]] = df

                        self.local_db[ser_list[k]] = df

                    else:

                        result_dict[suffix_list[k]] = self.local_db[ser_list[k]]

                    continue

                if load_fn_list[k] == 'csv':

                    assert location_list[k] != 'invalid_name', 'sorry, the loc is invalid'

                    if not os.path.isfile(location_list[k]):

                        try:

                            loc = os.path.join(PROJ_DIR, location_list[k])

                            df = pd.read_csv(loc, index_col=0, header=0)

                        except:

                            result_dict[suffix_list[k]] = pd.DataFrame()

                            continue

                    else:

                        df = pd.read_csv(location_list[k], index_col=0, header=0)

                        df.index = pd.to_datetime(df.index)

                    if len(df.index) < 0.001 or (ser_list[k] not in df.columns.tolist()):
                        result_dict[suffix_list[k]] = pd.DataFrame()

                        continue

                    df.index = pd.to_datetime(df.index)

                    result_dict[suffix_list[k]] = df.loc[:, [ser_list[k]]]

                    continue

                if load_fn_list[k] == 'haver':

                    if ser_list[k] not in self.local_db.keys():

                        # print (ser_list[k])

                        try:

                            df = haver.get_data([ser_list[k]])

                        except:

                            print(ser_list[k])

                        # print (df)

                        try:

                            df.index = df.index.to_timestamp()

                        except:

                            print(df)

                            raise ValueError

                        result_dict[suffix_list[k]] = df

                        self.local_db[ser_list[k]] = df

                    else:

                        result_dict[suffix_list[k]] = self.local_db[ser_list[k]]

                    continue

        # dump the updated file to the pickle

        with open(TEMP_RAW_PICKLE, 'wb') as handle:

            pickle.dump(self.local_db, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print('done downloading data...')

        return result_dict

    def download_data_4(self, CONTROL_FILE_DIR, sheet_name='Control'):

        # data downloader for control file @FT format

        result_dict = collections.OrderedDict()

        import_df = pd.read_excel(CONTROL_FILE_DIR, sheet_name=sheet_name, index_col=False, header=0)

        iso_list = import_df['Country'].values.tolist()

        # download the data

        ser_list = import_df['Code'].values.tolist()

        for k, iso in enumerate(iso_list):
            result_dict[ser_list[k]] = self.conn_get_clean([ser_list[k]])

            print(iso, ser_list[k], ' is downloaded')

        return result_dict

    def import_csv_panel(self, MASTER_INPUT_DIR, Short_Name, csv_path, series):

        '''

        :return: import the csv files given be FT, which

        contains the very long historical real GDP data. Then sort them into a dictionary, with (iso,df) pairs

        '''

        SU = s_util.date_utils_collection()

        iso_with_series = self.import_names_to_list(MASTER_INPUT_DIR, Short_Name, series)

        raw_data = pd.read_csv(csv_path, index_col=0, header=0, na_values='NA')

        # print (raw_data)

        result_dict = {}

        for iso, series in iso_with_series:

            # assert series in raw_data.columns.tolist() , "sorry, series"+series+' is not in the raw data columns!'

            if series not in raw_data.columns.tolist():
                continue

            df = raw_data[[series]]

            df = SU.truncate_NAs(df)

            result_dict[iso] = df

        # print (self.raw_data)

        return result_dict

    def import_names_to_list(self, master_input_dir, Short_Name, col_name):

        import_df = pd.read_excel(master_input_dir, sheet_name=Short_Name, index_col=False, header=0)

        import_df.replace(np.nan, 'invalid_name', inplace=True)

        self.all_ISO = import_df.iloc[:, 0].values.tolist()

        series1 = import_df.loc[:, col_name].values.tolist()

        return list(zip(self.all_ISO, series1))

    def download_data(self, MASTER_INPUT_DIR, Short_Name, series_name):

        '''

        :return: connecting to the sql-server (in this case via a Downloader class), and converting into a dictionary of (ISO,dataframe) pair

        '''

        s_with_id = self.import_names_to_list(MASTER_INPUT_DIR, Short_Name, series_name)

        download = Downloader(s_with_id)

        # try:

        series_result = download.submit_with_mp()

        # except:

        # series_result = download.submit()

        result_dict = download.convert_result_to_dict(series_result, s_with_id)

        # print (result_dict)

        return result_dict

    def sanity_check(self):

        pass

    def apply_smoothing(self):

        pass

    def post_conversion(self):

        pass

    def convert_indicator_tolist(self, indicator, indicator_exp_dir):

        indicator_list = []

        for iso, content in indicator.items():

            if len(content['des']) == 0:

                continue

            else:

                zipped = zip(content['des'], content['data'])

                this_list = [(iso + '_' + str(v[0]), v[1]) for v in zipped]

                indicator_list = indicator_list + this_list

        self.indicator_list = indicator_list

        self.export_indicator(indicator_list, indicator_exp_dir)

        return

    def convert_indicator_tolist_new(self, indicator, indicator_exp_dir):

        self.create_indicator_group_folder(indicator_exp_dir)

        indicator_list = []

        for k, v in indicator.items():
            indicator_list = indicator_list + [(k, v)]

        self.indicator_list = indicator_list

        self.export_indicator(indicator_list, indicator_exp_dir)

        return

    def export_indicator(self, indicator_list, export_path):

        for ticker_df in indicator_list:
            # print (id_df)

            name_of_file = str(ticker_df[0])

            ticker_df[1].to_csv(os.path.join(export_path, name_of_file + '.csv'))

        return

    def create_indicator_group_folder(self, indicator_dir):

        """

        Tearsheet is saved to the reporting folder

        """

        destination = indicator_dir

        self.create_folder(destination)

        assert os.path.isdir(destination)

        assert os.path.exists(destination)

        return destination

    # TODO:might put a group of charting utilities, including pdf/tearsheet etc into a

    # tearsheet utility, so that can be used in the backtesting reporting in the future

    def create_tearsheet_folder(self, RPTDIR):

        """

        Tearsheet is saved to the reporting folder

        """

        destination = RPTDIR

        self.create_folder(destination)

        assert os.path.isdir(destination)

        assert os.path.exists(destination)

        return destination

    def create_folder(self, path):

        "Creates all folders necessary to create the given path."

        try:

            if not os.path.exists(path):
                os.makedirs(path)

        except IOError as io:

            print("Cannot create dir %s" % path)

            raise io

    def create_report_page(self, all_dict):

        pass

    def pickle_result(self, temp_pickle, strategy_work_file):

        '''

        save down the strategy container (work_frame) for future use

        :param temp_pickle: pickle path

        :param strategy_work_file: work_frame object

        '''

        # pickle the result to a temp folder

        with open(temp_pickle, 'wb') as handle:
            pickle.dump(strategy_work_file, handle, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def mask_list(l, m):

        return [z[0] for z in zip(l, m) if z[1]]

    def dump_param_to_txt(self, df, txt_path, type='data_ticker'):

        if type in ['data_ticker']:

            col = []

            for c in df.columns:

                if 'Unnamed' in c:

                    break

                else:

                    col = col + [c]

            df = df.loc[:, col]

            last_valid = df.iloc[:, 0].last_valid_index()

            df = df.loc[df.index <= last_valid, :]

            df.to_csv(txt_path)

            # j = df.to_json(orient='index')


class bulk_data_download(abstract_sig_genr):
    '''

    signal pre-processing

    '''

    def __init__(self, *args, **kwargs):

        super(bulk_data_download, self).__init__(*args, **kwargs)

    def add_dir_info(self, *args, **kwargs):

        '''

        adding directory information of the strategy

        '''

        self.time_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '%Y%m%d_%H%M')

        self.date_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '_%Y%m%d')

        self.WKDIR = os.path.dirname(os.path.realpath(__file__))

        self.PROJ_DIR = os.path.join(self.WKDIR, "..")

        self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")

        self.PARAM_DIR = os.path.join(self.PROJ_DIR, "input", 'param_backup', self.Short_Name)

        # self.PARAM_DIR_DATATICKER_BACKUP_TXT = os.path.join(self.PARAM_DIR,'DATA_TICKER'+self.date_stamp+'.txt')

        # self.PARAM_DIR_TRANSPARAM_BACKUP_TXT = os.path.join(self.PARAM_DIR,'TRANS_PARAM'+self.date_stamp+'.txt')

        self.PARAM_DIR_DATATICKER_BACKUP_TXT = None

        self.PARAM_DIR_TRANSPARAM_BACKUP_TXT = None

        self.create_folder(self.PARAM_DIR)

        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR, "output")

        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name)

        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name, 'indicator_group')

        self.LOCAL_MACRO_DATA_DIR = os.path.join(self.PROJ_DIR, 'macro_data')

        # ======================================================

        # ======================================================

        self.BT_ID1 = self.Short_Name + self.time_stamp + '.pdf'

        self.BT_ID1985_1 = self.Short_Name + '_1985_' + self.time_stamp + '.pdf'

        self.BT_ID2 = self.Short_Name + self.time_stamp + '_pnl.pdf'

        self.BT_WITH_RB_BREAKDOWN_ID2 = self.Short_Name + self.time_stamp + '_pnl_with_rb_breakdown.pdf'

        self.BT_ID2010_1 = self.Short_Name + self.time_stamp + '2010.pdf'

        self.BT_ID2010_2 = self.Short_Name + self.time_stamp + '_pnl2010.pdf'

        self.BT_ID1990_1 = self.Short_Name + self.time_stamp + '1990.pdf'

        self.BT_ID1990_2 = self.Short_Name + self.time_stamp + '_pnl1990.pdf'

        self.BT_BACKUP_ROOT_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP')

        # backup component since 2000

        self.BT_BACKUP_DIR1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1)

        # backup component since 1985

        self.BT_BACKUP1985_DIR1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1985_1)

        # backup pnl since 2000

        self.BT_BACKUP_DIR2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2)

        self.BT_BACKUP_WITH_RB_BREAKDOWN_DIR2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP',
                                                             self.BT_WITH_RB_BREAKDOWN_ID2)

        # backup component since 2000

        self.BT_BACKUP_DIR2010_1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2010_1)

        # backup pnl since 2010

        self.BT_BACKUP_DIR2010_2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2010_2)

        # backup pnl since 1990

        self.BT_BACKUP_DIR1990_2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1990_2)

        self.PARS_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'PARS' + self.time_stamp + '.csv')

        self.DATA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'DATA' + self.time_stamp + '.csv')

        self.ALPHA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'ALPHA' + self.time_stamp + '.csv')

        # Firstly export all the relevant result into the csv format. Secondly plot into the charts

        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'

        self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.Short_Name) if os.access(self.SHARE_DIR,
                                                                                              os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', self.Short_Name)

        # reporting file

        # reporting component since 2000

        self.RPT_COMPONENT_DIR = os.path.join(self.RPTDIR,
                                              self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '.pdf')

        # reporting component since 1985

        self.RPT_COMPONENT1985_DIR = os.path.join(self.RPTDIR,

                                                  self.Short_Name + '_1985_' + datetime.utcnow().strftime(
                                                      '%Y%m%d') + '.pdf')

        # reporting pnl 2010

        self.RPT_PNL2010_DIR = os.path.join(self.RPTDIR,
                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl2010.pdf')

        # reporting pnl since 2000

        self.RPT_PNL2000_DIR = os.path.join(self.RPTDIR,

                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl2000.pdf')

        self.RPT_PNL2000_WITH_RB_BREAKDOWN_DIR = os.path.join(self.RPTDIR,

                                                              self.Short_Name + datetime.utcnow().strftime(
                                                                  '%Y%m%d') + '_pnl2000_with_breakdown.pdf')

        # reporting pnl since 1990

        self.RPT_PNL1990_DIR = os.path.join(self.RPTDIR,

                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl1990.pdf')

        #######

        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')

        self.TEMP_LOCAL_PICKLE = os.path.join(os.environ['TEMP'], 'TEMP_JY_' + self.Short_Name + '_local_db.pickle')

        #######

        self.EXPORT_TREE_STRUCT_FOLDER = os.path.join(self.RPTDIR, 'Gauge_tree')

        self.create_folder(self.EXPORT_TREE_STRUCT_FOLDER)

        self.EXPORT_TREE_STRUCT_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'Gauge_tree' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        self.EXPORT_TRANS_PARAM_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'trans_param ' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        self.EXPORT_DATA_TICKER_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'data_ticker ' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        # self.EXPORT_TREE_STRUCT_DIR = None

        # self.EXPORT_TRANS_PARAM_DIR = None

        # self.EXPORT_DATA_TICKER_DIR = None

        self.HISTORICAL_POS_FOLDER = os.path.join(self.RPTDIR, 'History')

        self.create_folder(self.HISTORICAL_POS_FOLDER)

        self.HISTORICAL_POS_DIR = os.path.join(self.HISTORICAL_POS_FOLDER,

                                               'History_sig' + self.Short_Name + datetime.now().strftime(

                                                   '%Y%m%d') + '.csv')

        self.OUT_DATA_DIR = os.path.join(self.PROJ_DIR, 'output', self.Short_Name)

    def download_data_bulk_5(self, MASTER_INPUT_DIR, MASKET_PAGE_NAME='sig_for_chart', **kwargs):

        '''

        this is the downloader to download all data in 1 bulk, and cache it

        :param MASTER_INPUT_DIR:

        :param Short_Name:

        :param export_backup:

        :return:

        '''

        print('start downloading data...')

        pages = pd.ExcelFile(MASTER_INPUT_DIR).sheet_names

        master_page_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=MASKET_PAGE_NAME, skiprows=3, header=0, index_col=0)

        for sn in master_page_df.iloc[:, 0].values:

            if sn not in pages:
                continue

            import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=sn, index_col=False, header=0)

            import_df.replace(np.nan, 'invalid_name', inplace=True)

            if 'ISO' not in import_df.columns:
                continue

            iso_list = import_df['ISO'].values.tolist()

            if len(iso_list) < 0.1:
                continue

            # start download the data

            number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

            for i in range(number_of_series_col):

                i = str(int(i) + int(1))

                series_col = ('').join(['series', i])

                load_fn_col = ('').join(['load_fn', i])

                location_col = ('').join(['location', i])

                suffix_col = ('').join(['in_file_suf_fix', i])

                ser_list = import_df[series_col].values.tolist()

                load_fn_list = import_df[load_fn_col].values.tolist()

                # location_list = import_df[location_col].values.tolist()

                # suffix_list = import_df[suffix_col].values.tolist()

                for k, iso in enumerate(iso_list):

                    if ser_list[k] == 'invalid_name':
                        continue

                    if load_fn_list[k] == 'EconDB':

                        if ser_list[k] not in self.local_db.keys():
                            df = self.conn_get_clean([ser_list[k]])

                            self.local_db[ser_list[k]] = df

                        continue

                    if load_fn_list[k] == 'haver':

                        if ser_list[k] not in self.local_db.keys():

                            try:

                                df = haver.get_data([ser_list[k]])

                            except:

                                print(ser_list[k])

                            # print (df)

                            try:

                                df.index = df.index.to_timestamp()

                            except:

                                print(df)

                                raise ValueError

                            self.local_db[ser_list[k]] = df

                        continue

            # dump the updated file to the pickle

            with open(TEMP_RAW_PICKLE, 'wb') as handle:

                pickle.dump(self.local_db, handle, protocol=pickle.HIGHEST_PROTOCOL)

        print('done downloading data...')

        print(self.local_db.keys())


class abstract_sig_genr_for_rates_tree(abstract_sig_genr):

    def __init__(self):

        super(abstract_sig_genr_for_rates_tree, self).__init__()

    def add_dir_info(self, *args, **kwargs):

        '''

        adding directory information of the strategy

        '''

        self.time_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '%Y%m%d_%H%M')

        self.date_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '_%Y%m%d')

        self.WKDIR = os.path.dirname(os.path.realpath(__file__))

        self.PROJ_DIR = os.path.join(self.WKDIR, "..")

        self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")

        self.PARAM_DIR = os.path.join(self.PROJ_DIR, "input", 'param_backup', self.Short_Name)

        # self.PARAM_DIR_DATATICKER_BACKUP_TXT = os.path.join(self.PARAM_DIR,'DATA_TICKER'+self.date_stamp+'.txt')

        # self.PARAM_DIR_TRANSPARAM_BACKUP_TXT = os.path.join(self.PARAM_DIR,'TRANS_PARAM'+self.date_stamp+'.txt')

        self.PARAM_DIR_DATATICKER_BACKUP_TXT = None

        self.PARAM_DIR_TRANSPARAM_BACKUP_TXT = None

        self.create_folder(self.PARAM_DIR)

        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR, "output")

        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name)

        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name, 'indicator_group')

        self.LOCAL_MACRO_DATA_DIR = os.path.join(self.PROJ_DIR, 'macro_data')

        self.BT_ID1 = self.Short_Name + self.time_stamp + '.pdf'

        self.BT_ID1985_1 = self.Short_Name + '_1985_' + self.time_stamp + '.pdf'

        self.BT_ID2 = self.Short_Name + self.time_stamp + '_pnl.pdf'

        self.BT_WITH_RB_BREAKDOWN_ID2 = self.Short_Name + self.time_stamp + '_pnl_with_rb_breakdown.pdf'

        self.BT_ID2010_1 = self.Short_Name + self.time_stamp + '2010.pdf'

        self.BT_ID2010_2 = self.Short_Name + self.time_stamp + '_pnl2010.pdf'

        self.BT_ID1990_1 = self.Short_Name + self.time_stamp + '1990.pdf'

        self.BT_ID1990_2 = self.Short_Name + self.time_stamp + '_pnl1990.pdf'

        self.BT_BACKUP_ROOT_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP')

        # backup component since 2000

        self.BT_BACKUP_DIR1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1)

        # backup component since 1985

        self.BT_BACKUP1985_DIR1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1985_1)

        # backup pnl since 2000

        self.BT_BACKUP_DIR2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2)

        self.BT_BACKUP_WITH_RB_BREAKDOWN_DIR2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP',
                                                             self.BT_WITH_RB_BREAKDOWN_ID2)

        # backup component since 2000

        self.BT_BACKUP_DIR2010_1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2010_1)

        # backup pnl since 2010

        self.BT_BACKUP_DIR2010_2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2010_2)

        # backup pnl since 1990

        self.BT_BACKUP_DIR1990_2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1990_2)

        self.PARS_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'PARS' + self.time_stamp + '.csv')

        self.DATA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'DATA' + self.time_stamp + '.csv')

        self.ALPHA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'ALPHA' + self.time_stamp + '.csv')

        # Firstly export all the relevant result into the csv format. Secondly plot into the charts

        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'

        self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.Short_Name) if os.access(self.SHARE_DIR,
                                                                                              os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', self.Short_Name)

        # reporting file

        # reporting component since 2000

        self.RPT_COMPONENT_DIR = os.path.join(self.RPTDIR,
                                              self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '.pdf')

        # reporting component since 1985

        self.RPT_COMPONENT1985_DIR = os.path.join(self.RPTDIR,

                                                  self.Short_Name + '_1985_' + datetime.utcnow().strftime(
                                                      '%Y%m%d') + '.pdf')

        # reporting pnl 2010

        self.RPT_PNL2010_DIR = os.path.join(self.RPTDIR,
                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl2010.pdf')

        # reporting pnl since 2000

        self.RPT_PNL2000_DIR = os.path.join(self.RPTDIR,

                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl2000.pdf')

        self.RPT_PNL2000_WITH_RB_BREAKDOWN_DIR = os.path.join(self.RPTDIR,

                                                              self.Short_Name + datetime.utcnow().strftime(
                                                                  '%Y%m%d') + '_pnl2000_with_breakdown.pdf')

        # reporting pnl since 1990

        self.RPT_PNL1990_DIR = os.path.join(self.RPTDIR,

                                            self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_pnl1990.pdf')

        #######

        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')

        self.TEMP_LOCAL_PICKLE = os.path.join(os.environ['TEMP'], 'TEMP_JY_' + self.Short_Name + '_local_db.pickle')

        #######

        self.EXPORT_TREE_STRUCT_FOLDER = os.path.join(self.RPTDIR, 'Gauge_tree')

        self.create_folder(self.EXPORT_TREE_STRUCT_FOLDER)

        self.EXPORT_TREE_STRUCT_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'Gauge_tree' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        self.EXPORT_TRANS_PARAM_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'trans_param ' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        self.EXPORT_DATA_TICKER_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'data_ticker ' + self.Short_Name + datetime.utcnow().strftime(
                                                       '%Y%m%d') + '.csv')

        # self.EXPORT_TREE_STRUCT_DIR = None

        # self.EXPORT_TRANS_PARAM_DIR = None

        # self.EXPORT_DATA_TICKER_DIR = None

        self.HISTORICAL_POS_FOLDER = os.path.join(self.RPTDIR, 'History')

        self.create_folder(self.HISTORICAL_POS_FOLDER)

        self.HISTORICAL_POS_DIR = os.path.join(self.HISTORICAL_POS_FOLDER,

                                               'History_sig' + self.Short_Name + datetime.now().strftime(

                                                   '%Y%m%d') + '.csv')

        self.OUT_DATA_DIR = os.path.join(self.PROJ_DIR, 'output', self.Short_Name)

    def import_parse_param(self, master_input_dir, short_name):

        '''

        import and parse the parameters of the strategy

        :param master_input_dir: the excel contains the parameter and weights

        :param short_name: the short name of the strategy

        :return: dictionary that contains the parameters and the weights

        '''

        param_df = pd.read_excel(master_input_dir, sheet_name=short_name, index_col=False, header=0,

                                 na_values=['NA', ''])

        self.all_ISO = param_df.iloc[:, 0].values.tolist()

        self.trans_param_df = param_df.loc[:, 'Param Table':'type'].set_index('Param Table').dropna().T.to_dict('list')

        # convert to int

        for key, value in self.trans_param_df.items():
            self.trans_param_df[key][0] = int(self.trans_param_df[key][0]) if self.trans_param_df[key][1] == 'int' else \
            self.trans_param_df[key][0]

        param_df.loc[:, 'Param Table':'type'].set_index('Param Table').dropna().to_csv(self.EXPORT_TRANS_PARAM_DIR)

        param_df.loc[:, 'ISO':'in_file_suf_fix1'].set_index('ISO').to_csv(self.EXPORT_DATA_TICKER_DIR)

    def run_indicator_and_post_sig_genr(self, *args, **kwargs):

        '''

        generate the trading signal and positions, apply transaction cost reduction algo

        :param args: parameters to pass : curve names, transaction-cost reduction algo etc

        :param kwargs:

        :return:

        '''

        # check if condition minus pricing exists:

        assert 'condition_m_pricing' in self.new_wf.df.keys(), 'sorry, condition minus pricing z-score is not in the new_wf!!!'

        z_diff = self.new_wf.df['condition_m_pricing'].dropna()

        new_name = 'condition - pricing'

        z_diff.columns = [new_name]

        z_diff = z_diff.dropna().rolling(window=self.trans_param_df['con_smooth_window'][0]).apply(np.mean)

        # switching_off_period

        switch_off_start = kwargs['switch_off_start'] if 'switch_off_start' in kwargs else '2030-01-01'

        switch_off_end = kwargs['switch_off_end'] if 'switch_off_end' in kwargs else '1901-01-01'

        mask = (z_diff.index >= switch_off_start) & (z_diff.index < switch_off_end)

        z_diff.loc[mask, :] = 0

        self.new_wf.update_df(new_name, z_diff[[new_name]])

        self.new_wf.add_df('indicator_group', -z_diff[[new_name]].shift(self.trans_param_df['indicator_lag'][0]),
                           repeat=True)

        self.post_indicator_genr(*args, **kwargs)

    def post_indicator_genr(self, *args, **kwargs):

        '''

        generate trading signals and performance matrix

        :param args:

        :param kwargs:

        :return: trading signals and performance matrix

        '''

        self.sci = self.sci_panel(**kwargs)

        self.run_calc_duration(**kwargs)

        signal_dict = post_signal_genr.LS_filtered(self.new_wf.df['indicator_group'], self.sci, method='s_curve',

                                                   tc_reduction_method='inertia', inertia_torlerance=[50])

        self.new_wf.add_df('signal_group', signal_dict['signal_group'])

        self.new_wf.add_df('signal_group_flip', signal_dict['signal_group_flip'])

        self.new_wf.add_df('actual_trade_group', signal_dict['actual_trade_group'])

        self.calc_DV01()

        # since 1990

        self.profit_sample_1990 = ['1990-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

        self.pnl = post_signal_genr.profit(self.new_wf.df['actual_trade_group'], self.sci, 1000,
                                           self.profit_sample_1990)

        self.new_wf.add_df('equity_curve', self.pnl)

        self.new_wf.add_df('cumprof', self.pnl[['cumprof']])

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample_1990,

                                                          benchmark=self.new_wf.df[kwargs['tri_name']])

        self.new_wf.alpha['ann_mean_1990'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std_1990'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe_1990'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar_1990'] = retstats_dict['calmar']

        self.new_wf.alpha['skewness_1990'] = retstats_dict['skewness']

        self.new_wf.add_df('drawdown_1990', retstats_dict['drawdown'])

        self.new_wf.add_df('rolling_corr_1y_bm_1990', retstats_dict['1y_corr'])

        self.new_wf.add_df('avg_corr_bm_1990', retstats_dict['avg_corr'])

        self.profit_sample = ['2000-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

        # profit_sample = ['2000-01-01', '2016-12-31']

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample,
                                                          benchmark=self.new_wf.df[kwargs['tri_name']])

        self.new_wf.alpha['ann_mean'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar'] = retstats_dict['calmar']

        self.new_wf.alpha['skewness'] = retstats_dict['skewness']

        self.new_wf.add_df('drawdown', retstats_dict['drawdown'])

        self.new_wf.add_df('rolling_corr_1y_bm', retstats_dict['1y_corr'])

        self.new_wf.add_df('avg_corr_bm', retstats_dict['avg_corr'])

        self.profit_sample_2010 = ['2010-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

        # profit_sample_2010 = ['2010-01-01', '2016-12-31']

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample_2010,
                                                          benchmark=self.new_wf.df[kwargs['tri_name']])

        self.new_wf.alpha['ann_mean_2010'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std_2010'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe_2010'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar_2010'] = retstats_dict['calmar']

        self.new_wf.alpha['skewness_2010'] = retstats_dict['skewness']

        self.new_wf.add_df('drawdown_2010', retstats_dict['drawdown'])

        self.new_wf.add_df('rolling_corr_1y_bm_2010', retstats_dict['1y_corr'])

        self.new_wf.add_df('avg_corr_bm_2010', retstats_dict['avg_corr'])

        return

    def run_calc_duration(self, *args, **kwargs):

        rate_name = kwargs.get('rate_name')

        tenor = kwargs.get('tenor')

        df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[rate_name]).dropna()

        _values = df.iloc[:, 0].values

        df.iloc[:, 0] = np.array([fin_lib.calc_duration(i, tenor) for i in _values])

        df.columns = ['trading_instrument_duration']

        self.new_wf.add_df('trading_instrument_duration', df)

    def calc_DV01(self):

        df1 = self.new_wf.df['signal_group'].dropna()

        df2 = self.new_wf.df['actual_trade_group'].dropna()

        df3 = self.new_wf.df['trading_instrument_duration'].dropna()

        df_DV01 = df1.iloc[:, 0] * df3.iloc[:, 0] / 10000

        df_DV01 = df_DV01.to_frame()

        df_DV01.columns = ['DV01_raw']

        self.new_wf.add_df('DV01_raw', df_DV01)

        df_DV01 = df1.iloc[:, 0] * df3.iloc[:, 0].shift(1) / 10000

        df_DV01 = df_DV01.to_frame()

        df_DV01.columns = ['DV01_t_1']

        self.new_wf.add_df('DV01_t_1', df_DV01)

        df_DV01 = df2.iloc[:, 0] * df3.iloc[:, 0].shift(1) / 10000

        df_DV01 = df_DV01.to_frame()

        df_DV01.columns = ['DV01_actual_trading_t_1']

        self.new_wf.add_df('DV01_actual_trading_t_1', df_DV01)

    def sci_panel(self, **kwargs):

        importdir = os.path.join(self.PROJ_DIR, 'SCI', 'Market_data', kwargs['tri_file'])

        self.new_wf.importts(importdir, filetype='csv')

        return self.new_wf.df[kwargs['tri_name']]

    def run_step3(self, *args, **kwargs):

        '''

        run the reporting system and save down the bactesting result

        '''

        if kwargs['run_charting']:
            root = self.tree.nodes['condition_m_pricing']

            chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_component_chart(root)

            # component chart 2000

            TCT.rates_tree_component_plot(plot_dict=chart_pack_dict, chart_start_dt='2000-01-01',
                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,
                                          bt_backup_dir=self.RPT_COMPONENT_DIR, pdfpath=self.BT_BACKUP_DIR1)

            # component chart since 1985

            TCT.rates_tree_component_plot(plot_dict=chart_pack_dict, chart_start_dt='1985-01-01',

                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                          bt_backup_dir=self.RPT_COMPONENT1985_DIR, pdfpath=self.BT_BACKUP1985_DIR1)

            # calculate the pnl

            root = self.tree.nodes['condition_m_pricing']

            self.tree.get_rid_of_candidate_node(root)

            self.tree.print_structure(root)

            # plot pnl since 2000

            self.series_name_dict = {'yield_series': 'IRS_5Y',

                                     'signal_flip': 'signal_group_flip',

                                     'indicator_group': 'indicator_group',

                                     'cumprof': 'cumprof',

                                     'TRI': 'USA_5y_TRI',

                                     'drawdown': 'drawdown',

                                     'corr': 'rolling_corr_1y_bm_2010',

                                     }

            self.alpha_name_dict = {'ann_mean': 'ann_mean',

                                    'ann_std': 'ann_std',

                                    'ann_sharpe': 'ann_sharpe',

                                    'calmar': 'calmar',

                                    'avg_corr': 'avg_corr',

                                    'skewness': 'skewness'

                                    }

            pnl_chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_pnl_chart(root, self.new_wf.df,
                                                                                          self.series_name_dict,
                                                                                          self.new_wf.alpha,
                                                                                          self.alpha_name_dict)

            TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2000-01-01',
                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,
                                          bt_backup_dir=self.RPT_PNL2000_DIR, pdfpath=self.BT_BACKUP_DIR2)

            # plot pnl since 2010

            self.series_name_dict.update({'drawdown': 'drawdown_2010'})

            self.alpha_name_dict = {'ann_mean': 'ann_mean_2010',

                                    'ann_std': 'ann_std_2010',

                                    'ann_sharpe': 'ann_sharpe_2010',

                                    'calmar': 'calmar_2010',

                                    'avg_corr': 'avg_corr_2010',

                                    'skewness': 'skewness_2010'

                                    }

            pnl_chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_pnl_chart(root, self.new_wf.df,

                                                                                          self.series_name_dict,

                                                                                          self.new_wf.alpha,

                                                                                          self.alpha_name_dict)

            TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2010-01-01',

                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                          bt_backup_dir=self.RPT_PNL2010_DIR, pdfpath=self.BT_BACKUP_DIR2010_2)

            # plot pnl since 1990

            self.series_name_dict.update({'drawdown': 'drawdown_1990'})

            self.alpha_name_dict = {'ann_mean': 'ann_mean_1990',

                                    'ann_std': 'ann_std_1990',

                                    'ann_sharpe': 'ann_sharpe_1990',

                                    'calmar': 'calmar_1990',

                                    'avg_corr': 'avg_corr_1990',

                                    'skewness': 'skewness_1990'

                                    }

            pnl_chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_pnl_chart(root, self.new_wf.df,

                                                                                          self.series_name_dict,

                                                                                          self.new_wf.alpha,

                                                                                          self.alpha_name_dict)

            TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='1990-01-01',

                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                          bt_backup_dir=self.RPT_PNL1990_DIR, pdfpath=self.BT_BACKUP_DIR1990_2)

        # dump to csv

        post_signal_genr.write_pars_to_csv(self.trans_param_df, self.PARS_DIR)

        post_signal_genr.write_pars_to_csv(self.trans_param_df, self.PARAM_DIR_TRANSPARAM_BACKUP_TXT)

        post_signal_genr.write_pars_to_csv(self.new_wf.alpha, self.ALPHA_DIR)

        post_signal_genr.dump_wf_obj_to_csv(self.new_wf, self.DATA_DIR)


class abstract_sig_genr_portfolio(abstract_sig_genr_for_rates_tree):

    def __init__(self):

        super(abstract_sig_genr_portfolio, self).__init__()

    def dump_all_dfs_param(self):

        post_signal_genr.write_pars_to_csv(self.basket_list_dict, self.PARS_DIR)

        post_signal_genr.write_pars_to_csv(self.new_wf.alpha, self.ALPHA_DIR)

        post_signal_genr.dump_wf_obj_to_csv(self.new_wf, self.DATA_DIR)

    def import_parse_param(self, master_input_dir, short_name):

        param_df = pd.read_excel(master_input_dir, sheet_name=short_name, index_col=False, header=0,

                                 na_values=['NA', ''])

        # parameter should exclude 'Portfolio' row

        param_df = param_df.loc[:, 'basket_list':'order'].set_index('basket_list').dropna()

        ex_portfolio_index = [i for i in param_df.index if i not in ['Portfolio']]

        param_df = param_df.reindex(ex_portfolio_index)

        self.basket_list_dict = param_df.T.to_dict('list')

        self.ordered_basket = param_df.sort_values(by=['order']).index.tolist()

        self.ordered_iso = param_df.sort_values(by=['order']).iso.tolist()

    def sample_period(self):

        self.in_sample2000 = [pd.to_datetime(d) for d in ['2000-01-01', '2016-12-31']]

        self.in_sample2010 = [pd.to_datetime(d) for d in ['2010-01-01', '2016-12-31']]

        self.full_sample2000 = [pd.to_datetime(d) for d in ['2000-01-01', '2050-12-31']]

        self.full_sample2010 = [pd.to_datetime(d) for d in ['2010-01-01', '2050-12-31']]

        self.in_sample1990 = [pd.to_datetime(d) for d in ['1990-01-01', '2050-12-31']]

        self.full_sample1990 = [pd.to_datetime(d) for d in ['1990-01-01', '2050-12-31']]

    def check_latest_run(self, this_short_name, min_limit):

        '''

        :param this_short_name:

        :param min_limit: limit on time to allow use the cached results

        :return:

        '''

        basket_data_dir = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", this_short_name, 'BT_BACKUP', 'DATA*.csv')

        basket_alpha_dir = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", this_short_name, 'BT_BACKUP',

                                        'ALPHA*.csv')

        try:

            all_files = glob.glob(basket_data_dir)

            all_ts = [os.path.getmtime(f) for f in all_files]

            all_alpha = glob.glob(basket_alpha_dir)

            all_ts2 = [os.path.getmtime(f) for f in all_alpha]

            if max(all_ts) > datetime.timestamp(datetime.now() - timedelta(minutes=min_limit)):

                return {'data_file': all_files[all_ts.index(max(all_ts))],

                        'alpha_file': all_alpha[all_ts2.index(max(all_ts2))]}

            else:

                return {'data_file': 'need_to_re_run',

                        'alpha_file': 'need_to_re_run'}

        except:

            return {'data_file': 'need_to_re_run',

                    'alpha_file': 'need_to_re_run'}

    def re_run_if_necessary(self, min_limit, **kwargs):

        for basket_name in self.ordered_basket:

            print('checking...:', basket_name)

            f = self.check_latest_run(basket_name, min_limit)

            if 'need_to_re_run' in [v for v in f.values()]:
                # re-run the basket

                import_name = 'basket.' + self.basket_list_dict[basket_name][0] + '.' + \
 \
                              self.basket_list_dict[basket_name][1]

            _mod = importlib.import_module(import_name)

            _signal3 = _mod.signal3()

            _signal3.initialise_and_run(run_charting=False, **kwargs)

    for basket_name in self.ordered_basket:
        f = self.check_latest_run(basket_name, min_limit)

        self.use_result(basket_name, f)


def use_result(self, basket_name, file_dict):
    _new_wf = wf.swf()

    df_res = pd.read_csv(file_dict['data_file'], index_col=0, header=0)

    df_res.index = pd.to_datetime(df_res.index)

    list_of_column = {'IRS_5Y': 'IRS_5Y',

                      'cumprof': 'cumprof',

                      'signal_group': 'condition - pricing_(signal_group)',  # raw trading signal

                      'signal_group_flip': 'condition-pricing',  # flipped signal group

                      'trading_instrument_duration': 'trading_instrument_duration',

                      'pnl': 'pnl',

                      'drawdown_1990': 'drawdown',

                      'drawdown': 'drawdown_(drawdown)',

                      'drawdown_2010': 'drawdown_(drawdown_2010)',

                      'indicator_group': 'condition - pricing_(indicator_group)',  # original z_score

                      'TRI': self.basket_list_dict[basket_name][6],

                      'Global_sum_Z': 'Global_sum_Z',

                      'ForwardCPI_sum_Z': 'ForwardCPI_sum_Z',

                      'ForwardGrow_sum_Z': 'ForwardGrow_sum_Z',

                      'Changes_sum_Z': 'Changes_sum_Z',

                      'Credit_Agg_sum_Z': 'Credit_Agg_sum_Z',

                      'Level_sum_Z': 'Level_sum_Z',

                      'GrowthmPotential_sum_Z': 'GrowthmPotential_sum_Z',

                      'Condition_sum_Z': 'Condition_sum_Z',

                      'pricing_sum_z': 'pricing_sum_z',

                      'condition_m_pricing': 'condition_m_pricing'

                      }

    for k, col in list_of_column.items():
        df = df_res.loc[:, [col]].dropna()

        _new_wf.add_df(k, df)

    # re-construct equity_curve df

    _new_wf.add_df('equity_curve', df_res.loc[:, ['pnl', 'cumprof']].dropna())

    # adding alpha perf to it

    # loading_alpha_file

    alpha_res = pd.read_csv(file_dict['alpha_file'], header=0)

    for col in alpha_res.columns:
        _new_wf.alpha[col] = alpha_res.loc[:, col].iloc[0]

    self.wf_container[basket_name] = _new_wf


def abs_run_diff(self, item_name):
    df_list = []

    for basket in self.ordered_basket:
        iso = self.basket_list_dict[basket][3]

        this_df = self.wf_container[basket].df[item_name].copy()

        this_df.columns = [iso]

        df_list.append(this_df)

    df_comb = pd.concat(df_list, axis=1)

    new_name = item_name + '_diff'

    df_comb[new_name] = df_comb.iloc[:, 0] - df_comb.iloc[:, 1]

    self.new_wf.add_df(new_name, df_comb[[new_name]])


def run_diff_of_1st_diff(self, item_name, window=63):
    df_list = []

    for basket in self.ordered_basket:
        iso = self.basket_list_dict[basket][3]

        this_df = self.wf_container[basket].df[item_name].copy()

        this_df.columns = [iso]

        df_list.append(this_df)

    df_comb = pd.concat(df_list, axis=1)

    new_name = item_name + '_diff_1st'

    df_comb[new_name] = df_comb.iloc[:, 0].diff(periods=window) - df_comb.iloc[:, 1].diff(periods=window)

    self.new_wf.add_df(new_name, df_comb[[new_name]])


def sci_panel(self):
    if not 'sci' in self.new_wf.df.keys():

        df_list = []

        for basket in self.ordered_basket:
            df_list.append(self.wf_container[basket].df['TRI'])

        df = pd.concat(df_list, axis=1)

        df = df.sort_index().dropna()

        self.new_wf.add_df('sci', df)

    df_ave = df.mean(axis=1)

    df_ave = df_ave.to_frame()

    df_ave.columns = ['AVG_TRI']

    self.new_wf.add_df('AVG_TRI', df_ave)


return self.new_wf.df['sci']


def pair_hml(self, name_of_top_root='condition_m_pricing_diff', **kwargs):
    # check if condition minus pricing exists

    # adjust the DV01.

    assert name_of_top_root in self.new_wf.df.keys(), 'sorry, condition minus pricing z-score is not in the new_wf!!!'

    z_diff = self.new_wf.df[name_of_top_root].dropna()

    z_diff = -z_diff

    # z_diff = z_diff.dropna().rolling(window=self.trans_param_df['con_smooth_window'][0]).apply(np.mean)

    # switching_off_period

    switch_off_start = kwargs['switch_off_start'] if 'switch_off_start' in kwargs else '2030-01-01'

    switch_off_end = kwargs['switch_off_end'] if 'switch_off_end' in kwargs else '1901-01-01'

    mask = (z_diff.index >= switch_off_start) & (z_diff.index < switch_off_end)

    z_diff.loc[mask, :] = 0

    z_diff['new_col'] = -z_diff.iloc[:, 0]

    z_diff.columns = self.ordered_iso

    # print(z_diff.dropna().tail(5))

    # switching off the below thres convictions

    if 'z_thres' in kwargs.keys():
        mask = z_diff.iloc[:, 0].abs() <= kwargs.get('z_thres')

        z_diff.loc[mask, :] = 0

    d1, d2 = self.wf_container[self.ordered_basket[0]].df['trading_instrument_duration'], \
             self.wf_container[self.ordered_basket[1]].df['trading_instrument_duration']

    df_comb = pd.concat([z_diff, d1, d2], axis=1)

    df_comb.sort_index(inplace=True)

    df_comb.iloc[:, 0] = df_comb.iloc[:, 0] / df_comb.iloc[:, 2].shift(1) * df_comb.iloc[:, 3].shift(1)

    z_diff = df_comb.iloc[:, [0, 1]]

    # print(z_diff.dropna().tail(5))

    # self.new_wf.update_df(new_name, z_diff[[new_name]])

    self.new_wf.add_df('indicator_group', z_diff, repeat=True)


def post_indicator_genr_4RV(self, *args, **kwargs):
    '''

    generate trading signals and performance matrix

    :param args:

    :param kwargs:

    :return: trading signals and performance matrix

    '''

    self.sci = self.sci_panel()

    signal_dict = post_signal_genr.LS_filtered(self.new_wf.df['indicator_group'], self.sci, method='s_curve',

                                               tc_reduction_method=kwargs.get('tc_reduction_method'),
                                               inertia_torlerance=[50], country_order=self.ordered_iso)

    self.new_wf.add_df('signal_group', signal_dict['signal_group'])

    self.new_wf.add_df('signal_group_flip', signal_dict['signal_group_flip'])

    self.new_wf.add_df('actual_trade_group', signal_dict['actual_trade_group'])

    # self.calc_DV01()

    # since 1990

    self.profit_sample_1990 = ['1990-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

    self.pnl = post_signal_genr.profit(self.new_wf.df['actual_trade_group'], self.sci, 1000, self.profit_sample_1990)

    self.new_wf.add_df('equity_curve', self.pnl)

    self.new_wf.add_df('cumprof', self.pnl[['cumprof']])

    retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample_1990)

    self.new_wf.alpha['ann_mean_1990'] = retstats_dict['ann_mean']

    self.new_wf.alpha['ann_std_1990'] = retstats_dict['ann_std']

    self.new_wf.alpha['ann_sharpe_1990'] = retstats_dict['ann_sharpe']

    self.new_wf.alpha['calmar_1990'] = retstats_dict['calmar']

    self.new_wf.alpha['skewness_1990'] = retstats_dict['skewness']

    self.new_wf.add_df('drawdown_1990', retstats_dict['drawdown'])

    self.profit_sample = ['2000-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

    # profit_sample = ['2000-01-01', '2016-12-31']

    retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample)

    self.new_wf.alpha['ann_mean'] = retstats_dict['ann_mean']

    self.new_wf.alpha['ann_std'] = retstats_dict['ann_std']

    self.new_wf.alpha['ann_sharpe'] = retstats_dict['ann_sharpe']

    self.new_wf.alpha['calmar'] = retstats_dict['calmar']

    self.new_wf.alpha['skewness'] = retstats_dict['skewness']

    self.new_wf.add_df('drawdown', retstats_dict['drawdown'])

    self.profit_sample_2010 = ['2010-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]

    # profit_sample_2010 = ['2010-01-01', '2016-12-31']

    retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample_2010)

    self.new_wf.alpha['ann_mean_2010'] = retstats_dict['ann_mean']

    self.new_wf.alpha['ann_std_2010'] = retstats_dict['ann_std']

    self.new_wf.alpha['ann_sharpe_2010'] = retstats_dict['ann_sharpe']

    self.new_wf.alpha['calmar_2010'] = retstats_dict['calmar']

    self.new_wf.alpha['skewness_2010'] = retstats_dict['skewness']

    self.new_wf.add_df('drawdown_2010', retstats_dict['drawdown'])

    return


def run_charting4RV(self, *args, **kwargs):
    if kwargs.get('run_charting') == True:
        root = self.tree.nodes['condition_m_pricing_diff']

        who_m_who = kwargs.get('iso_list')[0] + '-' + kwargs.get('iso_list')[1] + ' : '

        # plot pnl since 2000

        self.series_name_dict = {'yield_series': 'IRS_5Y_diff',

                                 'signal_flip': 'signal_group_flip',

                                 'indicator_group': 'indicator_group',

                                 'cumprof': 'cumprof',

                                 'drawdown': 'drawdown',

                                 }

        self.alpha_name_dict = {'ann_mean': 'ann_mean',

                                'ann_std': 'ann_std',

                                'ann_sharpe': 'ann_sharpe',

                                'calmar': 'calmar',

                                'skewness': 'skewness'

                                }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(who_m_who, root, self.new_wf.df,

                                                                                      self.series_name_dict,

                                                                                      self.new_wf.alpha,

                                                                                      self.alpha_name_dict)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2000-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL2000_DIR, pdfpath=self.BT_BACKUP_DIR2)

        # plot pnl since 2010

        self.series_name_dict.update({'drawdown': 'drawdown_2010'})

        self.alpha_name_dict = {'ann_mean': 'ann_mean_2010',

                                'ann_std': 'ann_std_2010',

                                'ann_sharpe': 'ann_sharpe_2010',

                                'calmar': 'calmar_2010',

                                'skewness': 'skewness_2010'

                                }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(who_m_who, root, self.new_wf.df,

                                                                                      self.series_name_dict,

                                                                                      self.new_wf.alpha,

                                                                                      self.alpha_name_dict)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2010-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL2010_DIR, pdfpath=self.BT_BACKUP_DIR2010_2)

        # plot pnl since 1990

        self.series_name_dict.update({'drawdown': 'drawdown_1990'})

        self.alpha_name_dict = {'ann_mean': 'ann_mean_1990',

                                'ann_std': 'ann_std_1990',

                                'ann_sharpe': 'ann_sharpe_1990',

                                'calmar': 'calmar_1990',

                                'skewness': 'skewness_1990'

                                }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(who_m_who, root, self.new_wf.df,

                                                                                      self.series_name_dict,

                                                                                      self.new_wf.alpha,

                                                                                      self.alpha_name_dict)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='1990-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL1990_DIR, pdfpath=self.BT_BACKUP_DIR1990_2)

    # dump to csv

    # post_signal_genr.write_pars_to_csv(self.trans_param_df, self.PARS_DIR)

    # post_signal_genr.write_pars_to_csv(self.trans_param_df, self.PARAM_DIR_TRANSPARAM_BACKUP_TXT)

    post_signal_genr.write_pars_to_csv(self.new_wf.alpha, self.ALPHA_DIR)

    post_signal_genr.dump_wf_obj_to_csv(self.new_wf, self.DATA_DIR)


class abstract_sig_genr_for_fwdGrow_tree(abstract_sig_genr):

    def __init__(self):

        super(abstract_sig_genr_for_fwdGrow_tree, self).__init__()

    def add_dir_info(self):

        self.WKDIR = os.path.dirname(os.path.realpath(__file__))

        self.PROJ_DIR = os.path.join(self.WKDIR, "..")

        self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")

        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR, "output")

        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name)

        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name, 'indicator_group')

        self.TEMP_LOCAL_PICKLE = os.path.join(os.environ['TEMP'], 'TEMP_' + self.Short_Name + '_local_db.pickle')

        # Firstly export all the relevant result into the csv format. Secondly plot into the charts

        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'

        self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.Short_Name) if os.access(self.SHARE_DIR,
                                                                                              os.W_OK) else os.path.join(

            self.PROJ_DIR, 'reporting', self.Short_Name)

        #######

        self.EXPORT_TREE_STRUCT_FOLDER = os.path.join(self.RPTDIR, 'Gauge_tree')

        self.create_folder(self.EXPORT_TREE_STRUCT_FOLDER)

        self.EXPORT_TREE_STRUCT_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                   'Gauge_tree ' + self.Short_Name + datetime.now().strftime(
                                                       '%Y%m%d') + '.csv')

        # self.EXPORT_TREE_STRUCT_DIR = None

        self.EXPORT_TREE_PARAM_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                  'trans_param ' + self.Short_Name + datetime.now().strftime(
                                                      '%Y%m%d') + '.csv')

        self.EXPORT_TREE_DATA_TICKER = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,
                                                    'data_ticker ' + self.Short_Name + datetime.now().strftime(
                                                        '%Y%m%d') + '.csv')

        # print (self.EXPORT_TREE_STRUCT_DIR)

        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')

    def create_report_page(self, plot_dict, chart_start_dt='1990-01-01', centre_ma=False, content='original',
                           expansion_method='expansion1', top_root='GS_CAI',
                           sub_root_list=['PCE', 'Investment_Residential', 'Investment_nonResidential_exEnergy',
                                          'Investment_nonResidential_Energy', 'Export'], iso='USA'):

        '''

        :param all_dict: usually use the dictionary tha has the result in the form of (iso,df_series) format

        :param centre_ma: whether to convert the high-freq into a centre moving average for better visibility

        :return: the target is to create academic style

        '''

        # this is to create the report in pdf, from the dictionary generated

        # (which should include all times series, with the ISO country name as the key

        # and all time-series as the item)

        chart_start_dt = chart_start_dt

        chart_end_dt = (datetime.now() + timedelta(days=126)).strftime('%Y-%m-%d')

        # TODO: check first if the PDF file is open, kill pdf first

        file_name = self.Short_Name + datetime.now().strftime('%Y%m%d')

        if expansion_method == 'expansion1':

            file_name = file_name + '(1st'

        elif expansion_method == 'expansion2':

            file_name = file_name + '(2nd'

        else:

            print("expansion method must be 1 or 2")

            raise ValueError

        if content == 'crosscorr':

            file_name = file_name + ' corr).pdf'

        elif content == 'original':

            file_name = file_name + '.pdf'

        else:

            print("must specify correlation or original chart!")

            raise ValueError

        try:

            report = PdfPages(os.path.join(self.RPTDIR, file_name))

        except:

            report = PdfPages(os.path.join(self.WKDIR, file_name))

        # # create the front page

        # fig, ax = plt.subplots(1, 1, figsize=(18.27, 12.69))

        # # plt.subplots_adjust(left=0, bottom=0, right=0, top=0, wspace=0, hspace=0)

        # last_update = datetime.strftime(datetime.now(), format='%Y-%m-%d')

        # txt = [['    FCI Impulse :'], ['    - FCI Impulse'], ['    - Current Activity Indicator'], [''],

        #        ['Last update : ' + last_update], [''], [''], [''], [''], [''], [''], [''], [''], ['']]

        # collabel = (['Global Growth : '])

        # # ax.axis('tight')

        # ax.axis('off')

        # table = ax.table(cellText=txt, colLabels=collabel, loc='center')

        # for key, cell in table.get_celld().items():

        #     cell.set_linewidth(0)

        # cells = [key for key in table._cells]

        # print(cells)

        # for cell in cells:

        #     table._cells[cell]._loc = 'left'

        #     table._cells[cell].set_text_props(fontproperties=FontProperties(family='serif', size=20))

        #     table._cells[cell].set_height(.05)

        # table._cells[(0, 0)].set_text_props(fontproperties=FontProperties(weight='bold', family='serif', size=30))

        #

        # # table.scale(1, 4)

        # report.savefig(fig, bbox_inches='tight', dpi=100)

        chart_each_page = 12

        chart_rows = 4

        chart_cols = 3

        # construct the list of copy

        list_of_dict = []

        root = self.tree.nodes[top_root]

        if expansion_method == 'expansion1':

            list_of_dict.append(self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs(root, maxlevel=2,
                                                                                                     plot_only_branch=False))

            for root in [self.tree.nodes[r] for r in sub_root_list]:
                list_of_dict.append(
                    self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs(root, maxlevel=999,
                                                                                         plot_only_branch=True))

        elif expansion_method == 'expansion2':

            list_of_dict.append(self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs2(root, maxlevel=2,

                                                                                                      plot_only_branch=False))

            for root in [self.tree.nodes[r] for r in sub_root_list]:
                list_of_dict.append(

                    self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs2(root, maxlevel=999,

                                                                                          plot_only_branch=True))

        page_counter = 0

        for plot_dict in list_of_dict:

            legend_list = plot_dict['legend']

            lag_list = plot_dict['lag']

            df_pair_list = plot_dict['df_pairs']

            title_list = plot_dict['title']

            axis_list = plot_dict['axis']

            footnote_list = plot_dict['footnote']

            # corr_list = plot_dict['corr']

            # only include chart with lead lag relationship

            m = [True if l > 0.01 else False for l in lag_list]

            legend_list, df_pair_list, title_list, axis_list, footnote_list, lag_list = self.mask_list(legend_list,
                                                                                                       m), self.mask_list(
                df_pair_list, m), self.mask_list(title_list, m), self.mask_list(axis_list, m), self.mask_list(
                footnote_list, m), self.mask_list(lag_list, m)

            chart_list = ['chart_' + str(i) for i in range(len(legend_list))]

            pages_number = math.ceil(len(chart_list) / chart_each_page)

            chart_in_page = [chart_each_page] * (pages_number - 1) + [
                len(chart_list) - chart_each_page * (pages_number - 1)]

            print('chart_in_each_page=', chart_in_page)

            print("CREATING PAGE")

            for i, n in enumerate(chart_in_page):

                fig, axarr = plt.subplots(chart_rows, chart_cols, figsize=(18.27, 12.69), dpi=100)

                start_idx = i * chart_each_page

                end_idx = start_idx + n

                df_in_this_page = df_pair_list[start_idx:end_idx]

                title_in_this_page = title_list[start_idx:end_idx]

                axis_in_this_page = axis_list[start_idx:end_idx]

                footnote_in_this_page = footnote_list[start_idx:end_idx]

                name_in_this_page = legend_list[start_idx:end_idx]

                # corr_in_this_page = corr_list[start_idx:end_idx]

                lag_in_this_page = lag_list[start_idx:end_idx]

                # print (df_in_this_page)

                page_counter += 1

                for i in range(chart_rows):

                    for j in range(chart_cols):

                        if i * chart_cols + j < len(df_in_this_page):

                            ax = axarr[i, j]

                            df1, df2 = df_in_this_page[i * chart_cols + j]

                            df1, df2 = df1.dropna(), df2.dropna()

                            if self.SU.get_freq(df1) != 'D':
                                df1 = self.SU.conversion_to_bDay(df1)

                            if centre_ma:
                                df1 = df1.rolling(window=21, center=True).mean()

                            if self.SU.get_freq(df2) != 'D':
                                df2 = self.SU.conversion_to_bDay(df2)

                            name1, name2, trans_type = name_in_this_page[i * chart_cols + j]

                            name1, name2 = "\n".join(wrap(name1, 35)), "\n".join(wrap(name2, 35))

                            # corr = corr_in_this_page[i * chart_cols + j]

                            lag = lag_in_this_page[i * chart_cols + j]

                            title = title_in_this_page[i * chart_cols + j]

                            footnote = footnote_in_this_page[i * chart_cols + j]

                            axis_unit = axis_in_this_page[i * chart_cols + j]

                            df1 = df1.loc[chart_start_dt:chart_end_dt, :]

                            df2 = df2.loc[chart_start_dt:chart_end_dt, :]

                            if content == 'original':

                                if lag > 0.01:

                                    print('charting...', name1, name2, trans_type)

                                    # chart_end_dt = df1.index[-1] if df1.index[-1]>df2.index[-1] else df2.index[-1]

                                    print('df1,df2 columns are : ', df1.columns[0], df2.columns[0])

                                    x1 = pd.to_datetime(df1.index).date

                                    x2 = pd.to_datetime(df2.index).date

                                    # print (type(x[0]))

                                    y1 = df1.iloc[:, 0]  # avg GDP data, blue

                                    y2 = df2.iloc[:, 0]  # avg potential GDP data, black dash

                                    if trans_type in ['6x6', 'hf_Impulse']:

                                        line1 = ax.plot(x2, y2, color='black', ls='solid', lw=0.9, label=name2,
                                                        zorder=999)

                                    elif trans_type == '6m':

                                        line1 = ax.plot(x2, y2, color='b', ls='solid', lw=0.9, label=name2, zorder=999)

                                    ax.set_zorder(10)

                                    ax.patch.set_visible(False)

                                    if trans_type in ['6x6', 'hf_Impulse']:

                                        ax2 = ax.twinx()

                                        line2 = ax2.plot(x1, y1, color='deepskyblue', lw=0.3, label='_nolabel_',
                                                         zorder=1)

                                        line2_shade = ax2.fill_between(x1, 0, y1, facecolor='aqua', alpha=0.3,
                                                                       label='_nolabel_')

                                        # line3 is solely for the purpose of a dummy legend

                                        line3 = ax.fill_between(x1, 0, y1 - y1, facecolor='aqua', alpha=0.3,
                                                                label=name1)

                                    elif trans_type == '6m':

                                        line2 = ax.plot(x1, y1, color='r', ls='solid', lw=0.9, label=name1, zorder=1)

                                    # lns = line1

                                    # labs = ['CAI','FCI_Impulse']

                                    ax.legend(fontsize=8, loc=9, frameon=False)

                                    # ax2.legend(fontsize=8, loc=9,frameon=False)

                                    # ax.legend(lns, labs, loc=9, frameon=False, fontsize=8)

                                    # set the range

                                    df2_mean = df2.loc['2013-01-01':].mean().values

                                    df2_min = df2.min().values

                                    df2_max = df2.max().values

                                    y_max1 = df2_mean + (df2_mean - df2_min) * 1.1

                                    y_max2 = df2_mean + (df2_max - df2_mean) * 1.1

                                    y_max = y_max1 if y_max1 > y_max2 else y_max2

                                    y_min = df2_mean - (df2_mean - df2_min) * 1.1

                                    ax.set_ylim(y_min, y_max)

                                    # set range for df2

                                    if trans_type in ['6x6', 'hf_Impulse']:
                                        df_min = -df1.min().values

                                        df_max = df1.max().values

                                        df_max_abs = df_min if df_min > df_max else df_max

                                        y_max = df_max_abs * 1.1

                                        y_min = -df_max_abs * 1.1

                                        ax2.set_ylim(y_min, y_max)

                                    ax.set_xlabel('')

                                    ax.set_ylabel('')

                                    # ax2.set_xlabel('')

                                    # ax2.set_ylabel('')

                                    # last1,last2 = y1.dropna()[-1] , y2.dropna()[-1]

                                    # "\n".join(wrap(current_iso + ' ' + current_title, 60))

                                    title = "\n".join(wrap(iso + ' : ' + title, 60))

                                    ax.set_title(title, y=1.02, fontsize=10, fontweight=600)

                                    # add legend

                                    # legend_name = df.columns.tolist()[-2]

                                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)

                                    # set ticks label size and width

                                    ax.tick_params(labelsize=9, width=0.01)

                                    # ax2.tick_params(labelsize=5, width=0.01)

                                    # change the y tick label color

                                    if trans_type in ['6x6', 'hf_Impulse']:

                                        ax.tick_params(axis='y', labelcolor='k')

                                        ax2.tick_params(axis='y', labelcolor='deepskyblue')

                                    elif trans_type == '6m':

                                        ax.tick_params(axis='y', labelcolor='b')

                                    # add a zero line

                                    if trans_type == '6m':
                                        ax.axhline(linewidth=0.5, color='k')

                                    if trans_type in ['6x6', 'hf_Impulse']:
                                        ax2.axhline(linewidth=0.5, color='k')

                                    # adding correlation text

                                    # corr_annotate = 'Corr : ' + "{0:.2f}".format(corr)

                                    # if lag>.01 and lag<999:

                                    #     corr_annotate = 'Corr : '+"{0:.2f}".format(corr) + " (with "+str(lag)+' periods ahead '+name2+')'

                                    # ann_x = x1[0]

                                    # ann_y = ax.get_ylim()[0] + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02

                                    # ax.annotate(corr_annotate, (ann_x, ann_y), fontsize='small')

                                    # adding footnote text

                                    ann_x = x1[0]

                                    ann_y = ax.get_ylim()[0] + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02

                                    ax.annotate("\n".join(wrap(footnote, 80)), (ann_x, ann_y), fontsize='x-small',
                                                family='DejaVu Serif', zorder=9999)

                                    # set the unit

                                    if trans_type == '6m':
                                        unit = axis_unit[0]

                                        ax.set_ylabel(unit, rotation=0, color='k', fontsize=7, fontname="DejaVu Serif")

                                        ax.yaxis.set_label_coords(0.9, 1.01)

                                    if trans_type in ['6x6', 'hf_Impulse']:
                                        unit1, unit2 = axis_unit

                                        ax.set_ylabel(unit1, rotation=0, color='k', fontsize=7, fontname="DejaVu Serif")

                                        ax.yaxis.set_label_coords(0.9, 1.01)

                                        ax2.set_ylabel(unit2, rotation=0, color='k', fontsize=7,
                                                       fontname="DejaVu Serif")

                                        ax2.yaxis.set_label_coords(0, 1.04)

                                    # set border color and width

                                    for spine in ax.spines.values():
                                        spine.set_edgecolor('grey')

                                        spine.set_linewidth(0.5)

                                    # add year tickers as minor tick

                                    years = mdates.YearLocator()

                                    yearsFmt = mdates.DateFormatter('%Y')

                                    ax.xaxis.set_major_formatter(yearsFmt)

                                    ax.xaxis.set_minor_locator(years)

                                    # set the width of minor tick

                                    ax.tick_params(which='minor', width=0.008)

                                    # set y-label to the right hand side

                                    ax.yaxis.tick_right()

                                    if trans_type in ['6x6', 'hf_Impulse']:
                                        ax2.yaxis.tick_left()

                                    # set date max

                                    datemax = np.datetime64(x1[-1], 'Y')

                                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')

                                    # set the ticker to 2005, 2010, 2015 ...

                                    first_date_ticker = np.datetime64(chart_start_dt, 'Y')

                                    while int(str(np.datetime64(first_date_ticker, 'Y'))) % 5 != 0:
                                        first_date_ticker = first_date_ticker + np.timedelta64(1, 'Y')

                                    x_tick_overrive = [first_date_ticker, datemax]

                                    date_cursor = first_date_ticker

                                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                                        date_cursor = date_cursor + np.timedelta64(5, 'Y')

                                        x_tick_overrive.append(date_cursor)

                                    ax.xaxis.set_ticks(x_tick_overrive)

                                    if x1[-1].month > 10:

                                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))

                                    else:

                                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))



                                else:

                                    self.set_ax_invisible(axarr[i, j])



                            elif content == 'crosscorr':

                                if lag > 0.01:

                                    print('charting corr of ', name1, name2)

                                    x1, y1 = self.tree.calc_time_lag_corr_for_df1_df2(df1, df2, to_freq='M', max_lag=24)

                                    edgecolor_dict = {'6m': 'red', '6x6': 'green', 'hf_Impulse': 'blue'}

                                    ax.bar(x1, y1, fill=False, edgecolor=edgecolor_dict[trans_type], linewidth=0.5)

                                    max_position = y1.index(max(y1))

                                    ax.set_xlabel('')

                                    ax.set_ylabel('')

                                    title = "\n".join(wrap(
                                        iso + ' : cross correlation between change of ' + name2 + ' and lagged adj ' + name1 + ' ( peaked on : ' + str(
                                            int(max_position)) + ' months )', 60))

                                    ax.set_title(title, y=1.02, fontsize=10, fontweight=600)

                                    # add legend

                                    # legend_name = df.columns.tolist()[-2]

                                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)

                                    # set ticks label size and width

                                    ax.tick_params(labelsize=9, width=0.01)

                                    # ax2.tick_params(labelsize=5, width=0.01)

                                    # change the y tick label color

                                    ax.tick_params(axis='y', labelcolor='k')

                                    # if trans_type in ['6x6', 'hf_Impulse']:

                                    #     ax.tick_params(axis='y', labelcolor='k')

                                    #     ax2.tick_params(axis='y', labelcolor='deepskyblue')

                                    # elif trans_type == '6m':

                                    #     ax.tick_params(axis='y', labelcolor='b')

                                    # adding correlation text

                                    # corr_annotate = 'Corr : ' + "{0:.2f}".format(corr)

                                    # if lag>.01 and lag<999:

                                    #     corr_annotate = 'Corr : '+"{0:.2f}".format(corr) + " (with "+str(lag)+' periods ahead '+name2+')'

                                    # ann_x = x1[0]

                                    # ann_y = ax.get_ylim()[0] + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02

                                    # ax.annotate(corr_annotate, (ann_x, ann_y), fontsize='small')

                                    # adding footnote text

                                    ann_x = x1[0]

                                    ann_y = ax.get_ylim()[0] + (ax.get_ylim()[1] - ax.get_ylim()[0]) * 0.02

                                    ax.annotate("\n".join(wrap(footnote, 80)), (ann_x, ann_y), fontsize='x-small',

                                                family='DejaVu Serif', zorder=9999)

                                    # # set the unit

                                    # if trans_type == '6m':

                                    #     unit = axis_unit[0]

                                    #     ax.set_ylabel(unit, rotation=0, color='k', fontsize=7, fontname="DejaVu Serif")

                                    #     ax.yaxis.set_label_coords(0.9, 1.01)

                                    # if trans_type in ['6x6', 'hf_Impulse']:

                                    #     unit1, unit2 = axis_unit

                                    #     ax.set_ylabel(unit1, rotation=0, color='k', fontsize=7, fontname="DejaVu Serif")

                                    #     ax.yaxis.set_label_coords(0.9, 1.01)

                                    #     ax2.set_ylabel(unit2, rotation=0, color='k', fontsize=7,

                                    #                    fontname="DejaVu Serif")

                                    #     ax2.yaxis.set_label_coords(0, 1.04)

                                    # set border color and width

                                    for spine in ax.spines.values():
                                        spine.set_edgecolor('grey')

                                        spine.set_linewidth(0.5)

                                    # add year tickers as minor tick

                                    # years = mdates.YearLocator()

                                    # ax.xaxis.set_minor_locator(years)

                                    # set the width of minor tick

                                    # ax.tick_params(which='minor', width=0.008)

                                    # set y-label to the right hand side

                                    ax.yaxis.tick_right()

                                    # if trans_type in ['6x6', 'hf_Impulse']:

                                    #     ax2.yaxis.tick_left()

                                    # set date max

                                    # datemax = np.datetime64(x1[-1], 'Y')

                                    # datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')

                                    # set the ticker to 2005, 2010, 2015 ...

                                    # first_date_ticker = np.datetime64(chart_start_dt, 'Y')

                                    # while int(str(np.datetime64(first_date_ticker, 'Y'))) % 5 != 0:

                                    #   first_date_ticker = first_date_ticker + np.timedelta64(1, 'Y')

                                    # x_tick_overrive = [first_date_ticker, datemax]

                                    # date_cursor = first_date_ticker

                                    # while date_cursor + np.timedelta64(5, 'Y') < datemax:

                                    #   date_cursor = date_cursor + np.timedelta64(5, 'Y')

                                    #   x_tick_overrive.append(date_cursor)

                                    # ax.xaxis.set_ticks(x_tick_overrive)

                                    # ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))

                                else:

                                    self.set_ax_invisible(axarr[i, j])

                        else:

                            self.set_ax_invisible(axarr[i, j])

                plt.subplots_adjust(wspace=0.2, hspace=0.5)

                fig.text(4.5 / 8.5, 0.5 / 11., str(page_counter), ha='center', fontsize=12)

                # plt.tight_layout()

                report.savefig(fig, bbox_inches='tight')  # the current page is saved

        report.close()

    def set_ax_invisible(self, ax):

        ax.axis('off')
