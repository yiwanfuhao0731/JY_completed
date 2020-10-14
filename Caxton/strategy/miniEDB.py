# given input spreadsheet, process functions like EDB

import pandas as pd

import numpy as np

import statsmodels.api as sm

import collections

import os

from datetime import datetime,timedelta

from Analytics.abstract_sig import abstract_sig_genr

from Analytics import transform_fns as tFunc

from backtesting_utils.cache import cache_response, clear_cache

 

class miniEDB(abstract_sig_genr):

    def __init__(self,**kwargs):

        '''

        download data and perform transformation like EconDB

        '''

        super(miniEDB, self).__init__()

 

    def add_dir_info(self,**kwargs):

        self.WKDIR = os.path.dirname(os.path.realpath(__file__))

        self.PROJ_DIR = os.path.join(self.WKDIR,"..")

        if kwargs.get('get_or_set_market_data') in ['set']:

            self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "offline_data/master_input_baseline.xlsx")

        else:

            self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")

        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR, "output")

        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name)

        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name, 'indicator_group')

 

        # TODO: separate the reporting process and signal generation process

        # Firstly export all the relevant result into the csv format. Secondly plot into the charts

        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'

        self.category = self.identify_category(self.MASTER_INPUT_DIR, self.Short_Name)

        if kwargs.get('get_or_set_market_data') in ['set'] or kwargs.get('reporting_to') not in ['group_wide_reporting']:

            self.RPTDIR = os.path.join(self.PROJ_DIR, 'reporting', self.Short_Name)

        else:

            self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.category,self.Short_Name) if os.access(self.SHARE_DIR,

                                                                                                  os.W_OK) else os.path.join(

                self.PROJ_DIR, 'reporting', self.Short_Name)

        #######

        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')

        self.TEAM_EXPORT_DIR = r"Y:\MacroQuant\Macro_data"

        self.TEAM_EXPORT_DIR = self.TEAM_EXPORT_DIR if os.access(self.TEAM_EXPORT_DIR, os.W_OK) else os.path.join(self.PROJ_DIR,

                                                                                                                  'reporting',

                                                                                                                  self.Short_Name)

        self.H5_LOCALDB_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", 'local_db.h5')

        self.FLAG_FILE_DIR = os.path.join(self.SHARE_DIR, 'reporting', 'Historical_trade_blotter',

                                          'Flaggings') if os.access(self.SHARE_DIR, os.W_OK) else os.path.join(

            self.PROJ_DIR, 'reporting', 'Historical_trade_blotter', 'Flaggings')

        self.create_folder(self.FLAG_FILE_DIR)

        self.FLAG_FILE = os.path.join(self.FLAG_FILE_DIR, 'flags ' + datetime.now().strftime('%Y%m%d') + '.txt')

 

        self.OUTLIER_DIR = os.path.join(self.FLAG_FILE_DIR, 'outliers')

        self.create_folder(self.OUTLIER_DIR)

        self.OUTLIER_FILE = os.path.join(self.OUTLIER_DIR,

                                         self.Short_Name + ' ' + datetime.now().strftime('%Y%m%d') + '.pdf')

 

    @cache_response('FLOW_BOP_CA_TB_BREAKDOWN_EDBmini', 'disk_8h', skip_first_arg=True)

    def get_data_dict(self,**kwargs):

        result_dict = collections.OrderedDict()

        self.add_spread_sheet_info(**kwargs)

        self.add_dir_info(**kwargs)

        self.raw_data_new_fmt = self.load_data_wrapper(self.xls_dir, self.sheet_name)

        result_dict.update(self.raw_data_new_fmt)

        result_dict.update(self.perform_transformation(self.parse_input_table()))

        return result_dict

 

    def perform_transformation(self,parsed_table):

        for i,r in parsed_table.iterrows():

            s1,s2,arg = self.format_s1_s2_arg(r['TRANS_SERIES1'],r['TRANS_SERIES2'],r['TRANS_FUNC'],r['TRANS_FUNC_ARGS'],self.raw_data_new_fmt)

            try:

                df = self.get_trans_func(r['TRANS_FUNC'])(s1,s2,arg)

            except:

                print (r['TRANS_SERIES1'],r['TRANS_SERIES2'],r['TRANS_FUNC'],r['TRANS_FUNC_ARGS'])

                print (s1.dropna())

                raise ValueError(df+'can\'t run function'+r['TRANS_FUNC'])

            if isinstance(df,pd.Series):

                df = df.to_frame()

            df.columns = [r['TRANS_NEW_NAME']]

            self.raw_data_new_fmt[r['TRANS_NEW_NAME']] = df

        return self.raw_data_new_fmt

 

    def add_spread_sheet_info(self,**kwargs):

        spread_sht_parse_dict = kwargs.get('spread_sht_parse_dict')

        self.exec_path = __file__

        self.Short_Name,self.xls_dir,self.sheet_name = spread_sht_parse_dict['Short_Name'],spread_sht_parse_dict['xls_dir'],spread_sht_parse_dict['sheet_name']

 

    def parse_input_table(self):

        input_table_df = pd.read_excel(self.xls_dir, sheet_name=self.sheet_name, index_col=False, header=0).loc[:,'TRANS_SERIES1':'TRANS_NEW_NAME']

        mask1 = input_table_df['TRANS_SERIES1'].notnull()

        mask2 = input_table_df['TRANS_SERIES2'].notnull()

        mask = mask1 + mask2

        input_table_df = input_table_df.loc[mask,:]

        input_table_df.fillna('invalid_name',inplace=True)

        return input_table_df

 

    def format_s1_s2_arg(self,s1,s2,func,arg,data_dict):

        '''

        :param s1: return pd.Series

        :param s2: return pd.Series

        :param arg:

        :param data_dict:

        :return:

        '''

        s1 = s1.replace(' ;;;; ',';;;;')

        s1 = s1.replace(';;;; ',';;;;')

        s1 = s1.replace(' ;;;;',';;;;')

        if func in ['group_sum_and_sa','group_sum','group_sum2']:

            if ';;;;' in s1:

                s1 = s1.split(';;;;')

            else:

                s1 = [s1]

            s1 = [data_dict[ss].iloc[:,0] for ss in s1]

        elif not s1 in [np.nan,'invalid_name']:

            try:

                s1 = data_dict[s1].iloc[:,0]

            except:

                print (s1,data_dict[s1])

                raise ValueError

        if not s2 in [np.nan,'invalid_name']:

            try:

                s2 = data_dict[s2].iloc[:,0]

            except:

                print (s2)

                raise ValueError

        return s1,s2,arg

 

    def get_trans_func(self,trans_func_name):

        func_dict = {'splice2':tFunc.splice2,

                     'splice3':tFunc.splice3,

                     'splice_geometric':tFunc.splice_geometric,

                     'subtract_series':tFunc.subtract_series,

                     'divide_series':tFunc.divide_series,

                     'divide_series_mul100':tFunc.divide_series_mul100,

                     'multiply_series':tFunc.multiply_series,

                     'sum_series':tFunc.sum_series,

                     'name':tFunc.name,

                     'minterp':tFunc.minterp,

                     'qinterp':tFunc.qinterp,

                     'minterp_gdp':tFunc.minterp_gdp,

                     'mult_constant':tFunc.mult_constant,

                     'div_constant':tFunc.div_constant,

                     'group_sum':tFunc.group_sum,

                     'group_sum2':tFunc.group_sum2,

                     'inf_adj':tFunc.inf_adj,

                     'ytd_m':tFunc.ytd_m,

                     'seasonaladjustx13':tFunc.seasonaladjustx13,

                     'conversion_to_m':tFunc.conversion_to_m,

                     'extend_to_2050':tFunc.extend_to_2050,

                     'group_sum_and_sa':tFunc.group_sum_and_sa,

                     'YTD_to_m_apr_sea_adj':tFunc.YTD_to_m_apr_sea_adj,

                     'ann_6m_geo_smooth_chg':tFunc.ann_6m_geo_smooth_chg,

                     'ann_6x6_geo_aris':tFunc.ann_6x6_geo_aris,

                     'ann_6m_aris_smooth_chg':tFunc.ann_6m_aris_smooth_chg,

                     'roll_mean':tFunc.roll_mean

                     }

        return func_dict[trans_func_name]

 

if __name__=='__main__':

    spread_sheet_parser1 = miniEDB()

    data = spread_sheet_parser1.get_data_dict(spread_sht_parse_dict={'Short_Name':'FLOW_BOP_TB_BREAKDOWN','xls_dir':r"C:\_CODE\JYang\JY_Project\signals\FLOW_BOP_TB_BREAKDOWN\trade categories.xlsx",'sheet_name':'AUS'})

    print (data.keys())