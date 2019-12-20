import os
import sys
import pandas as pd
import numpy as np
import collections
import panormus.data.bo.econ as econ

import Analytics.series_utils as s_util
from Analytics.download_util import Downloader as Downloader
import os
import pickle
from datetime import datetime,timedelta
import Analytics.wfcreate as wf
from panormus.data import haver
import backtesting_utils.post_signal_genr as post_signal_genr
import backtesting_utils.chart as TCT

# part of the utilities
dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
dateparse2 = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')
conn = econ.get_instance_using_credentials()
WKDIR = os.path.dirname(os.path.realpath(__file__))
PROJ_DIR = os.path.join(WKDIR,"..")
TEMP_DIR = os.environ['TEMP']
TEMP_RAW_PICKLE = os.path.join(TEMP_DIR,'TEMP_JY_local_db.pickle')

class abstract_sig_genr:
    def __init__(self):
        self.local_db = self.read_pickle()
        self.SU = s_util.date_utils_collection()

    def conn_get_clean(self,ticker):
        df_raw = conn.get(ticker)
        df_list = []
        if len(df_raw.columns)<0.01:
            df_list.append(df_raw)
        for col in df_raw.columns:
            df = df_raw[[col]]
            if len(df.index) > 0.01:
                test_index = [d.date() for d in df.loc[:'2019-8-19',:].index[-1000:]]
                if len(test_index) != len(set(test_index)):
                    new_index = [d.replace(hour=0, minute=0, second=0) for d in df.index]
                    df.index = new_index
                    df.dropna(inplace=True)
                    df = df[~df.index.duplicated(keep='last')]
            df_list.append(df)

        return pd.concat(df_list, axis=1)

    def read_pickle(self):
        '''
        :return: read from local database (in pickle) the series.
        '''
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
        #TODO: this file is to check if the pdf charts file is in use
        '''
        :return: check if the pdf file is in use
        '''
        pass

    #TODO: try to use the suffix-name in the file , as the key to the data set, must be much easier to

    def download_data_2(self,MASTER_INPUT_DIR,Short_Name):
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
            i = str(int(i)+int(1))
            series_col = ('').join(['series',i])
            load_fn_col = ('').join(['load_fn',i])
            location_col = ('').join(['location',i])

            ser_list = import_df[series_col].values.tolist()
            load_fn_list = import_df[load_fn_col].values.tolist()
            location_list = import_df[location_col].values.tolist()

            for k,iso in enumerate(iso_list):
                if ser_list[k] == 'invalid_name':
                    continue
                if load_fn_list[k] == 'EconDB':

                    des = len(result_dict[iso]['des'])
                    result_dict[iso]['des'].append(des)
                    data = self.conn_get_clean([ser_list[k]])
                    print (iso, ser_list[k],' is done!')
                    result_dict[iso]['data'].append(data)
                if load_fn_list[k] == 'csv':
                    print ('Sorry, currently does not support csv import, to be added...')
        return result_dict

    def download_data_3(self,MASTER_INPUT_DIR,Short_Name):
        '''
        downloading method allows for more sources (EconDB, csv or haver), return a dictionary
        the returned data structure is different, as {'AUS_SER1':df_AUS_SER1, 'AUS_SER2':df_AUS_SER2 ...}, so that you can use wildcard to iterate through
        :return: an ordered dictionary in the format like: {iso1:{'data':[df1,df2],'des':[des1,des2]}}
        :param MASTER_INPUT_DIR: excel spread sheet of data info
        :param Short_Name: excel tac name
        :return: dict of data
        '''

        print('start downloading data...')
        result_dict = collections.OrderedDict()
        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)
        import_df.replace(np.nan, 'invalid_name', inplace=True)
        iso_list = import_df['ISO'].values.tolist()

        # start download the data
        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())
        for i in range(number_of_series_col):
            i = str(int(i)+int(1))
            series_col = ('').join(['series',i])
            load_fn_col = ('').join(['load_fn',i])
            location_col = ('').join(['location',i])
            suffix_col = ('').join(['in_file_suf_fix',i])

            ser_list = import_df[series_col].values.tolist()
            load_fn_list = import_df[load_fn_col].values.tolist()
            location_list = import_df[location_col].values.tolist()
            suffix_list = import_df[suffix_col].values.tolist()

            for k,iso in enumerate(iso_list):
                if ser_list[k] == 'invalid_name':
                    result_dict[suffix_list[k]] = pd.DataFrame()
                    continue
                if 'panel' in ser_list[k] :
                    assert location_list[k] != 'invalid_name', 'sorry, the loc is invalid'
                    try:
                        result_dict[suffix_list[k]] = pd.read_csv(location_list[k],index_col=0,header=0)
                    except:
                        loc = os.path.join(PROJ_DIR,location_list[k])
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
                    assert location_list[k]!='invalid_name','sorry, the loc is invalid'
                    if not os.path.isfile(location_list[k]):
                        try:
                            loc = os.path.join(PROJ_DIR, location_list[k])
                            df = pd.read_csv(loc, index_col=0, header=0)
                        except:
                            result_dict[suffix_list[k]] = pd.DataFrame()
                            continue
                    else:
                        df = pd.read_csv(location_list[k],index_col=0,header=0)
                        df.index = pd.to_datetime(df.index)
                    if len(df.index)<0.001 or (ser_list[k] not in df.columns.tolist()):
                        result_dict[suffix_list[k]] = pd.DataFrame()
                        continue
                    df.index = pd.to_datetime(df.index)
                    result_dict[suffix_list[k]] = df.loc[:,[ser_list[k]]]
                    continue
                if load_fn_list[k] == 'haver':
                    if ser_list[k] not in self.local_db.keys():
                        #print (ser_list[k])
                        try:
                            df = haver.get_data([ser_list[k]])
                        except:
                            print (ser_list[k])
                        #print (df)
                        df.index = df.index.to_timestamp()
                        result_dict[suffix_list[k]] = df
                        self.local_db[ser_list[k]] = df
                    else:
                        result_dict[suffix_list[k]] = self.local_db[ser_list[k]]
                    continue

        # dump the updated file to the pickle
        with open(TEMP_RAW_PICKLE, 'wb') as handle:
            pickle.dump(self.local_db, handle, protocol=pickle.HIGHEST_PROTOCOL)
        print ('done downloading data...')
        return result_dict

    def download_data_4(self,CONTROL_FILE_DIR,sheet_name = 'Control'):
        # data downloader for control file @FT format
        result_dict = collections.OrderedDict()
        import_df = pd.read_excel(CONTROL_FILE_DIR,sheet_name=sheet_name,index_col=False,header=0)
        iso_list = import_df['Country'].values.tolist()

       #download the data
        ser_list = import_df['Code'].values.tolist()
        for k,iso in enumerate(iso_list):
            result_dict[ser_list[k]] = self.conn_get_clean([ser_list[k]])
            print (iso, ser_list[k],' is downloaded')
        return result_dict

    def import_csv_panel(self,MASTER_INPUT_DIR,Short_Name,csv_path,series):
        '''
        :return: import the csv files given be FT, which
        contains the very long historical real GDP data. Then sort them into a dictionary, with (iso,df) pairs
        '''
        SU = s_util.date_utils_collection()
        iso_with_series = self.import_names_to_list(MASTER_INPUT_DIR,Short_Name,series)
        raw_data = pd.read_csv(csv_path,index_col=0,header=0,na_values='NA')
        #print (raw_data)
        result_dict = {}
        for iso,series in iso_with_series:
            #assert series in raw_data.columns.tolist() , "sorry, series"+series+' is not in the raw data columns!'
            if series not in raw_data.columns.tolist():
                continue
            df = raw_data[[series]]
            df  = SU.truncate_NAs(df)
            result_dict[iso] = df
        #print (self.raw_data)
        return result_dict

    def import_names_to_list(self, master_input_dir,Short_Name, col_name):
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
        #try:
        series_result = download.submit_with_mp()
        #except:
        #series_result = download.submit()
        result_dict = download.convert_result_to_dict(series_result, s_with_id)
        # print (result_dict)
        return result_dict

    def sanity_check(self):
        pass

    def apply_smoothing(self):
        pass

    def post_conversion(self):
        pass

    def convert_indicator_tolist(self,indicator,indicator_exp_dir):
        indicator_list = []
        for iso, content in indicator.items():
            if len(content['des'])==0:
                continue
            else:
                zipped = zip(content['des'],content['data'])
                this_list = [(iso+'_'+str(v[0]),v[1]) for v in zipped]
                indicator_list = indicator_list + this_list
        self.indicator_list = indicator_list
        self.export_indicator(indicator_list,indicator_exp_dir)
        return

    def convert_indicator_tolist_new(self,indicator,indicator_exp_dir):
        self.create_indicator_group_folder(indicator_exp_dir)
        indicator_list = []
        for k, v in indicator.items():
            indicator_list = indicator_list + [(k,v)]
        self.indicator_list = indicator_list
        self.export_indicator(indicator_list,indicator_exp_dir)
        return

    def export_indicator(self,indicator_list,export_path):
        for ticker_df in indicator_list:
            #print (id_df)
            name_of_file = str(ticker_df[0])
            ticker_df[1].to_csv(os.path.join(export_path,name_of_file+'.csv'))
        return

    def create_indicator_group_folder(self,indicator_dir):
        """
        Tearsheet is saved to the reporting folder
        """
        destination = indicator_dir

        self.create_folder(destination)
        assert os.path.isdir(destination)
        assert os.path.exists(destination)
        return destination

    #TODO:might put a group of charting utilities, including pdf/tearsheet etc into a
    # tearsheet utility, so that can be used in the backtesting reporting in the future
    def create_tearsheet_folder(self,RPTDIR):
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

    def pickle_result(self,temp_pickle,strategy_work_file):
        '''
        save down the strategy container (work_frame) for future use
        :param temp_pickle: pickle path
        :param strategy_work_file: work_frame object
        '''
        # pickle the result to a temp folder
        with open(temp_pickle, 'wb') as handle:
            pickle.dump(strategy_work_file, handle, protocol=pickle.HIGHEST_PROTOCOL)

    @staticmethod
    def mask_list(l,m):
        return [z[0] for z in zip(l, m) if z[1]]

class abstract_sig_genr_for_rates_tree(abstract_sig_genr):
    def __init__(self):
        super(abstract_sig_genr_for_rates_tree, self).__init__()

    def add_dir_info(self,*args,**kwargs):
        '''
        adding directory information of the strategy
        '''
        self.WKDIR = os.path.dirname(os.path.realpath(__file__))
        self.PROJ_DIR = os.path.join(self.WKDIR,"..")
        self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR,"input/master_input.xlsx")
        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR,"output")
        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR,"zzz_NO_commit_folder",self.Short_Name)
        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR,"zzz_NO_commit_folder",self.Short_Name,'indicator_group')
        self.LOCAL_MACRO_DATA_DIR = os.path.join(self.PROJ_DIR,'macro_data')
        self.time_stamp =str(datetime.timestamp(datetime.now().replace(microsecond=0)))
        self.BT_ID1 = self.Short_Name+self.time_stamp+'.pdf'
        self.BT_ID2 = self.Short_Name+self.time_stamp+'_pnl.pdf'
        self.BT_ID2010_1 = self.Short_Name+self.time_stamp+'2010.pdf'
        self.BT_ID2010_2 = self.Short_Name+self.time_stamp+'_pnl2010.pdf'
        self.BT_BACKUP_ROOT_DIR = os.path.join(self.SCRATCH_DIR,'BT_BACKUP')
        # backup component since 2000
        self.BT_BACKUP_DIR1 = os.path.join(self.SCRATCH_DIR,'BT_BACKUP',self.BT_ID1)
        # backup pnl since 2000
        self.BT_BACKUP_DIR2 = os.path.join(self.SCRATCH_DIR,'BT_BACKUP',self.BT_ID2)
        # backup component since 2000
        self.BT_BACKUP_DIR2010_1 = os.path.join(self.SCRATCH_DIR,'BT_BACKUP',self.BT_ID2010_1)
        # backup pnl since 2010
        self.BT_BACKUP_DIR2010_2 = os.path.join(self.SCRATCH_DIR,'BT_BACKUP',self.BT_ID2010_2)

        self.PARS_DIR = os.path.join(self.SCRATCH_DIR,'BT_BACKUP','PARS'+self.time_stamp+'.csv')
        self.DATA_DIR = os.path.join(self.SCRATCH_DIR,'BT_BACKUP','DATA'+self.time_stamp+'.csv')
        self.ALPHA_DIR = os.path.join(self.SCRATCH_DIR,'BT_BACKUP','ALPHA'+self.time_stamp+'.csv')

        #Firstly export all the relevant result into the csv format. Secondly plot into the charts
        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'

        if kwargs.get('run_mode')=='production':
            self.RPTDIR = os.path.join(self.SHARE_DIR,'reporting',self.Short_Name) if os.access(self.SHARE_DIR, os.W_OK) else os.path.join(self.PROJ_DIR,'reporting',self.Short_Name)
        else:
            self.RPTDIR = os.path.join(self.PROJ_DIR, 'reporting', self.Short_Name)

        # reporting file
        # reporting component since 2000
        self.RPT_COMPONENT_DIR = os.path.join(self.RPTDIR, self.Short_Name + datetime.now().strftime('%Y%m%d') + '.pdf')
        # reporting pnl 2010
        self.RPT_PNL2010_DIR = os.path.join(self.RPTDIR, self.Short_Name + datetime.now().strftime('%Y%m%d') + '_pnl2010.pdf')
        # reporting pnl since 2000
        self.RPT_PNL2000_DIR = os.path.join(self.RPTDIR,
                                            self.Short_Name + datetime.now().strftime('%Y%m%d') + '_pnl2000.pdf')
        self.HISTORICAL_POS_FOLDER = os.path.join(self.RPTDIR,'History')
        self.create_folder(self.HISTORICAL_POS_FOLDER)
        self.HISTORICAL_POS_DIR = os.path.join(self.HISTORICAL_POS_FOLDER,
                                                   'History_sig' + self.Short_Name + datetime.now().strftime(
                                                       '%Y%m%d') + '.csv')

        #######
        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR,'   ')
        self.TEMP_LOCAL_PICKLE = os.path.join(os.environ['TEMP'],'TEMP_JY_'+self.Short_Name+'_local_db.pickle')
        #######
        self.EXPORT_TREE_STRUCT_FOLDER = os.path.join(self.RPTDIR,'Gauge_tree')
        self.create_folder(self.EXPORT_TREE_STRUCT_FOLDER)
        self.EXPORT_TREE_STRUCT_DIR = os.path.join(self.EXPORT_TREE_STRUCT_FOLDER,'Gauge_tree'+self.Short_Name + datetime.now().strftime('%Y%m%d') +'.csv')
        self.OUT_DATA_DIR = os.path.join(self.PROJ_DIR, 'output', self.Short_Name)

    def import_parse_param(self,master_input_dir,short_name):
        '''
        import and parse the parameters of the strategy
        :param master_input_dir: the excel contains the parameter and weights
        :param short_name: the short name of the strategy
        :return: dictionary that contains the parameters and the weights
        '''
        param_df = pd.read_excel(master_input_dir, sheet_name=short_name, index_col=False, header=0,
                                 na_values=['NA', ''])
        self.all_ISO = param_df.iloc[:,0].values.tolist()
        self.trans_param_df = param_df.loc[:,'Param Table':'type'].set_index('Param Table').dropna().T.to_dict('list')

        # convert to int
        for key,value in self.trans_param_df.items():
            self.trans_param_df[key][0]=int(self.trans_param_df[key][0]) if self.trans_param_df[key][1]=='int' else self.trans_param_df[key][0]

    def run_calc_duration(self,*args,**kwargs):
        import Analytics.finance as fin_lib
        rate_name = kwargs.get('rate_name')
        tenor = kwargs.get('tenor')
        df = self.SU.conversion_to_bday(self.raw_new_fmt_data[rate_name]).dropna()

        _values = df.values
        df.iloc[:,0]=[fin_lib.calc_duration(i, 5) for i in _values]
        df.columns = ['trading_instrument_duration']
        self.new_wf.add_df('trading_instrument_duration',df)

    def run_indicator_and_post_sig_genr(self,*args,**kwargs):
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

        self.new_wf.update_df(new_name, z_diff[[new_name]])
        self.new_wf.add_df('indicator_group', -z_diff[[new_name]].shift(self.trans_param_df['indicator_lag'][0]), repeat=True)
        self.post_indicator_genr(*args,**kwargs)

    def calc_DV01(self):
        df1 = self.new_wf.df['signal_group'].dropna()
        df2 = self.new_wf.df['actual_trade_group'].dropna()
        df3 = self.new_wf.df['trading_instrument_duration'].dropna()

        df_DV01 = df1.iloc[:,0]*df3.iloc[:,0]
        df_DV01.columns = ['DV01_raw']
        self.new_wf.add_df('DV01_raw',df_DV01)

        df_DV01 = df1.iloc[:, 0] * df3.iloc[:, 0].shift(1)
        df_DV01.columns = ['DV01_t_1']
        self.new_wf.add_df('DV01_t_1', df_DV01)

        df_DV01 = df1.iloc[:, 2] * df3.iloc[:, 0].shift(1)
        df_DV01.columns = ['DV01_actual_trading_t_1']
        self.new_wf.add_df('DV01_actual_trading_t_1', df_DV01)

    def post_indicator_genr(self,*args,**kwargs):
        '''
        generate trading signals and performance matrix
        :param args:
        :param kwargs:
        :return: trading signals and performance matrix
        '''
        self.sci = self.sci_panel(**kwargs)
        self.run_calc_duration(*args,**kwargs)
        # TODO: make it
        signal_dict = post_signal_genr.LS_filtered(self.new_wf.df['indicator_group'], self.sci, method='s_curve',
                                                   tc_reduction_method='inertia', inertia_torlerance=[50])
        self.new_wf.add_df('signal_group', signal_dict['signal_group'])
        self.new_wf.add_df('signal_group_flip', signal_dict['signal_group_flip'])
        self.new_wf.add_df('actual_trade_group', signal_dict['actual_trade_group'])
        self.calc_DV01()

        self.profit_sample = ['2000-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]
        # profit_sample = ['2000-01-01', '2016-12-31']
        self.pnl = post_signal_genr.profit(self.new_wf.df['actual_trade_group'], self.sci, 1000, self.profit_sample)
        self.new_wf.add_df('equity_curve', self.pnl)
        self.new_wf.add_df('cumprof', self.pnl[['cumprof']])

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample, benchmark=self.new_wf.df[kwargs['tri_name']])
        self.new_wf.alpha['ann_mean'] = retstats_dict['ann_mean']
        self.new_wf.alpha['ann_std'] = retstats_dict['ann_std']
        self.new_wf.alpha['ann_sharpe'] = retstats_dict['ann_sharpe']
        self.new_wf.alpha['calmar'] = retstats_dict['calmar']
        self.new_wf.add_df('drawdown', retstats_dict['drawdown'])
        self.new_wf.add_df('rolling_corr_1y_bm', retstats_dict['1y_corr'])
        self.new_wf.add_df('avg_corr_bm', retstats_dict['avg_corr'])

        self.profit_sample_2010 = ['2010-01-01', datetime.strftime(datetime.now() - timedelta(days=1), '%Y-%m-%d')]
        # profit_sample_2010 = ['2010-01-01', '2016-12-31']
        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000,self.profit_sample_2010, benchmark=self.new_wf.df[kwargs['tri_name']])

        self.new_wf.alpha['ann_mean_2010'] = retstats_dict['ann_mean']
        self.new_wf.alpha['ann_std_2010'] = retstats_dict['ann_std']
        self.new_wf.alpha['ann_sharpe_2010'] = retstats_dict['ann_sharpe']
        self.new_wf.alpha['calmar_2010'] = retstats_dict['calmar']
        self.new_wf.add_df('drawdown_2010', retstats_dict['drawdown'])
        self.new_wf.add_df('rolling_corr_1y_bm_2010', retstats_dict['1y_corr'])
        self.new_wf.add_df('avg_corr_bm_2010', retstats_dict['avg_corr'])
        return

    def run_step3(self,*args,**kwargs):
        '''
        run the reporting system and save down the bactesting result
        '''
        # dump to csv
        post_signal_genr.write_pars_to_csv(self.trans_param_df, self.PARS_DIR)
        post_signal_genr.write_pars_to_csv(self.new_wf.alpha, self.ALPHA_DIR)
        post_signal_genr.dump_wf_obj_to_csv(self.new_wf, self.DATA_DIR)

        if kwargs['run_charting']:
            root = self.tree.nodes['condition_m_pricing']
            chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_component_chart(root)
            # component chart 2000
            TCT.rates_tree_component_plot(plot_dict=chart_pack_dict,chart_start_dt='2000-01-01',chart_end_dt='2030-01-01',rate_rise_fall=self.rates_rise_fall_path,bt_backup_dir=self.RPT_COMPONENT_DIR,pdfpath=self.BT_BACKUP_DIR1)

            #calculate the pnl
            root = self.tree.nodes['condition_m_pricing']
            self.tree.get_rid_of_candidate_node(root)
            self.tree.print_structure(root)
            # plot pnl since 2000
            self.series_name_dict = {'yield_series':'IRS_5Y',
                                     'signal_flip':'signal_group_flip',
                                     'indicator_group':'indicator_group',
                                     'cumprof':'cumprof',
                                     'TRI':'USA_5y_TRI',
                                     'drawdown':'drawdown',
                                     'corr':'rolling_corr_1y_bm_2010'
                                     }
            self.alpha_name_dict = {'ann_mean':'ann_mean',
                                    'ann_std':'ann_std',
                                    'ann_sharpe':'ann_sharpe',
                                    'calmar':'calmar',
                                    'avg_corr':'avg_corr'
                                     }
            pnl_chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_pnl_chart(root,self.new_wf.df,self.series_name_dict,self.new_wf.alpha,self.alpha_name_dict)
            TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2000-01-01', chart_end_dt='2030-01-01',rate_rise_fall=self.rates_rise_fall_path, bt_backup_dir=self.RPT_PNL2000_DIR ,pdfpath=self.BT_BACKUP_DIR2)

            # plot pnl since 2010
            self.series_name_dict.update({'drawdown':'drawdown_2010'})
            self.alpha_name_dict = {'ann_mean': 'ann_mean_2010',
                                    'ann_std': 'ann_std_2010',
                                    'ann_sharpe': 'ann_sharpe_2010',
                                    'calmar': 'calmar_2010',
                                    'avg_corr': 'avg_corr_2010'
                                    }
            pnl_chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_pnl_chart(root, self.new_wf.df,
                                                                                          self.series_name_dict,
                                                                                          self.new_wf.alpha,
                                                                                          self.alpha_name_dict)
            TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2010-01-01',
                                          chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,
                                          bt_backup_dir=self.RPT_PNL2010_DIR, pdfpath=self.BT_BACKUP_DIR2010_2)

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
        self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.Short_Name) if os.access(self.SHARE_DIR, os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', self.Short_Name)
        #######
        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')