from Analytics.abstract_sig import abstract_sig_genr
from datetime import datetime
import pandas as pd
from backtesting_utils.chart import generic_plot
import os


class sig_commodity_trade_tree(abstract_sig_genr):
    def __init__(self):
        super(sig_commodity_trade_tree, self).__init__()

    def add_dir_info(self, *args, **kwargs):
        '''
        adding directory information of the strategy
        '''
        self.time_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '%Y%m%d_%H%M')
        self.date_stamp = datetime.strftime(datetime.utcnow().replace(second=0, microsecond=0), '_%Y%m%d')
        self.WKDIR = os.path.dirname(os.path.realpath(__file__))
        self.PROJ_DIR = os.path.join(self.WKDIR, "..")
        if kwargs.get('get_or_set_market_data') in ['set']:
            self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "offline_data/master_input_baseline.xlsx")
        else:
            self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")
        self.category = self.identify_category(self.MASTER_INPUT_DIR, self.Short_Name)
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
        self.BT_ID1 = self.Short_Name + self.time_stamp + '_2002.pdf'
        self.BT_ID2 = self.Short_Name + self.time_stamp + '_2010.pdf'

        self.BT_BACKUP_ROOT_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP')
        # backup component since 2000
        self.BT_BACKUP_DIR1 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID1)
        # backup component since 1985
        self.BT_BACKUP_DIR2 = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', self.BT_ID2)

        self.PARS_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'PARS' + self.time_stamp + '.csv')
        self.DATA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'DATA' + self.time_stamp + '.csv')
        self.ALPHA_DIR = os.path.join(self.SCRATCH_DIR, 'BT_BACKUP', 'ALPHA' + self.time_stamp + '.csv')

        # Firstly export all the relevant result into the csv format. Secondly plot into the charts
        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'
        if kwargs.get('get_or_set_market_data') in ['set'] or kwargs.get('reporting_to') not in [
            'group_wide_reporting']:
            self.RPTDIR = os.path.join(self.PROJ_DIR, 'reporting', self.Short_Name)
        else:
            self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.category, self.Short_Name) if os.access(
                self.SHARE_DIR,
                os.W_OK) else os.path.join(
                self.PROJ_DIR, 'reporting', self.Short_Name)

        # reporting file
        self.RPT_2002_DIR = os.path.join(self.RPTDIR,
                                         self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_2002.pdf')
        self.RPT_2010_DIR = os.path.join(self.RPTDIR,
                                         self.Short_Name + datetime.utcnow().strftime('%Y%m%d') + '_2010.pdf')
        self.create_folder(self.INDICATOR_EXP_DIR)
        self.out_folder = self.create_tearsheet_folder(self.RPTDIR)
        self.create_folder(self.BT_BACKUP_ROOT_DIR)
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
        self.H5_LOCALDB_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", 'local_db.h5')
        self.TRADE_TABLE_DIR = os.path.join(self.SHARE_DIR, 'reporting', 'Historical_trade_blotter',
                                            'Theotical_position') if os.access(self.SHARE_DIR,
                                                                               os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', 'Historical_trade_blotter', 'Theotical_position')
        self.create_folder(self.TRADE_TABLE_DIR)
        self.TRADE_TABLE_FILE = os.path.join(self.TRADE_TABLE_DIR,
                                             'trade_table' + datetime.now().strftime('%Y%m%d') + '.csv')
        self.FLAG_FILE_DIR = os.path.join(self.SHARE_DIR, 'reporting', 'Historical_trade_blotter',
                                          'Flaggings') if os.access(self.SHARE_DIR, os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', 'Historical_trade_blotter', 'Flaggings')
        self.create_folder(self.FLAG_FILE_DIR)
        self.FLAG_FILE = os.path.join(self.FLAG_FILE_DIR, 'flags ' + datetime.now().strftime('%Y%m%d') + '.txt')

        self.OUTLIER_DIR = os.path.join(self.FLAG_FILE_DIR, 'outliers')
        self.create_folder(self.OUTLIER_DIR)
        self.OUTLIER_FILE = os.path.join(self.OUTLIER_DIR,
                                         self.Short_Name + ' ' + datetime.now().strftime('%Y%m%d') + '.pdf')

        # Export to Andrew's reporting data dir
        self.AL_RPT_DATA_DIR = os.path.join(self.SHARE_DIR, 'reporting', 'For Andrew', 'data') if os.access(
            self.SHARE_DIR,
            os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', 'For Andrew', 'data')
        self.create_folder(self.AL_RPT_DATA_DIR)
        self.AL_RPT_DATA_FILE = os.path.join(self.AL_RPT_DATA_DIR,
                                             self.Short_Name + ' ' + datetime.now().strftime('%Y%m%d') + '.csv')

    def run_step3(self, root='Current account balance', **kwargs):
        if kwargs['run_charting'] and kwargs.get('get_or_set_market_data') not in ['set']:
            root = self.tree.nodes[root]
        self.series_name_dict = {
            'fx_spot_series': 'FX_USD',
            'iso': kwargs.get('iso') if 'iso' in kwargs.keys() else None,
            'CA_GDP': 'CA % GDP'
        }
        chart_pack_dict = self.tree.expand_tree_below_a_node_and_return_balance_vs_fx_chart(root, self.new_wf.df,
                                                                                            self.series_name_dict)
        start_dt = '2002-01-01'
        generic_plot(plot_dict=chart_pack_dict, chart_start_dt=start_dt, pdfpath=self.BT_BACKUP_DIR1,
                     bt_backup_dir=self.RPT_2002_DIR)
        start_dt = '2010-01-01'
        generic_plot(plot_dict=chart_pack_dict, chart_start_dt=start_dt, pdfpath=self.BT_BACKUP_DIR2,
                     bt_backup_dir=self.RPT_2010_DIR)

