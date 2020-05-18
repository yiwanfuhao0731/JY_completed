# import sys when runing from the batch code
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import Analytics.series_utils as s_util
from Analytics.abstract_sig import abstract_sig_genr_for_fwdGrow_tree as abs_sig
import Analytics.wfcreate as wf
from Analytics.wfcreate import use_cached_in_class
from Analytics.wfcreate import fwd_growthTree
from panormus.utils.cache import cache_response, clear_cache
from signals.RATES_LVL_TW_GROW.run_signal import signal3 as RATES_LVL_TW_GROW_sig
from SCI.SCI_Master import signal3 as SCI_Master_sig
from signals.RATES_LVL_GROWmPOT.run_signal import get_data as RATES_LVL_GROWmPOT_get


class signal3(abs_sig):
    def __init__(self):
        super(signal3, self).__init__()

    def add_strat_info(self):
        # this is the part to override
        self.signal_ID = 'sig_0022'
        self.Short_Name = 'RATES_FWDGROW_USA'
        self.Description = '''
                       Cutting through the USA forward growth, i.e estimate the growth of different component individually with the impulse methodology.
                       GDP = C + I + G + X - M. In theory estimating each component and sum up to figure will give the best estimates on the forward growth.
                       The target is to estimate the forward 6m growth, but ideally 3m.
                       More specifically, to estimate the change of a variable A impact on the change of variable B. A should be normalised (maybe a second order diff is a good starting point).
                       B should be the change on the individual component of GDP.

                       C: bond prices, housing prices, equity prices, credit
                       I (residential): housing prices, housing start, mortgage rate
                       I (business): corporate credit spread
                       G: Fiscal impulse
                       X: trading partners growth, commodity, FX
                       M: mainly depends on the domestic demand. maybe we don't really care too much of this because the trend should in theory be the same with consumption.

                       The weight: should be able to decide the weight based on regression analysis.
                       However, maybe it is not a bad idea to decide the weight simply with correlation or visualisation??

                       Visualisation is also a key part of the analysis, especially the impulse responsive function is in particular a 
                       good starting point of visualisation already.
                       '''
        self.exec_path = __file__

    @cache_response('RATES_FWDGROW_USA', 'disk_8h', skip_first_arg=True)
    def get_data_dict(self, *args, **kwargs):
        self.add_strat_info()
        self.add_dir_info(**kwargs)
        self.new_wf = wf.initialise_wf()
        # in this case, both Econ and csv file data are used
        self.pre_run_result = self.pre_run(**kwargs)
        self.raw_data_new_fmt = self.load_data_wrapper(self.MASTER_INPUT_DIR, self.Short_Name, **kwargs)
        self.import_parse_param(self.MASTER_INPUT_DIR, self.Short_Name)
        self.create_folder(self.INDICATOR_EXP_DIR)
        self.out_folder = self.create_tearsheet_folder(self.RPTDIR)
        self.tree = fwd_growthTree()
        self.run_step1()
        self.indicator_dict = self.run_step2(*args, **kwargs)
        result_dict = self.fullkey_result_dict(self.indicator_dict)

        return result_dict

    def pre_run(self, **kwargs):
        result_dict = {}
        RATES_LVL_TW_GROW_sig1 = RATES_LVL_TW_GROW_sig()
        result_dict.update(RATES_LVL_TW_GROW_sig1.get_data_dict(**kwargs))
        SCI_Master1 = SCI_Master_sig()
        result_dict.update(SCI_Master1.get_data_dict(**kwargs))

        result_dict.update(RATES_LVL_GROWmPOT_get(**kwargs))
        return result_dict

    @staticmethod
    def mask_list(l, m):
        return [z[0] for z in zip(l, m) if z[1]]

    def import_parse_param(self, master_input_dir, short_name):
        '''
        :return:
        '''
        param_df = pd.read_excel(master_input_dir, sheet_name=short_name, index_col=False, header=0,
                                 na_values=['NA', ''])
        self.all_ISO = param_df.iloc[:, 0].values.tolist()

        self.trans_param_df = param_df.loc[:, 'Param Table':'smooth_front'].set_index('Param Table').dropna().T.to_dict(
            'list')
        # convert to int
        for key, value in self.trans_param_df.items():
            self.trans_param_df[key] = [int(v) for v in value]
        self.roll_mean = 5 * 12 * 21
        self.roll_sd = 5 * 12 * 21

        # export to csv
        param_df.loc[:, 'Param Table':'smooth_front'].set_index('Param Table').dropna().to_csv(
            self.EXPORT_TREE_PARAM_DIR)
        param_df.loc[:, 'ISO':'in_file_suf_fix1'].set_index('ISO').to_csv(self.EXPORT_TREE_DATA_TICKER)
        return

    def run_step1(self):
        self.run_from_scratch = True
        self.run_RGDP()
        self.run_PCE_Exp_Imp()
        self.run_InvResi()
        self.run_InvnonResiexEn()
        self.run_inv_nonresi_energy()
        self.run_eq_oil_bond()
        self.run_eq2()
        # self.eq_1st()
        self.run_fx()
        self.run_oil_flip()
        self.run_eps()
        self.run_mort_rate_irs()
        self.run_salary()
        self.run_saving_rate()
        self.run_mortgage_orig()
        self.run_housing_supply_month()
        self.run_HP_Permit_retail_cumining_jpmtracker()
        self.run_PPI()
        self.run_trading_partn_grow_cai_Conf()
        self.run_capexsurvey()
        self.run_new_order_survey()
        self.run_corporate_spread()
        self.run_con_conf_survey()
        self.run_rigcount()
        self.run_mba_app()

        self.pickle_result(self.TEMP_LOCAL_PICKLE, self.new_wf)

    def run_step2(self, *args, **kwargs):
        '''
        build trees
        '''
        COMPONENT_NODE_DICT = self.tree.read_node_info_from_excel(import_dir=self.MASTER_INPUT_DIR,
                                                                  sheet_name=self.Short_Name, **kwargs)
        self.tree.run_build_tree(COMPONENT_NODE_DICT, self.new_wf.df)
        # root = self.tree.nodes['RGDP']
        # root = self.tree.nodes['Export']
        # root = self.tree.nodes['Investment_nonResidential_Energy']
        # root = self.tree.nodes['Investment_nonResidential_exEnergy']
        # root = self.tree.nodes['PCE']
        # root = self.tree.nodes['Investment_Residential']
        root = self.tree.nodes['GS_CAI']
        # root = self.tree.nodes['GS_CAI']
        self.tree.add_zto_its_parent(root)
        # self.tree.add_corr_to_its_parent(root)
        self.tree.print_structure(root, export_to_csv_dir=self.EXPORT_TREE_STRUCT_DIR)
        self.tree.add_hf_impulse_zto_impulse_actual_release(root, export_to=self.INDICATOR_EXP_DIR,
                                                            flagging_dir=self.FLAG_FILE, curr_exec_path=self.exec_path,
                                                            **kwargs)
        indicator_dict = self.tree.weighted_sum_impulse_actual_release(root, export_to=self.INDICATOR_EXP_DIR, **kwargs)
        # print (root.hf_Impulse_mapped_actual_release.tail())
        # print (self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs(root)['df_pairs'])
        # self.plot_dict = self.tree.expand_tree_below_a_node_and_return_all_parent_child_pairs(root,maxlevel=999,plot_only_branch=True)
        if kwargs.get('run_chart') == True and kwargs.get('get_or_set_market_data') not in ['set']:
            self.create_report_page(None, chart_start_dt='1990-01-01', centre_ma=False, expansion_method='expansion1',
                                    top_root='GS_CAI', sub_root_list=['PCE', 'Investment_Residential',
                                                                      'Investment_nonResidential_exEnergy',
                                                                      'Investment_nonResidential_Energy', 'Export'],
                                    iso='USA')
            self.create_report_page(None, chart_start_dt='1990-01-01', centre_ma=True, expansion_method='expansion2',
                                    content='crosscorr', top_root='GS_CAI',
                                    sub_root_list=['PCE', 'Investment_Residential',
                                                   'Investment_nonResidential_exEnergy',
                                                   'Investment_nonResidential_Energy', 'Export'], iso='USA')

        return indicator_dict

    @use_cached_in_class(['RGDP_2nd'])
    def run_RGDP(self):
        df = self.raw_data_new_fmt['RGDP'].copy()
        df = self.SU.conversion_to_q(df)
        df = df.dropna().pct_change(2) * 2 * 100
        df.columns = ['RGDP_1st']
        self.new_wf.add_df('RGDP_1st', df, to_freq='Q')

        df = df.diff(2)
        df.columns = ['RGDP_2nd']
        self.new_wf.add_df('RGDP_2nd', df, to_freq='Q')

    # @use_cached_in_class(['PCE_Contrib_2nd'])
    # def run_PCE_contrib(self):
    #     # trans to 6m annualised
    #     df = self.raw_data_new_fmt['PCE_Contrib'].copy()
    #     df = SU.conversion_to_q(df)*4 #annualised
    #     df = df.rolling(2).mean()
    #     df.columns = ['PCE_Contrib_1st']
    #     new_wf.add_df('PCE_Contrib_1st',df,to_freq='Q')
    #
    #     df = df.diff(2)
    #     df.columns = ['PCE_Contrib_2nd']
    #     new_wf.add_df('PCE_Contrib_2nd',df,to_freq='Q')
    @use_cached_in_class([t + '_2nd' for t in ['PCE', 'Export', 'Import']])
    def run_PCE_Exp_Imp(self):
        for t in ['PCE', 'Export', 'Import']:
            df = self.raw_data_new_fmt[t].copy()
            df = self.SU.conversion_to_q(df) * 4 * 1000  # annualised
            if t in ['Export']:
                df = df * 0.5
            # calculate the weights
            gdp = self.SU.conversion_to_q(self.raw_data_new_fmt['RGDP'])
            df_comb = pd.merge(df, gdp, left_index=True, right_index=True, how='outer').fillna(method='ffill')
            df_comb[t + '_RGDP'] = df_comb.iloc[:, 0] / df_comb.iloc[:, 1]
            self.new_wf.add_df(t + '_RGDP', df_comb[[t + '_RGDP']], to_freq='Q')

            df = df.dropna().pct_change(2) * 2 * 100
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='Q')

            df = df.diff(2)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='Q')

    @use_cached_in_class([t + '_2nd' for t in ['Investment_Residential']])
    def run_InvResi(self):
        for t in ['Investment_Residential']:
            df = self.raw_data_new_fmt[t].copy()
            df = self.SU.conversion_to_q(df) * 1000  # in million
            # calculate the weights
            gdp = self.SU.conversion_to_q(self.raw_data_new_fmt['RGDP'])
            df_comb = pd.merge(df, gdp, left_index=True, right_index=True, how='outer').fillna(method='ffill')
            df_comb[t + '_RGDP'] = df_comb.iloc[:, 0] / df_comb.iloc[:, 1]
            self.new_wf.add_df(t + '_RGDP', df_comb[[t + '_RGDP']], to_freq='Q')

            df = df.dropna().pct_change(2) * 2 * 100
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='Q')

            df = df.diff(2)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='Q')

    @use_cached_in_class([t + '_2nd' for t in ['Investment_nonResidential_exEnergy']])
    def run_InvnonResiexEn(self):
        # non residential investment ex energy
        for t in ['Investment_nonResidential']:
            df = self.raw_data_new_fmt[t].copy()
            df_en = self.raw_data_new_fmt['Investment_Non_Resi_Mining_Shafts_Well'].copy()
            df = self.SU.conversion_to_q(df) * 1000  # in million
            df_en = self.SU.conversion_to_q(df_en) * 1000
            df = self.SU.minus_df1_df2(df, df_en, na='drop')
            # calculate the weights
            gdp = self.SU.conversion_to_q(self.raw_data_new_fmt['RGDP'])
            df_comb = pd.merge(df, gdp, left_index=True, right_index=True, how='outer').fillna(method='ffill')
            # new name
            tt = 'Investment_nonResidential_exEnergy'
            df_comb[tt + '_RGDP'] = df_comb.iloc[:, 0] / df_comb.iloc[:, 1]
            # print (df_comb[t+'_RGDP'] .tail())
            self.new_wf.add_df(tt + '_RGDP', df_comb[[tt + '_RGDP']], to_freq='Q')
            # df_comb[[tt + '_RGDP']].to_csv('nonresiexEn_gdp.csv')

            df = df.dropna().pct_change(2) * 2 * 100
            df.columns = [tt + '_1st']
            self.new_wf.add_df(tt + '_1st', df, to_freq='Q')

            df = df.diff(2)
            df.columns = [tt + '_2nd']
            self.new_wf.add_df(tt + '_2nd', df, to_freq='Q')

    # @use_cached_in_class(['Government_Contrib_2nd'])
    # def run_gov_contrib(self):
    #     df = self.raw_data_new_fmt['Government_Contrib'].copy()
    #     #print(df.tail())
    #     df = SU.conversion_to_q(df)
    #     #print (df.tail())
    #     df = df.rolling(2).mean()
    #     df.columns = ['Government_Contrib_1st']
    #     new_wf.add_df('Government_Contrib_1st',df,to_freq='Q')
    #
    #     df = df.diff(2)
    #     df.columns = ['Government_Contrib_2nd']
    #     new_wf.add_df('Government_Contrib_2nd',df,to_freq='Q')

    # @use_cached_in_class(['Investment_Contrib_2nd'], new_wf)
    # def run_inv_contrib(self):
    #     df = self.raw_data_new_fmt['Investment_Contrib'].copy()
    #     df = SU.conversion_to_q(df)
    #     df = df.rolling(2).mean()
    #     df.columns = ['Investment_Contrib_1st']
    #     new_wf.add_df('Investment_Contrib_1st',df,to_freq='Q')
    #
    #     df = df.diff(2)
    #     df.columns  = ['Investment_Contrib_2nd']
    #     new_wf.add_df('Investment_Contrib_2nd',df,to_freq='Q')

    # @use_cached_in_class(['Investment_Residential_Contrib_2nd'], new_wf)
    # def run_inv_resi_contrib(self):
    #     df = self.raw_data_new_fmt['Investment_Residential_Contrib'].copy()
    #     df  = SU.conversion_to_q(df)
    #     df = df.rolling(2).mean()
    #     df.columns = ['Investment_Residential_Contrib_1st']
    #     new_wf.add_df('Investment_Residential_Contrib_1st', df, to_freq='Q')
    #
    #     df = df.diff(2)
    #     df.columns = ['Investment_Residential_Contrib_2nd']
    #     new_wf.add_df('Investment_Residential_Contrib_2nd', df, to_freq='Q')
    #
    # @use_cached_in_class(['Investment_nonResidential_Contrib_2nd'], new_wf)
    # def run_inv_nonresi(self):
    #     df = self.raw_data_new_fmt['Investment_nonResidential_Contrib'].copy()
    #     df = SU.conversion_to_q(df)
    #     df = df.rolling(2).mean()
    #     df.columns = ['Investment_nonResidential_Contrib_1st']
    #     new_wf.add_df('Investment_nonResidential_Contrib_1st',df,to_freq='Q')
    #
    #     df = df.diff(2)
    #     df.columns = ['Investment_nonResidential_Contrib_2nd']
    #     new_wf.add_df('Investment_nonResidential_Contrib_2nd',df,to_freq='Q')
    @use_cached_in_class(['Investment_nonResidential_Energy_1st'])
    def run_inv_nonresi_energy(self):
        df1 = self.SU.conversion_to_q(self.raw_data_new_fmt['Investment_Non_Resi_Mining_Shafts_Well']) * 1000
        df2 = self.SU.conversion_to_q(self.raw_data_new_fmt['RGDP'])
        df_comb = pd.merge(df1, df2, left_index=True, right_index=True, how='outer')
        df_comb['Investment_nonResidential_Energy' + '_RGDP'] = df_comb.iloc[:, 0] / df_comb.iloc[:, 1]
        self.new_wf.add_df('Investment_nonResidential_Energy_RGDP',
                           df_comb[['Investment_nonResidential_Energy' + '_RGDP']], to_freq='Q')
        # new_wf.df['Investment_nonResidential_Energy_RGDP'].to_csv('energy_gdp.csv')
        df1 = df1.dropna().pct_change(2) * 2 * 100
        df1.columns = ['Investment_nonResidential_Energy_1st']
        self.new_wf.add_df('Investment_nonResidential_Energy_1st', df1, to_freq='Q')

        df1 = df1.diff(2)
        df1.columns = ['Investment_nonResidential_Energy_2nd']
        self.new_wf.add_df('Investment_nonResidential_Energy_2nd', df1, to_freq='Q')
        # print (df1.dropna().tail())

    @use_cached_in_class(['Investment_nonResidential_Ind_eq_2nd'])
    def run_inv_nonresi_ind_eq(self):
        df = self.SU.conversion_to_q(self.raw_data_new_fmt['Investment_Non_Resi_Industrial_Equi'])
        df = df.rolling(2).mean()
        df.columns = ['Investment_nonResidential_Ind_eq_1st']
        self.new_wf.add_df('Investment_nonResidential_Ind_eq_1st', df, to_freq='Q')

        df = df.diff(2)
        df.columns = ['Investment_nonResidential_Ind_eq_2nd']
        self.new_wf.add_df('Investment_nonResidential_Ind_eq_2nd', df, to_freq='Q')

    @use_cached_in_class(['Export_Contrib_2nd', 'Import_Contrib_2nd'])
    def run_exp_imp_contrib(self):
        for t in ['Export_Contrib', 'Import_Contrib']:
            df = self.SU.conversion_to_q(self.raw_data_new_fmt[t]) * 4
            df = df.rolling(2).mean()
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='Q')

            df = df.diff(2)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='Q')

    @use_cached_in_class(
        [t + '_2nd' for t in ['Equity', '10Y_Gov_Bond', 'Oil_Price_WTI', 'Oil_Price_Brent', 'HB_BB_px', 'EN_BB_px']])
    def run_eq_oil_bond(self):
        for t in ['Equity', '10Y_Gov_Bond', 'Oil_Price_WTI', 'Oil_Price_Brent', 'HB_BB_px', 'EN_BB_px']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][0])
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='bday')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris')
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='bday')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='bday')

    # def eq_1st(self):
    #     for t in ['Equity']:
    #         df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t]).dropna()
    #         tt = 'Equity_1st^PCE^'
    #         df = self.SU.smooth_change(df,periods=self.trans_param_df[tt][0])
    #         df = df.rolling(window=self.trans_param_df[tt][2]).mean()
    #         df.columns = ['Equity_1st']
    #         self.new_wf.add_df(tt + '_1st', df, to_freq='bday')
    #         df['trend'] = df.iloc[:,0].rolling(window=self.trans_param_df[tt][1]).mean()
    #         df['Equity_detrend'] = df.iloc[:,0]-df.loc[:,'trend'].fillna(method='bfill')
    #
    #         df = df[['Equity_detrend']]
    #         self.new_wf.add_df(t + '_detrend', df, to_freq='bday')
    #         self.new_wf.add_df(t + '_detrend_Impulse', df, to_freq='bday')

    @use_cached_in_class([t + '_2nd' for t in ['Equity^Invest^']])
    def run_eq2(self):
        for t in ['Equity']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            tt = t + '^Invest^'
            df = self.SU.smooth_change(df, periods=self.trans_param_df[tt][0], ann=True) * 100
            df.columns = [t + '_1st']
            self.new_wf.add_df(tt + '_1st', df, to_freq='bday')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[tt][1], ann=True, ann_type='aris')
            df.columns = [t + '_2nd']
            self.new_wf.add_df(tt + '_2nd', df, to_freq='bday')
            self.new_wf.add_df(tt + '_Impulse', df, to_freq='bday')

    @use_cached_in_class([t + '_flip_2nd' for t in ['Oil_Price_WTI']])
    def run_oil_flip(self):
        for t in ['Oil_Price_WTI']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            df = -self.SU.smooth_change(df, periods=self.trans_param_df['Oil_Price_WTI_flip'][0], ann=True)
            df.columns = [t + '_flip_1st']
            self.new_wf.add_df(t + '_flip_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df['Oil_Price_WTI_flip'][1], ann=True,
                                       ann_type='aris') * 100
            df.columns = [t + '_flip_2nd']
            self.new_wf.add_df(t + '_flip_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_flip_Impulse', df, to_freq='bday')

    @use_cached_in_class([t + '_2nd' for t in ['House_Builder_Bloomberg_EPS_Estimate', 'EN_BB_eps']])
    def run_eps(self):
        for t in ['House_Builder_Bloomberg_EPS_Estimate', 'EN_BB_eps']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][0], ann_type='aris', ann=True)
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann=True, ann_type='aris')
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['Mortgage_Rate_30Y']])
    def run_mort_rate_irs(self):
        for t in ['Mortgage_Rate_30Y']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            df = -self.SU.smooth_change(df, periods=self.trans_param_df[t][0], ann=True, ann_type='aris')
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann=True, ann_type='aris')
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['FX_NEER']])
    def run_fx(self):
        for t in ['FX_NEER']:
            df = self.SU.conversion_to_bDay(self.raw_data_new_fmt[t])
            df = -self.SU.smooth_change(df, periods=self.trans_param_df[t][0], ann=True)
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann=True, ann_type='aris')
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class(['salary_2nd'])
    def run_salary(self):
        # the total salary = (hours worked * empl level * hours earning)
        df1 = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Hourly_Earnings'])
        df2 = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Empl_Level'])
        df3 = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Hours_Worked_Weekly'])
        df = pd.concat([df1, df2, df3], axis=1).dropna()
        df['salary'] = df.iloc[:, 0] * df.iloc[:, 1] * df.iloc[:, 2]
        t = 'salary'
        df_s = self.SU.smooth_change(df[['salary']], periods=self.trans_param_df[t][1], ann=True) * 100
        df_s.columns = ['salary_1st']
        self.new_wf.add_df('salary_1st', df_s, to_freq='no_change')

        df_s = self.SU.smooth_change(df_s, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
        df_s.columns = ['salary_2nd']
        self.new_wf.add_df('salary_2nd', df_s, to_freq='no_change')
        self.new_wf.add_df('salary_Impulse', df_s, to_freq='no_change')

    @use_cached_in_class(['saving_rate_2nd'])
    def run_saving_rate(self):
        df = -self.SU.conversion_down_to_m(self.raw_data_new_fmt['saving_rate'])
        t = 'saving_rate'
        if self.trans_param_df[t][0] > 1.01:
            df = df.ewm(span=self.trans_param_df[t][0] / 2).mean()
        df.columns = [t + '_1st']
        self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

        df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris')
        df.columns = [t + '_2nd']
        self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
        self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class(['mortgage_orig_2nd'])
    def run_mortgage_orig(self):
        # excluding refinancing
        df = self.SU.conversion_to_q(self.raw_data_new_fmt['mortgage_orig'])
        df = self.SU.sea_adj(df)
        df = df.dropna().pct_change(4) * 2
        t = 'mortgage_orig'
        df.columns = ['mortgage_orig_1st']
        self.new_wf.add_df('mortgage_orig_1st', df, to_freq='no_change')

        df = df.diff(2) * 2
        df.columns = ['mortgage_orig_2nd']
        self.new_wf.add_df('mortgage_orig_2nd', df, to_freq='no_change')
        self.new_wf.add_df('mortgage_orig_Impulse', df, to_freq='no_change')

    @use_cached_in_class(['Housing_supply_to_sales_ratio_2nd'])
    def run_housing_supply_month(self):
        df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Housing_supply_to_sales_ratio'])
        df = -self.SU.smooth_change(df, periods=self.trans_param_df['Housing_supply_to_sales_ratio'][0], ann=True,
                                    ann_type='aris')
        df.columns = ['Housing_supply_to_sales_ratio_1st']
        self.new_wf.add_df('Housing_supply_to_sales_ratio_1st', df, to_freq='no_change')

        df = self.SU.smooth_change(df, periods=self.trans_param_df['Housing_supply_to_sales_ratio'][1], ann=True,
                                   ann_type='aris')
        df.columns = ['Housing_supply_to_sales_ratio_2nd']
        self.new_wf.add_df('Housing_supply_to_sales_ratio_2nd', df, to_freq='no_change')
        self.new_wf.add_df('Housing_supply_to_sales_ratio_Impulse', df, to_freq='no_change')

    @use_cached_in_class(
        [t + '_2nd' for t in ['Housing_Prices', 'Housing_Permit', 'Retail_Sales_ex_Auto_Fuel', 'Capex_tracker_JPM']])
    def run_HP_Permit_retail_cumining_jpmtracker(self):
        for t in ['Housing_Prices', 'Housing_Permit', 'Retail_Sales_ex_Auto_Fuel', 'Capex_tracker_JPM']:
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][0], ann=True) * 100
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['PPI_Manufacturing']])
    def run_PPI(self):
        for t in ['PPI_Manufacturing']:
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = self.SU.sea_adj(df)
            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][0], ann=True) * 100
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['GS_CAI', 'Trading_Partner_Growth', 'Capacity_Utilisation_Mining']])
    def run_trading_partn_grow_cai_Conf(self):
        for t in ['GS_CAI', 'Trading_Partner_Growth', 'Capacity_Utilisation_Mining']:
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['Leading_Credit_Index']])
    def run_lead_credit_index(self):
        for t in ['Leading_Credit_Index']:
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = -df.dropna().rolling(self.trans_param_df[t][0]).mean()
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in
                          ['Capex_Plan_Survey_Composite', 'Capex_Plan_Survey_FEDTexas', 'Capex_Plan_Survey_FEDKansas',
                           'Capex_Plan_Survey_FEDRichmond', 'Capex_Plan_Survey_FEDPhilly', 'Capex_Plan_Survey_FEDNY']])
    def run_capexsurvey(self):
        # combine the manu and non-manu
        self.raw_data_new_fmt['Capex_Plan_Survey_SERV_FEDNY'] = self.SU.sea_adj(
            self.SU.conversion_down_to_m(self.raw_data_new_fmt['Capex_Plan_Survey_SERV_FEDNY']))
        for fed in ['FEDTexas', 'FEDKansas', 'FEDRichmond', 'FEDNY']:
            manu, serv = 'Capex_Plan_Survey_MANU_' + fed, 'Capex_Plan_Survey_SERV_' + fed
            df_manu, df_serv = self.SU.conversion_down_to_m(self.raw_data_new_fmt[manu]), self.SU.conversion_down_to_m(
                self.raw_data_new_fmt[serv].loc['1988':])
            comb = pd.merge(df_manu, df_serv, left_index=True, right_index=True, how='left')
            comb.iloc[:, 1].fillna(comb.iloc[:, 0], inplace=True)
            comb['Capex_Plan_Survey_' + fed] = (comb.iloc[:, 0] + comb.iloc[:, 1]) / 2
            # comb['Capex_Plan_Survey_' + fed] = (comb.iloc[:, 0]*1 + comb.iloc[:, 1]*0)
            self.raw_data_new_fmt['Capex_Plan_Survey_' + fed] = comb[['Capex_Plan_Survey_' + fed]]

        for fed in ['FEDTexas', 'FEDKansas', 'FEDRichmond', 'FEDPhilly', 'FEDNY']:
            t = 'Capex_Plan_Survey_' + fed
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

        # calc the composite index (only the first and the second for charting purposes, the hf_impulse will be calc automatically in the tree)
        df_list = []
        for fed in ['FEDTexas', 'FEDKansas', 'FEDRichmond', 'FEDPhilly', 'FEDNY']:
            t = 'Capex_Plan_Survey_' + fed
            df_list.append(self.SU.conversion_down_to_m(self.raw_data_new_fmt[t]))

        df_comb = pd.concat(df_list, axis=1)
        df_comb['Capex_Plan_Survey_Composite'] = df_comb.mean(axis=1)
        t = 'Capex_Plan_Survey_Composite'
        df = df_comb[['Capex_Plan_Survey_Composite']]
        df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
        df.columns = [t + '_1st']
        self.new_wf.add_df(t + '_1st', df, to_freq='no_change')
        df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
        df.columns = [t + '_2nd']
        self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')

    @use_cached_in_class([t + '_2nd' for t in ['Expected_New_Order_Survey_Composite']])
    def run_new_order_survey(self):
        # expected new orders over the next 6 months
        for fed in ['FEDTexas', 'FEDKansas', 'FEDRichmond', 'FEDPhilly', 'FEDNY']:
            t = 'Expected_New_Order_Survey_MANU_' + fed
            df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
            df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
            df.columns = [t + '_1st']
            self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

            df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
            df.columns = [t + '_2nd']
            self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
            self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

        # calc the composite index (only the first and the second for charting purposes, the hf_impulse will be calc automatically in the tree)
        df_list = []
        for fed in ['FEDTexas', 'FEDKansas', 'FEDRichmond', 'FEDPhilly', 'FEDNY']:
            t = 'Expected_New_Order_Survey_MANU_' + fed
            df_list.append(self.SU.conversion_down_to_m(self.raw_data_new_fmt[t]))

        df_comb = pd.concat(df_list, axis=1)
        df_comb['Expected_New_Order_Survey_Composite'] = df_comb.mean(axis=1)
        t = 'Expected_New_Order_Survey_Composite'

    df = df_comb[[t]]
    df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
    df.columns = [t + '_1st']
    self.new_wf.add_df(t + '_1st', df, to_freq='no_change')
    df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
    df.columns = [t + '_2nd']
    self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')


@use_cached_in_class([t + '_2nd' for t in ['Consumer_Confidence']])
def run_con_conf_survey(self):
    df = self.raw_data_new_fmt['Consumer_Confidence_UMICH'].loc['1980':]
    self.raw_data_new_fmt['Consumer_Confidence_UMICH'] = self.SU.sea_adj(df)
    for s in ['CRB', 'UMICH']:
        t = 'Consumer_Confidence_' + s
        df = self.SU.conversion_down_to_m(self.raw_data_new_fmt[t])
        df = self.SU.delete_zero_tail(df, delete_repeat_value=True)
        df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
        df.columns = [t + '_1st']
        self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

        df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
        df.columns = [t + '_2nd']
        self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
        self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')

        # calc the composite index (only the first and the second for charting purposes, the hf_impulse will be calc automatically in the tree)
    df_list = []
    for s in ['CRB', 'UMICH']:
        t = 'Consumer_Confidence_' + s
        df_list.append(self.SU.conversion_down_to_m(self.raw_data_new_fmt[t]))

    df_comb = pd.concat(df_list, axis=1)
    df_comb['Consumer_Confidence'] = df_comb.mean(axis=1)
    t = 'Consumer_Confidence'
    df = df_comb[[t]]
    df = df.dropna().rolling(self.trans_param_df[t][0]).mean()
    df.columns = [t + '_1st']
    self.new_wf.add_df(t + '_1st', df, to_freq='no_change')
    df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris', ann=True)
    df.columns = [t + '_2nd']
    self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')


@use_cached_in_class(['Corporate_BBB_Yield_5Y_2nd'])
def run_corporate_spread(self):
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Corporate_BBB_Yield_5Y'])
    df = -self.SU.smooth_change(df, periods=6, ann=True, ann_type='aris')
    df.columns = ['Corporate_BBB_Yield_5Y_1st']
    self.new_wf.add_df('Corporate_BBB_Yield_5Y_1st', df, to_freq='no_change')

    df = self.SU.smooth_change(df, periods=6, ann_type='aris', ann=True)
    df.columns = ['Corporate_BBB_Yield_5Y_2nd']
    self.new_wf.add_df('Corporate_BBB_Yield_5Y_2nd', df, to_freq='no_change')
    self.new_wf.add_df('Corporate_BBB_Yield_5Y' + '_Impulse', df, to_freq='no_change')


@use_cached_in_class([t + '_2nd' for t in ['Total_Rig_Count']])
def run_rigcount(self):
    for t in ['Total_Rig_Count']:
        df = self.SU.smooth_change(self.raw_data_new_fmt[t], periods=self.trans_param_df[t][0])
        df.index = [self.SU.get_previous_Friday(d) for d in df.index]
        df.columns = [t + '1st']
        self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

        df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris')
        df.columns = [t + '_2nd']
        self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
        self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')


@use_cached_in_class([t + '_2nd' for t in ['Mortgage_Application_Total_Volume_exRefinancing']])
def run_mba_app(self):
    for t in ['Mortgage_Application_Total_Volume_exRefinancing']:
        df = self.SU.smooth_change(self.raw_data_new_fmt[t].dropna().rolling(window=self.trans_param_df[t][2]).mean(),
                                   periods=self.trans_param_df[t][0])
        df = df.rolling(window=4).mean()
        df.index = [self.SU.get_previous_Friday(d) for d in df.index]
        df.columns = [t + '1st']
        self.new_wf.add_df(t + '_1st', df, to_freq='no_change')

        df = self.SU.smooth_change(df, periods=self.trans_param_df[t][1], ann_type='aris')
        df.columns = [t + '_2nd']
        self.new_wf.add_df(t + '_2nd', df, to_freq='no_change')
        self.new_wf.add_df(t + '_Impulse', df, to_freq='no_change')


def run(*args, **kwargs):
    sig = signal3()
    data = sig.get_data_dict(*args, **kwargs)
    print(data)


if __name__ == "__main__":
    clear_cache('RATES_FWDGROW_USA', 'disk_8h')
    reporting_to = sys.argv[1] if len(sys.argv) > 1.01 else None
    data = run(run_chart=True, running_mode='production', reporting_to=reporting_to)
    print(data)

