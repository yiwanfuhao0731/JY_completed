# import sys when runing from the batch code

'''jargon list

indiviual df:



signal_group_flip: original signal group (flip)

risk_budget: risk budget

vol_constant_factor: inverse of the volatility of the diff of assets

trading_instrument_duration: duration of the trading instrument



total_scale_factor: volFactor * fundVolfactor



estimate the vol with analytical method:

implied_vol: extended implied vol

rolling_vol_long: long rolling volatility of the assets

rolling_vol_short: short rolling volatility of the assets

estimate_vol_asset: estimated volatility of the asset



total_vol_factor_post_vol_cap

signal_before_fund_vol_cap

signal_after_fund_vol_cap

DV01_before_fund_vol_cap

DV01_after_fund_vol_cap



portfolio wf:



un_re_scaled_portfolio_ret: the return of the portfolio after making everything the same species and apply risk budget, before lift up the vol once again

rollVol_un_rescale: rolling vol of portfolio before the 2nd lift up

unscale_portfolio_vol_trend: vol trend of rolling portfolio vol, before 2nd lift up

fundVolFactor_constant: inverse of unscale_portfolio_vol_trend



fundRB_ScaledRet: total fund return before vol capped

fundRB_ScaledCumProf: total cumprof before vol capped



estimated_1d_vol: estimated 1 day volatility

fundRB_Scaled_rollVol_3m: un-capped portfolio 3m rolling volatility

'''

import sys

import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))

import pandas as pd

import numpy as np

from datetime import datetime, timedelta

import collections

import Analytics.series_utils as s_util

from Analytics.scoring_methods import scoring_method_collection as smc

import Analytics.wfcreate as wf

import backtesting_utils.chart as TCT

import backtesting_utils.post_signal_genr as post_signal_genr

import Analytics.finance as fin_lib

from Analytics.abstract_sig import abstract_sig_genr_portfolio as abs_sig

from Analytics.wfcreate import portfolio_tree

import matplotlib.pyplot as plt

import Analytics.loess_filter as lf

import itertools


class signal3(abs_sig):

    def __init__(self):

        super(signal3, self).__init__()

    def add_strat_info(self):

        # Global Variables

        self.basket_ID = 'basket_0018'

        self.Short_Name = 'RATES_portfolio_G4b'

        self.Description = '''

                      An evolution on the previous G4 portfolio: all using in-house growth estimates

                      portfolio that combines all the G4 rate models equally weighted among : AUS CAN GBR USA

                      '''

    def initialise_and_run(self, run_charting=True, **kwargs):

        self.add_strat_info()

        self.add_dir_info()

        self.sample_period()

        self.new_wf = wf.initialise_wf(self.TEMP_LOCAL_PICKLE)

        self.new_wf.create_folder(self.BT_BACKUP_ROOT_DIR)

        self.wf_container = collections.OrderedDict()

        self.raw_data_new_fmt = self.download_data_3(self.MASTER_INPUT_DIR, self.Short_Name)

        self.import_parse_param(self.MASTER_INPUT_DIR, self.Short_Name)

        self.run_step1(**kwargs)

        self.run_charting()

    def run_step1(self, **kwargs):

        self.re_run_time_thres = 2500000  # time threshold to rerun the whole thing

        self.re_run_if_necessary(self.re_run_time_thres, **kwargs)

        # self.run_rb_main(target_vol=4)

        self.extract_avg_yield()

        self.run_rb_constant_main(target_vol=0.04, vol_cap_max_n=2)

        if 1 == 1:
            self.extract_indicator_group()

            self.extract_average_flip_signal()

            # self.extract_average_condition_m_pricing_z()

            self.post_indicator_genr()

            self.wf_container['Portfolio'] = self.new_wf

            self.tree = portfolio_tree()

            COMPONENT_NODE_DICT = self.tree.read_node_info_from_excel(import_dir=self.MASTER_INPUT_DIR,

                                                                      sheet_name=self.Short_Name)

            self.tree.run_build_tree(COMPONENT_NODE_DICT, self.wf_container)

            root = self.tree.nodes['Portfolio']

            self.tree.print_structure(root, export_to_csv_dir=self.EXPORT_TREE_STRUCT_DIR)

            self.dump_all_dfs_param()

    def run_charting(self):

        # create chart for 2000

        root = self.tree.nodes['Portfolio']

        self.rates_rise_fall_path = os.path.join(self.PROJ_DIR, r"basket\USA_Rates1\usaratesfalling_rising_period.xlsx")

        self.series_name_dict = {'drawdown': 'drawdown'

                                 }

        self.alpha_name_dict = {'ann_mean': 'ann_mean',

                                'ann_std': 'ann_std',

                                'ann_sharpe': 'ann_sharpe',

                                'calmar': 'calmar', }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(root, self.series_name_dict,

                                                                                      self.alpha_name_dict)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2000-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL2000_DIR, pdfpath=self.BT_BACKUP_DIR2)

        # plot with risk budgeting breakdown since 2000

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(root, self.series_name_dict,

                                                                                      self.alpha_name_dict,
                                                                                      plot_risk_budgeting_break_down=True)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='2000-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL2000_WITH_RB_BREAKDOWN_DIR,
                                      pdfpath=self.BT_BACKUP_WITH_RB_BREAKDOWN_DIR2)

        # plot pnl since 2010

        self.series_name_dict.update({'drawdown': 'drawdown_2010'})

        self.alpha_name_dict = {'ann_mean': 'ann_mean_2010',

                                'ann_std': 'ann_std_2010',

                                'ann_sharpe': 'ann_sharpe_2010',

                                'calmar': 'calmar_2010',

                                }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(root, self.series_name_dict,

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

                                }

        pnl_chart_pack_dict = self.tree.expand_tree_below_node_and_return_chart_paris(root, self.series_name_dict,

                                                                                      self.alpha_name_dict)

        TCT.rates_tree_component_plot(plot_dict=pnl_chart_pack_dict, chart_start_dt='1990-01-01',

                                      chart_end_dt='2030-01-01', rate_rise_fall=self.rates_rise_fall_path,

                                      bt_backup_dir=self.RPT_PNL1990_DIR, pdfpath=self.BT_BACKUP_DIR1990_2)

    def get_signal_after_vol_cap(self):

        sig_df_list = []

        for bn, iso in zip(self.ordered_basket, self.ordered_iso):
            df = self.wf_container[bn].df['signal_after_fund_vol_cap'].copy()

            df.columns = [df.columns[0] + '_' + iso]

            sig_df_list.append(df)

        return pd.concat(sig_df_list, axis=1)

    def post_indicator_genr(self, tc_reduction_mehod='inertia'):

        sci = self.sci_panel()

        signal_after_fund_vol_cap_group = self.get_signal_after_vol_cap()

        # signal_dict = post_signal_genr.apply_inertia_filtered(self.new_wf.df['signal_after_fund_vol_cap'], sci,country_order=self.ordered_iso, tc_reduction_method='inertia',inertia_torlerance=[50, 50, 50, 50])

        signal_dict = post_signal_genr.apply_inertia(signal_after_fund_vol_cap_group,

                                                     tc_reduction_method='inertia',

                                                     inertia_torlerance=[50, 50, 50, 50])

        self.new_wf.add_df('signal_group', signal_dict['signal_group'])

        self.new_wf.add_df('signal_group_flip', signal_dict['signal_group_flip'])

        self.new_wf.add_df('actual_trade_group', signal_dict['actual_trade_group'])

        # signal_dict['actual_trade_group'].to_csv('actual_trade_group.csv')

        # signal_dict['signal_group'].to_csv('signal_group.csv')

        # sci.to_csv('excess_ret_index.csv')

        # pd.concat([new_wf.df['signal_group'].dropna(),new_wf.df['actual_trade_group'].dropna()],axis=1).to_csv('inertia.csv')

        self.profit_sample = self.full_sample1990

        # profit_sample_2010 = ['2010-01-01', '2016-12-31']

        pnl = post_signal_genr.profit(self.new_wf.df['actual_trade_group'], sci, 1000, self.profit_sample)

        self.new_wf.df['actual_trade_group'].dropna()

        sci.dropna()

        self.new_wf.add_df('equity_curve', pnl)

        self.new_wf.add_df('cumprof', pnl[['cumprof']])

        self.new_wf.add_df('ret', pnl[['ret']])

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample,

                                                          benchmark=None)

        print(retstats_dict)

        self.new_wf.alpha['ann_mean_1990'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std_1990'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe_1990'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar_1990'] = retstats_dict['calmar']

        self.new_wf.add_df('drawdown_1990', retstats_dict['drawdown'])

        print(self.new_wf.df['drawdown_1990'])

        self.profit_sample = self.full_sample2000

        # profit_sample = ['2000-01-01', '2016-12-31']

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample,

                                                          benchmark=None)

        self.new_wf.alpha['ann_mean'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar'] = retstats_dict['calmar']

        self.new_wf.add_df('drawdown', retstats_dict['drawdown'])

        self.profit_sample_2010 = self.full_sample2010

        # profit_sample_2010 = ['2010-01-01', '2016-12-31']

        retstats_dict = post_signal_genr.returnstats_dict(self.new_wf.df['equity_curve'], 1000, self.profit_sample_2010,

                                                          benchmark=None)

        self.new_wf.alpha['ann_mean_2010'] = retstats_dict['ann_mean']

        self.new_wf.alpha['ann_std_2010'] = retstats_dict['ann_std']

        self.new_wf.alpha['ann_sharpe_2010'] = retstats_dict['ann_sharpe']

        self.new_wf.alpha['calmar_2010'] = retstats_dict['calmar']

        self.new_wf.add_df('drawdown_2010', retstats_dict['drawdown'])

        return

    def run_rb_constant_main(self, *args, **kwargs):

        self.run_rb_constant_fund_scale(*args, **kwargs)

        # now apply the vol cap by using analytical estimation of vol

        self.run_implied_vol_and_asset_group()

        # further risk capping

        # df_var_cov.dropna().tail()

        df_var_cov = self.run_estimate_var_cov_mat(self.new_wf.df['df_asset_group'], self.new_wf.df['df_iv_group'],

                                                   rolling_long=252, rolling_short=63)

        df_weight_matrix = self.run_get_weight_mat()

        self.run_estimate_1d_vol_with_weight(df_var_cov, df_weight_matrix)

        # pd.concat([self.new_wf.df['estimated_1d_vol'],

        #            self.new_wf.df['bulk_result'].loc[:, ['fundRB_ScaledRet']].dropna().rolling(

        #                window=63).std().shift(-63)*np.sqrt(252)], axis=1).dropna().plot(grid=True, figsize=(10, 5))

        # plt.show()

        self.vol_cap(*args, **kwargs)

        self.run_post_vol_cap_signals()

    # TODO: properly read in risk budget

    def read_in_risk_budget(self, *args, **kwargs):

        # construct risk budgets

        list_country = [basket for basket in self.ordered_basket]

        risk_budget_df = self.SU.empty_BDay_df()

        risk_budget_df.set_index('Date', inplace=True)

        risk_budget_df.index = pd.to_datetime(risk_budget_df.index)

        risk_budget_df = risk_budget_df.reindex(columns=[i + '_rb' for i in list_country])

        risk_budget_df.loc[:, :] = 1

        self.df_rb = risk_budget_df

        # adding to the wf of each individual strategy

        for basket_name in self.ordered_basket:
            rb = self.df_rb[[basket_name + '_rb']]

            rb.columns = ['risk_budget']

            self.wf_container[basket_name].add_df('risk_budget', rb)

    def vol_cap(self, *args, **kwargs):

        target_vol = kwargs.get('target_vol')

        max_n = kwargs.get('vol_cap_max_n')

        assert 'estimated_1d_vol' in self.new_wf.df.keys(), 'Sorry, estimated_1d_vol is not yet generated'

        df = self.new_wf.df['estimated_1d_vol'].copy().dropna()

        mask = (df.iloc[:, 0] > target_vol * max_n)

        df.loc[mask, :] = (target_vol * max_n) / df.loc[mask, :]

        df.loc[~mask, :] = 1

        df.plot()

        df.columns = ['fund_cap_max_vol_factor']

        self.new_wf.add_df('fund_cap_max_vol_factor', df)

        # df.plot(figsize=(10,5),grid=True)

        # plt.show()

        return

    def run_post_vol_cap_signals(self):

        # get the final signals

        for basket_name in self.ordered_basket:
            print(basket_name)

            _wf = self.wf_container[basket_name]

            df_original_signal = _wf.df['signal_group'].dropna()

            df_rb = _wf.df['risk_budget'].dropna()

            df_total_scale_factor = _wf.df['total_scale_factor'].dropna()

            df_duration = _wf.df['trading_instrument_duration'].dropna()

            df_vol_cap_factor = self.new_wf.df['fund_cap_max_vol_factor'].dropna()

            df_comb = pd.concat([df_original_signal, df_rb, df_total_scale_factor, df_duration, df_vol_cap_factor],
                                axis=1)

            df_comb['total_vol_factor_post_vol_cap'] = df_comb.iloc[:, 1] * df_comb.iloc[:, 2] / df_comb.iloc[:,
                                                                                                 3].shift(
                1) * df_comb.iloc[:, 4]

            df_comb['signal_before_fund_vol_cap'] = df_comb.iloc[:, 0] * df_comb.iloc[:, 1] * df_comb.iloc[:,
                                                                                              2] / df_comb.iloc[:,
                                                                                                   3].shift(1)

            df_comb['signal_after_fund_vol_cap'] = df_comb['signal_before_fund_vol_cap'] * df_comb.iloc[:, 4]

            df_comb['DV01_before_fund_vol_cap'] = df_comb['signal_before_fund_vol_cap'] * df_comb.iloc[:, 3].shift(
                1) / 10000

            df_comb['DV01_after_fund_vol_cap'] = df_comb['signal_after_fund_vol_cap'] * df_comb.iloc[:, 3].shift(
                1) / 10000

            _wf.add_df('total_vol_factor_post_vol_cap', df_comb[['total_vol_factor_post_vol_cap']])

            # df_comb[['total_vol_factor_post_vol_cap']].dropna().plot(figsize=(10, 5), grid=True, title=basket_name,color='red')

            # plt.show()

            _wf.add_df('signal_before_fund_vol_cap', df_comb[['signal_before_fund_vol_cap']])

            df = -df_comb[['signal_before_fund_vol_cap']]

            df.columns = ['signal (flip)']

            _wf.add_df('signal before vol cap (flip)', df)

            # df_comb[['signal_before_fund_vol_cap']].dropna().plot(figsize=(10, 5), grid=True,title=basket_name,color='green')

            # plt.show()

            _wf.add_df('signal_after_fund_vol_cap', df_comb[['signal_after_fund_vol_cap']])

            # df_comb[['signal_after_fund_vol_cap']].dropna().plot(figsize=(10,5),grid=True,title=basket_name,color='blue')

            # plt.show()

            _wf.add_df('DV01_before_fund_vol_cap', df_comb[['DV01_before_fund_vol_cap']])

            # df_comb[['DV01_before_fund_vol_cap']].dropna().plot(figsize=(10, 5), grid=True,title=basket_name,color='black')

            # plt.show()

            _wf.add_df('DV01_after_fund_vol_cap', df_comb[['DV01_after_fund_vol_cap']])

            # df_comb[['DV01_after_fund_vol_cap']].dropna().plot(figsize=(10, 5), grid=True,title=basket_name,color='orange')

            # plt.show()

    def risk_ratio_constant(self, asset_name='IRS_5Y'):

        # find a best constant factor that force the long-term volatility

        # of each basket to be the same. e.g. TRY is twice as volatile as EUR, thus EUR should has a factor of 2

        # use as long of common history as available

        '''

        : return: a constant factor for each asset.

        The assumption is that the beta between different assets should remain the same.

        '''

        for basket_name in self.ordered_basket:
            _wf = self.wf_container[basket_name]

            df_irs = _wf.df[asset_name].copy()

            df_irs_diff = df_irs.dropna().diff(1) / 100

            df_irs_rollVol = df_irs_diff.dropna().rolling(window=500).std() * np.sqrt(252)

            # apply loess filter

            y_ticker = df_irs_rollVol.columns.tolist()[0]

            Lo_Filter = lf.loess_filter(df_irs_rollVol, None, y_ticker, None, False, 0.75)

            df_trend = Lo_Filter.estimate()[['y_fitted']]

            df_trend.columns = ['irs_vol_trend']

            # fixed the trend since 2013 so that it is not "trending"

            df_trend.loc['2014':, :] = np.nan

            df_trend.fillna(method='ffill', inplace=True)

            _wf.add_df('irs_vol_trend', df_trend)

            # calc the vol factor

            df_trend['vol_constant_factor'] = np.nan

            df_trend = df_trend.reindex(index=_wf.df['signal_group_flip'].index)

            df_trend = df_trend.sort_index()

            df_trend['vol_constant_factor'] = 0.04 / df_trend.iloc[:, 0].shift(1)

            # this 0.04 can be hard coded since just want to make them the same species

            # fill back the vol_constant_factor back to the beginning of the signal

            first_signal_index = _wf.df['signal_group_flip'].dropna().index[0]

            df_trend.loc[df_trend.index >= first_signal_index, 'vol_constant_factor'].fillna(method='bfill',

                                                                                             inplace=True)

            _wf.add_df('vol_constant_factor', df_trend[['vol_constant_factor']])

    def run_rb_constant_fund_scale(self, *args, **kwargs):

        self.read_in_risk_budget()

        self.risk_ratio_constant()

        target_vol = kwargs.get('target_vol')

        # find the fund vol factor, just looking at the realized volatility of the fund?

        df_list = [self.df_rb]

        # unscaled return

        for basket_name in self.ordered_basket:
            df_ret = self.wf_container[basket_name].df['pnl'].copy() / 1000

            df_ret.columns = [basket_name + '_ret']

            df_list.append(df_ret)

        for basket_name in self.ordered_basket:
            df_volFactor = self.wf_container[basket_name].df['vol_constant_factor'].copy()

            df_volFactor.columns = [basket_name + '_volFactor']

            df_list.append(df_volFactor)

        for basket_name in self.ordered_basket:
            df_instru_dura = self.wf_container[basket_name].df['trading_instrument_duration'].copy()

            df_instru_dura.columns = [basket_name + '_MD']  # modified duration

            df_list.append(df_instru_dura)

        df_matrix = pd.concat(df_list, axis=1)

        df_matrix = df_matrix.sort_index()

        # assert len(df_matrix.index) == len(set(df_matrix.index)), 'Sorry the length does not match for df_matrix'

        # adj the columns order

        ordered_col = [bn + '_ret' for bn in self.ordered_basket] + [bn + '_volFactor' for bn in

                                                                     self.ordered_basket] + [bn + '_MD' for bn in

                                                                                             self.ordered_basket] + [
                          bn + '_rb' for bn in self.ordered_basket]

        df_matrix = df_matrix[ordered_col]

        n_basket = len(self.ordered_basket)

        # get the loc of returns

        iloc_ret = [i for i in range(n_basket)]

        iloc_volFactor = [i + n_basket for i in range(n_basket)]

        iloc_MD = [i + 2 * n_basket for i in range(n_basket)]

        iloc_rb = [i + 3 * n_basket for i in range(n_basket)]

        # make sure the first day of the portfolio is the first common

        first_common_ret_index = df_matrix.iloc[:, iloc_ret].dropna().index[0]

        last_common_ret_index = df_matrix.iloc[:, iloc_ret].dropna().index[-1]

        # print(first_common_ret_index)

        # calculate the return of the un-re-scaled portfolio

        df_matrix['un_re_scaled_portfolio_ret'] = 0

        for ir, iv, i_md, i_rb in zip(iloc_ret, iloc_volFactor, iloc_MD, iloc_rb):
            df_matrix['un_re_scaled_portfolio_ret'] = df_matrix['un_re_scaled_portfolio_ret'] + df_matrix.iloc[:,
                                                                                                ir] * df_matrix.iloc[:,
                                                                                                      iv].shift(
                1) / df_matrix.iloc[:, i_md].shift(2) * df_matrix.iloc[:, i_rb].shift(1)

            # Note:  shift the modified duration by 2 days because it is the duration for T, observed in T+1, and effective on T+2 ret

        df_matrix.loc[df_matrix.index < first_common_ret_index, 'un_re_scaled_portfolio_ret'] = np.nan

        df_matrix.loc[df_matrix.index > last_common_ret_index, 'un_re_scaled_portfolio_ret'] = np.nan

        self.new_wf.add_df('un_re_scaled_portfolio_ret', df_matrix[['un_re_scaled_portfolio_ret']])

        df_matrix['rollVol_un_rescale'] = df_matrix.loc[:, 'un_re_scaled_portfolio_ret'].dropna().rolling(
            500).std() * np.sqrt(252)

        # backward fill to the begin of the portfolio

        df_matrix.loc[df_matrix.index >= first_common_ret_index, 'rollVol_un_rescale'].fillna(method='bfill',
                                                                                              inplace=True)

        self.new_wf.add_df('rollVol_un_rescale', df_matrix[['rollVol_un_rescale']])

        # apply loess filter

        y_ticker = df_matrix[['rollVol_un_rescale']].columns.tolist()[0]

        Lo_Filter = lf.loess_filter(df_matrix[['rollVol_un_rescale']], None, y_ticker, None, False, 0.75)

        df_trend = Lo_Filter.estimate()[['y_fitted']]

        df_trend.columns = ['vol_trend']

        df_matrix['unscale_portfolio_vol_trend'] = df_trend['vol_trend']

        # fix the value since 2014

        df_matrix.loc['2014':, 'unscale_portfolio_vol_trend'] = np.nan

        df_matrix.loc[:, 'unscale_portfolio_vol_trend'].fillna(method='ffill', inplace=True)

        self.new_wf.add_df('unscale_portfolio_vol_trend', df_matrix[['unscale_portfolio_vol_trend']])

        df_matrix['fundVolFactor_constant'] = target_vol / df_matrix['unscale_portfolio_vol_trend'].shift(1)

        # fill backward the fund constant factor

        df_matrix.loc[first_common_ret_index:, 'fundVolFactor_constant'].fillna(method='bfill', inplace=True)

        # get total scale factor

        for basket_name, iv in zip(self.ordered_basket, iloc_volFactor):
            df_matrix[basket_name + '_total_scale_factor'] = df_matrix.iloc[:, iv] * df_matrix['fundVolFactor_constant']

            df = df_matrix[[basket_name + '_total_scale_factor']]

            df.columns = ['total_scale_factor']

            self.wf_container[basket_name].add_df('total_scale_factor', df)

        # df_matrix.to_csv('df_matrix.csv')

        # calc the total return after rescaling at fund level;

        for basket_name in self.ordered_basket:
            total_scale_col = basket_name + '_total_scale_factor'

            ret_col = basket_name + '_ret'

            rb_col = basket_name + '_rb'

            md_col = basket_name + '_MD'

            # Example: assuming we have USD 1y and USD 30y, volFactor should be the same assuming paralel shift. We actually want volFactor/30 for

            # 30y swap since the price is way more volatile. That's why modified duration is "division"

            df_matrix[basket_name + '_RB_ScaledRet'] = df_matrix.loc[:, total_scale_col].shift(1) * df_matrix.loc[:,
                                                                                                    ret_col] * df_matrix.loc[
                                                                                                               :,
                                                                                                               rb_col].shift(
                1) / df_matrix.loc[:, md_col].shift(2)

        df_matrix['fundRB_ScaledRet'] = df_matrix.loc[:,
                                        [basket_name + '_RB_ScaledRet' for basket_name in self.ordered_basket]].sum(
            axis=1)

        df_matrix.loc[df_matrix.index < first_common_ret_index, 'fundRB_ScaledRet'] = np.nan

        df_matrix.loc[df_matrix.index > last_common_ret_index, 'fundRB_ScaledRet'] = np.nan

        self.new_wf.add_df('fundRB_ScaledRet', df_matrix[['fundRB_ScaledRet']])

        df_matrix['fundRB_ScaledCumProf'] = df_matrix['fundRB_ScaledRet'].cumsum()

        self.new_wf.add_df('fundRB_ScaledCumProf', df_matrix[['fundRB_ScaledCumProf']])

        df_matrix['fundRB_Scaled_rollVol_3m'] = df_matrix['fundRB_ScaledRet'].rolling(63).std() * np.sqrt(252)

        self.new_wf.add_df('fundRB_Scaled_rollVol_3m', df_matrix[['fundRB_Scaled_rollVol_3m']])

        self.new_wf.add_df('bulk_result', df_matrix)

    def run_get_weight_mat(self):

        # get weight matrix from the bulk result

        df_signal_group = pd.concat(

            [self.wf_container[basket_name].df['signal_group'] / 1000 for basket_name in self.ordered_basket], axis=1)

        self.new_wf.df['bulk_result'].to_csv('bulk_result2.csv')

        df_scaled_factor_group = self.new_wf.df['bulk_result'].loc[:,

                                 [basket_name + '_total_scale_factor' for basket_name in self.ordered_basket]]

        df_rb_group = self.new_wf.df['bulk_result'].loc[:, [basket_name + '_rb' for basket_name in self.ordered_basket]]

        df_signal_group.columns = [i for i in range(len(self.ordered_basket))]

        df_scaled_factor_group.columns = df_signal_group.columns

        df_rb_group.columns = df_signal_group.columns

        df_weight_mat = df_scaled_factor_group * df_signal_group * df_rb_group

        df_weight_mat.columns = [iso + '_RB_scaled_signal' for iso in self.ordered_iso]

        return df_weight_mat

    def run_estimate_var_cov_mat(self, df_asset_group, df_iv_group, rolling_long, rolling_short):

        '''

        :param df_asset_group: asset price dataframe

        :param df_iv: implied volatility dataframe. Data should be extended back (by realized volatility).

        Using this to estimate the diagonal element

        implied volatility and asset group should be in the same order

        :return: var_cov matrix

        '''

        # weekly correlation matrix

        df_corr_mat = df_asset_group.diff(5).ewm(span=1000).corr().dropna()

        # annualised volatility

        df_rollVol_long_group = (df_asset_group.diff(1) / 100).rolling(window=rolling_long).std() * np.sqrt(252)

        df_rollVol_short_group = (df_asset_group.diff(1) / 100).rolling(window=rolling_short).std() * np.sqrt(252)

        df_iv_group = df_iv_group / 10000

        df_vol_list = []

        for i in range(len(df_asset_group.columns)):
            df1, df2, df3 = df_rollVol_long_group.iloc[:, [i]], df_rollVol_short_group.iloc[:, [i]], df_iv_group.iloc[:,

                                                                                                     [i]]

            df_vol_estimate = pd.concat([df1, df2, df3], axis=1).mean(axis=1, skipna=True)

            df_vol_estimate = df_vol_estimate.to_frame()

            # df_vol_estimate=df1

            df_vol_estimate.columns = [df_asset_group.columns[i] + '_volBar']

            df_vol_list.append(df_vol_estimate)

            # df_vol_estimate.plot(figsize=(10,5),grid=True)

        df_vol_grp = pd.concat(df_vol_list, axis=1)

        # construct var-cov matrix with correlation matrix and estimated_vol

        df_vol_grp.fillna(method='bfill', inplace=True)

        for i, bn in enumerate(self.ordered_basket):
            self.wf_container[bn].add_df('rolling_vol_long', df_rollVol_long_group.iloc[:, [i]])

            self.wf_container[bn].add_df('rolling_vol_short', df_rollVol_short_group.iloc[:, [i]])

            self.wf_container[bn].add_df('estimate_vol_asset', df_vol_grp.iloc[:, [i]])

        # find common dates

        common_date = sorted(

            list(set(df_vol_grp.dropna().index.intersection(df_corr_mat.dropna().index.get_level_values(0)))))

        # array_vol_group, array_corr_mat = df_vol_grp.loc[common_date,:].values,df_corr_mat.loc[common_date,:].values

        # iterate through to get the matrix

        df_vol_grp = df_vol_grp.loc[common_date, :]

        df_corr_mat = df_corr_mat.loc[common_date, :]

        df_vol_grp_values = df_vol_grp.values

        df_corr_mat_values = df_corr_mat.values

        n = df_corr_mat_values.shape[1]

        df_cov_mat_values = df_corr_mat_values.copy()

        df_cov_mat_values[:, :] = np.nan

        # print (df_corr_mat_values.shape[1])

        for i in range(0, df_corr_mat_values.shape[0], df_corr_mat_values.shape[1]):

            vol_grp_index = int(i / df_corr_mat_values.shape[1])

            this_vol = df_vol_grp_values[vol_grp_index]

            this_corr = df_corr_mat_values[i:i + n, :]

            for (a, b) in itertools.product(list(range(len(this_vol))), repeat=2):
                this_corr[a, b] = this_corr[a, b] * this_vol[a] * this_vol[b]

            df_cov_mat_values[i:i + n, :] = this_corr

        df_var_cov = pd.DataFrame(index=df_corr_mat.index, columns=df_corr_mat.columns, data=df_cov_mat_values)

        # note : the variance - covariance matrix's index is not shifted yet

        return df_var_cov

    def run_estimate_1d_vol_with_weight(self, var_cov_mat, df_weight):

        '''

        :param var_cov_mat: using variance-covariance matrix

        :param df_weight: and the weight matrix to figure out the

        :return: return the estimated 1 day volatility, and whether it need to be capped

        '''

        # print(df_weight)

        df_weight = df_weight.shift(-1)  # shift back by 1 day. shift forward later on

        common_date = sorted(

            list(set(df_weight.dropna().index.intersection(var_cov_mat.dropna().index.get_level_values(0)))))

        df_weight, var_cov_mat = df_weight.loc[common_date, :], var_cov_mat.loc[common_date, :]

        df_1d_vol = df_weight.iloc[:, [0]]  # just to make a copy of the df_weight matrix

        df_1d_vol.columns = ['estimated_1d_vol']

        df_1d_vol.loc[:, :] = np.nan

        df_cov_array = var_cov_mat.values

        df_weight_array = df_weight.values

        n = var_cov_mat.shape[1]

        var_result_list = []

        for i in range(0, var_cov_mat.shape[0], n):
            weight_index = int(i / n)

            w = df_weight_array[weight_index, :]

            big_sigma = df_cov_array[i:i + n, :]

            assert w.shape[0] == big_sigma.shape[0], 'the shape of weight and sigma are not the same'

            var_result_list.append(np.sqrt(((w.transpose()).dot(big_sigma)).dot(w)))

        df_1d_vol.iloc[:, 0] = var_result_list

        self.new_wf.add_df('estimated_1d_vol', df_1d_vol)

        self.new_wf.df['estimated_1d_vol'] = self.new_wf.df['estimated_1d_vol'].shift(1)

        return

    def run_implied_vol_and_asset_group(self):

        # fetch the data of implied volaitlity

        df_iv_list = []

        for iso in self.ordered_iso:
            name = '{}_3m_5y_iv'.format(iso)

            df = self.SU.remove_outlier(self.SU.conversion_to_bDay(self.raw_data_new_fmt[name]), n=4)

            df_iv_list.append(df)

        df_iv_group = pd.concat(df_iv_list, axis=1)

        # extend the IV back with USA as benchmark

        # get underlying assets group

        df_asset_group = self.new_wf.df['df_asset_group'].copy()

        # calculate the relative ratio of long_term_vol

        df_diffAsset_group = df_asset_group.diff(1)

        # all align to USA vol

        assert 'USA' in df_diffAsset_group.columns[0].split(

            '_'), 'Sorry, by default the USA should be in the first column'

        in_sample_ratio_end_date = '2015-12-31'

        USA_vol_ratio_list = [1]

        for i in range(df_diffAsset_group.shape[1])[1:]:
            df_usa_and_other = df_diffAsset_group.iloc[:, [0, i]].loc[:, :in_sample_ratio_end_date].dropna()

            this_std = df_usa_and_other.std().values

            USA_vol_ratio_list.append(this_std[1] / this_std[0])

        # extend the implied volatility with the ratio list

        for i, v in enumerate(USA_vol_ratio_list):
            df_iv_group.iloc[:, i].fillna(value=df_iv_group.iloc[:, 0] * v, inplace=True)

        # df_iv_group.plot(grid=True, figsize=(10, 5))

        # plt.show()

        for i, bn in enumerate(self.ordered_basket):
            self.wf_container[bn].add_df('implied_vol', df_iv_group.iloc[:, [i]])

        self.new_wf.add_df('df_iv_group', df_iv_group)

    def extract_indicator_group(self):

        # TODO: adding risk budgeting parameters to the indicator group

        df = pd.DataFrame()

        for basket in self.ordered_basket:

            iso = self.basket_list_dict[basket][3]

            this_df = self.wf_container[basket].df['indicator_group'].copy()

            this_df.columns = [iso + '_indicator_group']

            if len(df.index) <= 0.1:

                df = this_df

            else:

                df = pd.merge(df, this_df, left_index=True, right_index=True, how='outer')

        self.new_wf.add_df('indicator_group', df)

        df.to_csv('G4 fair value signals.csv')

    def extract_avg_yield(self):

        df = pd.DataFrame()

        for basket in self.ordered_basket:

            iso = self.basket_list_dict[basket][3]

            this_df = self.wf_container[basket].df['IRS_5Y'].copy()

            this_df.columns = [iso + '_IRS_5Y']

            if len(df.index) <= 0.1:

                df = this_df

            else:

                df = pd.merge(df, this_df, left_index=True, right_index=True, how='outer')

        df = df.dropna()

        self.new_wf.add_df('df_asset_group', df)

        df_ave = df.mean(axis=1)

        df_ave = df_ave.to_frame()

        df_ave.columns = ['Avg_IRS_5Y']

        self.new_wf.add_df('Avg_IRS_5Y', df_ave)

    def extract_average_flip_signal(self):

        df = pd.DataFrame()

        for basket in self.ordered_basket:

            this_df = -self.wf_container[basket].df['indicator_group'].copy()

            if len(df.index) <= 0.1:

                df = this_df

            else:

                df = pd.merge(df, this_df, left_index=True, right_index=True, how='outer')

        df = df.dropna()

        df_ave = df.mean(axis=1)

        df_ave = df_ave.to_frame()

        df_ave.columns = ['avg condition-pricing']

        self.new_wf.add_df('Avg_(flipped)signal', df_ave)

    def extract_average_condition_m_pricing_z(self):

        df = pd.DataFrame()

        for basket in self.ordered_basket:

            this_df = -self.wf_container[basket].df['condition_m_pricing'].copy()

            if len(df.index) <= 0.1:

                df = this_df

            else:

                df = pd.merge(df, this_df, left_index=True, right_index=True, how='outer')

        df = df.dropna()

        df_ave = df.mean(axis=1)

        df_ave = df_ave.to_frame()

        df_ave.columns = ['avg condition-pricing']

        self.new_wf.add_df('Avg_condition_m_pricing_z', df_ave)

    @DeprecationWarning
    def risk_metric(self, basket_name, window_long=1260, window_short=630, target_vol=0.04):

        '''

        :param basket_name: the name of the basket, e.g. basket_USA_Rates1c

        :param window_long: long term window

        :param window_short: short term window

        :param target_vol: target vol

        :return: vol adjustment factor to bring the single strategy vol up to 4%

        '''

        # after sucking in all the data, get the vol_adj_factor

        # calculate the return of tri and volatility of tri using the windows parameter

        _wf = self.wf_container[basket_name]

        df_tri = _wf.df['TRI'].copy()

        df_tri_ret = df_tri.pct_change(1)

        df_pnl_ret = _wf.df['pnl'].copy() / 1000

        df_tri_vol_long = df_tri_ret.dropna().rolling(window=window_long, min_periods=window_long).std() * np.sqrt(252)

        df_tri_vol_short = df_tri_ret.dropna().rolling(window=window_short, min_periods=window_short).std() * np.sqrt(

            252)

        df_pnl_vol_long = df_pnl_ret.dropna().rolling(window=window_long).std() * np.sqrt(252)

        df_comb = pd.concat([df_tri_vol_long, df_pnl_vol_long], axis=1)

        df_comb.sort_index(inplace=True)

        df_comb['ratio_factor'] = df_comb.iloc[:, 1] / df_comb.iloc[:, 0]

        df_comb['ratio_factor'] = df_comb['ratio_factor'].rolling(window=window_long).mean()

        # fill ratio factor back until the first pnl - 1 day, as it is relatively stable

        first_pnl_index_l1 = df_pnl_ret.shift(-1).dropna().index[0]

        df_comb.loc[first_pnl_index_l1:, 'ratio_factor'].fillna(method='bfill', inplace=True)

        # assuming the ratio factor is relatively stable

        ratio_factor = df_comb.loc[:, ['ratio_factor']]

        self.wf_container[basket_name].df['ratio_factor'] = ratio_factor

        # estimate the volatility of tri using the 1 year and 6 months tri vol

        df_comb = pd.concat([df_tri_vol_long, df_tri_vol_short], axis=1)

        df_comb['vol_estimate'] = (df_comb.iloc[:, 0] + df_comb.iloc[:, 1]) / 2

        # only need the vol_estimate since the start of the pnl - 1 day

        df_comb.loc[df_comb.index < first_pnl_index_l1, 'vol_estimate'] = np.nan

        vol_estimate = df_comb.loc[:, ['vol_estimate']]

        self.wf_container[basket_name].df['vol_estimate'] = vol_estimate

        # total estimate = vol_estimate*ratio_factor

        df_comb = pd.concat([ratio_factor, vol_estimate], axis=1)

        df_comb.sort_index(inplace=True)

        df_comb['total_vol_estimate'] = df_comb.iloc[:, 0] * df_comb.iloc[:, 1]

        df_comb['vol_factor'] = target_vol / df_comb['total_vol_estimate'].shift(1)

        vol_factor = df_comb.loc[:, ['vol_factor']].dropna()

        self.wf_container[basket_name].df['vol_factor'] = vol_factor

    @DeprecationWarning
    def series_vol_adj(self, basket_name, window_long=1260, window_short=630):

        _wf = self.wf_container[basket_name]

        # vol adj returns

        vol_factor = _wf.df['vol_factor'].copy()

        #

        df_pnl_ret = _wf.df['pnl'].copy() / 1000

        df_comb = pd.concat([vol_factor, df_pnl_ret], axis=1)

        df_comb['vol_adj_ret'] = df_comb.iloc[:, 0].shift(1) * df_comb.iloc[:, 1]

        vol_adj_ret = df_comb.loc[:, ['vol_adj_ret']]

        self.wf_container[basket_name].df['vol_adj_ret'] = vol_adj_ret

        # the volatility of voltility adjusted return

        vol_vol_adj_ret_long = vol_adj_ret.rolling(window=window_long).std() * np.sqrt(252)

        vol_vol_adj_ret_short = vol_adj_ret.rolling(window=window_short).std() * np.sqrt(252)

        df_comb = pd.concat([vol_vol_adj_ret_long, vol_vol_adj_ret_short], axis=1)

        df_comb.sort_index(inplace=True)

        df_comb.fillna(method='bfill', inplace=True)

        df_comb['vol_vol_adj_ret'] = (df_comb.iloc[:, 0] + df_comb.iloc[:, 1]) / 2

        vol_vol_adj_ret = df_comb.loc[:, ['vol_vol_adj_ret']]

        vol_vol_adj_ret.reindex(_wf.df['indicator_group'].index)

        # vol_vol_adj_ret.fillna(method='ffill',inplace=True)

        # vol_vol_adj_ret.fillna(method='bfill', inplace=True)

        self.wf_container[basket_name].df['vol_vol_adj_ret'] = vol_vol_adj_ret

    @DeprecationWarning
    def run_rb_main(self, *args, window=1260, **kwargs):

        target_vol = kwargs.get('target_vol')

        self.read_in_risk_budget()

        for basket_name in self.ordered_basket:
            self.risk_metric(basket_name)

            self.series_vol_adj(basket_name)

        # find the fund vol factor

        # construct the matrix of individual returns, series vol_factor, df_rb

        df_list = [self.df_rb]

        for basket_name in self.ordered_basket:
            df_ret = self.wf_container[basket_name].df['pnl'].copy() / 1000

            df_ret.columns = [basket_name + '_ret']

            df_list.append(df_ret)

        for basket_name in self.ordered_basket:
            df_volFactor = self.wf_container[basket_name].df['vol_factor'].copy()

            df_volFactor.columns = [basket_name + '_volFactor']

            df_list.append(df_volFactor)

        df_matrix = pd.concat(df_list, axis=1)

        df_matrix = df_matrix.sort_index()

        assert len(df_matrix.index) == len(set(df_matrix.index)), 'Sorry the length does not match for df_matrix'

        # adj the columns order

        ordered_col = [bn + '_ret' for bn in self.ordered_basket] + [bn + '_volFactor' for bn in

                                                                     self.ordered_basket] + [

                          bn + '_rb' for bn in self.ordered_basket]

        df_matrix = df_matrix[ordered_col]

        n_basket = len(self.ordered_basket)

        # get the loc of returns

        # this is hard coded

        iloc_ret = [i for i in range(n_basket)]

        iloc_volFactor = [i + n_basket for i in range(n_basket)]

        iloc_rb = [i + 2 * n_basket for i in range(n_basket)]

        first_Obs = df_matrix.index.get_loc(df_matrix.iloc[:, iloc_ret].dropna().index[0]) - 1 + window

        last_Obs = df_matrix.index.get_loc(df_matrix.iloc[:, iloc_volFactor].dropna().index[-1])

        # fund vol factor

        df_matrix['fundVolFactor'] = np.nan

        iloc_fundVolFactor = len(iloc_ret + iloc_volFactor + iloc_rb)

        df_matrix['scaledRBRet'] = np.nan

        iloc_scaledRBRet = iloc_fundVolFactor + 1

        df_matrix['scaledRBProf'] = np.nan

        iloc_scaledRBProf = iloc_scaledRBRet + 1

        df_matrix_value = df_matrix.values

        for i in range(first_Obs, last_Obs + 1):

            is_start = i - window

            is_end = i

            staticRBRet = np.zeros(window)

            for j in range(len(iloc_ret)):
                this_rb = df_matrix_value[i, iloc_rb[j]]

                # this_volFactor = df_matrix_value[i, iloc_volFactor[j]]

                this_volFactor = df_matrix_value[is_start - 1:is_end - 1, iloc_volFactor[j]]

                this_ret = df_matrix_value[is_start:is_end, iloc_ret[j]]

                staticRBRet = staticRBRet + this_ret * this_volFactor * this_rb

            this_fund_scale_factor = target_vol / (np.std(staticRBRet) * np.sqrt(252) * 100)

            df_matrix_value[i, iloc_fundVolFactor] = this_fund_scale_factor

        # fill backward the fund vol factor, as there are short history

        df_matrix_new = pd.DataFrame(index=df_matrix.index, columns=df_matrix.columns, data=df_matrix_value)

        first_pnl_index_l1 = df_matrix.iloc[:, iloc_ret].shift(-1).dropna().index[0]

        df_matrix_new.loc[first_pnl_index_l1:, :].iloc[:, iloc_fundVolFactor].fillna(method='bfill', inplace=True)

        df_matrix_value = df_matrix_new.values

        # calculate the return after scale

        # ret_start = first_Obs + 1

        # the new ret_start:

        ret_start = df_matrix.index.get_loc(df_matrix_new.iloc[:, iloc_fundVolFactor].dropna().index[0]) + 1

        ret_end = last_Obs

        scaledRBRet = np.zeros(ret_end - ret_start)

        print('scaledRBRet shape is : ', scaledRBRet.shape)

        for k in range(len(iloc_ret)):
            this_ret = df_matrix_value[ret_start:ret_end, iloc_ret[k]]

            # print ('first and last 5 of return : ',this_ret[:5],this_ret[-5:])

            this_volFactor = df_matrix_value[ret_start - 1:ret_end - 1, iloc_volFactor[k]]

            # print ('first and last 5 of vol factor : ',this_volFactor[:5],this_volFactor[-5:])

            this_rb = df_matrix_value[ret_start - 1:ret_end - 1, iloc_rb[k]]

            # print('first and last 5 of vol factor : ', this_volFactor[:5], this_volFactor[-5:])

            this_fund_scale_factor = df_matrix_value[ret_start - 1:ret_end - 1, iloc_fundVolFactor]

            # print ('first and last 5 of fund scale factor : ',this_fund_scale_factor[:5],this_fund_scale_factor[-5:])

            scaledRBRet = scaledRBRet + this_ret * this_volFactor * this_rb * this_fund_scale_factor

        df_matrix_value[ret_start:ret_end, iloc_scaledRBRet] = scaledRBRet

        # calculate the cum return

        df_matrix_new = pd.DataFrame(index=df_matrix.index, columns=df_matrix.columns, data=df_matrix_value)

        # iloc_fund_scale_factor = len(df_dummy_new.columns)-1

        # calc the actual return after scaling

        df_matrix_new.loc['1990':'2020', :].to_csv('df_matrix_new.csv')

        # to csv for checking

        df_list = []

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['TRI']

            df.columns = [basket_name + '_excess_ret_index']

            df_list.append(df)

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['vol_estimate']

            df.columns = [basket_name + '_vol_estimate']

            df_list.append(df)

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['vol_factor']

            df.columns = [basket_name + '_vol_factor']

            df_list.append(df)

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['ratio_factor']

            df.columns = [basket_name + '_ratio_factor']

            df_list.append(df)

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['signal_group_flip']

            df.columns = [basket_name + '_signal_group_flip']

            df_list.append(df)

        for basket_name in self.ordered_basket:
            df = self.wf_container[basket_name].df['pnl'] / 1000

            df.columns = [basket_name + '_pnl']

            df_list.append(df)

        pd.concat(df_list, axis=1).loc['1990':'2020', :].to_csv('df_matrix2.csv')

        # scaling factor to align combined baskets to target fund vol.

        # Note: keep in mind that if only 1 strategy is in place,

        # this step is unnecessary

        # this implicitly takes correlations into account as the basket-level

        # scaling does not make sure that the fund ol target is reached


a = signal3()

a.initialise_and_run(running_mode='production')