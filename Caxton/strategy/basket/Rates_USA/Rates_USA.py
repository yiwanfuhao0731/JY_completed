# refactoring: refactoring the strategy to a tree structure
# import sys when runing from the batch code
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import Analytics.loess_filter as lf
import Analytics.series_utils as s_util
from Analytics.scoring_methods import scoring_method_collection as smc
import Analytics.finance as fin_lib
import backtesting_utils.chart as TCT
import backtesting_utils.post_signal_genr as post_signal_genr

import Analytics.wfcreate as wf
from Analytics.abstract_sig import abstract_sig_genr_for_rates_tree as abs_sig
from panormus.utils.cache import cache_response, clear_cache
from signals.RATES_LVL_GROWmPOT.run_signal import get_data as RATES_LVL_GROWmPOT_get
from signals.RATES_LVL_INFLATmTARGET.run_signal import get_data as RATES_LVL_INFLATmTARGET_get
from signals.RATES_FWDGROW_USA.run_signal_b import signal3 as RATES_FWDGROW_USA_6m_sig
from signals.FCI_zto_CAI_AGG.run_signal import get_data as FCI_zto_CAI_AGG_get
from SCI.SCI_Master import signal3 as SCI_Master_sig

from Analytics.wfcreate import rates_model_tree


class signal3(abs_sig):
    def __init__(self):
        super(signal3, self).__init__()

    def add_strat_info(self):
        # this is the part to override
        self.basket_ID = 'basket_0017'
        self.Short_Name = 'basket_USA_Rates1c'
        self.Description = '''
              Variation of USA rates model :  using in-house forward growth estimates
              Backtesting infrastructure which enables to prototype the USA strategy quickly
              The inputs are: level, change, forward of the economic fundamentals.

              Level: economic slack; growth vs trend; unemployment vs trend; wage vs trend; capacity utilization vs trend; inflation vs target
              Change: change in wage growth; change in growth rate
              Forward growth: FCI impulse; Case Shiller housing price second order diff
              Forward CPI: Brent oil impulse; Effective FX impulse.  
              Global growth: global ex-USA CAI and FCI
              '''
        self.exec_path = __file__

    @cache_response('basket_USA_Rates1c', 'disk_8h', skip_first_arg=True)
    def get_data_dict(self, run_charting=True, **kwargs):
        # for risk1 in [1,2,3,4,5,8,10,13,16,20,25,30,35]:
        #     for risk2 in [i/2 for i in range(6)]:
        self.add_strat_info()
        self.add_dir_info(**kwargs)
        self.new_wf = wf.initialise_wf()
        # in this case, both Econ and csv file data are used
        self.pre_run_result = self.pre_run(**kwargs)
        self.raw_data_new_fmt = self.load_data_wrapper(self.MASTER_INPUT_DIR, self.Short_Name,
                                                       self.PARAM_DIR_DATATICKER_BACKUP_TXT)
        self.import_parse_param(self.MASTER_INPUT_DIR, self.Short_Name)
        # self.trans_param_df['risk_rule_1'] = [int(risk1)]
        # self.trans_param_df['risk_rule_2'] = [float(risk2)]
        self.create_folder(self.INDICATOR_EXP_DIR)
        self.out_folder = self.create_tearsheet_folder(self.RPTDIR)
        self.new_wf.create_folder(self.BT_BACKUP_ROOT_DIR)
        self.rates_rise_fall_path = os.path.join(self.PROJ_DIR, r"basket\USA_Rates1\usaratesfalling_rising_period.xlsx")
        self.tree = rates_model_tree()
        self.customed_titles = {}
        self.SM = smc()
        self.SU = s_util.date_utils_collection()
        self.run_step1()
        self.run_step2(**kwargs)
        result_dict = self.run_step3(run_charting=run_charting, **kwargs)
        return result_dict

    def pre_run(self, **kwargs):
        result_dict = {}
        result_dict.update(RATES_LVL_GROWmPOT_get(run_chart=False, **kwargs))
        result_dict.update(RATES_LVL_INFLATmTARGET_get(run_chart=False, **kwargs))
        RATES_FWDGROW_USA_6m_sig1 = RATES_FWDGROW_USA_6m_sig()
        result_dict.update(RATES_FWDGROW_USA_6m_sig1.get_data_dict(run_chart=False, **kwargs))
        result_dict.update(FCI_zto_CAI_AGG_get(run_chart=False, **kwargs))

        SCI_Master1 = SCI_Master_sig()
        result_dict.update(SCI_Master1.get_data_dict(**kwargs))

        return result_dict

    def run_step1(self):
        self.run_from_scratch = True
        self.run_level_Growth_Slack()
        self.run_level_Growth_vs_Pot()
        self.run_level_URate_trend()
        self.run_level_CapU_trend()
        self.run_level_wages_trend()
        self.run_level_wages_atlant_trend()
        self.run_level_wages_comp_hour()
        self.run_level_wages_bls_median()
        self.run_level_wages_eci()
        self.run_level_inf_targ()
        self.run_level_inf_targ_GS_tracker()
        self.run_level_bei5_targ()
        self.run_change_in_growth()
        self.run_change_in_growth_citi_econ()
        self.run_chg_on_wage_growth()
        self.run_chg_on_wage_atl()
        self.run_chg_on_wage_eci()
        self.run_chg_on_wage_bls_median()
        self.run_chg_on_comp_hour()
        self.run_chg_in_cpi()
        self.run_chg_in_cpi_pce()
        self.run_surprise_in_cpi()
        self.run_chg_in_GS_cpi_tracker()
        self.run_chg_in_cpi_bei5()
        self.run_HP_impulse()
        self.run_total_cai_impulse()
        self.run_oil_impulse()
        self.run_fx_impulse()
        self.run_pricing()
        self.run_pricing_2y()
        self.run_pricing_2y_swap()
        self.run_glob_level_growth()
        self.run_glob_FCI()
        self.run_glob_chg_growth()

        self.pickle_result(self.TEMP_LOCAL_PICKLE, self.new_wf)

    def run_step2(self, **kwargs):
        COMPONENT_NODE_DICT = self.tree.read_node_info_from_excel(import_dir=self.MASTER_INPUT_DIR,
                                                                  sheet_name=self.Short_Name, **kwargs)
        self.tree.run_build_tree(COMPONENT_NODE_DICT, self.new_wf.df)
        root = self.tree.nodes['condition_m_pricing']
        self.tree.print_structure(root, export_to_csv_dir=self.EXPORT_TREE_STRUCT_DIR)
        self.tree.sum_up_Z_on_each_branch(root, self.new_wf, flagging_dir=self.FLAG_FILE, curr_exec_path=self.exec_path,
                                          **kwargs)
        self.run_indicator_and_post_sig_genr(tri_file='DATA_RATES_SCI_5Y_SWAP/USA_5y_TRI', tri_name='USA_5y_TRI',
                                             rate_name='IRS_5Y', tenor=5)
        # self.run_indicator_and_post_sig_genr(tri_file='Bond Total Return Index.csv', tri_name='USA_Bond_excess_ret')

    def run_level_Growth_Slack(self):
        # should return things: 1: signal 2: name of the chart 3: tuple of all the data in the chart
        df = self.raw_data_new_fmt['RGDP'].dropna()
        df = self.SU.conversion_to_q(df)
        self.new_wf.add_df('RGDP(raw index)', df, to_freq='Q')

        y_ticker = df.columns.tolist()[0]
        Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False, self.trans_param_df['pot_gdp_loess_frac'][0])
        df_trend = Lo_Filter.estimate()[['y_fitted']]

        df_trend.columns = ['USA_potential_GDP']
        self.new_wf.add_df('RGDP(index trend line)', df_trend, to_freq='Q')

        df_gap = self.SU.minus_df1_df2(self.new_wf.df['RGDP(raw index)'], self.new_wf.df['RGDP(index trend line)'])
        df_gap = self.SU.divide_df1_df2(df_gap, self.new_wf.df['RGDP(index trend line)'])
        df_gap = self.SU.conversion_down_to_m(df_gap)  # be prepared to be extended with CAI

        # extend the gap with GDP Nowcast - potential
        self.new_wf.add_df('GDP Nowcast', self.raw_data_new_fmt['RGDP_EXT_yoy'], to_freq='M')
        self.new_wf.add_df('potential_yoy', self.raw_data_new_fmt['potential_yoy'], to_freq='M')

        df_gap_cai = self.SU.minus_df1_df2(self.new_wf.df['GDP Nowcast'], self.new_wf.df['potential_yoy']) / 12 / 100
        df_gap_cai.columns = ['cai-gdp_month']
        df_gap_cai = df_gap_cai.cumsum()

        # splice together
        last_date = df_gap.last_valid_index()
        shift = df_gap.loc[last_date].values - df_gap_cai.loc[last_date].values
        df_gap_cai = df_gap_cai + shift
        df_gap_cai = df_gap_cai.loc[last_date:, :]
        df_gap.columns = df_gap_cai.columns
        df_gap = pd.concat([df_gap, df_gap_cai], axis=0)
        df_gap = self.SU.drop_duplicate(df_gap)

        # adding raw data: series + trend line
        self.new_wf.add_df('growth rate (raw)', self.new_wf.df['RGDP(raw index)'].dropna().pct_change(4) * 100,
                           to_freq='Q')
        self.new_wf.add_df('growth rate (trend)', self.new_wf.df['RGDP(index trend line)'].dropna().pct_change(4) * 100,
                           to_freq='Q')

        new_name = 'USA_GDP_slack_z'
        df_gap = self.SM.z(df_gap, mean_type='custom', sd_type='rolling', custom_mean=0,
                           roll_sd=self.trans_param_df['rgdp_pot_z_sd_roll'][0], new_name=new_name)
        self.new_wf.add_df('Growth Slack (z score)', df_gap[[new_name]])

        return

    def run_level_Growth_vs_Pot(self):
        # calculate the difference between growth and potential
        df = self.SU.minus_df1_df2(self.new_wf.df['GDP Nowcast'], self.new_wf.df['potential_yoy'])
        # calculate the  score
        new_name = 'Growth_Pot_z'
        df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                       roll_sd=self.trans_param_df['cai_pot_z_sd_roll'][0], new_name=new_name)
        self.new_wf.add_df('growth - potential(z score)', df[[new_name]], to_freq='bday')

    def run_level_URate_trend(self):
        '''
        :return:economic slack is the combination of the unemployment rate, capacity utility, real gdp growth, wage and inflation
        '''
        # import the unemloyment rate (URATE)
        df_urate = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Unemployment rate'])
        df_urate.loc[:'1989-12-31'] = np.nan
        y_ticker = df_urate.columns.tolist()[0]
        Lo_Filter = lf.loess_filter(df_urate, None, y_ticker, None, False, 0.65)
        df_trend_urate = Lo_Filter.estimate()[['y_fitted']]
        df_trend_urate.columns = ['USA_urate_trend']
        # adding raw data and trend line
        self.new_wf.add_df('unemployment rate (trend)', df_trend_urate, to_freq='M')
        self.new_wf.add_df('unemployment rate (raw)', df_urate, to_freq='M')

        df_URATEmTREND = self.SU.minus_df1_df2(self.new_wf.df['unemployment rate (raw)'],
                                               self.new_wf.df['unemployment rate (trend)'])

        # calc the z_score
        new_name = 'URATEmTREND_z'
        df_urate_z = -self.SM.z(df_URATEmTREND, mean_type='custom', sd_type='rolling', roll_sd=12 * 20, custom_mean=0,
                                new_name=new_name)
        self.new_wf.add_df('unemployment rate-trend(z score)', df_urate_z[[new_name]], to_freq='bday')

    def run_level_CapU_trend(self):
        '''
         :return:economic slack is the combination of the unemployment rate, capacity utility, real gdp growth, wage and inflation
         '''
        df_capu = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Capacity_Utilisation'])
        y_ticker = df_capu.columns.tolist()[0]
        Lo_Filter = lf.loess_filter(df_capu, None, y_ticker, None, False, 0.75)
        df_trend_capu = Lo_Filter.estimate()[['y_fitted']]
        df_trend_capu.columns = ['capacity utilisation trend']
        self.new_wf.add_df('capacity utilisation (raw)', df_capu, to_freq='M')
        self.new_wf.add_df('capacity utilisation (trend)', df_trend_capu, to_freq='M')

        # calculate the z score of trailing diff
        df_CAPUmTREND = self.SU.minus_df1_df2(self.new_wf.df['capacity utilisation (raw)'],
                                              self.new_wf.df['capacity utilisation (trend)'])
        new_name = 'Cap_Util_Trend_z'
        df_CAPUmTREND = self.SM.z(df_CAPUmTREND, mean_type='custom', sd_type='rolling', roll_sd=12 * 20, custom_mean=0,
                                  new_name=new_name)
        self.new_wf.add_df('capacity utilisation (z score)', df_CAPUmTREND[[new_name]], to_freq='bday')


def run_level_inf_targ(self):
    # Using a better gauge for inflation
    # FRB cpi
    df_cpi = self.SU.conversion_down_to_m(self.raw_data_new_fmt['USA_FRB_CPI'])
    df_targ = self.SU.conversion_down_to_m(self.raw_data_new_fmt['inflation target'])
    self.new_wf.add_df('CPI FRB-Trimmed mean 6m (raw)', df_cpi, to_freq='M')
    self.new_wf.add_df('inflation target', df_targ, to_freq='M')
    self.new_wf.df['inflation target'] = self.new_wf.df['inflation target'].fillna(method='bfill')

    df1 = self.SU.minus_df1_df2(self.new_wf.df['CPI FRB-Trimmed mean 6m (raw)'], self.new_wf.df['inflation target'],
                                na='drop')
    new_name = 'inflation_target_z'
    df1 = self.SM.z(df1, mean_type='custom', sd_type='rolling',
                    roll_sd=self.trans_param_df['core_cpi_targ_z_sd_roll'][0],
                    custom_mean=self.trans_param_df['core_cpi_targ_z_mean'][0], new_name=new_name)

    df1 = df1[[new_name]]
    self.new_wf.add_df('cpi(frb trimmed mean) vs target (z score)', df1)
    return


def run_level_inf_targ_GS_tracker(self):
    # REF: https://nam03.safelinks.protection.outlook.com/?url=https%3A%2F%2Fresearch.gs.com%2Fcontent%2Fresearch%2Fsite%2Fsearch.html%3Fq0%3D%2522GS%2520ECONOMIC%2520INDICATORS%2522%26f0%3Dquery_title%26type1%3DAND%26q1%3Dmodel%26f1%3Dquery_reportTypes%26type2%3DNOT%26q2%3DGLOBAL%26f2%3Dquery_title%26page%3D1%26frompg%3D1%26topg%3D999%26minpg%3D1%26maxpg%3D201%26lang%3Den%26sort%3Dtime&data=02%7C01%7Cjyang%40caxton.com%7Cf54564e01adb4ba8fb1308d6e4f97a77%7C70c99524736c49f4b73e75d7fa126cf9%7C0%7C0%7C636948157507900657&sdata=7XyWEKfLd7rM9RHJTytSUWBVzAZxkLvrEFbMklcTk6c%3D&reserved=0
    if not 'GS Core Inflation Tracker' in self.new_wf.df.keys():
        import_dir = os.path.join(self.LOCAL_MACRO_DATA_DIR, "GS porttal indicators", "model.xlsx")
        df = pd.read_excel(import_dir, sheet_name='Core Inflation Tracker', index_col=0)
        df.index = pd.to_datetime(df.index)
        self.new_wf.add_df('GS Core Inflation Tracker (raw)', df[['GS Core Inflation Tracker']], to_freq='M')
    if not 'inflation target' in self.new_wf.df.keys():
        df_targ = self.SU.conversion_down_to_m(self.raw_data_new_fmt['inflation target'])
        self.new_wf.add_df('inflation target', df_targ, to_freq='M')
        self.new_wf.df['inflation target'] = self.new_wf.df['inflation target'].fillna(method='bfill')

    df1 = self.SU.minus_df1_df2(self.new_wf.df['GS Core Inflation Tracker (raw)'], self.new_wf.df['inflation target'],
                                na='drop')
    new_name = 'inflation_target_z(GS_tracker)'
    df1 = self.SM.z(df1, mean_type='custom', sd_type='rolling',
                    roll_sd=self.trans_param_df['core_cpi_targ_z_sd_roll'][0],
                    custom_mean=self.trans_param_df['core_cpi_targ_z_mean'][0], new_name=new_name)

    df1 = df1[[new_name]]
    self.new_wf.add_df('cpi(GS tracker) vs target (z score)', df1)


def run_level_bei5_targ(self):
    # concept: breakeven inflation compared with the inflation target
    if not 'inflation target' in self.new_wf.df.keys():
        df_targ = self.SU.conversion_down_to_m(self.raw_data_new_fmt['inflation target'])
        self.new_wf.add_df('inflation target', df_targ, to_freq='M')
        self.new_wf.df['inflation target'] = self.new_wf.df['inflation target'].fillna(method='bfill')

    self.new_wf.add_df('Inf_Targ_daily', self.new_wf.df['inflation target'], to_freq='bday')
    self.new_wf.add_df('BEI 5Y (raw)', self.raw_data_new_fmt['BEI_5Y'])

    df = self.SU.minus_df1_df2(self.new_wf.df['BEI 5Y (raw)'], self.new_wf.df['Inf_Targ_daily'], na='drop')
    new_name = 'Breakeven_CPI_Targ_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', roll_sd=252 * 20, custom_mean=0, new_name=new_name)
    self.new_wf.add_df('BEI 5y (z score)', df[[new_name]])


def run_level_wages_trend(self):
    # download the wage data
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['wage_nfp'])
    df = self.SU.smooth_change(df.dropna(), periods=self.trans_param_df['wages_trend_growth'][0], ann=True,
                               ann_type='geo')
    #######################################################################
    ##### # fix the compositional issue with GS wage leading indicator#####
    #######################################################################
    df.loc['2020-3-1':'2020-10-1'] = np.nan
    if not 'GS Wage Survey Leading Indicator' in self.new_wf.df.keys():
        import_dir = os.path.join(self.LOCAL_MACRO_DATA_DIR, "GS porttal indicators", "model.xlsx")
        _df = pd.read_excel(import_dir, sheet_name='Wage Leading Indicator', index_col=0, skiprows=2)
        _df.index = pd.to_datetime(_df.index)
        self.new_wf.add_df('GS Wage Survey Leading Indicator (raw)',
                           (_df[['Wage Survey Leading Indicator']] / 100).dropna(), to_freq='M')
    df_gs_leading = self.new_wf.df['GS Wage Survey Leading Indicator (raw)'].dropna()
    df = self.SU.extend_df1_with_most_recent_df2(df, df_gs_leading)

    df.columns = ['wages_6m_chg']
    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False,
                                self.trans_param_df['wages_trend_growth_loess_frac'][0])
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['wages_6m_trend']

    self.new_wf.add_df('wage_6m (raw)', df, to_freq='M')
    self.new_wf.add_df('wages_6m (trend)', df_trend, to_freq='M')


df_diff = self.SU.minus_df1_df2(self.new_wf.df['wage_6m (raw)'], self.new_wf.df['wages_6m (trend)'], na='drop')
new_name = 'wages_trend_6m_z'
df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling',
                    roll_sd=self.trans_param_df['wages_trend_z_sd_roll'][0],
                    custom_mean=self.trans_param_df['wages_trend_z_mean'][0], new_name=new_name)

self.new_wf.add_df('wage_6m (z_score)', df_diff[[new_name]], to_freq='bday')
return


def run_level_wages_atlant_trend(self):
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['wage_atlanta_tracker'])
    df = self.SU.sea_adj(df)

    # fill the gaps of the data
    df.fillna(method='ffill', inplace=True)

    # smoothing with ewma
    df = df.ewm(self.trans_param_df['wage_atl_ewm_halflife'][0]).mean()
    df.columns = ['wage_atlanta_yoy']
    self.new_wf.add_df('wage(atlanta) yoy (raw)', df, to_freq='M')

    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False,
                                self.trans_param_df['wages_trend_growth_loess_frac'][0])
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['wages_atl_yoy_trend']

    self.new_wf.add_df('wages_atlanta_yoy (trend)', df_trend, to_freq='M')

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['wage(atlanta) yoy (raw)'],
                                    self.new_wf.df['wages_atlanta_yoy (trend)'], na='drop')
    new_name = 'wages(atlanta)_trend_z'
    df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling',
                        roll_sd=self.trans_param_df['wages_trend_z_sd_roll'][0],
                        custom_mean=self.trans_param_df['wages_trend_z_mean'][0], new_name=new_name)

    self.new_wf.add_df('wages(atlanta)_trend (z score)', df_diff[[new_name]], to_freq='bday')
    return


def run_level_wages_eci(self):
    # ECI: wages and salary, ex-incentives
    df = self.SU.conversion_to_q(self.raw_data_new_fmt['wage_eci_index'])

    df = df.pct_change(periods=self.trans_param_df['wages_q_trend_growth'][0])
    df.columns = ['wages_eci_1st']
    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False,
                                self.trans_param_df['wages_trend_growth_loess_frac'][0])
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['wages_eci_1st_trend']

    self.new_wf.add_df('wage_eci_1st (raw)', df, to_freq='Q')
    self.new_wf.add_df('wages_eci_1st (trend)', df_trend, to_freq='Q')

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['wage_eci_1st (raw)'], self.new_wf.df['wages_eci_1st (trend)'],
                                    na='drop')
    new_name = 'wages_eci_trend_1st_z'
    df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling',
                        roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0],
                        custom_mean=0, new_name=new_name)

    self.new_wf.add_df('wage_eci_1st (z_score)', df_diff[[new_name]], to_freq='bday')
    return


def run_level_wages_bls_median(self):
    # BLS, median wage
    df = self.SU.conversion_to_q(self.raw_data_new_fmt['wage_median_BLS'])

    df = df.pct_change(periods=self.trans_param_df['wages_q_trend_growth'][0])
    df.columns = ['wages_BLS_median_1st']
    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False,
                                self.trans_param_df['wages_trend_growth_loess_frac'][0])
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['wages_BLS_1st_trend']

    self.new_wf.add_df('wage_BLS_1st (raw)', df, to_freq='Q')
    self.new_wf.add_df('wages_BLS_1st (trend)', df_trend, to_freq='Q')

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['wage_BLS_1st (raw)'], self.new_wf.df['wages_BLS_1st (trend)'],
                                    na='drop')
    new_name = 'wages_BLS_trend_1st_z'
    df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling',
                        roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0],
                        custom_mean=0, new_name=new_name)

    self.new_wf.add_df('wage_BLS_1st (z_score)', df_diff[[new_name]], to_freq='bday')
    return


def run_level_wages_comp_hour(self):
    # nonfarm business sector: compensation per hour
    df = self.SU.conversion_to_q(self.raw_data_new_fmt['wage_comp_hour_nf'])

    df = df.pct_change(periods=self.trans_param_df['wages_q_trend_growth'][0])
    df.columns = ['wages_comp_hour_1st']
    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False,
                                self.trans_param_df['wages_trend_growth_loess_frac'][0])
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['wages_comp_hour_1st_trend']

    self.new_wf.add_df('wage_comp_hour_1st (raw)', df, to_freq='Q')
    self.new_wf.add_df('wages_comp_hour_1st (trend)', df_trend, to_freq='Q')

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['wage_comp_hour_1st (raw)'],
                                    self.new_wf.df['wages_comp_hour_1st (trend)'], na='drop')
    new_name = 'wages_comp_hour_trend_1st_z'
    df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling',
                        roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0],
                        custom_mean=0, new_name=new_name)

    self.new_wf.add_df('wage_comp_hour_1st (z_score)', df_diff[[new_name]], to_freq='bday')
    return


def run_change_in_growth(self):
    # x months of level change in now cast
    df = self.SU.smooth_change(self.new_wf.df['GDP Nowcast'], self.trans_param_df['cai_chg_roll'][0], ann_type='aris',
                               ann=True)
    self.new_wf.add_df('growth_3x6 (raw)', df, to_freq='M')
    new_name = 'growth_3x6_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['cai_chg_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('growth 3x6 (z score)', df[[new_name]], to_freq='bday')
    return


def run_change_in_growth_citi_econ(self):
    # level change in citi change index
    df = self.raw_data_new_fmt['Citi_Econ_Chg_Index'].dropna()
    df = self.SU.smooth_change(df, periods=self.trans_param_df['citi_chg_index_1st'][0], ann_type='aris', ann=True)
    self.new_wf.add_df('Citi_Econ_Chg_Index_1st (raw)', df)
    new_name = 'Citi_Econ_Chg_Index_1st_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['citi_chg_index_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('Citi_Econ_Chg_Index_1st (z score)', df[[new_name]], to_freq='bday')
    return


def run_chg_on_wage_growth(self):
    df = self.SU.smooth_change(self.new_wf.df['wage_6m (raw)'],
                               self.trans_param_df['wage_chg_in_growth_roll_second'][0], ann_type='aris', ann=True)
    self.new_wf.add_df('wage_6x6m (raw)', df, to_freq='M')
    new_name = 'wage_growth_6x6_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling',
                   custom_mean=self.trans_param_df['wage_chg_in_growth_z_mean'][0],
                   roll_sd=self.trans_param_df['wage_chg_in_growth_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('wage growth 6x6 (z score)', df[[new_name]])
    return


def run_chg_on_wage_atl(self):
    df = self.SU.smooth_change(self.new_wf.df['wage(atlanta) yoy (raw)'],
                               self.trans_param_df['wage_chg_in_growth_roll_second'][0], ann_type='aris', ann=True)
    self.new_wf.add_df('wage(atlanta)_2nd (raw)', df, to_freq='M')
    new_name = 'wage_atl_growth_2nd_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['wage_chg_in_growth_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('wage(atlanta) growth 2nd (z score)', df[[new_name]])
    return


def run_chg_on_wage_eci(self):
    df = self.new_wf.df['wage_eci_1st (raw)'].dropna().diff(self.trans_param_df['wage_q_chg_in_growth_roll_second'][0])
    self.new_wf.add_df('wage_eci_2nd (raw)', df, to_freq='Q')
    new_name = 'wage_eci_growth_2nd_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('wage(eci) growth 2nd (z score)', df[[new_name]])
    return


def run_chg_on_wage_bls_median(self):
    df = self.new_wf.df['wage_BLS_1st (raw)'].dropna().diff(
        self.trans_param_df['wage_q_chg_in_growth_roll_second'][0])
    self.new_wf.add_df('wage_bls_2nd (raw)', df, to_freq='Q')
    new_name = 'wage_bls_growth_2nd_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('wage(bls median) growth 2nd (z score)', df[[new_name]])
    return


def run_chg_on_comp_hour(self):
    df = self.new_wf.df['wage_comp_hour_1st (raw)'].dropna().diff(
        self.trans_param_df['wage_q_chg_in_growth_roll_second'][0])
    self.new_wf.add_df('wage_comp_hour_2nd (raw)', df, to_freq='Q')
    new_name = 'wage_comp_hour_growth_2nd_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['wages_q_trend_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('wage(comp hour nonfarm) growth 2nd (z score)', df[[new_name]])
    return


def run_chg_in_cpi(self):
    df = self.SU.smooth_change(self.new_wf.df['CPI FRB-Trimmed mean 6m (raw)'], self.trans_param_df['core_cpi_chg'][0],
                               ann_type='aris')
    df = self.SU.smooth_change(df, self.trans_param_df['core_cpi_chg_second'][0], ann_type='aris')

    new_name = 'CPI FRB-timmed_mean_2nd_z'
    df = self.SM.z(df, mean_type='custom', sd_type='rolling', custom_mean=0, roll_sd=12 * 20, new_name=new_name)

    self.new_wf.add_df('CPI FRB-timmed_mean_2nd (z score)', df[[new_name]])


def run_chg_in_cpi_pce(self):
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['Core_PCE_CPI'])
    df_1st = self.SU.smooth_change(df, self.trans_param_df['cpi_pce_1st'][0])
    self.new_wf.add_df('cpi pce_1st (raw)', df_1st, to_freq='M')
    df_2nd = self.SU.smooth_change(df_1st, self.trans_param_df['cpi_pce_2nd'][0], ann_type='aris')
    df_2nd.columns = ['cpi_pce_2nd']
    self.new_wf.add_df('cpi pce 2nd (raw)', df_2nd, to_freq='M')

    new_name = 'cpi_pce_2nd_z'
    df_z = self.SM.z(df_2nd, mean_type='custom', sd_type='rolling', custom_mean=0,
                     roll_sd=self.trans_param_df['cpi_pce_chg_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('cpi(pce)_2nd (z score)', df_z[[new_name]], to_freq='bday')


def run_surprise_in_cpi(self):
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['inflation_surprise'])
    df = self.SU.smooth_change(df, self.trans_param_df['cpi_surprise_first'][0], ann_type='aris', ann=True)
    df.columns = ['cpi surprise first']
    self.new_wf.add_df('cpi surprise first (raw)', df, to_freq='M')
    new_name = 'CPI_Surprise_diff_z'
    df = self.SM.z(df, mean_type='custom', sd_type='full', custom_mean=0, new_name=new_name)
    df.fillna(method='bfill', inplace=True)
    self.new_wf.add_df('CPI surprise diff (z score)', df[[new_name]])


def run_chg_in_GS_cpi_tracker(self):
    # using the core inflation tracker from GS
    if not 'GS Core Inflation Tracker' in self.new_wf.df.keys():
        import_dir = os.path.join(self.LOCAL_MACRO_DATA_DIR, "GS porttal indicators", "model.xlsx")
        df = pd.read_excel(import_dir, sheet_name='Core Inflation Tracker', index_col=0, header=0)
        df.index = pd.to_datetime(df.index)
        self.new_wf.add_df('GS Core Inflation Tracker', df[['GS Core Inflation Tracker']], to_freq='M')

    new_name = 'USA_CPI_diff(GS_Tracker)'
    df = self.SU.smooth_change(self.new_wf.df['GS Core Inflation Tracker'],
                               periods=self.trans_param_df['core_cpi_chg'][0], ann_type='aris')
    df.columns = [new_name]
    self.new_wf.add_df('CPI_diff(GS_Tracker) (raw)', df[[new_name]], to_freq='M')

    df = self.SM.z(df, mean_type='custom', custom_mean=0, sd_type='rolling', roll_sd=12 * 20, new_name=new_name)
    self.new_wf.add_df('CPI_diff(GS_Tracker)(z score)', df[[new_name]], to_freq='bday')


def run_chg_in_cpi_bei5(self):
    # run the change of cpi
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['BEI_5Y'])
    df = self.SU.smooth_change(df, periods=6 * 21, ann_type='aris')
    self.new_wf.add_df('Chg_on_Breakeven_CPI (raw)', df)
    new_name = 'Chg_on_Breakeven_CPI_z'
    df = self.SM.z(df, mean_type='custom', custom_mean=0, sd_type='rolling', roll_sd=252 * 20, new_name=new_name)
    self.new_wf.add_df('Chg_on_Breakeven_CPI (z score)', df[[new_name]])


def run_HP_impulse(self):
    # 6 month change in HP
    # download the data
    df = self.SU.conversion_down_to_m(self.raw_data_new_fmt['housing_prices_CS'])
    df.columns = ['CS_housing_index']

    # add 6m_annualised chg
    df = self.SU.smooth_change(df, periods=self.trans_param_df['hp_impulse_trans_first'][0], ann_type='geo', ann=True)
    new_name = 'CS_housing_index_1st'
    df.columns = ['CS_housing_index_1st']
    self.new_wf.add_df('CS_housing_index_1st (raw)', df[[new_name]], to_freq='M')

    # adding housing price trend
    y_ticker = df.columns[0]
    Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False, 0.75)
    df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend.columns = ['housing_price_1st_trend']
    self.new_wf.add_df('housing_prices_1st (trend)', df_trend, to_freq='M')

    # z score
    df_diff = self.SU.minus_df1_df2(self.new_wf.df['CS_housing_index_1st (raw)'],
                                    self.new_wf.df['housing_prices_1st (trend)'], na='drop')
    new_name = 'housing_price_1st_z'
    df_diff = self.SM.z(df_diff, mean_type='custom', sd_type='rolling', custom_mean=0,
                        roll_sd=self.trans_param_df['hp_impulse_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('housing_prices_1st (z score)', df_diff[[new_name]], to_freq='bday')

    # add 2nd order transformation, and converting to z score
    df2 = self.SU.smooth_change(self.new_wf.df['CS_housing_index_1st (raw)'],
                                periods=self.trans_param_df['hp_impulse_trans_second'][0], ann_type='aris', ann=True)
    self.new_wf.add_df('CS_housing_index_2nd (raw)', df2, to_freq='M')
    #
    new_name = 'housing_price_2nd_z'
    df2 = self.SM.z(df2, mean_type='custom', sd_type='rolling', custom_mean=0,
                    roll_sd=self.trans_param_df['hp_impulse_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('housing_prices_2nd (z score)', df2[[new_name]])


def run_total_cai_impulse(self):
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['GS_CAI_Impulse']).dropna()
    self.new_wf.add_df('GS_CAI_Impulse (raw)', df)
    new_name = 'GS_CAI_Impulse_z'
    # df = self.SM.z(df,mean_type='custom',sd_type='ewm',ewm_alpha=0.01,custom_mean=0,new_name=new_name)
    df = self.SM.z(df, mean_type='custom', sd_type='ewm', custom_mean=0, new_name=new_name)

    df.plot(figsize=(10, 5), grid=True)
    self.new_wf.add_df('GS_CAI_Impulse (z score)', df[[new_name]])

    new_name = 'GS_CAI_Impulse_z_smooth'
    df['GS_CAI_Impulse_z_smooth'] = df.iloc[:, -1].rolling(window=self.trans_param_df['cai_impulse_z_smooth'][0]).mean()
    self.new_wf.add_df('GS_CAI_Impulse (z score smooth)', df[[new_name]])

    return


def run_oil_impulse(self):
    df_oil = self.raw_data_new_fmt['brent']
    df_oil.columns = ['Oil']
    self.new_wf.add_df('Oil (raw)', df_oil)

    df_oil = self.SU.smooth_change(df_oil.dropna(), periods=self.trans_param_df['oil_impulse_trans_1st'][0])
    df_oil.columns = ['Oil_lc_1st']
    self.new_wf.add_df('Oil_1st (raw)', df_oil)

    # estimate the trend with loess
    # y_ticker = self.new_wf.df['Oil_1st (raw)'].columns.tolist()[0]
    # Lo_Filter = lf.loess_filter(self.new_wf.df['Oil_1st (raw)'], None,y_ticker, None,False, 0.75)
    # df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend = df_oil.rolling(window=252 * 5).mean()
    df_trend.columns = ['Oil_1st_trend']
    self.new_wf.add_df('Oil_1st (trend)', df_trend)

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['Oil_1st (raw)'], self.new_wf.df['Oil_1st (trend)'], na='drop')
    new_name = 'Oil_1st_z'
    df = self.SM.z(df_diff, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['oil_impulse_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('Oil_1st (z_score)', df[[new_name]])

    df_oil = self.SU.smooth_change(df_oil, periods=self.trans_param_df['oil_impulse_trans_second'][0],
                                   ann_type='aris')
    self.new_wf.add_df('Oil_impulse_Lc_2nd (raw)', df_oil)

    new_name = 'Oil_Impulse'
    df_oil = self.SM.z(df_oil, mean_type='custom', custom_mean=0, sd_type='ewm', new_name=new_name)

    self.new_wf.add_df('Oil_Impulse (z score)', df_oil[[new_name]])
    return


def run_fx_impulse(self):
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['FX_NEER'])
    self.new_wf.add_df('FX (raw)', df)

    df = -self.SU.smooth_change(df.dropna(), periods=self.trans_param_df['fx_impulse_trans_1st'][0])
    df.columns = ['FX_1st']
    self.new_wf.add_df('FX_1st (raw)', df)

    # estimate the trend with loess
    # y_ticker = self.new_wf.df['FX_1st (raw)'].columns.tolist()[0]
    # Lo_Filter = lf.loess_filter(self.new_wf.df['FX_1st (raw)'], None, y_ticker, None, False, 0.75)
    # df_trend = Lo_Filter.estimate()[['y_fitted']]
    df_trend = df.rolling(window=252 * 5).mean()
    df_trend.columns = ['FX_1st_trend']
    self.new_wf.add_df('FX_1st (trend)', df_trend)

    df_diff = self.SU.minus_df1_df2(self.new_wf.df['FX_1st (raw)'], self.new_wf.df['FX_1st (trend)'], na='drop')
    new_name = 'FX_1st_z'
    df = self.SM.z(df_diff, mean_type='custom', sd_type='rolling', custom_mean=0,
                   roll_sd=self.trans_param_df['fx_impulse_z_sd_roll'][0], new_name=new_name)


self.new_wf.add_df('FX_1st (z_score)', df[[new_name]])

df = self.SU.smooth_change(df, periods=self.trans_param_df['fx_impulse_trans_second'][0], ann_type='aris')
self.new_wf.add_df('FX_impulse_2nd (raw)', df)

new_name = 'FX_Impulse'
df = self.SM.z(df, mean_type='custom', custom_mean=0, sd_type='ewm', new_name=new_name)
df = self.SU.rolling_ignore_nan(df, _window=self.trans_param_df['fx_impulse_roll'][0], _func=np.mean)
self.new_wf.add_df('FX_Impulse (z score)', df[[new_name]])


def run_pricing_2y(self, ):
    import_dir = os.path.join(self.PROJ_DIR, r"macro_data\rates\ratesTable.csv")
    df = pd.read_csv(import_dir, index_col=0, header=0)
    df.index = pd.to_datetime(df.index)
    df_1w = df[['USA_Swap_1wk']].dropna()
    df_pr = self.new_wf.df['policy_rate'].dropna()
    df_1w = self.SU.extend_backward_df1_by_df2(df_1w, df_pr, method='aris')

    df_1w_od = self.SU.conversion_to_bDay(self.raw_data_new_fmt['IRS_1W_OIS_OPENDATA'])
    # print (df_1w_od.tail())
    df_1w = self.SU.extend_df1_with_most_recent_df2(df_1w, df_1w_od, method='aris')
    # print (self.raw_data_new_fmt['IRS_2y1m_OIS_Citi'])
    # prepare the 2y1d data
    df_2y1d, df_3y = self.SU.conversion_to_bDay(
        self.raw_data_new_fmt['IRS_2y1m_OIS_Citi'].dropna()), self.SU.conversion_to_bDay(
        df[['USA_Swap_3yr']].dropna())
    # print (df_2y1d.tail(10),df_3y.tail(10))
    df_2y1d = self.SU.extend_backward_df1_by_df2(df_2y1d, df_3y, method='aris')
    # print (df_2y1d.tail(10))
    # print (self.SU.conversion_to_bDay(self.SU.drop_duplicate(df_2y1d)).dropna())
    df_2y1d = self.SU.remove_outlier(self.SU.conversion_to_bDay(self.SU.drop_duplicate(df_2y1d)).dropna(),
                                     use_end_point=True, end_point='2020-01-01')
    # print(df_2y1d.tail(10))
    df_comb = pd.concat([df_2y1d, df_1w], axis=1)
    # print (df_comb.tail(10))
    self.new_wf.add_df('ois_1w', df_1w)
    self.new_wf.add_df('2y1d', df_2y1d)

    df_pricing = df_comb.iloc[:, 0] - df_comb.iloc[:, 1]
    df_pricing = df_pricing.to_frame()
    df_pricing = df_pricing - 0.2
    df_pricing.columns = ['2y1d-1w-20bps']

    self.new_wf.add_df('2y1d-1w-20bps', df_pricing)
    new_name = 'pricing_2y (z score)'
    df_z = self.SM.z(df_pricing, mean_type='custom', sd_type='rolling',
                     custom_mean=0, roll_sd=self.trans_param_df['priced_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df(new_name, df_z[[new_name]])


def run_pricing_2y_swap(self):
    # a variation of 2y1d premium : using simply 2y swap, rather than 2y1d, the rationle is that back in early 1990s there were really high risk premium for expected CPI
    import_dir = r"C:\_CODE\JYang\JY_Project\macro_data\rates\ratesTable.csv"
    df = pd.read_csv(import_dir, index_col=0, header=0)
    df.index = pd.to_datetime(df.index)
    df_1w = df[['USA_Swap_1wk']].dropna()
    df_pr = self.new_wf.df['policy_rate'].dropna()
    df_1w = self.SU.extend_backward_df1_by_df2(df_1w, df_pr, method='aris')

    # prepare the 2y swap data
    df_2y, df_3y = self.SU.conversion_to_bDay(
        self.raw_data_new_fmt['IRS_2Y'].dropna()), self.SU.conversion_to_bDay(
        df[['USA_Swap_3yr']].dropna())
    df_2y = self.SU.extend_backward_df1_by_df2(df_2y, df_3y, method='aris')
    df_2y = self.SU.remove_outlier(self.SU.conversion_to_bDay(self.SU.drop_duplicate(df_2y)).dropna())

    df_comb = pd.concat([df_2y, df_1w], axis=1)
    self.new_wf.add_df('ois_1w', df_1w)
    self.new_wf.add_df('2y_swap', df_2y)

    df_pricing = df_comb.iloc[:, 0] - df_comb.iloc[:, 1]
    df_pricing = df_pricing.to_frame()
    # df_pricing.to_csv('USA_pricing.csv')


df_pricing = df_pricing - 0.2
df_pricing.columns = ['2y-1w-20bps']

self.new_wf.add_df('2y-1w-20bps', df_pricing)
new_name = 'pricing_2y_swap (z score)'
df_z = self.SM.z(df_pricing, mean_type='custom', sd_type='rolling',
                 custom_mean=0, roll_sd=self.trans_param_df['priced_z_sd_roll'][0], new_name=new_name)
self.new_wf.add_df(new_name, df_z[[new_name]])


def run_pricing(self):
    df_pr = self.SU.conversion_to_bDay(self.raw_data_new_fmt['policy_rate'])
    self.new_wf.add_df('policy_rate', df_pr)
    df_irs = self.SU.conversion_to_bDay(self.raw_data_new_fmt['IRS_5Y'])
    self.new_wf.add_df('IRS_5Y', df_irs)

    delta_5y_pr = self.SU.minus_df1_df2(self.new_wf.df['IRS_5Y'], self.new_wf.df['policy_rate'])
    delta_5y_pr.columns = ['5y-PR']
    self.new_wf.add_df('5y-PR', delta_5y_pr, to_freq='bday')

    df_prm = self.new_wf.df['IRS_5Y'].copy()
    df_prm.columns = ['IRS_5Y']
    _5y_values = df_prm.iloc[:, 0].values
    _5y_values = [fin_lib.calc_duration(i, 5) for i in _5y_values]
    df_prm['_5y_duration'] = _5y_values
    df_prm['IRS_5Y'] = df_prm['IRS_5Y'] / 100
    df_prm['premium'] = df_prm['IRS_5Y'].diff(1).rolling(
        window=self.trans_param_df['pricing_z_premium_sdRoll'][0]).apply(np.std) * np.sqrt(252) * 0.25 * df_prm[
                            '_5y_duration'] * 100
    self.new_wf.add_df('premium', df_prm[['premium']])

    delta_5y_pr_premium = self.SU.minus_df1_df2(self.new_wf.df['5y-PR'], df_prm[['premium']])
    delta_5y_pr_premium.columns = ['5y-PR-Premium']
    self.new_wf.add_df('5y-PR-Premium', delta_5y_pr_premium)

    new_name = 'pricing_z'

    df_z = self.SM.z(self.new_wf.df['5y-PR-Premium'], mean_type='rolling', sd_type='rolling',
                     roll_mean=self.trans_param_df['priced_z_mean_roll'][0],
                     roll_sd=self.trans_param_df['priced_z_sd_roll'][0], new_name=new_name)
    self.new_wf.add_df('pricing (z score)', df_z[[new_name]])


def run_glob_level_growth(self):
    # using global CAI ex self as a measurement global growth
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['World_ex_USA_avg_GDP']).ewm(
        span=self.trans_param_df['glob_roll_mean'][0]).mean()
    df2 = self.SU.conversion_to_bDay(self.raw_data_new_fmt['World_ex_USA_avg_pot_GDP'])

    df_comb = pd.concat([df, df2], axis=1)
    df_comb['gap'] = df_comb.iloc[:, 0] - df_comb.iloc[:, 1]
    df_gap = df_comb[['gap']]
    self.new_wf.add_df('glob growth vs pot (raw)', df_gap)

    new_name = 'Glob_Growth_Pot_z'
    # df_gap = self.SM.z(df_gap,mean_type='custom',sd_type='rolling',custom_mean=0,roll_sd=self.trans_param_df['glob_growth_pot_sd_roll'][0],new_name=new_name)
    df_gap['sd'] = df_gap.rolling(window=self.trans_param_df['glob_growth_pot_sd_roll'][0]).std()
    df_gap.loc['2010':, 'sd'] = df_gap.loc['2010':'2016', 'gap'].std()
    df_gap[new_name] = df_gap.iloc[:, 0] / df_gap.iloc[:, 1]

    self.new_wf.add_df('glob growth vs pot (z score)', df_gap[[new_name]])


def run_glob_FCI(self):
    # using global FCI ex USA as a measurement of forward global growth
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['World_avg_FCI'])
    self.new_wf.add_df('World_avg_FCI (raw)', df)
    # note raw and z score are the same thing
    self.new_wf.add_df('World_avg_FCI (z score)', df)


def run_glob_chg_growth(self):
    # change on the global growth, using the same parameter as in the  change on growth
    df = self.SU.conversion_to_bDay(self.raw_data_new_fmt['World_ex_USA_avg_GDP']).ewm(
        span=self.trans_param_df['glob_roll_mean'][0]).mean()

    df = self.SU.smooth_change(df, periods=self.trans_param_df['glob_chg_growth_1st'][0], ann_type='aris', ann=True)
    self.new_wf.add_df('Glob_Growth_3x6 (raw)', df, to_freq='no_change')

    new_name = 'Glob_Growth_3x6'
    # calc the z score manually for 2nd diff
    df['sd'] = self.SU.rolling_ignore_nan(df.iloc[:, [0]], _window=252 * 20, _func=np.std)
    df['sd'].fillna(inplace=True, method='bfill')
    sd_2010 = df.loc['2010-01-01':'2020-1-31', :].iloc[:, 0].std()
    df.loc['2010-01-01':, 'sd'] = sd_2010
    df[new_name] = df.iloc[:, 0] / df.iloc[:, 1]
    self.new_wf.add_df('Glob_Growth_3x6 (z score)', df[[new_name]])


# this should be called from a separate master module called 'bt'
if __name__ == '__main__':
    clear_cache('basket_USA_Rates1c', 'disk_8h')
    strat = signal3()
reporting_to = sys.argv[1] if len(sys.argv) > 1.01 else None
data = strat.get_data_dict(running_mode='production', signal_delay_bday=0, reporting_to=reporting_to)
print(data)

