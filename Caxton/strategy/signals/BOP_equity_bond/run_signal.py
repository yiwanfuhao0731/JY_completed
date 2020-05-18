# import sys when runing from the batch code
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))
import pandas as pd
import numpy as np
from datetime import datetime
# visualisation packages
import math
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import collections
from matplotlib.font_manager import FontProperties

import Analytics.series_utils as s_util
from Analytics.scoring_methods import scoring_method_collection as smc
import Analytics.abstract_sig as abs_sig
from panormus.utils.cache import cache_response, clear_cache

# part of the utilities
dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
dateparse2 = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')


class signal3(abs_sig.abstract_sig_genr):
    def __init__(self):
        # in this case, both Econ and csv file data are used
        super(signal3, self).__init__()

    def run_data_dict(self, *args, **kwargs):
        self.add_sig_info()
        self.add_dir_info(**kwargs)
        self.pre_run_result = self.pre_run()
        self.raw_data_new_fmt = self.load_data_wrapper(self.MASTER_INPUT_DIR, self.Short_Name, **kwargs)
        self.import_parse_param(self.MASTER_INPUT_DIR, self.Short_Name)
        self.cleaned_data = self.sanity_check()
        self.indicator = self.apply_sig_genr()
        self.convert_indicator_tolist_new(self.indicator, self.INDICATOR_EXP_DIR)
        result_dict = self.fullkey_result_dict(self.indicator)

        if kwargs.get('run_chart') == True:
            self.out_folder = self.create_tearsheet_folder(self.RPTDIR)
            self.create_report_page()

        return result_dict

    def pre_run(self):
        '''
        :return: dictionary of pre-processing data from all pre-running result
        '''
        result_dict = {}
        return result_dict

    def add_sig_info(self):
        self.signal_ID = 'sig_0011'
        self.Short_Name = 'FLOW_EM_BOP_EQ_BOND_HF'
        self.Description = '''
                       EM high frequency equity and bond flow data mapping to BOP invt flow
                      '''
        self.exec_path = __file__

    def import_parse_param(self, master_input_dir, Short_Name):
        '''
        :return: import from a csv file and parse the relevant parameters
        three parameter, orders should be the same in the csv file
        loess_start_year: the start year for the potential growth
        loess_end_year: the end year for the potential growth
        regression_param: the frac parameter in the loess regression
        '''

    param_df = pd.read_excel(master_input_dir, sheet_name=Short_Name, index_col=False, header=0, na_values=['NA', ''])
    self.all_ISO = param_df.iloc[:, 0].values.tolist()

    # in this case, should be hard-coded as 1800, or 360*5
    self.roll_mean = 5 * 12 * 30
    self.roll_std = 5 * 12 * 30
    return


@cache_response('FLOW_EM_BOP_EQ_BOND_HF_sanity_check', 'disk_8h', skip_first_arg=True)
def sanity_check(self):
    '''
    :return: convert the date to monthly
    other types of conversion is also possible
    '''
    SU = s_util.date_utils_collection()

    self.cleaned_data = collections.OrderedDict()
    assert len(self.all_ISO) > 0, 'sorry, list of countries have not been provided'

    # calc the difference for MEX
    # TODO: convert MEX to daily will overweight the Friday since it is always repeated on the weekend. Might go for bsiness days so it's more logical
    df = self.raw_data_new_fmt['MEX_DEBT_FLOW_HF'].copy()
    df = SU.delete_zero_beginning(df)
    df = df.diff(periods=1)
    self.raw_data_new_fmt['MEX_DEBT_FLOW_HF'] = df

    # calc the sum of flows for MAL
    df1 = self.raw_data_new_fmt['MAL_EQ_FLOW_HF']
    df1 = SU.conversion_down_to_m(df1, agg_method='sum')
    df2 = self.raw_data_new_fmt['MAL_DEBT_FLOW_HF']
    df2 = SU.conversion_down_to_m(df2)
    df1.iloc[:, 0] = df1.iloc[:, 0] + df2.iloc[:, 0]
    df1.columns = ['MAL_TOTAL_FLOW_HF']
    self.raw_data_new_fmt['MAL_TOTAL_FLOW_HF'] = df1

    # convert the BOP data as well as the high-freq data into daily data, repeat the values for NAs, get 6mma
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        key1 = iso + '_EQ_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_DEBT_FLOW_HF'
        key4 = iso + '_EQ_NET_LIAB_NSA'
        key5 = iso + '_DEBT_NET_LIAB_NSA'

        for k in [key1, key2, key3, key4, key5]:
            new_key = k + '_6mma'

            if k not in self.raw_data_new_fmt:
                self.cleaned_data[new_key] = pd.DataFrame()
                continue

            df = self.raw_data_new_fmt[k].copy()
            if len(df.index) < 0.00001:
                self.cleaned_data[new_key] = pd.DataFrame()
            else:
                # get rid of the zeros at the beginning. BOP might have a lot of zeros
                df = SU.delete_zero_beginning(df)
                # if Qtr data, convert to monthly first and convert to daily;
                # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
                if SU.get_freq(df) == 'Q':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=6).apply(np.mean, raw=True)
                elif SU.get_freq(df) == 'M':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=6).apply(np.mean, raw=True)
                elif (SU.get_freq(df) == 'W'):
                    df = SU.conversion_to_Day(df, method='nothing')
                    df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=6 * 30, _func=np.mean)
                elif (SU.get_freq(df) == 'D'):
                    df = SU.conversion_to_Day(df, method='nothing')
                    df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=6 * 30, _func=np.mean)
                # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
                self.cleaned_data[new_key] = SU.conversion_to_Day(df, method='nothing', col_to_repeat=-1)

    # convert the MAL BOP data as well as the high-freq data into monthly data, repeat the values for NAs, get 6mma
    for iso in ['MAL']:
        key1 = iso + '_TOTAL_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_NET_LIAB_NSA'

        for k in [key1, key2, key3]:
            new_key = k + '_6mma'

            if k not in self.raw_data_new_fmt:
                self.cleaned_data[new_key] = pd.DataFrame()
                continue

            df = self.raw_data_new_fmt[k].copy()
            if len(df.index) < 0.00001:
                self.cleaned_data[new_key] = pd.DataFrame()
            else:
                # get rid of the zeros at the beginning. BOP might have a lot of zeros
                df = SU.delete_zero_beginning(df)
                # if Qtr data, convert to monthly first and convert to daily;
                # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
                if SU.get_freq(df) == 'Q':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=6).apply(np.mean)
                elif SU.get_freq(df) == 'M':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=6).apply(np.mean)
                elif (SU.get_freq(df) == 'W') or (SU.get_freq(df) == 'D'):
                    df = SU.conversion_to_Day(df, method='nothing')
                    df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=6 * 30, _func=np.mean)
                # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
                self.cleaned_data[new_key] = df

    # convert the BOP data as well as high-freq data into daily data, repeat the values for NAs, get 3mma
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        key1 = iso + '_EQ_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_DEBT_FLOW_HF'
        key4 = iso + '_EQ_NET_LIAB_NSA'
        key5 = iso + '_DEBT_NET_LIAB_NSA'
        for k in [key1, key2, key3, key4, key5]:
            new_key = k + '_3mma'

            if k not in self.raw_data_new_fmt:
                self.cleaned_data[new_key] = pd.DataFrame()
                continue

            df = self.raw_data_new_fmt[k].copy()
            if len(df.index) < 0.00001:
                self.cleaned_data[new_key] = pd.DataFrame()
        else:
            # get rid of the zeros at the beginning. BOP might have a lot of zeros
            df = SU.delete_zero_beginning(df)
            # if Qtr data, convert to monthly first and convert to daily;
            # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
            if SU.get_freq(df) == 'Q':
                df = SU.conversion_to_m(df)
                df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean, raw=True)
            elif SU.get_freq(df) == 'M':
                df = SU.conversion_to_m(df)
                df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean, raw=True)
            elif (SU.get_freq(df) == 'W') or (SU.get_freq(df) == 'D'):
                df = SU.conversion_to_Day(df, method='nothing')
                df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=3 * 30, _func=np.mean)
            # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
            self.cleaned_data[new_key] = SU.conversion_to_Day(df, method='nothing', col_to_repeat=-1)

    # convert the MAL BOP data as well as the high-freq data into monthly data, repeat the values for NAs, get 3mma
    for iso in ['MAL']:
        key1 = iso + '_TOTAL_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_NET_LIAB_NSA'

        for k in [key1, key2, key3]:
            new_key = k + '_3mma'

            if k not in self.raw_data_new_fmt:
                self.cleaned_data[new_key] = pd.DataFrame()
                continue

            df = self.raw_data_new_fmt[k].copy()
            if len(df.index) < 0.00001:
                self.cleaned_data[new_key] = pd.DataFrame()
            else:
                # get rid of the zeros at the beginning. BOP might have a lot of zeros
                df = SU.delete_zero_beginning(df)
                # if Qtr data, convert to monthly first and convert to daily;
                # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
                if SU.get_freq(df) == 'Q':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean, raw=True)
                elif SU.get_freq(df) == 'M':
                    df = SU.conversion_to_m(df)
                    df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean, raw=True)
                elif (SU.get_freq(df) == 'W') or (SU.get_freq(df) == 'D'):
                    df = SU.conversion_to_Day(df, method='nothing')
                    df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=3 * 30, _func=np.mean)
                # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
                self.cleaned_data[new_key] = df

    # convert the BOP data as well as high-freq data into daily data, repeat the values for NAs, get 1mma
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        key1 = iso + '_EQ_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_DEBT_FLOW_HF'
        key4 = iso + '_EQ_NET_LIAB_NSA'
        key5 = iso + '_DEBT_NET_LIAB_NSA'
        for k in [key1, key2, key3, key4, key5]:
            new_key = k + '_1mma'

            if k not in self.raw_data_new_fmt:
                self.cleaned_data[new_key] = pd.DataFrame()
                continue

            df = self.raw_data_new_fmt[k].copy()
            if len(df.index) < 0.00001:
                self.cleaned_data[new_key] = pd.DataFrame()
            else:
                # get rid of the zeros at the beginning. BOP might have a lot of zeros
                df = SU.delete_zero_beginning(df)
                # if Qtr data, convert to monthly first and convert to daily;
                # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
                if SU.get_freq(df) == 'Q':
                    df = SU.conversion_to_m(df)
                    # df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean)
                elif SU.get_freq(df) == 'M':
                    df = SU.conversion_to_m(df)
                    # df.iloc[:, 0] = df.iloc[:, 0].rolling(window=3).apply(np.mean)
                elif (SU.get_freq(df) == 'W') or (SU.get_freq(df) == 'D'):
                    df = SU.conversion_to_Day(df, method='nothing')
                    df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=1 * 30, _func=np.mean)
                # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
                self.cleaned_data[new_key] = SU.conversion_to_Day(df, method='nothing', col_to_repeat=-1)

    # convert the MAL BOP data as well as the high-freq data into monthly data, repeat the values for NAs, get 1mma
    for iso in ['MAL']:
        key1 = iso + '_TOTAL_FLOW_HF'
        key2 = iso + '_FXUSD'
        key3 = iso + '_NET_LIAB_NSA'

    for k in [key1, key2, key3]:
        new_key = k + '_1mma'

        if k not in self.raw_data_new_fmt:
            self.cleaned_data[new_key] = pd.DataFrame()
            continue

        df = self.raw_data_new_fmt[k].copy()
        if len(df.index) < 0.00001:
            self.cleaned_data[new_key] = pd.DataFrame()
        else:
            # get rid of the zeros at the beginning. BOP might have a lot of zeros
            df = SU.delete_zero_beginning(df)
            # if Qtr data, convert to monthly first and convert to daily;
            # if monthly data, convert to month-beginning, apply 3m average, and convert to daily
            if SU.get_freq(df) == 'Q':
                df = SU.conversion_to_m(df)
                df.iloc[:, 0] = df.iloc[:, 0].rolling(window=1).apply(np.mean, raw=True)
            elif SU.get_freq(df) == 'M':
                df = SU.conversion_to_m(df)
                df.iloc[:, 0] = df.iloc[:, 0].rolling(window=1).apply(np.mean, raw=True)
            elif (SU.get_freq(df) == 'W') or (SU.get_freq(df) == 'D'):
                df = SU.conversion_to_Day(df, method='nothing')
                df.iloc[:, 0] = SU.rolling_ignore_nan(df, _window=1 * 30, _func=np.mean)
            # TODO: potential issue with converting to business day: weekends are repeated with Fridays value thus not realistic!!
            self.cleaned_data[new_key] = df

    return self.cleaned_data


@cache_response('FLOW_EM_BOP_EQ_BOND_HF_apply_sig_genr', 'disk_8h', skip_first_arg=True)
def apply_sig_genr(self):
    SM = smc()
    SU = s_util.date_utils_collection()
    # init chart info dict
    self.indicator = collections.OrderedDict()

    # z_to mapping for 6m Equity flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_EQ_FLOW_HF_6mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_6m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_6mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_6m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_EQ_FLOW_HF_6mma']
        right = self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_6mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_EQ_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std, sd_type='rolling',
                     col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_EQ_FLOW_HF_6m'] = df

    # z_to mapping for 6m MAL Total flows
    for iso in ['MAL']:
        print('mapping total flow for ', iso)
        if len(self.cleaned_data[iso + '_TOTAL_FLOW_HF_6mma'].index) < 0.0001:
            self.indicator[iso + '_TOTAL_FLOW_HF_6m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_NET_LIAB_NSA_6mma'].index) < 0.0001:
            self.indicator[iso + '_FLOW_HF_6m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_TOTAL_FLOW_HF_6mma']
        right = self.cleaned_data[iso + '_NET_LIAB_NSA_6mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_TOTAL_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=60, roll_sd=60,
                     sd_type='rolling', col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_TOTAL_FLOW_HF_6m'] = df

    # z_to mapping for 3m Equity flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_EQ_FLOW_HF_3mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_3m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_3mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_3m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_EQ_FLOW_HF_3mma']
        right = self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_3mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_EQ_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std,
                     sd_type='rolling', col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_EQ_FLOW_HF_3m'] = df

    # z_to mapping for 3m MAL Total flows
    for iso in ['MAL']:
        print('mapping total flow for ', iso)
        if len(self.cleaned_data[iso + '_TOTAL_FLOW_HF_3mma'].index) < 0.0001:
            self.indicator[iso + '_TOTAL_FLOW_HF_3m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_NET_LIAB_NSA_3mma'].index) < 0.0001:
            self.indicator[iso + '_FLOW_HF_3m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_TOTAL_FLOW_HF_3mma']
        right = self.cleaned_data[iso + '_NET_LIAB_NSA_3mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_TOTAL_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=60, roll_sd=60,
                     sd_type='rolling', col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_TOTAL_FLOW_HF_3m'] = df

    # z_to mapping for 1m Equity flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_EQ_FLOW_HF_1mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_1m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_1mma'].index) < 0.0001:
            self.indicator[iso + '_EQ_FLOW_HF_1m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_EQ_FLOW_HF_1mma']
        right = self.cleaned_data[iso + '_EQ_NET_LIAB_NSA_1mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_EQ_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std,
                     sd_type='rolling', col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_EQ_FLOW_HF_1m'] = df

    # z_to mapping for 1m MAL Total flows
    for iso in ['MAL']:
        print('mapping total flow for ', iso)
        if len(self.cleaned_data[iso + '_TOTAL_FLOW_HF_1mma'].index) < 0.0001:
            self.indicator[iso + '_TOTAL_FLOW_HF_1m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_NET_LIAB_NSA_1mma'].index) < 0.0001:
            self.indicator[iso + '_FLOW_HF_1m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_TOTAL_FLOW_HF_1mma']
        right = self.cleaned_data[iso + '_NET_LIAB_NSA_1mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_TOTAL_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=60, roll_sd=60,
                     sd_type='rolling', col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_TOTAL_FLOW_HF_1m'] = df

    # interpolate the BOP_EQ/BOP_DEBT/TOTAL data for prettier charting: converting back to monthly, and back to daily with interpolate
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        key1 = iso + '_EQ_NET_LIAB_NSA'
        key2 = iso + '_DEBT_NET_LIAB_NSA'
        for k in [key1, key2]:
            new_key = k + 'interpolate'
            if k not in self.raw_data_new_fmt.keys():
                self.indicator[new_key] = pd.DataFrame()
                continue
            if len(self.raw_data_new_fmt[k].index) < 0.0001:
                self.indicator[new_key] = pd.DataFrame()
                continue

            df_BOP = self.raw_data_new_fmt[k].copy()
            if SU.get_freq(df_BOP) == 'M':
                df_BOP.iloc[:, 0] = df_BOP.iloc[:, 0].rolling(window=6).apply(np.mean, raw=True)
            elif SU.get_freq(df_BOP) == 'Q':
                df_BOP.iloc[:, 0] = df_BOP.iloc[:, 0].rolling(window=2).apply(np.mean, raw=True)
            df_BOP = SU.conversion_to_Day(df_BOP, method='interpolate')
            df_BOP.rename(columns={df_BOP.columns.tolist()[0]: k}, inplace=True)

            self.indicator[new_key] = df_BOP

    # interpolate the MAL BOP_EQ/BOP_DEBT/TOTAL data for prettier charting: smoothed by 6 month and interpolate
    for iso in ['MAL']:
        key1 = iso + '_NET_LIAB_NSA'
        for k in [key1]:
            new_key = k + 'interpolate'
            if k not in self.raw_data_new_fmt.keys():
                self.indicator[new_key] = pd.DataFrame()
                continue
            if len(self.raw_data_new_fmt[k].index) < 0.0001:
                self.indicator[new_key] = pd.DataFrame()
                continue

            df_BOP = self.raw_data_new_fmt[k].copy()
            if SU.get_freq(df_BOP) == 'M':
                df_BOP.iloc[:, 0] = df_BOP.iloc[:, 0].rolling(window=6).apply(np.mean, raw=True)
            elif SU.get_freq(df_BOP) == 'Q':
                df_BOP.iloc[:, 0] = df_BOP.iloc[:, 0].rolling(window=2).apply(np.mean, raw=True)
            df_BOP = SU.conversion_to_m(df_BOP, method='interpolate')
            df_BOP.rename(columns={df_BOP.columns.tolist()[0]: k}, inplace=True)

            self.indicator[new_key] = df_BOP

    # z_to mapping for 6m Debt flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_DEBT_FLOW_HF_6mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_6m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_6mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_6m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_DEBT_FLOW_HF_6mma']
        right = self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_6mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_DEBT_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std, sd_type='rolling',
                     col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_DEBT_FLOW_HF_6m'] = df

    # z_to mapping for 3m Debt flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_DEBT_FLOW_HF_3mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_3m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_3mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_3m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_DEBT_FLOW_HF_3mma']
        right = self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_3mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_DEBT_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std, sd_type='rolling',
                     col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_DEBT_FLOW_HF_3m'] = df

    # z_to mapping for 1m Debt flows
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        print('mapping equity flow for ', iso)
        if len(self.cleaned_data[iso + '_DEBT_FLOW_HF_1mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_1m'] = pd.DataFrame()
            continue

        if len(self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_1mma'].index) < 0.0001:
            self.indicator[iso + '_DEBT_FLOW_HF_1m'] = pd.DataFrame()
            continue

        # ['AUS','BRA','CAN','CHL','CHN','CZE','EUR','GBR','HUN','IDN','IND','ISE','JPN','KOR','MAL','MEX','NOR','NZD','PHP','POL','RUS','SAF','SWE','THA','TUR','USA']:
        left = self.cleaned_data[iso + '_DEBT_FLOW_HF_1mma']
        right = self.cleaned_data[iso + '_DEBT_NET_LIAB_NSA_1mma']
        df = pd.merge(left, right, left_index=True, right_index=True, how='outer')
        zto_name = iso + '_DEBT_FLOW_HF'
        df = SM.z_to(df, mean_type='rolling', roll_mean=self.roll_mean, roll_sd=self.roll_std, sd_type='rolling',
                     col_number=[-2, -1], new_name=zto_name)
        self.indicator[iso + '_DEBT_FLOW_HF_1m'] = df

    # calc the 6m HF total, IF ONLY ONE SERIES EXISTS, USE THAT SERIES
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX']:
        key1 = iso + '_EQ_FLOW_HF_6m'
        key2 = iso + '_DEBT_FLOW_HF_6m'
        new_key = iso + '_TOTAL_FLOW_HF_6m'
        df_EQ = self.indicator[key1]
        df_DEBT = self.indicator[key2]
        HF_exist = [len(df_EQ.index) > 0.0001, len(df_DEBT.index) > 0.0001]
        if HF_exist == [True, True]:
            df = pd.merge(df_EQ[[iso + '_EQ_FLOW_HF']], df_DEBT[[iso + '_DEBT_FLOW_HF']], left_index=True,
                          right_index=True)
            df[iso + '_TOTAL_FLOW_HF'] = (df[iso + '_EQ_FLOW_HF'] + df[iso + '_DEBT_FLOW_HF']) / 2
            df = SU.delete_zero_beginning(df)
        elif HF_exist == [True, False]:
            df = df_EQ[[iso + '_EQ_FLOW_HF']]
            df.rename(columns={df.columns.tolist()[0]: iso + '_TOTAL_FLOW_HF'}, inplace=True)
        elif HF_exist == [False, True]:
            df = df_DEBT[[iso + '_DEBT_FLOW_HF']]
            df.rename(columns={df.columns.tolist()[0]: iso + '_TOTAL_FLOW_HF'}, inplace=True)
        elif HF_exist == [False, False]:
            print('sorry, this country doesn have any data: ', iso)
            raise IOError
        self.indicator[new_key] = df

    # calc the 1m HF total, IF ONLY ONE SERIES EXISTS, USE THAT SERIES
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX']:
        key1 = iso + '_EQ_FLOW_HF_1m'
        key2 = iso + '_DEBT_FLOW_HF_1m'
        new_key = iso + '_TOTAL_FLOW_HF_1m'
        df_EQ = self.indicator[key1]
        df_DEBT = self.indicator[key2]
        HF_exist = [len(df_EQ.index) > 0.0001, len(df_DEBT.index) > 0.0001]
        if HF_exist == [True, True]:
            df = pd.merge(df_EQ[[iso + '_EQ_FLOW_HF']], df_DEBT[[iso + '_DEBT_FLOW_HF']], left_index=True,
                          right_index=True)
            df[iso + '_TOTAL_FLOW_HF'] = (df[iso + '_EQ_FLOW_HF'] + df[iso + '_DEBT_FLOW_HF']) / 2
            df = SU.delete_zero_beginning(df)
        elif HF_exist == [True, False]:
            df = df_EQ[[iso + '_EQ_FLOW_HF']]
            df.rename(columns={df.columns.tolist()[0]: iso + '_TOTAL_FLOW_HF'}, inplace=True)
        elif HF_exist == [False, True]:
            df = df_DEBT[[iso + '_DEBT_FLOW_HF']]
            df.rename(columns={df.columns.tolist()[0]: iso + '_TOTAL_FLOW_HF'}, inplace=True)
        elif HF_exist == [False, False]:
            print('sorry, this country doesn have any data: ', iso)
            raise IOError
        self.indicator[new_key] = df

    # inverse the FX rate and convert to daily
    for iso in ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']:
        key = ('_').join([iso, 'FXUSD'])
        df = self.raw_data_new_fmt[key].copy()
        df = SU.conversion_to_Day(df)
        df = SU.delete_zero_beginning(df)
        df.iloc[:, 0] = 1 / df.iloc[:, 0]
        key2 = ('_').join([key, 'inversed'])
        self.indicator[key2] = df
    return self.indicator


def create_report_page(self):
    '''
    :param all_dict: usually use the dictionary tha has the result in the form of (iso,df_series) format
    :return: the target is to create academic style
    '''
    # this is to create the report in pdf, from the dictionary generated
    # (which should include all times series, with the ISO country name as the key
    # and all time-series as the item)


chart_start_dt = '2010-01-01'
chart_start_dt_fx = '2013-01-01'
chart_end_dt = datetime.now().strftime('%Y-%m-%d')
# TODO: check first if the PDF file is open, kill pdf first
report = PdfPages(
    os.path.join(self.RPTDIR, self.Short_Name + datetime.now().strftime('%Y%m%d') + '.pdf'))

# create the front page
fig, ax = plt.subplots(1, 1, figsize=(18.27, 12.69))
# plt.subplots_adjust(left=0, bottom=0, right=0, top=0, wspace=0, hspace=0)
last_update = datetime.strftime(datetime.now(), format='%Y-%m-%d')
txt = [['    EM BOP :'], ['    - BOP Net Equity Inflows (%GDP)'], ['    - High Frequency Net Equity Inflows (%GDP)'],
       ['    - BOP Net Debt Inflows (%GDP)'], ['    - High Frequency Net Debt Inflows (%GDP)'], [''],
       ['Last update : ' + last_update], [''], [''], [''], [''], [''], [''], [''], [''], ['']]
collabel = (['EM Flow : '])
# ax.axis('tight')
ax.axis('off')
table = ax.table(cellText=txt, colLabels=collabel, loc='center')
for key, cell in table.get_celld().items():
    cell.set_linewidth(0)
cells = [key for key in table._cells]
print(cells)
for cell in cells:
    table._cells[cell]._loc = 'left'
    table._cells[cell].set_text_props(fontproperties=FontProperties(family='serif', size=20))
    table._cells[cell].set_height(.05)
table._cells[(0, 0)].set_text_props(fontproperties=FontProperties(weight='bold', family='serif', size=30))

# table.scale(1, 4)
report.savefig(fig, bbox_inches='tight', dpi=100)

chart_each_page = 12
chart_rows = 4
chart_cols = 3

iso_list = ['IND', 'IDN', 'KOR', 'THA', 'SAF', 'TUR', 'PHP', 'TAW', 'BRA', 'MEX', 'MAL']
iso_list = sorted([iso + '_' + str(i) for i in range(6) for iso in iso_list])

pages_number = math.ceil(len(iso_list) / chart_each_page)
chart_in_page = [chart_each_page] * (pages_number - 1) + [len(iso_list) - chart_each_page * (pages_number - 1)]
print('chart_in_each_page=', chart_in_page)

print("CREATING RETURNS PAGE")

# print (sorted_list)
# split iso codes into each page!
for i, n in enumerate(chart_in_page):
    fig, axarr = plt.subplots(chart_rows, chart_cols, figsize=(18.27, 12.69), dpi=100)
    start_idx = i * chart_each_page
    end_idx = start_idx + n
    df_in_this_page = iso_list[start_idx:end_idx]
    # print (df_in_this_page)

    for i in range(chart_rows):
        for j in range(chart_cols):
            if i * chart_cols + j < len(df_in_this_page):
                ax = axarr[i, j]
                id = df_in_this_page[i * chart_cols + j]
                # print (i,j,iso_df)
                print(i, j, id)
                # extract the iso code and des for this country
                current_iso = id.split('_')[0]
                number = int(id.split('_')[1])

                if number == 0:  # chart 0: 6m Equity and 6m HF
                    print('charting...', current_iso, number)
                    if current_iso != 'MAL':
                        df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_EQ_FLOW_HF_6m' + '.csv',
                                          index_col=0, header=0)
                    else:
                        df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_TOTAL_FLOW_HF_6m' + '.csv',
                                          index_col=0, header=0)

                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]

                    if current_iso != 'MAL':
                        df2 = pd.read_csv(
                            self.INDICATOR_EXP_DIR + '/' + current_iso + '_EQ_NET_LIAB_NSAinterpolate' + '.csv',
                            index_col=0, header=0)
                    else:
                        df2 = pd.read_csv(
                            self.INDICATOR_EXP_DIR + '/' + current_iso + '_NET_LIAB_NSAinterpolate' + '.csv',
                            index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df1 = df1 * 100
                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    if current_iso != 'MAL':
                        y1 = df1.loc[:, current_iso + '_EQ_FLOW_HF']  # HF data, blue
                        y2 = df2.loc[:, current_iso + '_EQ_NET_LIAB_NSA']  # BOP data, black
                    else:
                        y1 = df1.loc[:, current_iso + '_TOTAL_FLOW_HF']  # HF data, blue
                        y2 = df2.loc[:, current_iso + '_NET_LIAB_NSA']  # BOP data, black

                    if current_iso != 'MAL':
                        ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_EQ_FLOW_HF')
                    else:
                        ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_TOTAL_FLOW_HF')
                    ax.legend(fontsize=7, loc=9, frameon=False)
                    ax.plot(x2, y2, color='black', ls='solid', lw=0.5, label='_nolabel_')

                    ax.set_xlabel('')
                    ax.set_ylabel('')

                    # get the title for this plot
                    if current_iso != 'MAL':
                        list_HF = df1.loc[:, current_iso + '_EQ_FLOW_HF']
                    else:
                        list_HF = df1.loc[:, current_iso + '_TOTAL_FLOW_HF']
                    last_valid_HF = list_HF[~np.isnan(list_HF)][-1]

                    # date
                    if current_iso != 'MAL':
                        last_date = df1.loc[:, [current_iso + '_EQ_FLOW_HF']].dropna().index[-1]
                    else:
                        last_date = df1.loc[:, [current_iso + '_TOTAL_FLOW_HF']].dropna().index[-1]
                    # convert to string
                    try:
                        last_date = last_date.strftime('%Y-%m-%d')
                    except:
                        last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')

                    # if 'EQ' in df.columns.tolist()[-1].split('_'):
                    title = current_iso + ' : ' + " 6m BOP : HF : " + "{0:.2f}".format(
                        last_valid_HF) + ' : ' + last_date

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    # add a zero line
                    ax.axhline(linewidth=0.5, color='k')

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

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))

                elif number == 1:  # 6m debt vs bop
                    print('charting...', current_iso, number)
                    df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_DEBT_FLOW_HF_6m' + '.csv',
                                      index_col=0, header=0)
                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]
                    df2 = pd.read_csv(
                        self.INDICATOR_EXP_DIR + '/' + current_iso + '_DEBT_NET_LIAB_NSAinterpolate' + '.csv',
                        index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df1 = df1 * 100
                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    y1 = df1.loc[:, current_iso + '_DEBT_FLOW_HF']  # HF data, blue
                    y2 = df2.loc[:, current_iso + '_DEBT_NET_LIAB_NSA']  # BOP data, black

                    ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_DEBT_FLOW_HF')
                    ax.legend(fontsize=7, loc=9, frameon=False)
                    ax.plot(x2, y2, color='black', ls='solid', lw=0.5, label='_nolabel_')

                    ax.set_xlabel('')
                    ax.set_ylabel('')

                    # get the title for this plot
                    list_HF = df1.loc[:, current_iso + '_DEBT_FLOW_HF']
                    last_valid_HF = list_HF[~np.isnan(list_HF)][-1]

                    # date
                    last_date = df1.loc[:, [current_iso + '_DEBT_FLOW_HF']].dropna().index[-1]
                    # convert to string
                    try:
                        last_date = last_date.strftime('%Y-%m-%d')
                    except:
                        last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')

                    # if 'EQ' in df.columns.tolist()[-1].split('_'):
                    title = current_iso + ' : ' + " 6m BOP : HF : " + "{0:.2f}".format(
                        last_valid_HF) + ' : ' + last_date

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    # add a zero line
                    ax.axhline(linewidth=0.5, color='k')

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

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))

                elif number == 2:  # FX vs total
                    print('charting...', current_iso, number)
                    df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_FXUSD_inversed' + '.csv',
                                      index_col=0, header=0)
                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]
                    df2 = pd.read_csv(
                        self.INDICATOR_EXP_DIR + '/' + current_iso + '_TOTAL_FLOW_HF_6m' + '.csv',
                        index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    if current_iso != 'TAW':
                        y1 = df1.loc[:, current_iso + '_FXUSD']  # fx data, blue
                    else:
                        y1 = df1.loc[:, 'TWN' + '_FXUSD']  # fx data, blue
                    y2 = df2.loc[:, current_iso + '_TOTAL_FLOW_HF']  # hf data, red

                    line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_FX')
                    # plot FX on the second ax
                    ax2 = ax.twinx()
                    line2 = ax2.plot(x2, y2, color='red', ls='solid', lw=0.5,
                                     label=current_iso + '_TOTAL_FLOW_HF')

                    lns = line1 + line2
                    labs = [l.get_label() for l in lns]
                    # ax.legend(fontsize=8, loc=1, frameon=False)
                    # ax2.legend(fontsize=8, loc=9,frameon=False)
                    ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)
                    # set the range for FX data in y axis
                    fx_max = y1.max()
                    fx_min = y1.min()
                    y_max = fx_max + (fx_max - fx_min) * 0.1
                    y_min = fx_min - (fx_max - fx_min) * 0.1
                    ax.set_ylim(y_min, y_max)

                    ax.set_xlabel('')
                    ax.set_ylabel('')
                    ax2.set_xlabel('')
                    ax2.set_ylabel('')

                    title = '6m ' + current_iso + "_FX vs " + current_iso + '_TOTAL_FLOW_HF'

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    ax2.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    ax2.tick_params(axis='y', labelcolor='r')
                    # add a zero line
                    ax2.axhline(linewidth=0.5, color='k')

                    # set border color and width
                    for spine in ax.spines.values():
                        spine.set_edgecolor('grey')
                        spine.set_linewidth(0.5)
                    for spine in ax2.spines.values():
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
                    ax2.yaxis.tick_left()

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))

                elif number == 3:  # chart3: 1m EQ_HF vs 3m EQ HF
                    print('charting...', current_iso, number)
                    if current_iso != 'MAL':
                        df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_EQ_FLOW_HF_1m' + '.csv',
                                          index_col=0, header=0)
                    else:
                        df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_TOTAL_FLOW_HF_1m' + '.csv',
                                          index_col=0, header=0)
                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]

                    if current_iso != 'MAL':
                        df2 = pd.read_csv(
                            self.INDICATOR_EXP_DIR + '/' + current_iso + '_EQ_FLOW_HF_3m' + '.csv',
                            index_col=0, header=0)
                    else:
                        df2 = pd.read_csv(
                            self.INDICATOR_EXP_DIR + '/' + current_iso + '_TOTAL_FLOW_HF_3m' + '.csv',
                            index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df1 = df1 * 100
                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    if current_iso != 'MAL':
                        y1 = df1.loc[:, current_iso + '_EQ_FLOW_HF']  # HF data, blue
                        y2 = df2.loc[:, current_iso + '_EQ_FLOW_HF']  # HF data, black
                    else:
                        y1 = df1.loc[:, current_iso + '_TOTAL_FLOW_HF']  # HF data, blue
                        y2 = df2.loc[:, current_iso + '_TOTAL_FLOW_HF']  # HF data, black

                    if current_iso != 'MAL':
                        ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_EQ_HF')
                    else:
                        ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_TOTAL_HF')
                    ax.plot(x2, y2, color='black', ls='solid', lw=0.5, label='_NO_LABEL_')
                    ax.legend(fontsize=7, loc=9, frameon=False)

                    ax.set_xlabel('')
                    ax.set_ylabel('')

                    # get the title for this plot
                    if current_iso != 'MAL':
                        list_1m = df1.loc[:, current_iso + '_EQ_FLOW_HF']
                    else:
                        list_1m = df1.loc[:, current_iso + '_TOTAL_FLOW_HF']

                    last_valid_1m = list_1m[~np.isnan(list_1m)][-1]

                    # date
                    if current_iso != 'MAL':
                        last_date = df1.loc[:, [current_iso + '_EQ_FLOW_HF']].dropna().index[-1]
                    else:
                        last_date = df1.loc[:, [current_iso + '_TOTAL_FLOW_HF']].dropna().index[-1]
                    # convert to string
                    try:
                        last_date = last_date.strftime('%Y-%m-%d')
                    except:
                        last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')

                    # if 'EQ' in df.columns.tolist()[-1].split('_'):
                    title = current_iso + ' : ' + " 3m HF vs 1m HF : " + "{0:.2f}".format(
                        last_valid_1m) + ' : ' + last_date

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    # add a zero line
                    ax.axhline(linewidth=0.5, color='k')

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

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                elif number == 4:  # chart4: 1m DEBT_HF vs 3m DEBT_HF
                    print('charting...', current_iso, number)
                    df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_DEBT_FLOW_HF_1m' + '.csv',
                                      index_col=0, header=0)
                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]
                    df2 = pd.read_csv(
                        self.INDICATOR_EXP_DIR + '/' + current_iso + '_DEBT_FLOW_HF_3m' + '.csv',
                        index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df1 = df1 * 100
                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    y1 = df1.loc[:, current_iso + '_DEBT_FLOW_HF']  # 1M DATA BLUE
                    y2 = df2.loc[:, current_iso + '_DEBT_FLOW_HF']  # 3M DATA BLUE

                    ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_DEBT_HF')
                    ax.plot(x2, y2, color='black', ls='solid', lw=0.5, label='_NOLABEL_')
                    ax.legend(fontsize=7, loc=9, frameon=False)

                    ax.set_xlabel('')
                    ax.set_ylabel('')

                    # get the title for this plot
                    list_1m = df1.loc[:, current_iso + '_DEBT_FLOW_HF']
                    last_valid_1m = list_1m[~np.isnan(list_1m)][-1]

                    # date
                    last_date = df1.loc[:, [current_iso + '_DEBT_FLOW_HF']].dropna().index[-1]
                    # convert to string
                    try:
                        last_date = last_date.strftime('%Y-%m-%d')
                    except:
                        last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')

                    # if 'EQ' in df.columns.tolist()[-1].split('_'):
                    title = current_iso + ' : ' + " 3m HF vs 1m HF : " + "{0:.2f}".format(
                        last_valid_1m) + ' : ' + last_date

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    # add a zero line
                    ax.axhline(linewidth=0.5, color='k')

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

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                elif number == 5:  # chart4: 1m Total vs FX since 2013
                    print('charting...', current_iso, number)
                    chart_start_dt_FX = '2013-01-01'
                    df1 = pd.read_csv(self.INDICATOR_EXP_DIR + '/' + current_iso + '_FXUSD_inversed' + '.csv',
                                      index_col=0, header=0)
                    if len(df1.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df1.index >= chart_start_dt_FX) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]
                    df2 = pd.read_csv(
                        self.INDICATOR_EXP_DIR + '/' + current_iso + '_TOTAL_FLOW_HF_1m' + '.csv',
                        index_col=0, header=0)
                    if len(df2.index) < 0.001:
                        self.set_ax_invisible(axarr[i, j])
                        continue
                    mask = (df2.index >= chart_start_dt_FX) & (df2.index <= chart_end_dt)
                    df2 = df2.loc[mask, :]

                    df2 = df2 * 100

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    x2 = pd.to_datetime(df2.index).date
                    # print (type(x[0]))
                    if current_iso != 'TAW':
                        y1 = df1.loc[:, current_iso + '_FXUSD']  # fx data, blue
                    else:
                        y1 = df1.loc[:, 'TWN' + '_FXUSD']  # fx data, blue
                    y2 = df2.loc[:, current_iso + '_TOTAL_FLOW_HF']  # hf data, red

                    line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.5, label=current_iso + '_FX')
                    # plot FX on the second ax
                    ax2 = ax.twinx()
                    line2 = ax2.plot(x2, y2, color='red', ls='solid', lw=0.5,
                                     label=current_iso + '_TOTAL_FLOW_HF')

                    lns = line1 + line2
                    labs = [l.get_label() for l in lns]
                    # ax.legend(fontsize=8, loc=1, frameon=False)
                    # ax2.legend(fontsize=8, loc=9,frameon=False)
                    ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)
                    # set the range for FX data in y axis
                    fx_max = y1.max()
                    fx_min = y1.min()
                    y_max = fx_max + (fx_max - fx_min) * 0.1
                    y_min = fx_min - (fx_max - fx_min) * 0.1
                    ax.set_ylim(y_min, y_max)

                    ax.set_xlabel('')
                    ax.set_ylabel('')
                    ax2.set_xlabel('')
                    ax2.set_ylabel('')

                    title = '1m ' + current_iso + "_FX vs " + current_iso + '_TOTAL_FLOW_HF'

                    ax.set_title(title, y=1, fontsize=8, fontweight=600)
                    # add legend
                    # legend_name = df.columns.tolist()[-2]
                    # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                    # set ticks label size and width
                    ax.tick_params(labelsize=5, width=0.01)
                    ax2.tick_params(labelsize=5, width=0.01)
                    # change the y tick label to blue
                    ax.tick_params(axis='y', labelcolor='b')
                    ax2.tick_params(axis='y', labelcolor='r')
                    # add a zero line
                    ax2.axhline(linewidth=0.5, color='k')

                    # set border color and width
                    for spine in ax.spines.values():
                        spine.set_edgecolor('grey')
                        spine.set_linewidth(0.5)
                    for spine in ax2.spines.values():
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
                    ax2.yaxis.tick_left()

                    # set date max
                    datemax = np.datetime64(x1[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    if x1[-1].month > 10:
                        ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                    else:
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                else:
                    print('The chart has not been plotted', current_iso, number)

            else:
                self.set_ax_invisible(axarr[i, j])

    plt.tight_layout()
    report.savefig(fig, bbox_inches='tight')  # the current page is saved
report.close()
plt.close('all')


def set_ax_invisible(self, ax):
    ax.axis('off')


@cache_response('FLOW_EM_BOP_EQ_BOND_HF', 'disk_8h')
def get_data(*args, **kwargs):
    sig = signal3()
    result_dict = sig.run_data_dict(*args, **kwargs)
    return result_dict


if __name__ == "__main__":
    clear_cache('FLOW_EM_BOP_EQ_BOND_HF', 'disk_8h')
    reporting_to = sys.argv[1] if len(sys.argv) > 1.01 else None
    data = get_data(run_chart=True, reporting_to=reporting_to)
    print(data)

