# import sys when runing from the batch code
import sys

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import Analytics.series_utils as su
import Analytics.chart_help as ch
import panormus.data.bo.econ as econ

SU = su.date_utils_collection()
from matplotlib import pyplot as plt
import panormus.data.haver as haver
from panormus.utils.cache import cache_response, clear_cache


# collection of method for all the motheods of scoring, standardisation etc.
class scoring_method_collection(object):
    def __init__(self):
        self.SU = su.date_utils_collection()

    def z(self, df, mean_type='full', sd_type='full', extend_back=True, extend_forward=True, roll_mean=12,
          custom_mean=0, roll_sd=12, custom_sd=0.1, col_number=-1, new_name='z_score', keep_mean_sd_col=False,
          ewm_alpha=None, **kwargs):
        '''
        calculate the z-score of the time series
        :param df: df better with dates as the index
        :param mean_type: what type of mean is used, full period, rolling mean or custom (character, default "full")
        :param sd_type: what type of standard deviation is used, full, rolling or custom
        :param roll_mean: rolling window of mean
        :param custom_mean: customed mean, a number
        :param roll_sd: rolling window of the standard deviation
        :param extend_back: if performing rolling estimation of mean and std, extend back allows to back fill the mean and std backward so that the first rolling window can be used
        :param extend_forward: same with extend_back but fill ahead
        :param custom_sd: customed standard deviation
        :param col_number: which column to perform the transfermation
        :param new_name: the new name for the
        :return: return a new time series which contains the original time series and the z_score
        '''
        df = df.copy()
        column_name = df.columns.tolist()[col_number]
        # check have enough data
        first_idx = df.loc[:, [column_name]].first_valid_index()
        last_idx = df.loc[:, [column_name]].last_valid_index()
        data_len = len(df.loc[first_idx:last_idx, :].index)
        if (mean_type == 'rolling') & (roll_mean >= data_len):
            print(
                'Warning: ' + column_name + 'does not have enough data to perform the rolling calculation, use the full mean_type instead')
            mean_type = 'full'
            sd_type = 'full'

        # create the mean column
        df['mean'] = np.nan
        if mean_type == 'full':
            df['mean'] = df.loc[:, column_name].mean(axis=0, skipna=True)
        elif mean_type == 'rolling':
            df['mean'] = self.SU.rolling_ignore_nan(df.loc[:, [column_name]], roll_mean, np.mean)
            if extend_back == True:  # extend back the rolling mean to the first data point
                df['mean'].fillna(inplace=True, method='backfill')
            if extend_forward == True:  # extend back the rolling mean to the first data point
                df['mean'].fillna(inplace=True, method='ffill')
        elif mean_type == 'ewm':  # exponential moving average
            if ewm_alpha == None:
                if self.SU.get_freq(df[[column_name]].dropna()) == 'D':
                    alpha = 0.005
                elif self.SU.get_freq(df[[column_name]].dropna()) == 'M':
                    alpha = 0.05
            else:
                alpha = ewm_alpha
            df['mean'] = df[column_name].ewm(alpha=alpha, ignore_na=True).mean()
            if extend_back == True:  # extend back the rolling mean to the first data point
                df['mean'].fillna(inplace=True, method='backfill')
            if extend_forward == True:  # extend back the rolling mean to the first data point
                df['mean'].fillna(inplace=True, method='ffill')

        if mean_type == 'custom':
            df['mean'] = custom_mean
        z_sd_option = kwargs.get('z_sd_option')
        # create the stdev column
        df['sd'] = np.nan
        if sd_type == 'full':
            df['sd'] = df.loc[:, column_name].std(axis=0, skipna=True)
        if sd_type == 'rolling':
            # just use full sample std if the length is not long enough
            if len(df.loc[:, [column_name]].dropna().index) - 1 < roll_sd:
                df['sd'] = df.loc[:, column_name].std(axis=0, skipna=True)
            else:
                df['sd'] = self.SU.rolling_ignore_nan(df.loc[:, [column_name]], roll_sd, np.std)

            if extend_back == True:  # extend back the rolling mean to the first data point
                df['sd'].fillna(inplace=True, method='backfill')
            if extend_forward == True:  # extend back the rolling mean to the first data point
                df['sd'].fillna(inplace=True, method='ffill')
            if z_sd_option in ['2010_2016']:
                sd_2010_2016 = df.loc['2010':'2016', column_name].std()
                df.loc['2010':, 'sd'] = sd_2010_2016
        elif sd_type == 'ewm':
            if ewm_alpha == None:
                if self.SU.get_freq(df[[column_name]].dropna()) == 'D':
                    alpha = 0.005
                elif self.SU.get_freq(df[[column_name]].dropna()) == 'M':
                    alpha = 0.05
            else:
                alpha = ewm_alpha
            df['d_mean'] = df[column_name] - df['mean']

            df['vol'] = df['d_mean'] * df['d_mean']
            df['sd'] = df['vol'].ewm(alpha=alpha, ignore_na=True).mean().shift(1)
            df['sd'] = df['sd'] ** (0.5)
            if extend_back == True:  # extend back the rolling mean to the first data point
                df['sd'].fillna(inplace=True, method='backfill')
            if extend_forward == True:  # extend back the rolling mean to the first data point
                df['sd'].fillna(inplace=True, method='ffill')
            df.drop(columns=['d_mean', 'vol'], inplace=True)
            if z_sd_option in ['2010_2016']:
                sd_2010_2016 = df.loc['2010':'2016', column_name].std()
                df.loc['2010':, 'sd'] = sd_2010_2016
        if sd_type == 'custom':
            df['sd'] = custom_sd

        # create the z_score column
        assert isinstance(new_name, str), 'new name must be a string' + str(new_name)
        df[new_name] = (df[column_name] - df['mean']) / df['sd']

        if keep_mean_sd_col:
            return df
        else:
            return df.drop(columns=['mean', 'sd'])

    # NOTE: personally think this method is quite similar to the regerssion method, just standardize the series1 to series2


def z_to(self, df, mean_type='full', sd_type='full', roll_mean=12, custom_mean=0, roll_sd=12, extend_back=True,
         extend_forward=True, custom_sd=0.1, col_number=[-2, -1], new_name='z_to'):
    '''
    :param df: a df that has already at least 2 columns, want to pr
    :param mean_type: same as z
    :param sd_type: same as z
    :param roll_mean: same as z
    :param custom_mean: same as z
    :param roll_sd: same as z
    :param custom_sd: same as z
    :param col_number: same as z
    :param new_name: same as z
    :param extend_back: in the rolling_mean/rolling_std, whether to extend backward before the first available data point
    :param extend_forward: same with extend_back but fill ahead
    :return: convert series1 to a z score, plus the mean of the second series, and times the std of the second time series to map the first time series to the second
    '''
    df = df.copy()
    # get the column names
    col_name1 = df.columns.tolist()[col_number[0]]
    col_name2 = df.columns.tolist()[col_number[1]]
    # produce the z_score
    df1 = df[[col_name1]]
    df2 = df[[col_name2]]
    df1 = self.z(df1, mean_type=mean_type, sd_type=sd_type, extend_back=extend_back, extend_forward=extend_forward,
                 roll_mean=roll_mean, custom_mean=custom_mean, roll_sd=roll_sd, custom_sd=custom_sd, col_number=-1,
                 new_name='z_score1', keep_mean_sd_col=True)
    df2 = self.z(df2, mean_type=mean_type, sd_type=sd_type, roll_mean=roll_mean, custom_mean=custom_mean,
                 roll_sd=roll_sd, custom_sd=custom_sd, col_number=-1, new_name='z_score2', keep_mean_sd_col=True)
    df1['z_to'] = df1['z_score1'] * df2['sd'] + df2['mean']
    assert isinstance(new_name, str), 'new name must be a string' + str(new_name)
    df[new_name] = df1['z_to']
    return df


def add_Zs(self, zs, w_override=None, rolling_window=252 * 20, fill_backward_na_with_zero=True, **kwargs):
    # if the series has only 1 sub-component, no re-z should be happening
    # get rid of those has 0 weight first
    default_add_Zs_param = {'re_z_sd_option': None}
    default_add_Zs_param.update(kwargs.get('override_add_Zs_param'))
    if not w_override in [None]:
        new_zs = []
        new_ws = []
        for df, w in zip(zs, w_override):
            if not np.abs(w * 1000000000000) < 0.000001:
                new_zs.append(df)
                new_ws.append(w)
        zs = new_zs
        w_override = new_ws

    number_of_series = len([w for w in w_override if w not in [0, 0.0]])

    if len(zs) <= 0.0001:
        return
    if w_override in [None]:
        w = [1 / len(zs) for i in range(len(zs))]
    else:
        sum_w = sum(w_override) if np.abs(sum(w_override)) > 0.00000001 else 1
        w = [i / sum_w for i in w_override]
    assert len(zs) == len(w), ' Sorry, the number of z scores is different from the number of the weights'
    # get the title
    new_name = ''
    for df in zs:
        if new_name == '':
            new_name = df.columns.tolist()[0]
        else:
            new_name = new_name + '+' + df.columns.tolist()[0]
    title = new_name + '_re_z'

    df_comb_z = None
    for i, df in enumerate(zs):
        if df_comb_z is None:
            df_comb_z = df.copy().dropna() * w[i]
        else:
            df_comb_z = pd.merge(df_comb_z, df.copy().dropna() * w[i], left_index=True, right_index=True, how='outer',
                                 sort=True)
    if fill_backward_na_with_zero:
        # this is to keep as long backward history as possible
        df_comb_z = self.SU.fill_backward_with_zero(df_comb_z)

    df_comb_z['sum'] = df_comb_z.sum(axis=1, skipna=False)
    df_comb_z = df_comb_z[['sum']]
    df_comb_z = self.SU.delete_zero_beginning(df_comb_z)
    if number_of_series > 1.01:
        df_comb_z = self.z(df_comb_z, mean_type='custom', custom_mean=0, sd_type='rolling', roll_sd=rolling_window,
                           new_name=title, z_sd_option=default_add_Zs_param['re_z_sd_option'])
    return df_comb_z[[title]]
    else:
    return df_comb_z


def breakdown_of_Zs(self, zs, w_override=None, type='z', z_to_target=None, rolling_window=252 * 20,
                    fill_backward_na_with_zero=True):
    '''
    :param zs:
    :return: the breakdown of Zs
    '''
    df_all_z = pd.concat(zs, axis=1)
    # construct the override weight


@staticmethod
def sigmoid_array(x):
    return 1 / (1 + np.exp(-x)) - 0.5


def extend_A_with_B_actual_release(self, df_low_freq, df_high_freq, release_lag1, release_lag2, df1_freq,
                                   assigned_w_B=None, new_name='extended_series'):
    # this method is used to extend the better quality but lower frequency series A, with higher frequency but noisy series B
    # the methodology is the weighted average of the A's own past history and the latest available series B
    # step 1 : understand how many lags need to be filled in
    # step 2 : zto from B to A, to bring in the same scale, conduct frequency transformation if necessary
    # step 3 : run regression (20 years rolling, no constant coefficient) to determine the weights
    # step 4 : apply lags and weighted average

    # this method is used to extend the better quality but lower frequency series A, with higher frequency but noisy series B
    # the methodology is the weighted average of the A's own past history and the latest available series B
    # step 1 : understand how many lags need to be filled in
    # step 2 : zto from B to A, to bring in the same scale, conduct frequency transformation if necessary
    # step 3 : run regression (20 years rolling, no constant coefficient) to determine the weights
    # step 4 : apply lags and weighted average
    import Analytics.wfcreate as wf
    new_wf = wf.swf()
    df1 = df_low_freq.copy()
    df2 = df_high_freq.copy()
    # convert df2 to df1's frequency, and apply z_to
    if df1_freq == 'M':
        df2 = SU.conversion_down_to_m(df2)
    elif df1_freq == 'Q':
        df2 = SU.conversion_to_q(df2)

    # convert B to the A
    df_comb = pd.merge(df2.dropna(), df1.dropna(), left_index=True, right_index=True, how='outer')
    if df1_freq == 'M':
        rolling_win = 12 * 20
    elif df1_freq == 'Q':
        rolling_win = 4 * 20
    else:
        rolling_win = 252 * 20

    df_comb = self.z_to(df_comb, mean_type='rolling', sd_type='rolling', roll_mean=rolling_win, roll_sd=rolling_win)

    if assigned_w_B == None:
        # get the correlation, to determine the weights

        _df1 = df_comb.iloc[:, [1]]
        last_idx = _df1.last_valid_index()
        length_of_missing = len([i for i in _df1.index if i > last_idx])
        _df1['lagged'] = _df1.shift(length_of_missing)
        _corr = _df1.loc['2000':].dropna().corr()
        _w1 = _corr.values[0][1]
        # get the correlation of df1 and df2
        _df1 = df_comb.iloc[:, [1, 2]]
        _corr = _df1.loc['2000':].dropna().corr()
        _w2 = _corr.values[0][1]

        w1 = _w1 / (_w1 + _w2)
        w2 = 1 - w1
    else:
        w2 = assigned_w_B
        w1 = 1 - w2

    # construct the new, etimated, higher frequency index
    if 'df1' in new_wf.df.keys():
        del new_wf.df['df1']
    if 'df2' in new_wf.df.keys():
        del new_wf.df['df2']

    new_wf.add_df('df1', df1)
    new_wf.df['df1'] = new_wf.df['df1'].shift(release_lag1)
    new_wf.add_df('df2', df_comb[['z_to']])
    new_wf.df['df2'] = new_wf.df['df2'].shift(release_lag2)

    # adding together
    new_wf.df['df2'].columns = new_wf.df['df1'].columns
    df = new_wf.df['df1'] * w1 + new_wf.df['df2'] * w2
    df.columns = [new_name]
    return df


def extend_A_with_B_simple(self, df_low_freq, df_high_freq, df1_freq, assigned_w_B=None, new_name='extended_series'):
    # simply extend the end of low frequency with the high frequency, without taking care of the history.
    import Analytics.wfcreate as wf
    new_wf = wf.swf()
    df1 = df_low_freq.copy()
    df2 = df_high_freq.copy()
    # convert df2 to df1's frequency, and apply z_to
    if df1_freq == 'M':
        df2 = SU.conversion_down_to_m(df2)
    elif df1_freq == 'Q':
        df2 = SU.conversion_to_q(df2)

    # convert B to the A
    df_comb = pd.merge(df2.dropna(), df1.dropna(), left_index=True, right_index=True, how='outer')
    if df1_freq == 'M':
        rolling_win = 12 * 20
    elif df1_freq == 'Q':
        rolling_win = 4 * 20
    else:
        rolling_win = 252 * 20

    df_comb = self.z_to(df_comb, mean_type='rolling', sd_type='rolling', roll_mean=rolling_win, roll_sd=rolling_win)
    _df1 = df_comb.iloc[:, [1]]
    last_idx = _df1.last_valid_index()
    length_of_missing = len([i for i in _df1.index if i > last_idx])
    # get the correlation, to determine the weights
    if assigned_w_B == None:
        _df1['lagged'] = _df1.shift(length_of_missing)
        _corr = _df1.loc['2000':].dropna().corr()
        _w1 = _corr.values[0][1]
        # get the correlation of df1 and df2
        _df1 = df_comb.iloc[:, [1, 2]]
        _corr = _df1.loc['2000':].dropna().corr()
        _w2 = _corr.values[0][1]

        w1 = _w1 / (_w1 + _w2)
        w2 = 1 - w1

else:
w2 = assigned_w_B
w1 = 1 - w2

df1['estimated'] = df1.shift(length_of_missing).iloc[:, 0] * w1 + df_comb['z_to'] * w2
df1.iloc[:, 0].fillna(df1['estimated'], inplace=True)

df = df1.iloc[:, [0]]
df.columns = [new_name]
return df

if __name__ == "__main__":
    # CANADA data
    SM = scoring_method_collection()

