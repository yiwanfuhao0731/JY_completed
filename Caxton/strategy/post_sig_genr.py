import pandas as pd
import csv
import numpy as np
import os
import pickle
from panormus.quant.portfolio import annual_return, annual_vol, calmar_ratio
from qs_common.trading.tradeutils import get_min_trade_within_tolerance

from Analytics.scoring_methods import scoring_method_collection as smc
from Analytics.series_utils import date_utils_collection as s_util
from qs_common.trading import tradeutils as trade_util
import warnings

# from qm_common.reporting.rp_pairwise_correl import PwCorrReport

SM = smc()
SU = s_util()


def dump_wf_obj_to_csv(work_file, dir):
    l_df = []
    existing_col = []
    for k, v in work_file.df.items():
        col = v.columns
        new_col = []
        for i in col:
            if i in existing_col:
                i = i + '_(' + k + ')'
                new_col.append(i)
            else:
                new_col.append(i)
            existing_col.append(i)
        v.columns = new_col
        l_df.append(v)
    pd.concat(l_df, axis=1).to_csv(dir)


def dump_perf_to_output(work_file, dir):
    create_folder(dir)
    cumprof_csv_dir = os.path.join(dir, 'cumprof.csv')
    pd.concat([work_file.df['cumprof'].dropna(), work_file.df['ret'].dropna()], axis=1).to_csv(cumprof_csv_dir)


def create_folder(path):
    "Creates all folders necessary to create the given path."
    try:
        if not os.path.exists(path):
            os.makedirs(path)

except IOError as io:
print("Cannot create dir %s" % path)
raise io


def write_pars_to_csv(param, param_csv_dir):
    with open(param_csv_dir, 'w') as f:  # Just use 'w' mode in 3.x
        w = csv.DictWriter(f, param.keys())
        w.writeheader()
        w.writerow(param)


def LS_filtered(df_indicator, df_sci, method='s_curve', country_order=[], tc_reduction_method='none',
                inertia_torlerance=[0]):
    '''
    translate the indicator group (filtered by sci) into a signal group.
    Note that the indicator group and sci group must have the same number of columns.
    If method is s_curve, then the position size is the score map to the sigmoid function

    param: exec_torlerance: the tolerance parameter in the t-cost reducing algorithms. The larger the torlerance, the smoothier the signals
    '''
    assert len(df_indicator.columns) == len(df_sci.columns), "Sorry, indicator group has different column with sci"
    assert len(df_indicator.index) == len(df_sci.index), "Sorry, indicator group has different index with sci"
    if len(df_indicator.columns) > 1.01:
        assert len(country_order) == len(
            df_indicator.columns), 'Sorry, please provide the correct order of countries in the indicator group'

    df_indicator_ = df_indicator.copy()
    df_sci_ = df_sci.copy()

    # step1: filtering out the period where the sci index doesn't exist.
    # last index should shift 1 day forward. e.g. allow signal exist for 1 day after the last sci day.

    first_idx_list = [df_sci_[[s]].first_valid_index() for s in df_sci_.columns]
    last_idx_list = [df_sci_[[s]].shift(1).last_valid_index() for s in df_sci_.columns]

    counter = 0
    for f, l in zip(first_idx_list, last_idx_list):
        col = df_indicator_.columns.tolist()[counter]
        mask = (df_indicator_.index > l) | (df_indicator_.index < f)
        df_indicator_.loc[mask, col] = np.nan

    signal_group = df_indicator_.copy()
    signal_group.loc[:, :] = np.nan

    if method == 's_curve':
        df_indicator_ = SM.sigmoid_array(df_indicator_)
        signal_group = df_indicator_ * 1000 * 2
    elif method == 'binary':
        signal_group = ((df_indicator_ > 0) * 2 - 1) * 1000
    elif method == 'origin_z':
        signal_group = df_indicator_

    signal_group.columns = [i for i in df_indicator_.columns]
    signal_group_flip = -signal_group
    signal_group_flip.columns = ['condition-pricing'] if len(signal_group_flip.columns) < 1.01 else [
        'condition-pricing (' + iso + ')' for iso in country_order]

    actual_trade_group = tc_reduction(signal_group, tc_reduction_method=tc_reduction_method,
                                      inertia_tolerance=inertia_torlerance)

    return {
        'signal_group': signal_group,
        'signal_group_flip': signal_group_flip,
        'actual_trade_group': actual_trade_group,
    }


def apply_inertia(signal_group, tc_reduction_method='none', inertia_torlerance=[0]):
    signal_group = signal_group.copy()
    actual_trade_group = tc_reduction(signal_group, tc_reduction_method=tc_reduction_method,
                                      inertia_tolerance=inertia_torlerance)
    signal_group_flip=-signal_group
    return {
        'signal_group': signal_group,
        'signal_group_flip': signal_group_flip,
        'actual_trade_group': actual_trade_group,
    }

def HmL_filtered(df_indicator, df_sci, type='number', cross=1, method='equal', capital=1000):
    # HmL basket which returns the high ranking to be positive and low ranking to be negative
    # all convert into matrix
    _mat = df_indicator.as_matrix().astype(np.float)
    _sci_mat = df_sci.as_matrix().astype(np.float)
    _precondition = (~np.isnan(_sci_mat))
    print('the shape of mat are', _mat.shape, _sci_mat.shape)

    # check the shape
    if _mat.shape != _precondition.shape:
        print("The matrix and mask matrix have different size! Please check!!!")

    else:
        output = _mat.copy()
        output[:] = np.nan

        if type == 'number':
            minimum_avail = cross * 2
            for i in range(_mat.shape[0]):
                this_mask = _precondition[i]
                if np.sum(this_mask) < minimum_avail:
                    pass
                else:
                    this_row = _mat[i]
                    # get the decending order of the masked matrix, select the first n of the index.
                    selec_index = np.argsort(-this_row[this_mask])[
                                  :cross]  # descending order ranking index, select the top quantile
                    # initialise an empty signal row
                    this_row_sig = this_row[this_mask].copy()
                    this_row_sig[:] = np.nan
                    if method == 'equal':
                        this_row_sig[selec_index] = capital / cross

                    # now the bottom part
                    selec_index = np.argsort(this_row[this_mask])[:cross]
                    if method == 'equal':
                        this_row_sig[selec_index] = -capital / cross
                    # fill in the output
                    output[i][this_mask] = this_row_sig
                    output[i][np.isnan(output[i])] = 0
        print('output shape is ', output.shape)
        signal_group = pd.DataFrame(data=output, index=df_indicator.index,
                                    columns=['signal_' + i for i in df_indicator.columns])
        for i in range(len(signal_group.columns)):
            first_idx = signal_group.iloc[:, [i]].first_valid_index()
            last_idx = signal_group.iloc[:, [i]].last_valid_index()
            signal_group.loc[first_idx:last_idx, :].iloc[:, i].fillna(0, inplace=True)
        signal_group_flip = -signal_group
        signal_group_flip.columns = ['(flipped)' + i for i in signal_group.columns]
        return signal_group, -signal_group


def profit(signal_panel, sci_panel, riskcapital, profsmpl):
    '''
    the function uses yesterday's signal and return from yesterday to today to calculate the return
    important: note that the date in return series refer to the day later than the signal
    '''
    assert len(signal_panel.columns) == len(
        sci_panel.columns), "Sorry, the number of signal col is differnt from number of sci col"
    assert len(signal_panel.index) == len(sci_panel.index), "Sorry, the number of index is different"

    ret_panel = sci_panel.pct_change(periods=1)
    ret_panel.columns = range(len(ret_panel.columns))
    ret_panel = SU.delete_zero_tail(ret_panel)

    sig_panel_1 = signal_panel.shift(1)
    sig_panel_1.columns = range(len(sig_panel_1.columns))

    sig_panel_1 = sig_panel_1 * ret_panel
    last_dt = sig_panel_1.last_valid_index()
    sig_panel_1['pnl'] = sig_panel_1.sum(axis=1)
    mask = (sig_panel_1.index > last_dt)
    sig_panel_1.loc[mask, :] = np.nan
    pnl = sig_panel_1[['pnl']]  # dollar pnl

    profsmpl = [pd.to_datetime(i) for i in profsmpl]
    mask = (pnl.index <= profsmpl[0]) | (pnl.index > profsmpl[1])
    pnl.loc[mask, :] = np.nan

    pnl['reinvestprof'] = (pnl['pnl'] / riskcapital + 1).cumprod() * riskcapital
    pnl.loc[pnl.index == profsmpl[0], 'reinvestprof'] = riskcapital
    pnl['cumprof'] = pnl['pnl'].cumsum()
    pnl['ret'] = pnl['pnl'] / riskcapital

    return pnl


def tc_reduction(raw_signal_group, tc_reduction_method='none', inertia_tolerance=[0]):
    if tc_reduction_method == 'none':
        return raw_signal_group
    elif tc_reduction_method == 'inertia':
        assert len(raw_signal_group.columns) == len(
            inertia_tolerance), 'Please make sure the number of signal is the same as the number of transaction cost reduction torlerance provided'
        df_list = []
        for tor, sig in zip(inertia_tolerance, raw_signal_group.columns):
            actual_trade = raw_signal_group.loc[:, [sig]].dropna().copy()
            actual_trade['current'] = np.nan
            actual_trade.loc[:, 'current'].iloc[0] = actual_trade.iloc[0, 0]
            actual_trade_matrix = actual_trade.as_matrix()
            for i in range(actual_trade_matrix.shape[0] - 1):
                amount = trade_util.get_min_trade_within_tolerance(actual_trade_matrix[i + 1][0],
                                                                   actual_trade_matrix[i][1], tor)
                actual_trade_matrix[i + 1][1] = actual_trade_matrix[i][1] + amount
            actual_trade.loc[:, :] = actual_trade_matrix
            df = actual_trade.iloc[:, [1]]
            df.columns = [actual_trade.columns[0] + ' (actual_trading_position)']
            df_list.append(df)
        actual_trade_group = pd.concat(df_list, axis=1)
        return actual_trade_group
    elif tc_reduction_method == 'dynamic_inertia':
        pass


def apply_risk_management_rule(df_underlying_signal_group, df_underlying_ret_group, rule_type=None, param_dict=None):
    '''
    :param df_underlying_signal_group: the underlying signal gorup
    :param df_underlying_ret_group: the underlying ret generated by the underlying signal group
    :param rule_type:
                    a- if_works_top_up:
                        if making money from close_T-1 to close_T, topping up, the logic is that:
                        we know that the model is calibrated to capture the big move in the market, and we know that the model
                        is faded away too quickly. keep the model running when model is making money

                    param_dict : rolling_win,z_thres

                    b- if_works_cash_in:

    :return: the actual trading group
   '''
    df_underlying_signal_group, df_underlying_ret_group = df_underlying_signal_group.copy(), df_underlying_ret_group.copy()
    if rule_type in ['None', None]:
        return df_underlying_signal_group
    elif rule_type in ['if_higher_top_up']:
        # remove the zeros at the beginning
        df_underlying_signal_group_list = []
        for col in df_underlying_signal_group.columns:
            df_underlying_signal_group_list.append(SU.delete_zero_beginning(df_underlying_signal_group.loc[:, [col]]))
        if len(df_underlying_signal_group_list) > 1.01:
            df_underlying_signal_group = pd.concat(df_underlying_signal_group_list, axis=1)
        df_underlying_ret_group_list = []
        for col in df_underlying_ret_group.columns:
            df_underlying_ret_group_list.append(SU.delete_zero_beginning(df_underlying_ret_group.loc[:, [col]]))
        if len(df_underlying_ret_group_list) > 1.01:
            df_underlying_ret_group = pd.concat(df_underlying_signal_group_list, axis=1)

        assert df_underlying_ret_group.shape[1] == df_underlying_signal_group.shape[
            1], ' sorry, the number of column number of underlying signal and underlying ret series are not the same'
        signal_group_res_list = []
        for signal_col, ret_col in zip(df_underlying_signal_group.columns, df_underlying_ret_group.columns):
            this_signal, this_ret = df_underlying_signal_group.loc[:,
                                    [signal_col]].dropna(), df_underlying_ret_group.loc[:, [ret_col]].dropna()
            # calc the z_score of return series, to check if the underlying model works
            this_ret['rolling_ret'] = this_ret.iloc[:, 0].rolling(window=param_dict['rolling_win'],
                                                                  min_periods=param_dict['rolling_win']).mean()
            new_name = 'rolling_z'
            this_ret = SM.z(this_ret, mean_type='custom', custom_mean=0, sd_type='rolling', roll_sd=252 * 2,
                            new_name=new_name)
            this_ret['higher'] = 0
            mask = this_ret['rolling_z'] >= param_dict['z_thres']
            this_ret.loc[mask, 'higher'] = 1
            df_comb = pd.merge(this_signal, this_ret.loc[:, ['higher']], left_index=True, right_index=True, how='left')
            df_comb.fillna(0, inplace=True)
            # find the max position in the past 21 days that have the same sign as signal_today
            df_comb['rolling_max'] = df_comb.iloc[:, 0].rolling(window=42).max()
            df_comb['rolling_min'] = df_comb.iloc[:, 0].rolling(window=42).min()
            df_comb['final'] = np.nan
            mask = (df_comb['higher'] == 1) & (df_comb.iloc[:, 0] > 0)
            df_comb.loc[mask, 'final'] = df_comb['rolling_max']
            mask = (df_comb['higher'] == 1) & (df_comb.iloc[:, 0] < 0)
            df_comb.loc[mask, 'final'] = df_comb['rolling_min']
            df_comb['final'].fillna(df_comb.iloc[:, 0], inplace=True)
            res = df_comb.loc[:, ['final']]
            res.columns = this_signal.columns
            signal_group_res_list.append(res)
        new_signal_group = pd.concat(signal_group_res_list, axis=1)
        return new_signal_group


def returnstats_dict(df_equitycurve, riskcapital, profsmpl, benchmark=None):
    '''
    return ann_mean, ann_std, ann_sharpe, drawdown_series, and rolling correlation
    '''
    mask = (df_equitycurve.index < profsmpl[0]) | (df_equitycurve.index > profsmpl[1])
    df1 = df_equitycurve.iloc[:, [0]]  # get the dollar pnl
    df1.loc[mask, :] = np.nan

    df1 = df1.dropna() / riskcapital

    ann_mean = annual_return(df1).values[0] * 100
    ann_std = annual_vol(df1).values[0] * 100
    ann_sharpe = ann_mean / ann_std
    calmar = calmar_ratio(df1).values[0]
    # print (ann_mean,ann_std)

    # get the cumprof
    df2 = df_equitycurve.loc[:, ['cumprof']]
    df2.loc[mask, :] = np.nan
    df1['drawdown'] = (df2 - df2.cummax()) / riskcapital

    if benchmark is None:
        return {
            'ann_mean': ann_mean,
            'ann_std': ann_std,
            'ann_sharpe': ann_sharpe,
            'drawdown': df1[['drawdown']],
            'calmar': calmar
        }

    else:
        # calculate the rolling correlation (5 business days return, rolling 1 year)
        df_comb = pd.merge(df_equitycurve[['reinvestprof']], benchmark, left_index=True, right_index=True, how='outer')
        df_comb.dropna(inplace=True)
        # get 5 business days interval
        mask = (df_comb.index.dayofweek == 3)
        df_comb = df_comb.loc[mask, :]
        df_comb = df_comb.pct_change(1)
        df_comb['1y_corr_benchmark'] = df_comb.iloc[:, 0].rolling(52).corr(df_comb.iloc[:, 1])
        mask = (df_comb.index < profsmpl[0]) | (df_comb.index > profsmpl[1])
        df_comb.loc[mask, :] = np.nan
        # correlation with benchmark
        df_comb['avg_corr'] = df_comb.iloc[:, [0, 1]].dropna().corr().values[0][1]
        return {
            'ann_mean': ann_mean,
            'ann_std': ann_std,
            'ann_sharpe': ann_sharpe,
            'drawdown': df1[['drawdown']],
            '1y_corr': df_comb[['1y_corr_benchmark']],
            'avg_corr': df_comb[['avg_corr']],
            'calmar': calmar
        }


def returnstats(df_equitycurve, riskcapital, profsmpl, benchmark=None):
    warnings.warn(
        "This function will be going away soon. Use the function returnstats_dict from this module instead.",
        PendingDeprecationWarning)
    '''
    return ann_mean, ann_std, ann_sharpe, drawdown_series, and rolling correlation
    '''
    mask = (df_equitycurve.index < profsmpl[0]) | (df_equitycurve.index > profsmpl[1])
    df1 = df_equitycurve.iloc[:, [0]]  # get the dollar pnl
    df1.loc[mask, :] = np.nan

    df1 = df1.dropna() / riskcapital

    ann_mean = annual_return(df1).values[0] * 100
    ann_std = annual_vol(df1).values[0] * 100
    ann_sharpe = ann_mean / ann_std
    # print (ann_mean,ann_std)

    # get the cumprof
    df2 = df_equitycurve.loc[:, ['cumprof']]
    df2.loc[mask, :] = np.nan
    df1['drawdown'] = (df2 - df2.cummax()) / riskcapital

    if benchmark is None:
        return ann_mean, ann_std, ann_sharpe, df1[['drawdown']],

    else:
        # calculate the rolling correlation (5 business days return, rolling 1 year)
        df_comb = pd.merge(df_equitycurve[['reinvestprof']], benchmark, left_index=True, right_index=True, how='outer')
        df_comb.dropna(inplace=True)
        # get 5 business days interval
        mask = (df_comb.index.dayofweek == 3)
        df_comb = df_comb.loc[mask, :]
        df_comb = df_comb.pct_change(1)
        df_comb['1y_corr_benchmark'] = df_comb.iloc[:, 0].rolling(52).corr(df_comb.iloc[:, 1])
        mask = (df_comb.index < profsmpl[0]) | (df_comb.index > profsmpl[1])
        df_comb.loc[mask, :] = np.nan
        # correlation with benchmark
        df_comb['avg_corr'] = df_comb.iloc[:, [0, 1]].dropna().corr().values[0][1]
        return ann_mean, ann_std, ann_sharpe, df1[['drawdown']], df_comb[['1y_corr_benchmark']], df_comb[['avg_corr']],


if __name__ == '__main__':
    pass


def pickle_result(temp_pickle, strategy_work_file):
    # pickle the result to a temp folder
    with open(temp_pickle, 'wb') as handle:
        pickle.dump(strategy_work_file, handle, protocol=pickle.HIGHEST_PROTOCOL)

