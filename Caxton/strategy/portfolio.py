from collections import OrderedDict
from enum import IntEnum

import numpy as np
import pandas as pd
import datetime as dt
import panormus.quant.alib.utils as qau
import warnings
from scipy import stats


class ReturnMethod(IntEnum):
    DIFF = 1
    RATIO = 2
    LOG = 3


def get_returns(before, after, return_method=ReturnMethod.DIFF):
    '''
    :param before: scalar, numpy array, or DataFrame of starting values
    :param after: scalar, numpy array, or DataFrame of ending values
    :param ReturnMethod return_method: enumerated return method.
    :return: returns (e.g. after - before)
    '''
    if return_method == ReturnMethod.DIFF:
        res = after - before
    elif return_method == ReturnMethod.RATIO:
        res = after / before - 1
    elif return_method == ReturnMethod.LOG:
        res = np.log(after / before)
    else:
        raise ValueError(f'Bad return_method {return_method}')

    return res


def accumulate_returns(rets, return_method=ReturnMethod.DIFF):
    '''
    :param pd.DataFrame|pd.Series rets: returns to accumulate downward!
    :param ReturnMethod return_method: enumerated return method.
    :return: cumulative returns
    '''
    if return_method == ReturnMethod.DIFF:
        res = rets.cumsum()
    elif return_method == ReturnMethod.RATIO:
        res = (1 + rets).cumprod() - 1
    elif return_method == ReturnMethod.LOG:
        res = rets.cumsum()
    else:
        raise ValueError(f'Bad return_method {return_method}')

    return res


def date_index_yearfrac(idx):
    n = len(idx)
    yf = qau.ac_day_cnt_frac(min(idx), max(idx), 'act/act') * (n + 1) / n
    return yf


def df_series_yearfrac(rets):
    """
   Yearfrac that spans rets.
    If rets is pd.Series, returns a scalar with the yearfrac spanning all non-nan observations
    If rets is pd.DataFrame, returns a pd.Series with the yearfrac spanning all non-nan observations for each column

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts or %-age)
    """
    if isinstance(rets, pd.Series):
        yf = date_index_yearfrac(rets.dropna().index)
    else:
        yf = pd.Series([date_index_yearfrac(rets[c].dropna().index) for c in rets.columns], index=rets.columns)
    return yf


def df_series_n(rets):
    """
    Number of observations in rets.
    If rets is pd.Series, returns a scalar with the number of non-nan observations
    If rets is pd.DataFrame, returns a pd.Series with the yearfrac spanning all non-nan observations for each column

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts or %-age)
    """
    if isinstance(rets, pd.Series):
        n = len(rets.dropna())
    else:
        n = pd.Series([len(rets[c].dropna().index) for c in rets.columns], index=rets.columns)
    return n


def annual_return(rets, year_frac=None):
    """
    Compute the annual return based on a set of arithmetic returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """
    yf = year_frac or df_series_yearfrac(rets)
    return rets.sum() / yf


def vol_of_returns(rets):
    '''
    Compute the vol (standard deviation) of returns without demeaning.

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    '''
    return (rets ** 2).mean() ** 0.5


def annual_vol(rets, year_frac=None):
    """
    Compute the annual vol based on a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """
    daily_vol = vol_of_returns(rets)

    n = df_series_n(rets)
    yf = year_frac or df_series_yearfrac(rets)
    return daily_vol * np.sqrt(n / yf)


def sharpe_ratio(rets, year_frac=None):
    """
    compute the annual sharpe ratio based on a set of daily arithmetic returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """

    return annual_return(rets, year_frac) / annual_vol(rets, year_frac)


def z_score(rets):
    """
    compute the z-score of a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts or %-age)
    """
    zscore = rets.mean() / vol_of_returns(rets)
    return zscore


def t_stat(rets):
    """
    compute the t-stat of a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts or %-age)
    """

    n = df_series_n(rets)
    tstat = z_score(rets) * n ** 0.5
    return tstat


def p_value(rets, target=0, test='at_least', measure='sr', year_frac=None):
    """
    compute the p-value that a sample Sharpe ratio or t-stat is [at_least/at_most/equal/unequal] the target
    Sharpe ratio or t-stat

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional)
    :param float target: target Sharpe Ratio that you want to test the return series has
    :param test: direction of the test: options ('at_least', 'at_most', 'equal', 'unequal')
    :param measure: the tested measure (for nonzero target, 'sr' targets Sharpe, 'rets' targets individual returns)
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """

    if measure in ('returns', 'rets'):
        t_statistic = t_stat(rets - target)
    elif measure in ('sr', 'sharpe', 'sharpe-ratio'):
        sr = sharpe_ratio(rets, year_frac)
        if isinstance(sr, pd.Series):
            yf = year_frac or pd.Series([date_index_yearfrac(rets[c].dropna().index) for c in rets.columns],
                                        index=rets.columns)
        else:
            yf = year_frac or date_index_yearfrac(rets.index)
        t_statistic = yf ** 0.5 * (sr - target)
    else:
        raise ValueError(f'Measure \'{measure}\' not recognized.')

    n = df_series_n(rets)

    if isinstance(t_statistic, pd.Series):
        p_onesided = pd.Series([stats.t.sf(t_statistic[c], n[c] - 1) for c in rets.columns], index=t_statistic.index)
    else:
        p_onesided = stats.t.sf(t_statistic, n - 1)

    if test == 'at_least':
        pvalue = 1 - p_onesided  # one-sided pvalue = Prob(sr > target)

elif test == 'at_most':
pvalue = p_onesided  # one-sided pvalue = Prob(sr < target)
elif test == 'equal':
pvalue = min(p_onesided, 1 - p_onesided) * 2  # two-sided pvalue = Prob(sr == target)
elif test == 'unequal':
pvalue = 1 - min(p_onesided, 1 - p_onesided) * 2  # two-sided pvalue = Prob(sr != target)

return pvalue


def rolling_dd(rets):
    """
    compute the max drawdown of a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    """

    dd = rets.cumsum() - rets.cumsum().expanding().max()
    return dd


def max_dd(rets):
    """
    compute the max drawdown of a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    """

    return rolling_dd(rets).min()


def calmar_ratio(rets, year_frac=None):
    """
    compute annual return over max drawdown of a set of returns

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """

    return annual_return(rets, year_frac) / -max_dd(rets)


def annual_costs(rets_net, rets_gross, year_frac=None):
    """
    Compute the annual costs based on a net and gross return series

    :param rets_net: pd.Series or pd.DataFrame of daily arithmetic returns net of costs
    :param rets_gross: pd.Series or pd.DataFrame of daily arithmetic returns gross of costs
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """

    if not rets_net.index.equals(rets_gross.index):
        raise ValueError('Indices of rets_net and rets_gross are not aligned.')

    # here
    total_costs = rets_net.sum() - rets_gross.sum()
    yf = year_frac or date_index_yearfrac(rets_net.index)
    return total_costs / yf


def perf_metrics_old(rets, rets_gross=None, year_frac=None):
    """
    returns a dict of the main performance metrics

    :param rets: pd.Series or pd.DataFrame of daily arithmetic returns (e.g. actual USD amounts returned or %-age \
    of a fixed notional.
    :param rets_gross: returns series excluding trading costs (rets must be net of trading costs in this case). If \
    this variable is passed, return metrics will include costs. The indices of rets and rets_gross must be aligned.
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """

    warnings.warn(
        "This function will be going away soon. Use the function perf_metrics(ret) from this module instead.",
        PendingDeprecationWarning)

    if rets_gross is None:
        ac = np.nan
    else:
        ac = annual_costs(rets, rets_gross, year_frac)

    metrics = dict(
        annual_return=annual_return(rets, year_frac),
        annual_vol=annual_vol(rets, year_frac),
        annual_costs=ac,
        sharpe_ratio=sharpe_ratio(rets, year_frac),
        max_dd=max_dd(rets),
        calmar_ratio=calmar_ratio(rets, year_frac),
    )

    return metrics


def perf_metrics(rets, rets_gross=None, year_frac=None):
    """
    returns a dict of performance metrics

    :param pd.Series rets: pd.Series of daily arithmetic returns
    :param pd.Series rets_gross: returns series excluding trading costs \
    (rets must be net of trading costs in this case). \
    If this variable is passed, return metrics will include costs. The indices of rets and rets_gross must be aligned.
    :param float year_frac: year fraction for returns sample. If None: infer from min and max dates in index.
    """
    if rets_gross is None:
        ac = np.nan
    else:
        ac = annual_costs(rets, rets_gross, year_frac)

    n = len(rets)
    yf = year_frac or date_index_yearfrac(rets.index)
    avg_worst_5 = rets.nsmallest(5).mean() if n > 5 else rets.mean()
    metrics = dict(
        trade_return=rets.mean(),
        trade_vol=vol_of_returns(rets),
        zscore=z_score(rets),
        zscore_scaled=t_stat(rets),
        worst_return=rets.min(),
        avg_worst_5=avg_worst_5,
        return_to_worst_5=rets.mean() / -avg_worst_5,
        prct_positive=len(rets[rets > 0]) / n,
        prct_negative=len(rets[rets < 0]) / n,
        trades_per_year=n / yf,
        annual_costs=ac,
        annual_return=annual_return(rets, year_frac),
        annual_vol=annual_vol(rets, year_frac),
        max_dd=max_dd(rets),
        calmar_ratio=calmar_ratio(rets, year_frac),
        sharpe_ratio=sharpe_ratio(rets, year_frac),
    )

    return metrics


def get_random_rets(seed=None, n=1000, vol=0.2, ret=0.03, start_date=dt.datetime(2010, 1, 1)):
    """
    get random normal returns series (for testing or showing examples)

    :param seed: optionally provide a fixed seed to reproduce the same random numbers each time.
    :param n: number of returns to draw
    :param vol: annualized vol
    :param ret: annualized return
    :param start_date: start date of return series
    """
    if seed is not None:
        np.random.seed(seed)

    dates = pd.bdate_range(start_date, periods=n)
    days_per_year = n / date_index_yearfrac(dates)

    z = np.random.randn(n) * vol / np.sqrt(days_per_year) + ret / days_per_year
    rets = pd.Series(z, dates)
    return rets


def get_random_perf(seed=None, n=1000, vol=0.2, ret=0.03, start_date=dt.datetime(2010, 1, 1)):
    """
    get random arith brownian motion perf series

    :param seed: optionally provide a fixed seed to reproduce the same random numbers each time.
    :param n: number of returns to draw
    :param vol: annualized vol
    :param ret: annualized return
    :param start_date: start date of return series
    """
    x = get_random_rets(seed, n, vol, ret, start_date)
    return x.cumsum()


# TODO: Functions below this line are not properly vetted.
def get_rolling_max_drawdown(cum_rets):
    """
    :description: rolling max drawdown time series and max drawdown float since start
    :param cum_rets: array-like cumulative returns
    :return: np.ndarray for rolling drawdown and max drawdown
    """
    warnings.warn(
        "This function will be going away soon. Use the function rolling_dd(ret).expanding().min() from this module " +
        "instead.", PendingDeprecationWarning)

    mdd = 0
    peak = cum_rets[0]
    roll_dd = np.empty(0)
    for cum_ret in cum_rets:
        peak = cum_ret if cum_ret > peak else peak
        dd = 100 * (cum_ret / peak - 1)
        mdd = dd if dd < mdd else mdd
        roll_dd = np.append(roll_dd, mdd)
    return roll_dd, mdd


def get_rolling_drawdown(cum_rets):
    """
    :description: rolling drawdown time series
    :param cum_rets: array-like cumulative returns
    :return: np.ndarray
    """

    warnings.warn(
        "This function will be going away soon. Use the function rolling_dd(ret) from this module instead.",
        PendingDeprecationWarning)

    return -(cum_rets.expanding().max() - np.array(cum_rets))


def get_perf_metrics(rets):
    """
    :description: calculate performance metrics given daily returns
    :param rets: daily return series
    :return: dataframe of scalar performance metrics and rolling drawdown
    """

    warnings.warn(
        "This function will be going away soon. Use the function perf_metrics from this module instead.",
        PendingDeprecationWarning)

    average_returns = rets.mean() * 252
    ir = average_returns / (rets.std() * np.sqrt(252))
    portfolio = 100 + rets.cumsum()
    rolling_dd, mdd = get_rolling_max_drawdown(portfolio)
    calmar = average_returns / abs(mdd)
    return pd.DataFrame(
        {'average_returns (%)': round(average_returns, 2), 'ir': round(ir, 2), 'max_dd (%)': round(mdd, 2),
         'calmar': round(calmar, 2)}, index=['performance']), rolling_dd


trade
utils.py


def get_min_trade_within_tolerance(target_position, current_position, tolerance):
    """
    find the amount to be traded when applying position inertia

    i.e. trade as little as possible subject to final position being within tolerance


    ---Input Arguments---
    :param float target_position:   target position
    :param float current_position:  current position
    :param float tolerance:         tolerance, trade is given such that new position will be in tolerance from target

    ---Returns---
    float   Amount to be traded
    """

    min_amount_to_trade, max_amount_to_trade = get_min_max_trade(target_position, current_position, tolerance)

    amount_to_trade = min(max_amount_to_trade, 0) + max(min_amount_to_trade, 0)

    return amount_to_trade


def get_max_roll_within_tolerance(target_position, current_position, tolerance, expiring_position):
    """find the amount to be roll when applying position inertia
    always roll as much as possible up to the target (but no more), within the tolerance
    (used when rolling is cheaper than closing a positions)

    IMPORTANT:  Traded amount will be capped at roll amount. I.e. you will still need to apply the
                get_min_trade_within_tolerance function

    ---Input Arguments---
    :param float target_position:   target position
    :param float current_position:  current position
    :param float tolerance:         tolerance, trade is given such that new position will be in tolerance from target
    :param float expiring_position: expiring position, i.e. its cheaper to roll this than to close it out

    ---Returns---
    float   Amount to be traded
    """

    min_amount_to_trade, max_amount_to_trade = get_min_max_trade(target_position, current_position, tolerance)

    if min_amount_to_trade >= 0:
        amount_to_roll = max(min(expiring_position, max_amount_to_trade), 0)
    elif min_amount_to_trade < 0 < max_amount_to_trade:
        amount_to_roll = max(min(expiring_position, max_amount_to_trade), min_amount_to_trade)
    elif max_amount_to_trade <= 0:
        amount_to_roll = max(min(expiring_position, 0), min_amount_to_trade)
    else:
        raise RuntimeError(f'Error in min_amount_to_roll or max_amount_to_roll, possible input error.')

    return amount_to_roll


def get_min_max_trade(target_position, current_position, tolerance):
    """find the min and max amounts that can still be traded within the tolerance

    ---Input Arguments---
    :param float target_position:   target position
    :param float current_position:  current position
    :param float tolerance:         tolerance, trade is given such that new position will be in tolerance from target

    ---Returns---
    2-tuple of floats   Lower and upper amount that can be traded to stay within tolerance
    """

    if tolerance < 0:
        ValueError(f'Tolerance must be non-negative. Input was: {tolerance}')

    net_position = current_position - target_position

    upper_limit = tolerance
    lower_limit = -tolerance

    # find max and min trades such that exposure remains within tolerance from 0
    min_amount_to_trade = lower_limit - net_position
    max_amount_to_trade = upper_limit - net_position

    return min_amount_to_trade, max_amount_to_trade