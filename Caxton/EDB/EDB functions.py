import pandas as pd
import numpy as np
import statsmodels.api as sm


def splice2(s_1, s_2, *_):
    """simply combine the two dataseries outright, filling any missing gaps in series 2 with series 1"""
    s_1.dropna(inplace=True)
    s_2.dropna(inplace=True)

    s_2_truncated = s_2.loc[:s_1.index[0]]
    s_2_truncated.drop(s_1.index[0], errors='ignore', inplace=True)

    # concatenate truncated time-series
    output_series = pd.concat([s_2_truncated, s_1], axis=0).sort_index()

    return output_series


def splice3(s_1, s_2, *_):
    """Stevens code for splice 3"""
    s_1.dropna(inplace=True)
    s_2.dropna(inplace=True)

    if s_2.dropna().empty:
        return s_1.dropna()

    if s_1.dropna().empty:
        return s_2.dropna()

    first_common_date = s_1.index.intersection(s_2.index).values[0]

    scale_s_2 = s_1[first_common_date] / s_2[first_common_date]

    data_out = splice2(s_1, s_2 * scale_s_2)

    return data_out


def splice_geometric(s_1, s_2, *_):
    """splice 2 series together, s_1 has precedence over s_2"""
    # test if first date of series one exists in series 2
    if len(s_1.dropna().index.intersection(s_2.dropna().index)) == 0:
        raise ValueError("Series dates do not overlap, cannot compute scale factor for series 2.")
    s_1.dropna(inplace=True)
    s_2.dropna(inplace=True)

    first_common_date = s_1.index.intersection(s_2.index).values[0]
    scale_s_2 = s_1[first_common_date] / s_2[first_common_date]

    # scale second series to match the first
    s_2_truncated_scaled = s_2.loc[:s_1.index[0]] * scale_s_2
    s_2_truncated_scaled.drop(s_1.index[0], errors='ignore', inplace=True)
    # s_2_truncated = s_2.loc[:s_1.index[0]].iloc[:-1] * s_1[0] / s_2[s_1.index[0]]

    # concatenate truncated time-series
    output_series = pd.concat([s_2_truncated_scaled, s_1], axis=0).sort_index()
    # output_series.plot()   s_2.plot()
    return output_series


def subtract_series(s_1, s_2, *_):
    """subtract series 2 from series 1"""
    s_out = s_1.dropna() - s_2.dropna()
    return s_out


# def divide_series(s_1, s_2, *_):
#     """divide series 1 by series 2"""
#     s_out = s_1.dropna() / s_2.dropna()
#     return s_out


def divide_series(s_1, s_2, *_):
    """divide series 1 by series 2"""
    s_1.dropna(inplace=True)
    _, s_2 = s_1.align(s_2, join='left', method='ffill')

    s_out = s_1 / s_2
    return s_out


def multiply_series(s_1, s_2, *_):
    """subtract series 2 from series 1"""
    # s_1.dropna(inplace=True)
    # s_2.dropna(inplace=True)

    s_out = s_1.dropna() * s_2.dropna()
    return s_out


def sum_series(s_1, s_2, *_):
    """add series 2 to series 1"""
    s_out = (s_1 + s_2).dropna()
    return s_out


def seasonaladjustx13(s_1, *_):
    """apply x13 seasonal adjustment to the series"""
    x12path = r"C:\Program Files\SeasonallyAdjust_WinX13\x13as\x13as.exe"
    s_1_clean = s_1.dropna()
    s_out = sm.tsa.x13_arima_analysis(s_1_clean, x12path=x12path).seasadj
    return s_out


def name(s_1, *_):
    """dummy function, don't do anything"""
    return s_1


# def minterp_STEVEN_OLD_corrected_10oct2019(s_1, *_):
#     """do monthly interpolation of dataseries"""
#     s_1_full = s_1.ix[pd.date_range(s_1.index[0], s_1.index[-1], freq='M')].copy()
#     s_1_interp = s_1_full.interpolate(limit_area='inside')
#     return s_1_interp
#
# def qinterp_STEVEN_OLD_corrected_10oct2019(s_1, *_):
#     """do quarterly interpolation of dataseries"""
#     s_1_full = s_1.ix[pd.date_range(s_1.index[0], s_1.index[-1], freq='Q')]
#     s_1_interp = s_1_full.interpolate(limit_area='inside')
#     return s_1_interp
#
# def minterp_gdp_STEVEN_OLD_corrected_10oct2019(s_1, *__):
#     swe_m2 = s_1.ix[pd.date_range(s_1.index[0], s_1.index[-1], freq='M')]
#     swe_m3 = swe_m2.interpolate(limit_area='outside', limit_direction='forward')
#     return swe_m3.interpolate(limit_area='inside', limit_direction='forward', method='zero')


def __resample(s_1, freq):
    daterange = pd.date_range(s_1.index[0], s_1.index[-1], freq=freq)
    s_1_full, _ = s_1.align(pd.Series(index=daterange), join='outer')
    s_1_filled = s_1_full.ffill()
    s_1_out = s_1_filled.loc[daterange]
    return s_1_out


def minterp(s_1, *_):
    """do monthly resampling of dataseries with fill forward on missing dates"""
    return __resample(s_1, 'M')


def qinterp(s_1, *_):
    """do quarterly resampling of dataseries with fill forward on missing dates"""
    return __resample(s_1, 'Q')


def minterp_gdp(s_1, *__):
    """do monthly resampling of dataseries with fill forward on missing dates"""
    return __resample(s_1, 'M')


def mult_constant(s_1, _, const, *__):
    return s_1 * float(const)


def div_constant(s_1, _, const, *__):
    return s_1.dropna() / float(const)


def group_sum(series_list):
    """sum of a list of series"""
    series_list_new = [s.copy().dropna() for s in series_list]
    return pd.concat(series_list_new, axis=1).dropna().sum(axis=1)


def group_sum2(series_list, *__):
    """sum of a list of series, fill in 0 when NaN"""
    series_list_new = [s.copy().fillna(0) for s in series_list]
    return pd.concat(series_list_new, axis=1).fillna(0).sum(axis=1)


def inf_adj(s_1, s_2, *__):
    cpi1 = s_2.reindex(pd.date_range(s_2.index[0], s_2.index[-1], freq='D'))
    cpi2 = cpi1.interpolate(limit_area='outside', limit_direction='forward')

    cpi3 = cpi2.interpolate(limit_area='inside', limit_direction='forward', method='zero')

    out_raw = (s_1 / cpi3) * 100
    out = out_raw[~out_raw.isin([np.nan, np.inf, -np.inf])]

    return out


def ytd_m(s_1, *__):
    """stevens function to turn ytd data into rolling data"""
    s_1 = s_1.dropna()
    mon = s_1.index.month

    s3 = pd.DataFrame(
        {'month': mon,
         's_2': s_1})

    s_jan = s3.loc[(s3['month'] == 1)]

    d1 = s3.s_2.diff(1)

    s3 = pd.DataFrame({
        'month': mon,
        's_2': d1})

    other = s3.loc[(s3['month'] > 1)]

    s_out = s_jan.append(other).drop('month', axis=1).iloc[:, 0]
    s_out.name = s_1.name

    return s_out

