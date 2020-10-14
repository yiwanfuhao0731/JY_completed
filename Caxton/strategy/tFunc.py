import pandas as pd

import numpy as np

import statsmodels.api as sm

from datetime import datetime,timedelta

import os

from Analytics.series_utils import date_utils_collection

SU = date_utils_collection()

 

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

 

def ann_6m_geo_smooth_chg(s_1,*_):

    f = SU.get_freq(s_1)

    s_1 = SU.convert_std_freq(s_1)

    if f in ['D']:

        out = SU.smooth_change(s_1,periods=126,ann=True)

    elif f in ['W']:

        out = SU.smooth_change(s_1, periods=26, ann=True)

    elif f in ['M']:

        out = SU.smooth_change(s_1, periods=6, ann=True)

    elif f in ['Q']:

        out = s_1.pct_change(2)*2

    else:

        out = s_1.pct_change()

    return out*100

 

 

def ann_6m_aris_smooth_chg(s_1,*_):

    f = SU.get_freq(s_1)

    s_1 = SU.convert_std_freq(s_1)

    if f in ['D']:

        out = SU.smooth_change(s_1,periods=126,ann=True,ann_type='aris')

    elif f in ['W']:

        out = SU.smooth_change(s_1, periods=26, ann=True,ann_type='aris')

    elif f in ['M']:

        out = SU.smooth_change(s_1, periods=6, ann=True,ann_type='aris')

    elif f in ['Q']:

        out = s_1.diff(2)*2

    else:

        out = s_1.diff()

    return out

 

def roll_mean(s_1,__,w,*_):

    return SU.convert_std_freq(s_1).rolling(window=int(w)).mean()

 

def ann_6x6_geo_aris(s_1,*_):

    # 6m lvl chg on 6m geo change

    s_1 = SU.convert_std_freq(s_1)

    s_1_6 = ann_6m_geo_smooth_chg(s_1)

    f = SU.get_freq(s_1)

    if f in ['D']:

        out = SU.smooth_change(s_1_6,periods=126,ann=True,ann_type='aris')

    elif f in ['W']:

        out = SU.smooth_change(s_1_6, periods=26, ann=True,ann_type='aris')

    elif f in ['M']:

        out = SU.smooth_change(s_1_6, periods=6, ann=True,ann_type='aris')

    elif f in ['Q']:

        out = s_1.diff(2)*2

    return out

 

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

    s_1, s_2 = _align_s1_s2(s1=s_1, s2=s_2) #don't align since you don't want to fill backward NA

    s_out = s_1.dropna() - s_2.dropna()

    return s_out

 

 

# def divide_series(s_1, s_2, *_):

#     """divide series 1 by series 2"""

#     s_out = s_1.dropna() / s_2.dropna()

#     return s_out

 

 

def divide_series(s_1, s_2, *_):

    """divide series 1 by series 2"""

    s_1.dropna(inplace=True)

    s_1,s_2 = SU.convert_std_freq(s_1),SU.convert_std_freq(s_2)

    _, s_2 = s_1.align(s_2, join='left', method='ffill')

 

    s_out = s_1 / s_2

    return s_out

 

def divide_series_mul100(s_1, s_2, *_):

    """divide series 1 by series 2"""

    s_1.dropna(inplace=True)

    s_1, s_2 = SU.convert_std_freq(s_1), SU.convert_std_freq(s_2)

    _, s_2 = s_1.align(s_2, join='left', method='ffill')

 

    s_out = s_1 / s_2 *100

    return s_out

 

def multiply_series(s_1, s_2, *_):

    """subtract series 2 from series 1"""

    # s_1.dropna(inplace=True)

    # s_2.dropna(inplace=True)

    s_1, s_2 = SU.convert_std_freq(s_1), SU.convert_std_freq(s_2)

    s_out = s_1.dropna() * s_2.dropna()

    return s_out

 

 

def sum_series(s_1, s_2, *_):

    """add series 2 to series 1"""

    s_1,s_2 = _align_s1_s2(s1=s_1,s2=s_2) #don't align since you don't want to fill backward na with 0

    if isinstance(s_1,pd.Series) and (not isinstance(s_2,pd.Series)):

        s_2 = s_1.copy()

        s_2.loc[:] = 0

    s_out = (s_1 + s_2).dropna()

    return s_out

 

def YTD_to_m_apr_sea_adj(s_1,*_):

    # convert from Apr starting YTD to monthly, and do seasonal adjustment

    s_out = s_1.diff(1)

    s_out.loc[s_out.index.month==4] = s_1

    return seasonaladjustx13(s_out)

 

def seasonaladjustx13(s_1, *_):

    """apply x13 seasonal adjustment to the series"""

    x12path =  os.path.dirname(os.path.realpath(__file__))

    s_1_clean = s_1.dropna().replace(0,0.01).replace(0.0, 0.01)

    try:

        assert s_1_clean.index.freq not in [None]

    except:

        s_1_clean = SU.convert_std_freq(s_1_clean)

    try:

        s_out = sm.tsa.x13_arima_analysis(s_1_clean, x12path=x12path).seasadj

    except:

        print ('s_1_clean : ',s_1_clean.index)

        try:

            s_out = sm.tsa.x13_arima_analysis(s_1_clean.loc['1995':], x12path=x12path).seasadj

        except:

            s_out = SU.self_made_sea_adj(s_1_clean).iloc[:,0]

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

    # if it is quarterly, convert to monthly first

    if date_utils_collection.get_freq(s_1) in ['Q']:

        s_1 = s_1.resample('M',convention='start').sum()

        s_1=s_1.replace(0.,np.nan)

        s_1.fillna(method='ffill',inplace=True)

    daterange = pd.date_range(s_1.index[0], s_1.index[-1], freq=freq)

    s_1_full, _ = s_1.align(pd.Series(index=daterange), join='outer')

    s_1_filled = s_1_full.ffill()

    s_1_out = s_1_filled.loc[daterange]

    return s_1_out

 

def conversion_to_m(s_1,*_):

    s_1 = __resample(s_1,'M')

    idx = [dt.replace(day=1) for dt in s_1.index]

    s_1.index = idx

    return s_1

 

def extend_to_2050(s_1,*_):

    f = date_utils_collection.get_freq(s_1)

    f = 'MS' if f in ['M'] else f

    f = 'QS' if f in ['Q'] else f

    f = 'YS' if f in ['Y'] else f

    daterange = pd.date_range(s_1.index[0],'2050-12-31',freq=f)

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

 

def group_sum(series_list,*__):

    """sum of a list of series"""

    series_list = _align_s1_s2(s1=series_list)[0] #don't align since you don't want to fill backward na with 0

    series_list_new = [s.copy().dropna() for s in series_list]

    return pd.concat(series_list_new, axis=1).dropna().sum(axis=1)

 

def group_sum2(series_list, *__):

    """sum of a list of series, fill in 0 when NaN"""

    series_list = _align_s1_s2(s1=series_list)[0] #don't align since you don't want to fill backward na with 0

    series_list_new = [s.copy().fillna(0) for s in series_list]

    return pd.concat(series_list_new, axis=1).fillna(0).sum(axis=1)

 

def group_sum_and_sa(series_list,*_):

    #do the group sum and then seasonal adjustment

    s = group_sum2(series_list)

    return seasonaladjustx13(s)

 

def inf_adj(s_1, s_2, *__):

    cpi1 = s_2.reindex(pd.date_range(s_2.index[0], s_2.index[-1], freq='D'))

    cpi2 = cpi1.interpolate(limit_area='outside', limit_direction='forward')

 

    cpi3 = cpi2.interpolate(limit_area='inside', limit_direction='forward', method='zero')

 

    out_raw = (s_1 / cpi3)*100

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

 

def _align_s1_s2(s1=np.nan,s2=np.nan):

    # this will be used in sum, substract, group_sum, to solve the problem that s1 and s2 have different index

    # if s1 is a group of df, make sure fillna for short ones

    if isinstance(s1, list):

        s1 = [SU.convert_std_freq(df) for df in s1]

        all_index = []

        for df in s1:

            all_index += df.dropna().index.tolist()

        all_index = sorted(list(set(all_index)))

        s1_new = []

        for df in s1:

            df = df.reindex(all_index)

            # if df is continuing to update, ffill, else fill in 0

            if is_continue(df):

                df = df.fillna(method='ffill')

            else:

                print (df)

                first_valid,last_valid= df.first_valid_index(),df.last_valid_index()

                df.loc[first_valid:last_valid].fillna(method='ffill',inplace=True)

                df.fillna(0,inplace=True)

            s1_new.append(df)

            s1 = s1_new

    # if s1 and s2 are both dataframe, make sure they have the same index

    elif isinstance(s1, pd.Series) and isinstance(s2, pd.Series):

        s1,s2 = SU.convert_std_freq(s1),SU.convert_std_freq(s2)

        all_index = sorted(list(set(s1.index.tolist() + s1.index.tolist())))

        s1,s2 = s1.reindex(all_index),s2.reindex(all_index)

        if is_continue(s1):

            s1 = s1.fillna(method='ffill')

        else:

            first_valid, last_valid = s1.first_valid_index(), s1.last_valid_index()

            s1.loc[first_valid:last_valid, :].fillna(method='ffill', inplace=True)

            s1.fillna(0, inplace=True)

        if is_continue(s2):

            s2 = s2.fillna(method='ffill')

        else:

            first_valid, last_valid = s2.first_valid_index(), s2.last_valid_index()

            s2.loc[first_valid:last_valid, :].fillna(method='ffill', inplace=True)

            s2.fillna(0, inplace=True)

   elif isinstance(s1,pd.Series) and (not isinstance(s2,pd.Series)):

        s2 = s1.copy()

        s2.loc[:] = 0

    elif isinstance(s2, pd.Series) and (not isinstance(s1, pd.Series)):

        s1 = s2.copy()

        s1.loc[:] = 0

    return s1,s2

 

def is_continue(s):

    # if the data ends 9 months ago, return

    if s.dropna().index[-1]<datetime.now()-timedelta(days=30*9):

        return False

    else:

        return True