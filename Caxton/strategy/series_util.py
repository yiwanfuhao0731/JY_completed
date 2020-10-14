#@zy 20190313: this file contains all sorts of frequently used time series utility methods

import pandas as pd

import numpy as np

from datetime import datetime, timedelta

import panormus.data.bo.econ as econ

import statsmodels.api as sta_m

from pandas.tseries.offsets import BMonthEnd,BQuarterEnd

import panormus.data.haver as haver

conn = econ.get_instance_using_credentials()

from Analytics.loggings import show_pop_up

from statsmodels.tsa.seasonal import seasonal_decompose

import os

WKDIR = os.path.dirname(os.path.realpath(__file__))

 

class date_utils_collection(object):

    def __init__(self):

        self.bday_index = pd.date_range('19450101',periods=25200,freq='B')

 

    def a_day_in_previous_month(self,dt):

        return dt.replace(day=1) - timedelta(days=1)

 

    def crosscorr(self,datax, datay, lag=0):

        """ Lag-N cross correlation.

        Parameters

        ----------

        lag : int, default 0

        datax, datay : pandas.Series objects of equal length

 

        Returns

        ----------

        crosscorr : float

        """

        datax,datay = datax.iloc[:,0],datay.iloc[:,0]

        return datay.corr(datax.shift(lag))

 

    def drop_duplicate(self,df):

        df.sort_index(inplace=True)

        return df.loc[~df.index.duplicated(keep='last')]

 

    def EOD_x_monthago(self,reference_dt, x):

       '''

        :param reference_dt: the reference date to deal with, input should be a string

        :param x: the month you want to move forward

        :return: will return the end of month of x before prior to the reference_dt

        '''

        reference_dt = datetime.strptime(reference_dt, '%Y-%m-%d')

        for i in range(x):

            reference_dt = self.a_day_in_previous_month(reference_dt)

        return reference_dt

 

    def FOD_x_monthago(self,reference_dt, x):

        '''

        :param reference_dt: the reference date to deal with, input should be a string

        :param x: the month you want to move forward

        :return: will return the end of month of x before prior to the reference_dt

        '''

        reference_dt = datetime.strptime(reference_dt, '%Y-%m-%d')

        for i in range(x):

            reference_dt = self.a_day_in_previous_month(reference_dt)

        return reference_dt.replace(day=1)

 

    def get_1st_index_date_as_str(self,df):

        df.sort_index(inplace=True)

        return df.index.strftime('%Y-%m-%d')[0]

 

    def first_day_or_last_day_of_a_month(self,dt):

        '''

        :param dt: identify whether this date is the first day of the month or last day of the month

        :return: True if LAST day, False if FIRST DAY

        '''

        return True if datetime.strptime(dt,'%Y-%m-%d').day>=15 else False

 

    def freq_conversion(self,freq):

        if freq.upper() == 'M':

            return 12

        if freq.upper() == 'Y':

            return 1

        if freq.upper() == 'Q':

            return 3

        if freq.upper() == "D":

            return 252

 

    def truncate_NAs(self,df,col=-1):

        '''

        :param df: this is to truncate the NA's at the beginning or at the end of the time series, ignore NAs in between though

        :param col: the number of col that has NAs, default set to the last column

        :return: the df tha has been truncated the NAs at the beginning of the dataframe and at the end of the dataframe

        '''

        first_idx = df.iloc[:,col].first_valid_index()

        last_idx = df.iloc[:,col].last_valid_index()

        return df.loc[first_idx:last_idx,:]

 

    def yoy_to_idx(self,df,freq='M', col=0,idx_col_name = 'new_Index'):

        '''

        :param df: convert from yoy change rate to index. NOTE: by default the index should be the date !!!

        :param freq: freq of the series, default as monthly

        :param col: the col number that contains the yoy change rate, default as the column 0

        :return: the original column PLUS the new column contains the index

        '''

        print ('please do not use method since it is inconsistent with Felix_s method')

        raise IOError

        first_date = self.get_1st_index_date_as_str(df) # now first_date is a string

        # identify if the date is the start of the month or end of the month

        if self.first_day_or_last_day_of_a_month(first_date) == True:

            insert_date = self.EOD_x_monthago(first_date,1)

        else:

            insert_date = self.FOD_x_monthago(first_date,1)

 

        #genr the inserted row a a list

        total_col = df.shape[1]

        insert_row = [0] * total_col

        insert_row = insert_row+[1]

        #print (insert_row)

        inserted_df = pd.DataFrame([insert_row],columns=[i for i in range(len(insert_row))],index=[insert_date])

        inserted_df.replace(0,np.nan,inplace=True)

        #print (inserted_df)

 

        df[idx_col_name] = np.nan

        inserted_df.columns = df.columns

        df = pd.concat([df,inserted_df],axis=0)

        # convert the str type to timestamp

        df.index = pd.to_datetime(df.index)

        df.sort_index(inplace=True)

 

        # convert the yoy to index using the fomula: index_t+1 = index_t*((1+yoy_t+1%/12))

       df.iloc[1:,-1] = (1+df.iloc[1:,col]/100/self.freq_conversion(freq))

        df.iloc[:,-1] = df.iloc[:,-1].cumprod()

        return df

 

    def yoy_from_idx(self,df,freq='M',col = 0,yoy_col_name = 'new_yoy'):

        '''

        :param df: the df that perform the yoy change calculation

        :param freq: freq of the calculation, default as monthly. This will be used in the annulisation calc

        :param col: the col number that

        :param yoy_col_name: new column name for the yoy changes

        :return:appending at the end of the column a yoy change

        '''

        mask = np.isnan(df.iloc[:,col])

        col = df.columns.tolist()[col]

        df.loc[~mask,yoy_col_name] = df.loc[~mask,col].pct_change(periods=12)*100

        return df

 

    def empty_M_df(self):

        x = [0]*2000

        df = pd.DataFrame({'1':x},index = pd.date_range('19200101',periods=2000,freq='M'))

        df['Date'] = df.index

        df.reset_index(drop=True,inplace=True)

        df['Date'] = [x.date() for x in df['Date']]

        df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

        #print (type(df['Date'].values[0]))

        return df[['Date']]

 

    def empty_Q_df(self):

        x = [0]*460

        df = pd.DataFrame({'1':x},index = pd.date_range('19201201',periods=460,freq='Q'))

        df['Date'] = df.index

        df.reset_index(drop=True,inplace=True)

        df['Date'] = [x.date() for x in df['Date']]

        df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

        #print (type(df['Date'].values[0]))

        return df[['Date']]

 

    def empty_BDay_df(self):

        x = [0]*252*100

        df = pd.DataFrame({'1':x},index = pd.date_range('19450101',periods=25200,freq='B'))

        df['Date'] = df.index

        df.reset_index(drop=True, inplace=True)

        df['Date'] = [x.date() for x in df['Date']]

        df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

        return df[['Date']]

 

    def empty_Day_df(self):

        x = [0]*365*100

        df = pd.DataFrame({'1':x},index = pd.date_range('19450101',periods=36500,freq='D'))

        df['Date'] = df.index

        df.reset_index(drop=True, inplace=True)

        df['Date'] = [x.date() for x in df['Date']]

        df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        return df[['Date']]

 

    def empty_Week_df(self):

        x = [0]*52*100

        df = pd.DataFrame({'1':x},index = pd.date_range('19450101',periods=5200,freq='W'))

        df['Date'] = df.index

        df.reset_index(drop=True, inplace=True)

        df['Date'] = [x.date() for x in df['Date']]

        df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        return df[['Date']]

 

    def extend_df1_with_most_recent_df2(self,df1,df2,method='geo'):

        '''

        :param df1: the dataframe that you want to use whenever available, but release slowly. Example: UK HPI

        :param df2: the dataframe to extend df1, release faster, Example: UK rightmove HP index

        :param method: geo: scale factor is df1/df2

                        aris: scale factor is df1-df2

        :return: full series with df1's column name, or 0 if df1 is Series

        '''

        assert df1.shape[1] * df2.shape[1] < 1.01, 'Sorry, df1 and df2 have more than 1 columns!!!'

        origin_col = [df1.columns[0],df2.columns[0]]

        df1,df2 = df1.dropna(),df2.dropna()

        df1.index, df2.index = pd.to_datetime(df1.index), pd.to_datetime(df2.index)

        # check if df2 is longer than df1

        if df1.index[-1]<df2.index[-1]:

            # convert to dataframe

            df1 = df1.to_frame() if len(df1.shape) < 1.01 else df1

            df2 = df2.to_frame() if len(df2.shape) < 1.01 else df2

            if len(df1.index.intersection(df2.index)) > 0:

                common_date1 = df1.index.intersection(df2.index)[-1]

                common_date2 = common_date1

            else:  # in case there's no exact intersection due to frequency issue

                try:

                    common_date1 = df1.index[-1]

                    common_date2 = df2.loc[common_date1:, :].index[0]

                except:

                    print(df1, df2)

                    raise ValueError

            if method=='geo':

                scale = df2.loc[common_date2, :].iloc[0] / df1.loc[common_date1, :].iloc[0]

                df2 = df2 / scale

            else:

                scale = df2.loc[common_date2, :].iloc[0] - df1.loc[common_date1, :].iloc[0]

                df2 = df2 - scale

            assert df2.dropna().shape[0] > 2, (df2.columns[0], 'splice method does not work, please check!!!')

            df2 = df2.loc[common_date2:, :]

            df2.columns = df1.columns

            df1 = pd.concat([df1, df2], axis=0)

            return self.drop_duplicate(df1)

        else:

            return df1

 

    def extend_backward_df1_by_df2(self,df1,df2,method='geo'):

        '''

        :param df1: the dataframe that you want to use whenever available, but has short history. Example: 1w OIS curve

        :param df2: the dataframe that has longer history, but you don;t want to use now. Example: policy rate

        :param method: geo: scale factor is df1/df2

                        aris: scale factor df1-df2

        :return: full long series with df1's column name, or 0 if df1 is Series

        '''

        assert df1.shape[1]*df2.shape[1] < 1.01,'Sorry, df1 and df2 have more than 1 columns!!!'

        origin_columns = [df1.columns[0]]

        df1,df2 = df1.dropna(),df2.dropna()

        df1.index,df2.index = pd.to_datetime(df1.index),pd.to_datetime(df2.index)

        #convert to dataframe

        df1 = df1.to_frame() if len(df1.shape)<1.01 else df1

        df2 = df2.to_frame() if len(df2.shape)<1.01 else df2

 

        if len(df1.index.intersection(df2.index))>0:

            common_date1 = df1.index.intersection(df2.index)[0]

            common_date2 = common_date1

        else: # in case there's no exact intersection due to frequency issue

            try:

                common_date1 = df1.index[0]

                common_date2 = df2.loc[common_date1:,:].index[0]

            except:

                print (df1,df2)

                raise ValueError

 

        if method=='geo':

            scale = df2.loc[common_date2,:].iloc[0]/df1.loc[common_date1,:].iloc[0]

            df2 = df2/scale

        else:

            scale = df2.loc[common_date2,:].iloc[0]-df1.loc[common_date1,:].iloc[0]

            df2 = df2 - scale

        assert df2.dropna().shape[0] > 2, (df2.columns[0], 'splice method does not work, please check!!!')

        df2 = df2.loc[:common_date2,:]

        df2.columns = df1.columns

        df1 = pd.concat([df1,df2],axis=0)

        df1.columns = origin_columns

        return self.drop_duplicate(df1)

 

    def repeat_value(self,df, first_idx, last_idx, col_number=-1):

        col_name = df.columns.tolist()[col_number]

        df[col_name].fillna(method="ffill", inplace=True)

        df.loc[(df.index<first_idx)|(df.index>last_idx),col_name] = np.nan

        return df

 

    # @v.i.

    def rolling_ignore_nan(self,_df, _window, _func):

        _df = _df.rolling(window=_window, min_periods=1).apply(lambda x: _func(x[~np.isnan(x)]),raw=True)

        idx_mask = (_df.index < _df.iloc[:, -1].dropna().index[_window - 1])

        _df.iloc[idx_mask] = np.nan

        return _df

 

    def conversion_to_FOM(self,df):

        '''

        simply convert to the first day of the month, no repeating is needed; no frequency change, no nothing!!

        :param df:

        :return:

        '''

        df = df.copy()

        if len(df.index) == 0:

            return df

 

        try:

            df['Date'] = pd.to_datetime(df.index)

            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        except:

            df['Date'] = df.index

            df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        df['Date'] = [datetime.strptime(dt, '%Y-%m-%d').replace(day=1) for dt in df['Date']]

        df['Date'] = pd.to_datetime(df['Date'])

        df.set_index('Date', inplace=True)

        return df

 

    def conversion_to_FOW(self,df):

        '''

        simply convert to the first day of the week, no repeating is needed; no frequency change, no nothing!!

        :param df:

        :return:

        '''

        df = df.copy()

        if len(df.index) == 0:

            return df

 

        try:

            df['Date'] = pd.to_datetime(df.index)

            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        except:

            df['Date'] = df.index

            df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        df['Date'] = [datetime.strptime(dt, '%Y-%m-%d')+timedelta(days=( - datetime.strptime(dt, '%Y-%m-%d').weekday())) for dt in df['Date']]

        df['Date'] = pd.to_datetime(df['Date'])

        df.set_index('Date', inplace=True)

        return df

 

    def conversion_to_m(self,df,method='repeat',col_to_repeat = -1):

        #convert the data series to from quarterly or longer to monthly frequency and repeat the values

        # OR set the monthly df date to the FIRST of the month

        '''

        convert the date of the data to the first day of the month, merge the data with a empty monthly time series

        :param df: dataframe, with date as its index

        :param col_to_repeat: the number of column of concerned

        :param method: method for and NAs in between, can choose either repeat or 'None'

        :return: return a series that is monthly freq, all repeated

        '''

        df = df.copy()

        if len(df.index) == 0:

            return df

 

        try:

            df['Date'] = pd.to_datetime(df.index)

            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        except:

            df['Date'] = df.index

            df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        df['Date'] = [datetime.strptime(dt, '%Y-%m-%d').replace(day=1) for dt in df['Date']]

        df['Date'] = pd.to_datetime(df['Date'])

        df.set_index('Date', inplace=True)

 

        # Second convert the Qtr freq into monthly freq:

        # empty_m_ts = self.empty_M_df()

        # empty_m_ts['Date'] = [datetime.strptime(dt, '%Y-%m-%d').replace(day=1) for dt in empty_m_ts['Date']]

        # empty_m_ts['Date'] = pd.to_datetime(empty_m_ts['Date'])

        # empty_m_ts.set_index('Date', inplace=True)

        # merge the quarterlt freq into monthly freq

        # left = empty_m_ts

        # right = df

        #df_monthly = pd.merge(left, right, how='left', left_index=True, right_index=True)

        df_monthly = self.drop_duplicate(df).reindex(pd.date_range('19200101',periods=2000,freq='MS'))

        df_monthly.sort_index(inplace=True)

 

        # repeat all the col_to_repeat or col_to interpolate or fill values

        if not isinstance(col_to_repeat,list):

            col_to_repeat = [col_to_repeat]

 

        for col_num in col_to_repeat:

            first_idx = df_monthly.iloc[:,col_num].first_valid_index()

            last_idx = df_monthly.iloc[:,col_num].last_valid_index()

 

            # repeat and truncate the NA value at the beginning and end of the series

            if method=='repeat':

                df_monthly = self.repeat_value(df_monthly, first_idx, last_idx, col_num)

            elif method == 'interpolate':

                col_name = df.columns.tolist()[col_num]

                df_monthly.loc[:, col_name].interpolate(inplace=True,method='linear')

                df_monthly.loc[(df_monthly.index < first_idx) | (df_monthly.index > last_idx), col_name] = np.nan

            else:

                pass

        first_idx = df_monthly.first_valid_index()

        last_idx = df_monthly.last_valid_index()

        return self.drop_duplicate(df_monthly.loc[first_idx:last_idx, :])

 

    def conversion_to_q(self,df,method='repeat',col_to_repeat = -1,keep_end_dates_longer=False):

        #convert the data series to from quarterly or longer to monthly frequency and repeat the values

        # OR set the monthly df date to the FIRST of the month

        '''

        convert the date of the data to the first day of the month, merge the data with a empty monthly time series

        :param df: dataframe, with date as its index

        :param col_to_repeat: the number of column of concerned

        :param method: method for and NAs in between, can choose either repeat or 'None'

        :return: return a series that is monthly freq, all repeated

        '''

        df = df.copy()

        if len(df.index) <= 0.1:

            return df

 

        try:

            df['Date'] = pd.to_datetime(df.index)

            df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')

        except:

            df['Date'] = df.index

            df['Date'] = [x.strftime('%Y-%m-%d') for x in df['Date']]

 

        df['Date'] = [datetime.strptime(dt, '%Y-%m-%d').replace(day=1) for dt in df['Date']]

        df['Date'] = pd.to_datetime(df['Date'])

        df.set_index('Date', inplace=True)

 

        # force all the date to the last month of the quarter. e.g.: 2019-10-01 => 2019-12-01

        new_idx = []

        for dt in df.index:

            if dt.month in [1,4,7,10]:

                new_idx.append(dt.replace(month=dt.month+2))

            elif dt.month in [2,5,8,11]:

                new_idx.append(dt.replace(month=dt.month + 1))

            else:

                new_idx.append(dt)

        df.index = new_idx

        df = df[~df.index.duplicated(keep='last')]

 

        # Second convert the whatever freq into Qtr freq:

        # empty_q_ts = self.empty_Q_df()

        # empty_q_ts['Date'] = [datetime.strptime(dt, '%Y-%m-%d').replace(day=1) for dt in empty_q_ts['Date']]

        # empty_q_ts['Date'] = pd.to_datetime(empty_q_ts['Date'])

        # empty_q_ts.set_index('Date', inplace=True)

        # # merge the quarterlt freq into monthly freq

        # left = empty_q_ts

        # right = df

        # df_q = pd.merge(left, right, how='left', left_index=True, right_index=True)

        df_q = df.reindex(pd.date_range('19200101',periods=800,freq='QS-MAR'))

        df_q.sort_index(inplace=True)

 

        # repeat all the col_to_repeat or col_to interpolate or fill values

        if not isinstance(col_to_repeat,list):

            col_to_repeat = [col_to_repeat]

 

        for col_num in col_to_repeat:

            first_idx = df_q.iloc[:,col_num].first_valid_index()

            last_idx = df_q.iloc[:,col_num].last_valid_index()

 

            # repeat and truncate the NA value at the beginning and end of the series

            if method=='repeat':

                df_q = self.repeat_value(df_q, first_idx, last_idx, col_num)

            elif method == 'interpolate':

                col_name = df.columns.tolist()[col_num]

                df_q.loc[:, col_name].interpolate(inplace=True,method='linear')

                df_q.loc[(df_q.index < first_idx) | (df_q.index > last_idx), col_name] = np.nan

            else:

                pass

        first_idx = df_q.first_valid_index()

        last_idx = df_q.last_valid_index()

        if keep_end_dates_longer:

            return df_q.loc[first_idx:, :]

        else:

            return df_q.loc[first_idx:last_idx, :]

 

    def conversion_q_to_m(self,df,method='repeat',col_to_repeat = -1):

        # firstly, convert all date to the first day of the quarter: e.g. 2019-3-31 => 2019-01-01

        # second, at the end add an extra quarter-end month. should be like this:

        '''

            2000-01-01

            2000-04-01

            ...

            2019-01-01

            + 2019-03-01

        '''

        # merge with monthly and repeat the value. This should work with both haver tickers and EconDB tickers

        _df = df.copy()

        new_index = []

        if self.get_freq(_df) != 'Q':

            print ('Sorry, df passed is not a quarterly freq!!')

            raise ValueError

        for dt in _df.index:

            dt = dt.replace(day=1)

            if dt.month in [1,4,7,10]:

                new_index.append(dt)

            elif dt.month in [2,5,8,11]:

                dt = dt.replace(month=dt.month-1)

                new_index.append(dt)

            else:

                dt = dt.replace(month=dt.month - 2)

                new_index.append(dt)

        _df.index = new_index

 

        _df = _df.dropna()

        dt_last = _df.index[-1].replace(month=_df.index[-1].month+2)

        value_last = _df.values[-1]

 

        df_to_append = pd.DataFrame(index=[dt_last],data=value_last,columns = _df.columns)

        _df = _df.append(df_to_append)

        _df = self.conversion_to_m(_df,method=method,col_to_repeat=col_to_repeat)

        return self.drop_duplicate(_df)

 

    def divide_df1_df2(self, df1, df2):

        df1, df2 = df1.copy(), df2.copy()

        assert len(df1.columns) == len(df2.columns), 'Sorry, the columns of the 2 dfs in devision are not the same'

 

        df1.index = pd.to_datetime(df1.index)

        df2.index = pd.to_datetime(df2.index)

 

        common_dates = df1.index.intersection(df2.index)

 

        df11 = df1.loc[common_dates, :]

        df22 = df2.loc[common_dates, :]

 

        df11 = df11.reset_index(drop=True)

        df22 = df22.reset_index(drop=True)

 

        df11.columns = range(len(df11.columns))

        df22.columns = range(len(df22.columns))

 

        df_final = df11 / df22

        df_final.index = common_dates

        df_final.columns = [i + '/(' + j + ')' for i, j in zip(df1.columns.tolist(), df2.columns.tolist())]

        return df_final

 

    def sum_df1_df2(self, df1, df2, na='drop'):

        df1, df2 = df1.copy(), df2.copy()

        assert len(df1.columns) == len(df2.columns), 'Sorry, the columns of the 2 dfs in devision are not the same'

 

        df1.index = pd.to_datetime(df1.index)

        df2.index = pd.to_datetime(df2.index)

 

        common_dates = df1.index.intersection(df2.index)

 

        df11 = df1.loc[common_dates, :]

        df22 = df2.loc[common_dates, :]

 

        df11 = df11.reset_index(drop=True)

        df22 = df22.reset_index(drop=True)

 

        df11.columns = range(len(df11.columns))

        df22.columns = range(len(df22.columns))

 

        if na == 'zero':

            df_final = df11.fillna(0) + df22.fillna(0)

        else:

            df_final = df11 + df22

 

        df_final.index = common_dates

        df_final.columns = [i + '+(' + j + ')' for i, j in zip(df1.columns.tolist(), df2.columns.tolist())]

        return df_final

 

    def minus_df1_df2(self, df1, df2,na='drop'):

        df1,df2 = df1.copy(),df2.copy()

        assert len(df1.columns) == len(df2.columns), 'Sorry, the columns of the 2 dfs in devision are not the same'

 

        df1.index = pd.to_datetime(df1.index)

        df2.index = pd.to_datetime(df2.index)

 

        common_dates = df1.index.intersection(df2.index)

 

        df11 = df1.loc[common_dates, :]

        df22 = df2.loc[common_dates, :]

 

        df11 = df11.reset_index(drop=True)

        df22 = df22.reset_index(drop=True)

 

        df11.columns = range(len(df11.columns))

        df22.columns = range(len(df22.columns))

 

        if na=='zero':

            df_final = df11.fillna(0) - df22.fillna(0)

        else:

            df_final = df11-df22

 

        df_final.index = common_dates

        df_final.columns = [i + '-(' + j + ')' for i, j in zip(df1.columns.tolist(), df2.columns.tolist())]

        return df_final

 

    def minus_df1_df2_mod(eslf,df1, df2, na='drop'):

        # modified version of df1 minus df2, doesnt need df1 and df2 to be same length

        df1, df2 = df1.copy(), df2.copy()

 

        df1.index = pd.to_datetime(df1.index)

        df2.index = pd.to_datetime(df2.index)

 

        common_dates = df1.index.intersection(df2.index)

 

        df11 = df1.loc[common_dates, :]

        df22 = df2.loc[common_dates, :]

 

        df11 = df11.reset_index(drop=True)

        df22 = df22.reset_index(drop=True)

 

        df11.columns = range(len(df11.columns))

        df22.columns = range(len(df22.columns))

 

        if na == 'zero':

            df_final = df11.fillna(0).iloc[:, 0] - df22.fillna(0).iloc[:, 0]

        else:

            df_final = df11.iloc[:, 0] - df22.iloc[:, 0]

        df_final = df_final.to_frame()

        df_final.index = common_dates

        df_final.dropna(inplace=True)

 

        df_final.columns = [i + '-(' + j + ')' for i, j in zip(df1.columns.tolist(), df2.columns.tolist())]

        return df_final

 

    # v.i.

    def conversion_down_to_m(self,df,method='repeat',col_to_repeat=-1, agg_method = 'last'):

        if len(df.index) == 0:

            return df

 

        _df = df.copy()

        _df.index = pd.to_datetime(_df.index)

        _df['yearrr'] = _df.index.year

        _df.index = pd.to_datetime(_df.index)#???

        _df['monthhh'] = _df.index.month

        if agg_method == 'last':

            _df = _df.groupby(['yearrr', 'monthhh']).last()

        if agg_method == 'sum':

            _df = _df.groupby(['yearrr', 'monthhh']).sum()

        new_index = []

        for i, yr in enumerate(_df.index.get_level_values(0).tolist()):

            mon = _df.index.get_level_values(1).tolist()[i]

            new_index.append(datetime(year=yr, month=mon, day=1))

        _df.index = new_index

        _df.index = pd.to_datetime(_df.index)

 

        # in case that there are missing month in between, merge with a monthly df

        if self.get_freq(_df) == 'Q':

            _df=self.conversion_q_to_m(_df)

        else:

            _df = self.conversion_to_m(_df,method=method,col_to_repeat=col_to_repeat)

        return self.drop_duplicate(_df)

    #TODO: the conversion is not properly converting monthly to daily.

    # What should be done is to convert the monthly to the business day of that month!

    # and repeat to the last business of the current month

    def conversion_to_bDay(self,df, method='repeat', col_to_repeat=-1,keep_end_dates_longer=False):

        '''

        convert daily or slower frequency into business day frequency, for NAs using chosen method to fill in the NAs

        :param df:

        :param method: filling NA method can be repeat the previous value or fill in a specific number

        :param col_to_repeat:

        :return:

        '''

        df = df.copy()

        if len(df.index) == 0:

            return df

 

        if self.get_freq(df) == 'M':

            index = [self.get_first_bday_of_month(i) for i in df.index]

            df.index = pd.to_datetime(index)

            df = df[~df.index.duplicated(keep='last')]

            df_bday = df.reindex(self.bday_index)

            df_bday.sort_index(inplace=True)

            # repeat all the col_to_repeat or col_to interpolate or fill values

            if not isinstance(col_to_repeat, list):

                col_to_repeat = [col_to_repeat]

 

            for col_num in col_to_repeat:

                first_idx = df_bday.iloc[:, col_num].first_valid_index()

                last_idx = self.get_last_bday_of_month(df_bday.dropna().index[-1])

 

                # repeat and truncate the NA value at the beginning and end of the series

                if method == 'repeat':

                    df_bday = self.repeat_value(df_bday, first_idx, last_idx, col_num)

                elif method == 'interpolate':

                    col_name = df.columns.tolist()[col_num]

                    df_bday.loc[:, col_name].interpolate(inplace=True, method='linear')

                    df_bday.loc[(df_bday.index < first_idx) | (df_bday.index > last_idx), col_name] = np.nan

                else:

                    pass

 

        elif self.get_freq(df) == 'Q':

            # firstly conversion to monthly data, then from monthly to bday, so that the repeat method would work

            df = self.conversion_down_to_m(df)

            index = [self.get_first_bday_of_month(i) for i in df.index]

            df.index = pd.to_datetime(index)

            df = df[~df.index.duplicated(keep='last')]

            df_bday = df.reindex(self.bday_index)

            df_bday.sort_index(inplace=True)

            # repeat all the col_to_repeat or col_to interpolate or fill values

            if not isinstance(col_to_repeat, list):

                col_to_repeat = [col_to_repeat]

 

            for col_num in col_to_repeat:

                first_idx = df_bday.iloc[:, col_num].first_valid_index()

                last_idx = df_bday.dropna().index[-1]

                last_idx = self.get_last_bday_of_quarter(last_idx)

 

                # repeat and truncate the NA value at the beginning and end of the series

                if method == 'repeat':

                    df_bday = self.repeat_value(df_bday, first_idx, last_idx, col_num)

                elif method == 'interpolate':

                    col_name = df.columns.tolist()[col_num]

                    df_bday.loc[:, col_name].interpolate(inplace=True, method='linear')

                    df_bday.loc[(df_bday.index < first_idx) | (df_bday.index > last_idx), col_name] = np.nan

                else:

                    pass

 

        elif self.get_freq(df)=='W':

            # convert data release on Sun, Sat to previous Fri.

            # For example: 3-31 is a Sat, then assume the data release is on 3-30, remove the Friday release then

            index = [self.get_previous_Friday(i) for i in df.index]

            df.index = pd.to_datetime(index)

            df = df[~df.index.duplicated(keep='last')]

 

            df_bday = df.reindex(self.bday_index)

            df_bday.sort_index(inplace=True)

            # repeat all the col_to_repeat or col_to interpolate or fill values

            if not isinstance(col_to_repeat, list):

                col_to_repeat = [col_to_repeat]

            for col_num in col_to_repeat:

                first_idx = df_bday.iloc[:, col_num].first_valid_index()

                last_idx = df_bday.iloc[:, col_num].last_valid_index()

                last_idx_plus_1_week = df_bday.index.get_loc(last_idx)+4

                last_idx = df_bday.index[last_idx_plus_1_week]

                # repeat and truncate the NA value at the beginning and end of the series

                if method == 'repeat':

                    df_bday = self.repeat_value(df_bday, first_idx, last_idx, col_num)

                elif method == 'interpolate':

                    col_name = df.columns.tolist()[col_num]

                    df_bday.loc[:, col_name].interpolate(inplace=True, method='linear')

                    df_bday.loc[(df_bday.index < first_idx) | (df_bday.index > last_idx), col_name] = np.nan

                else:

                    pass

        else:

            # convert data release on Sun, Sat to previous Fri.

            # For example: 3-31 is a Sat, then assume the data release is on 3-30, remove the Friday release then

            index = [self.get_previous_Friday(i) for i in df.index]

            df.index = pd.to_datetime(index)

            df = df[~df.index.duplicated(keep='last')]

 

            df_bday = df.reindex(self.bday_index)

            df_bday.sort_index(inplace=True)

            # repeat all the col_to_repeat or col_to interpolate or fill values

            if not isinstance(col_to_repeat, list):

                col_to_repeat = [col_to_repeat]

            for col_num in col_to_repeat:

                first_idx = df_bday.iloc[:, col_num].first_valid_index()

                last_idx = df_bday.iloc[:, col_num].last_valid_index()

 

                # repeat and truncate the NA value at the beginning and end of the series

                if method == 'repeat':

                    df_bday = self.repeat_value(df_bday, first_idx, last_idx, col_num)

                elif method == 'interpolate':

                    col_name = df.columns.tolist()[col_num]

                    df_bday.loc[:, col_name].interpolate(inplace=True, method='linear')

                    df_bday.loc[(df_bday.index < first_idx) | (df_bday.index > last_idx), col_name] = np.nan

                else:

                    pass

        first_idx = df_bday.first_valid_index()

        last_idx = df_bday.last_valid_index()

        #adding longer dates for the purpose of shifting

        if not keep_end_dates_longer:

            return df_bday.loc[first_idx:last_idx, :]

        else:

            df_bday = df_bday.reindex(self.bday_index)

            df_bday.sort_index(inplace=True)

            assert len(df_bday.loc[df_bday.index>df_bday.last_valid_index(),:].index)>100,(df_bday,' Sorry, the end date is not long enough')

            return df_bday.loc[first_idx:, :]

 

    def conversion_to_Day(self,df,method='repeat',col_to_repeat=-1,keep_end_dates_longer=False):

        '''

        convert daily or slower frequency into business day frequency, for NAs using chosen method to fill in the NAs

        :param df:

        :param method: filling NA method can be repeat the previous value or fill in a specific number

        :param col_to_repeat:

        :return:

        '''

 

        if len(df.index) == 0:

            return df

 

        #first perform conversoin_to_bday to make sure the quarterly is taken care of

        #df = self.conversion_to_bDay(df,method=method,col_to_repeat=col_to_repeat)

 

        empty_day = self.empty_Day_df()

        empty_day['Date'] = pd.to_datetime(empty_day['Date'])

        empty_day.set_index('Date', inplace=True)

 

        left = empty_day

        right = df

        df_day = pd.merge(left, right, how='left', left_index=True, right_index=True)

 

        df_day.sort_index(inplace=True)

        # repeat all the col_to_repeat or col_to interpolate or fill values

        if not isinstance(col_to_repeat, list):

            col_to_repeat = [col_to_repeat]

 

        for col_num in col_to_repeat:

            first_idx = df_day.iloc[:, col_num].first_valid_index()

            last_idx = df_day.iloc[:, col_num].last_valid_index()

 

            # repeat and truncate the NA value at the beginning and end of the series

            if method == 'repeat':

                df_day = self.repeat_value(df_day, first_idx, last_idx, col_num)

            elif method == 'interpolate':

                col_name = df.columns.tolist()[col_num]

                df_day.loc[:, col_name].interpolate(inplace=True, method='linear')

                df_day.loc[(df_day.index < first_idx) | (df_day.index > last_idx), col_name] = np.nan

            else:

                pass

        first_idx = df_day.first_valid_index()

        last_idx = df_day.last_valid_index()

        if not keep_end_dates_longer:

            return df_day.loc[first_idx:last_idx,:]

        else:

            df_day = df_day.reindex(empty_day.index)

            df_day.sort_index(inplace=True)

            assert len(df_day.loc[df_day.index > df_day.last_valid_index(), :].index) > 100, (

            df_day, ' Sorry, the end date is not long enough')

            return df_day.loc[first_idx:, :]

 

    def extend_end_index(self,df,freq = 'M',periods = 12):

        '''

        extend the periods at the end so that it can shifted

        :param df:

        :param freq:  freq_dict = {'M':'months',

                     'W':'weeks',

                     'D':'days'}

        :param periods:

        :return:

        '''

        if freq in ['M']:

            to_append = pd.DataFrame(index=[df.index[-1] + pd.tseries.offsets.DateOffset(months=i) for i in range(periods)], columns=df.columns,

                                 data=np.empty((periods,len(df.columns))))

 

        if freq in ['W']:

            to_append = pd.DataFrame(index=[df.index[-1] + pd.tseries.offsets.DateOffset(weeks=i) for i in range(periods)],

                                     columns=df.columns,

                                     data=np.empty((periods,len(df.columns))))

 

        if freq in ['D']:

            to_append = pd.DataFrame(index=[df.index[-1] + pd.tseries.offsets.BDay(i) for i in range(periods)],

                                     columns=df.columns,

                                     data=np.empty((periods,len(df.columns))))

 

        if freq in ['Q']:

            to_append = pd.DataFrame(index=[df.index[-1] + pd.tseries.offsets.DateOffset(months=3*i) for i in range(periods)],

                                     columns=df.columns,

                                     data=np.empty((periods,len(df.columns))))

 

        df = df.append(to_append)

        return self.drop_duplicate(df)

 

    # TODO: create a merge function that merge the 2 dataframe for different frequency

    def df1_to_df2_freq_merge(self,df1,df2,how='outer',reverse_order = False):

        # convert df1's freq to df2's freq and merge

        df1,df2 = df1.copy(),df2.copy()

        freq1 = self.get_freq(df1)

        freq2 = self.get_freq(df2)

        if freq1 == freq2:

            if reverse_order:

                df1,df2 = df2,df1

            return pd.merge(df1,df2,left_index=True,right_index=True,how=how)

        elif freq2 == 'D':

            df1 = self.conversion_to_bDay(df1)

            if reverse_order:

                df1, df2 = df2, df1

            return pd.merge(df1, df2, left_index=True, right_index=True, how=how)

        elif freq2 == 'W':

            df1 = self.conversion_to_bDay(df1)

            common_dates = df1.index.intersection(pd.to_datetime(df2.index))

            assert len(common_dates)>26,'Sorry, the length of common dates is too few, check if correct'

            df1 = df1.reindex(common_dates)

            if reverse_order:

                df1, df2 = df2, df1

            return pd.merge(df1, df2, left_index=True, right_index=True, how=how)

        elif freq2 == 'M':

            df1 = self.conversion_down_to_m(df1)

            if reverse_order:

                df1, df2 = df2, df1

            return pd.merge(df1, df2, left_index=True, right_index=True, how=how)

        elif freq2 == 'Q':

            df1 = self.conversion_to_q(df1)

            if reverse_order:

                df1, df2 = df2, df1

            return pd.merge(df1, df2, left_index=True, right_index=True, how=how)

        else:

            print ('the freq is : ',freq2,'We dont currently support this!!!!!')

            raise ValueError

 

    def df1_to_df2_freq_and_multiply(self,df1,df2,how='outer'):

        # convert df1's freq to df2 and multiply

        df1,df2 = df1.copy(),df2.copy()

        assert len(df1.columns)==1

        assert len(df2.columns)==1

        df_comb = self.df1_to_df2_freq_merge(df1,df2,how=how)

        col_name = df1.columns[0]+' multiply '+df2.columns[0]

        df_comb[col_name] = df_comb.iloc[:,0]*df_comb.iloc[:,1]

        return df_comb[[col_name]]

 

    @staticmethod

    def get_freq(df):

        # check the frequency of the df, return either 'D' or 'M', or 'Y', FOR THE LATEST PERIOD OF TIME!!!!

        # check the frequency of the df, return either 'D' or 'M', or 'Y', FOR THE LATEST PERIOD OF TIME!!!!

        if len(df.index) < 0.001:

            return 'invalid_name'

        df.index = pd.to_datetime(df.index)

        l1 = df.dropna().tail(20).index.tolist()

        l2 = df.dropna().tail(21).index.tolist()[:20]

 

        # print (len(l1),len(l2))

 

        diff = np.array([(z1 - z2).days for z1, z2 in zip(l1, l2)])

        # print (diff)

 

        avg_diff = diff.mean()

 

        if avg_diff <= 2:

            return 'D'

        elif avg_diff <= 10:

            return 'W'

        elif avg_diff <= 40:

            return 'M'

        elif avg_diff <= 105:

            return 'Q'

        else:

            return 'Y'

 

    def delete_zero_beginning(self,df):

        return df.loc[df.replace(0,np.nan,inplace=False).first_valid_index():,:]

 

    def delete_zero_tail(self,df,delete_repeat_value=False):

        mask = (df.index<=df.replace(0,np.nan,inplace=False).last_valid_index())

        # mask2 is to delete the repeated value at the end

        mask2 = (df.index<=df.diff().replace(0,np.nan,inplace=False).last_valid_index())

        if delete_repeat_value:

            mask = mask&mask2

        return df.loc[mask&mask2,:]

 

    def fill_backward_with_zero(self,df):

        df = df.copy()

        for col in df.columns:

            mask = (df.index<df.loc[:,[col]].first_valid_index())

            df.loc[mask,col]=0

        return df

 

    def delete_bad_fill_forward_values(self,df):

        # delete the tail where it is filled forward for too long

        df = df.copy()

        for col in df.columns:

            diff = df.loc[:,[col]].diff(1)

            mask = (np.abs(diff.iloc[:,0]) < 0.00000000001)

 

            diff.loc[mask,:]=np.nan

            last_idx = diff.last_valid_index()

            if not last_idx in [None]:

                df.loc[df.index>last_idx, col] = np.nan

        return df

 

    def get_previous_Friday(self,dt):

        if not isinstance(dt, datetime):

            try:

                dt = datetime.strptime(dt, '%Y-%m-%d')

            except:

                dt = dt.to_pydatetime('%Y-%m-%d')

 

        if dt.weekday() not in [5, 6]:

            return dt

        else:

            return dt - timedelta(dt.weekday() - 4)

 

    def get_first_bday_of_month(self, dt):

        if not isinstance(dt, datetime):

            try:

                dt = datetime.strptime(dt, '%Y-%m-%d')

            except:

                dt = dt.to_pydatetime('%Y-%m-%d')

        dt = dt.replace(day=1)

        if dt.weekday() not in [5,6]:

            return dt

        else:

            return dt + timedelta(days= 7-dt.weekday())

 

    def get_last_bday_of_month(self, dt):

        if not isinstance(dt, datetime):

            try:

                dt = datetime.strptime(dt, '%Y-%m-%d')

            except:

                dt = dt.to_pydatetime('%Y-%m-%d')

 

        offset = BMonthEnd()

        # Last day of current month

        return offset.rollforward(dt)

 

    def get_last_bday_of_quarter(self, dt):

        if not isinstance(dt, datetime):

            try:

                dt = datetime.strptime(dt, '%Y-%m-%d')

            except:

                dt = dt.to_pydatetime('%Y-%m-%d')

 

        dt = dt

        offset = BQuarterEnd()

        # Last day of current month

        return offset.rollforward(dt)

 

    def smooth_change(self,df,periods, ann = False,ann_type = 'geo',col = -1):

        '''

        return: calculate the yoy etc with back-end of the data smoothed

        :param periods: if yoy, then 12

        :param type: the way to annualise it

        '''

        try:

            df = df.to_frame()

        except:

            pass

        if periods<1.01:

            if self.get_freq(df.dropna())=='D':

                ann_factor=252

            elif self.get_freq(df.dropna())=='M':

                ann_factor=12

            elif self.get_freq(df.dropna())=='Q':

                ann_factor=4

            else:

                ann_factor=1

            return df.dropna().pct_change(1)*ann_factor if ann_type=='geo' else df.dropna().diff(1)*ann_factor

        df1 = df.iloc[:, [col]].dropna()

        periods2 = int(periods*1.5)

        if self.get_freq(df1) == 'M':

            ann_factor = 12/(periods2/2-0.5)

        elif self.get_freq(df1) == 'Q':

            print ('sorry the series is quarterly, use simple percentage change!')

            raise ValueError

        elif self.get_freq(df1)=='D':

            ann_factor = 252/(periods/2)

        elif self.get_freq(df1)=='W':

            ann_factor = 52 / (periods/2)

 

        if ann_type=='geo':

            if ann:

                return (df1/self.rolling_ignore_nan(df1,periods2,np.mean))**ann_factor-1

            else:

                return (df1 / self.rolling_ignore_nan(df1, periods2, np.mean)) - 1

        else:

            if ann:

                return (df1-self.rolling_ignore_nan(df1,periods2,np.mean))*ann_factor

            else:

                return (df1-self.rolling_ignore_nan(df1,periods2,np.mean))

 

    def reverse_smooth_change(self,df_to_restore,df_change,periods,ann=False,ann_type='geo',new_name = 'restore_series'):

        # reverse the smooth change, given the original series and change series. output reconstructued series

        df_to_restore,df_change = df_to_restore.dropna(),df_change.dropna()

        assert self.get_freq(df_to_restore)==self.get_freq(df_change), 'sorry, the freq of df_to_restore and df_change are different'

 

        periods2 = int(periods*1.5)

        if ann in [True]:

            if self.get_freq(df_to_restore) == 'M':

                ann_factor = 12/(periods2/2-0.5)

            elif self.get_freq(df_to_restore) == 'Q':

                print ('sorry the series is quarterly, unable to restore!')

                raise ValueError

            elif self.get_freq(df_to_restore)=='D':

                ann_factor = 252/(periods/2)

 

        df_comb = pd.concat([df_to_restore,df_change],axis=1)

        df_comb.iloc[:,0]=df_comb.iloc[:,0].rolling(periods2).mean()

        if ann_type == 'geo':

            df_comb.iloc[:,1] = df_comb.iloc[:,1]+1

        if ann in [True]:

            if ann_type == 'geo':

                df_comb.iloc[:,1]=df_comb.iloc[:,1]**(1/ann_factor)

            else:

                df_comb.iloc[:,1]=df_comb.iloc[:,1]*(1/ann_factor)

        if ann_type == 'geo':

            df_comb[new_name] = df_comb.iloc[:,0]*df_comb.iloc[:,1]

        elif ann_type == 'aris':

            df_comb[new_name] = df_comb.iloc[:, 0] + df_comb.iloc[:, 1]

        return df_comb[[new_name]].dropna()

 

    def sea_adj(self,df):

        try:

            df1 = df.copy()

            try:

                assert df1.index.freq not in [None]

            except:

                df1 = self.convert_std_freq(df1)

            df1.iloc[:,0] = sta_m.tsa.x13_arima_analysis(df1.dropna(),x12path=WKDIR).seasadj

            return df1

        except:

            return self.self_made_sea_adj(df1)

 

    # this is just because the usual sea adj failed so many times!!!

    def self_made_sea_adj(self,df_raw):

        df = df_raw.copy()

        if isinstance(df,pd.Series):

            df = df.to_frame()

        if len(df.loc[df.iloc[:, 0] <= 0, :].index) > 0.9:

            shift_factor = -df.min().values[0] + 0.000001

        else:

            shift_factor = 0

        df = df + shift_factor

 

        df['seasonal_factor'] = seasonal_decompose(df, model='m').seasonal

        #         df['centre_ma'] = df.iloc[:,0].rolling(12,center=True).mean()

        #         df['de_centre'] = df.iloc[:,0] / df['centre_ma']

        #         df_sea_factor = df[['de_centre']].groupby(by=[df.index.month]).mean()

        #         df_sea_factor.columns = ['seasonal_factor']

        #         df['month'] = df.index.month

        #         df = pd.merge(left=df,right=df_sea_factor,left_on=['month'],right_index=True).sort_index()

        df['sea_adj'] = df.iloc[:, 0] / df['seasonal_factor']

        df.iloc[:, 0] = df['sea_adj'] - shift_factor

        return df.iloc[:,[0]]

 

    def common_index(self,df1,df2):

        return sorted(list(set(df1.index.tolist()).intersection(set(df2.index.tolist()))))

 

    def hysteresis(self,df,method='bucket',bin=[-np.inf,-4,-3,-2,-1,0,1,2,3,4, np.inf],value = [-4,-3,-2,-1,0,1,2,3,4,4.01],new_name = 'discret_value',col = -1):

        df1 = df.copy()

        if method=='bucket':

            df1[new_name] = pd.cut(df1.iloc[:,col], bin, labels=value)

        return df1

 

    def splice_geometric_a(self,s_1, s_2, new_name='longer_series'):

        """splice 2 series together, s_1 has precedence over s_2"""

        # test if first date of series one exists in series 2

        if len(s_1.dropna().index.intersection(s_2.dropna().index)) == 0:

            raise ValueError("Series dates do not overlap, cannot compute scale factor for series 2.")

        s_1.dropna(inplace=True)

        s_2.dropna(inplace=True)

 

        first_common = s_1.index.intersection(s_2.index)[0]

 

        scale_s_2 = s_1.loc[first_common, :].values / s_2.loc[first_common, :].values

 

        # scale second series to match the first

        s_2_truncated_scaled = s_2.loc[:s_1.index[0]] * scale_s_2

        s_2_truncated_scaled.drop(s_1.index[0], errors='ignore', inplace=True)

        # s_2_truncated = s_2.loc[:s_1.index[0]].iloc[:-1] * s_1[0] / s_2[s_1.index[0]]

 

        # change the column name

        s_2_truncated_scaled.columns=[0]

        s_1.columns = [0]

 

        # concatenate truncated time-series

        output_series = pd.concat([s_2_truncated_scaled, s_1], axis=0).sort_index()

        output_series.columns = [new_name]

 

        return output_series

 

    def check_lag_enough(self,_df=[], pause=False):

        '''

        this is replaced by __adj_to_use_latest_value

        '''

        pass

        # for i,df in enumerate(_df):

        #     if df.dropna().index[-1]<pd.to_datetime(datetime.today().date()):

        #         title = 'missing value'

        #         content = 'Sorry, the last date in df : '+df.columns[0]+'is: '+df.dropna().index[-1].strftime('%Y-%m-%d')

        #         if pause:

        #             show_pop_up(title,content)

        #         #print ('Sorry, the last date in df : ',df.columns[0],'is: ',df.dropna().index[-1].strftime('%Y-%m-%d'))

        #         #print (df.dropna().index[-1])

        #         #print (pd.to_datetime(datetime.today().date()))

 

    def adj_so_that_use_latest_info(self, df, running_mode='production',curr_exec_path='unknown',flagging_dir = None,assume_today=None,**kwargs):

        '''

        in production environment, making sure last z_score used for today contains the latest available information

        this algorithm simply remove the signals from today onwards and fill the latest score with what ever is the last score. Advantage is minimum adjustment(shift) being made for historical result

        flagging_dir: a txt file which rec the fillna activities

        :return: adjust the latest

        '''

        if running_mode == 'research':

            return df

        else:

            df = df.copy()

            assert df.shape[1] < 1.1, ' sorry, the z_score has more than 1 columns, please check : ' + df.columns[0]

            today = pd.to_datetime(datetime.today().date()) if assume_today in [None] else pd.to_datetime(assume_today)

            today = today -pd.tseries.offsets.BDay(1)+pd.tseries.offsets.BDay(1) # making sure today is a business day

            # if lag is not enough, simply repeat value :

            if df.dropna().index[-1] < today:

                if flagging_dir != None:

                    with open(flagging_dir, "a") as flag:

                        flag.write(curr_exec_path + " : \n")

                        flag.write(df.columns[0] + 'stops at : ' + df.dropna().index[-1].strftime('%Y-%m-%d') + ' , \n')

 

                print('Sorry, the last date in df : ', df.columns[0], 'is: ',

                      df.dropna().index[-1].strftime('%Y-%m-%d'))

 

                print(today)

                print('fillna here!!')

                # extend the index to today

                while df.index[-1] < today:

                    to_append = pd.DataFrame(index=[df.index[-1]+pd.tseries.offsets.BDay(1)],columns = df.columns,data=[np.nan]*len(df.columns))

                    df = df.append(to_append,)

                df.fillna(method='ffill', inplace=True)

                df.loc[df.index > today, :] = np.nan

                # create flagging message:

                #print(curr_exec_path, flagging_dir)

                return df

            else:  # check the latest info is being used today

                df = df.dropna()

                latest_info = df.iloc[:, 0].values[-1]

                today_info = df.loc[today].values[0]

                if latest_info != today_info:

                    df.loc[today:,:]=np.nan

                    df.loc[today,:] = latest_info

               else:

                    df.loc[today:, :] = np.nan

                    df.loc[today, :] = latest_info

                return df

 

    def extend_to_x_bday_ago(self, df, days = -1,running_mode='production',curr_exec_path='unknown',flagging_dir = None):

        '''

        in production environment, making sure last z_score used for today contains the latest available information

        this algorithm simply remove the signals from today onwards and fill the latest score with what ever is the last score. Advantage is minimum adjustment(shift) being made for historical result

        flagging_dir: a txt file which rec the fillna activities

        :return: adjust the latest

        '''

        if running_mode == 'research':

            return df

        else:

            df = df.copy()

            assert df.shape[1] < 1.1, ' sorry, the z_score has more than 1 columns, please check : ' + df.columns[0]

            x_day_ago = pd.to_datetime(datetime.today().date())+pd.tseries.offsets.BDay(days)

            # if lag is not enough, simply repeat value :

            if df.dropna().index[-1] < x_day_ago:

                if flagging_dir != None:

                    with open(flagging_dir, "a") as flag:

                        flag.write(curr_exec_path + " : \n")

                        flag.write(df.columns[0] + 'stops at : ' + df.dropna().index[-1].strftime('%Y-%m-%d') + ' , \n')

 

                print('Sorry, the last date in df : ', df.columns[0], 'is: ',

                      df.dropna().index[-1].strftime('%Y-%m-%d'))

                print(df.dropna().index[-1])

                print(pd.to_datetime(datetime.today().date()))

                print('fillna here!!')

                # extend the index to today

                while df.index[-1] < x_day_ago:

                    to_append = pd.DataFrame(index=[df.index[-1]+pd.tseries.offsets.BDay(1)],columns = df.columns,data=[np.nan]*len(df.columns))

                    df = df.append(to_append,)

                    print (df.tail())

                df.fillna(method='ffill', inplace=True)

                df.loc[df.index > x_day_ago, :] = np.nan

                # create flagging message:

                #print(curr_exec_path, flagging_dir)

                return df

            else:  # check the latest info is being used today

                return df

 

    def remove_outlier(self,df,frac=0.01,n=3,use_end_point=True,end_point='2020-02-01'):

        '''

        :param df:

        :param frac:

        :param n:

        :param end_point: define the end of stop doing the filtering? remove outlier is mainly for the purpose of backtesting?

        :return:

        '''

        import Analytics.loess_filter as lf

        assert len(df.columns)<1.01,'only 1 columns can be taken!!'

        df = df.copy().dropna()

        y_ticker = df.columns.tolist()[0]

        Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False, frac)

        df_trend = Lo_Filter.estimate()[['y_fitted']]

        df_trend.columns = df.columns

        dif = abs(df-df_trend)

        dif.columns = df.columns

        thres = dif.iloc[:,0].mean()*n

 

        if use_end_point in [False]:

            mask = dif.iloc[:,0]>thres

            df.loc[mask , :] = np.nan

        if use_end_point in ['1m_ago']:

           # apply outlier remover until 1m ago

            last_dt = dif.dropna().index[-1]

            end_point = last_dt - pd.DateOffset(months=1)

            mask = dif.iloc[:, 0] > thres

            mask2 = dif.index<end_point

            df.loc[mask & mask2, :] = np.nan

        else:

            end_point = pd.to_datetime(end_point)

            mask = dif.iloc[:, 0] > thres

            mask2 = dif.index<=end_point

            df.loc[mask&mask2,:] = np.nan

        if self.get_freq(df) == 'D':

            return self.conversion_to_bDay(df)

        elif self.get_freq(df)=='M':

            return self.conversion_down_to_m(df)

        elif self.get_freq(df)=='Q':

            return self.conversion_to_q(df)

        else:

            return df.fillna(method='ffill')

 

    def is_out_lier(self,df,frac=0.2,n=3):

        # check if the data is outlier

        import Analytics.loess_filter as lf

        assert len(df.columns) < 1.01, 'only 1 columns can be taken!!'

        df = df.copy().dropna()

        # check if the data is stationary

        y_ticker = df.columns.tolist()[0]

        Lo_Filter = lf.loess_filter(df, None, y_ticker, None, False, frac)

        df_trend = Lo_Filter.estimate()[['y_fitted']]

        df_trend.columns = df.columns

        dif = abs(df - df_trend)

        dif.columns = df.columns

        thres = dif.iloc[:, 0].mean() * n

        if dif.iloc[-1, 0] > thres:

            return {'result':True,

                    'upper_band':df_trend + thres,

                    'lower_band':df_trend - thres}

        else:

            return {'result':False}

 

    def is_stationary(self,df):

        assert df.shape[1]<1.01,"Sorry, input series should be 1 column only"

        return True if df.iloc[:,0].autocorr()<0.99 else False

 

    def stationarify(self,df):

        is_stationary = self.is_stationary(df)

        if is_stationary in [False]:

 

            if df.min().iloc[0]<0:

                df = df.diff().dropna()

            else:

                df = df.pct_change().dropna()

 

        return {'is_stationary':is_stationary,

                'stationary_series':df}

 

    def x_month_later_yth_weekday_z(self,date_index, month, week_number, weekday):

        # genr the

        date_index = date_index + pd.DateOffset(months=month)

        new_index = []

        for d in date_index:

            d = d.replace(day=1)

            if d.weekday() > weekday:

                d = d + pd.DateOffset(day=7 + 1 + weekday - d.weekday() + 7 * (week_number - 1))

            else:

                d = d + pd.DateOffset(day=1 + (int(weekday - d.weekday() + 7 * (week_number - 1))))

 

            new_index.append(d)

        return new_index

 

    def Ave_index_df_short_hist(self,df_list,calc='mean'):

        df_list = [df.dropna() for df in df_list]

        df_comb = pd.concat(df_list, axis=1)

        df_change = df_comb.diff()

        if calc in ['mean']:

            df_change['ave_chg'] = df_change.mean(axis=1, skipna=True)

            df_index = df_change[['ave_chg']].cumsum()

            index_date = df_comb.dropna().index[0]

            shift_factor = df_comb.mean(axis=1).loc[index_date] - df_index.loc[index_date, :].values[0]

            df_index = df_index + shift_factor

            df_index.columns = ['average_index']

 

        elif calc in ['sum']:

            df_change['sum_chg'] = df_change.sum(axis=1, skipna=True)

            df_index = df_change[['sum_chg']].cumsum()

            index_date = df_comb.dropna().index[0]

            shift_factor = df_comb.sum(axis=1).loc[index_date] - df_index.loc[index_date, :].values[0]

            df_index = df_index + shift_factor

            df_index.columns = ['sum_index']

        return df_index

 

    def convert_currency(self,df_econ,df_fx_spot):

        df_econ,df_fx_spot = df_econ.dropna(),self.conversion_to_Day(df_fx_spot.dropna())

        if self.get_freq(df_econ) in ['M']:

            df_fx_spot = self.conversion_to_Day(df_fx_spot.resample('MS').mean())

        if self.get_freq(df_econ) in ['Q']:

            df_fx_spot = self.conversion_to_Day(df_fx_spot.resample('QS').mean())

        else:

            df_fx_spot = self.conversion_to_Day(df_fx_spot)

        df_fx_spot = df_fx_spot.reindex(df_econ.index)

        df = (df_econ.iloc[:,0]/df_fx_spot.iloc[:,0]).to_frame()

        df.columns = ['converted']

        return df

 

    def convert_std_freq(self, s_1):

        # convert to standardized freq

        f = self.get_freq(s_1)

        if isinstance(s_1,pd.Series):

            return_type = 's'

            s_1 = s_1.to_frame()

        else:

            return_type = 'unchange'

        if f in ['D']:

            out = self.conversion_to_bDay(s_1)

        elif f in ['W']:

            out = s_1

        elif f in ['M']:

            out = self.conversion_down_to_m(s_1)

        elif f in ['Q']:

            out = self.conversion_to_q(s_1)

        else:

            out = s_1

        if return_type in ['s']:

            return out.iloc[:,0]

        else:

            return out

 

 

def _haver_ticker_parser(ticker):

    if isinstance(ticker, list):

        assert len(ticker) <= 1.01, 'Sorry, the len of ticker passed is too long!!!' + ticker

        ticker = ticker[0]

    if '@' not in ticker:

        return [ticker]

    else:

        a, b = ticker.split('@')[0], ticker.split('@')[1]

        return [':'.join([b, a])]