import numpy as np
import statsmodels.api as sm
import matplotlib.pyplot as plt
from series_util_pc import date_utils_collection as su
import pandas as pd
from datetime import timedelta



def loess_regr(df,col_name,frac=0.75,no_forward_looking=False,re_estimate_freq = 'M'):
    '''
    :param df: times series to produce the loess regression
    : col_name: the y column name that perform the loess_regr
    :frac : how smooth it is, between 0 and 1
    : no forward looking: it using only the current available info. By default the first 20 datapoints just use the whole sample period estimate
    : re_estimate_freq : re-estimate by how long if no forward looking is allowed. For example, for daily data, if re-estimate freq is 'M', it re_estimate every 21 days.
    :return: another df with fitted line by its side
    '''
    SU = su()
    lowess = sm.nonparametric.lowess
    df = df.loc[:,[col_name]].dropna()
    y = df.iloc[:,0].values
    x = range(len(y))

    if no_forward_looking in [False]:
        res = pd.DataFrame(index = df.index,columns=['x','y_fitted'],data=lowess(y,x,frac=frac))
        df['y_fitted'] = res['y_fitted']
        return df
    else:
        # define: a) first 20 estimate, b) re-estimate freq. For 'M', just re-estimate at the first day of each month, etc
        # check if the data if longer than 20, if not, just use the whole sample period
        if len(df.index)<=20.01:
            res = pd.DataFrame(index=df.index, columns=['x', 'y_fitted'], data=lowess(y, x,frac=frac))
            df['y_fitted'] = res['y_fitted']
            return df
        else:
            df['y_fitted'] = np.nan
            re_estimate_date_thres_list = pd.date_range(start=df.index[20],end=df.index[-1],freq=re_estimate_freq)
            # figure out the re_estimate date
            list_of_iloc_re_estimate_date = [19]
            for thres in re_estimate_date_thres_list:
                last_date_by_thres = [d for d in df.index if d<=thres][-1]
                #print (last_date_by_thres)
                list_of_iloc_re_estimate_date.append(df.index.get_loc(last_date_by_thres))
            for i in list_of_iloc_re_estimate_date[:1]:
                this_y = y[:i+1]
                this_x = x[:i+1]
                #print (len(this_y))
                #print (lowess(this_y,this_x)[-1,1])
                #print (len(df.iloc[:i+1,1].values))
                #print (len(lowess(this_y,this_x)[:,1]))
                df.iloc[:i+1,1]=lowess(this_y,this_x,frac=frac)[:,1]
            for i in list_of_iloc_re_estimate_date[1:]:
                this_y = y[:i+1]
                this_x = x[:i+1]
                #print (len(this_y))
                #print (lowess(this_y,this_x)[-1,1])
                df.iloc[i,1]=lowess(this_y,this_x,frac=frac)[-1,1]

            df = df.fillna(method='ffill')
            return df

if __name__=='__main__':
    SU = su()
    import_dir = r'..//rates_table.csv'
    df = pd.read_csv(import_dir,header=0,index_col=0)
    df.index = pd.to_datetime(df.index)
    df = SU.conversion_down_to_m(df.iloc[:,[0]]).dropna()
    df = df.loc[:'2008',:]
    #print (df.index[0],df.index[-1])
    #re_estimate_date_list = pd.date_range(start=df.index[0], end=df.index[-1], freq='D')
    #print (re_estimate_date_list[0],re_estimate_date_list[-1])
    y_ticker = df.columns[0]
    df_fitted = loess_regr(df,col_name=y_ticker,no_forward_looking=True,frac=1)
    df_fitted.plot(grid=True)

    df_fitted = loess_regr(df, col_name=y_ticker, no_forward_looking=False,frac=1)
    df_fitted.plot(grid=True)
    plt.show()

# x = np.random.uniform(low = -2*np.pi, high = 2*np.pi, size=500)
# y = np.sin(x) + np.random.normal(size=len(x))
# z = lowess(y, x)
# w = lowess(y, x, frac=1./3)
#
# plt.plot(x,y,'r+')
# plt.show()
#
# plt.plot(z[:,0],z[:,1],'+')
# plt.show()