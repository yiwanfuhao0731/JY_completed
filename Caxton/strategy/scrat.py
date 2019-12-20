import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from Caxton.strategy.Analytics_for_pc.series_util_pc import date_utils_collection as su
import itertools

from Caxton.strategy.Analytics_for_pc.loess import loess_regr as lf

def estimate_var_cov_mat(df_asset_group,df_iv_group,rolling_long,rolling_short):
    '''
    :param df_asset_group: asset price dataframe
    :param df_iv: implied volatility dataframe. Data should be extended back (by realized volatility).
    Using this to estimate the diagonal element
    implied volatility and asset group should be in the same order
    :return: var_cov matrix
    '''
    # weekly correlation matrix
    df_corr_mat = df_asset_group.diff(5).ewm(span=252).corr()
    # annualised volatility
    df_rollVol_long_group = df_asset_group.diff(1).rolling(window=rolling_long).std()*np.sqrt(252)*100
    df_rollVol_short_group = df_asset_group.diff(1).rolling(window=rolling_short).std()*np.sqrt(252)*100
    print ('start')

    df_vol_list = []
    for i in range(len(df_asset_group.columns)):
        df1,df2,df3 = df_rollVol_long_group.iloc[:,[i]],df_rollVol_short_group.iloc[:,[i]],df_iv_group.iloc[:,[i]]
        df_vol_estimate = pd.concat([df1,df2,df3],axis=1).mean(axis=1,skipna=True)
        df_vol_estimate = df_vol_estimate.to_frame()
        df_vol_estimate.columns = [df_asset_group.columns[i]+'_volBar']
        df_vol_list.append(df_vol_estimate)
        #df_vol_estimate.plot(figsize=(10,5),grid=True)

    df_vol_grp = pd.concat(df_vol_list,axis=1)
    # construct var-cov matrix with correlation matrix and estimated_vol
    df_vol_grp.fillna(method='bfill',inplace=True)
    # find common dates
    common_date = sorted(list(set(df_vol_grp.dropna().index.intersection(df_corr_mat.dropna().index.get_level_values(0)))))
    #array_vol_group, array_corr_mat = df_vol_grp.loc[common_date,:].values,df_corr_mat.loc[common_date,:].values
    # iterate through to get the matrix
    for d in common_date:
        this_vol = df_vol_grp.loc[d,:].values
        this_corr = df_corr_mat.loc[d,:].values
        for (i, j) in itertools.product(list(range(len(this_vol))), repeat=2):
            this_corr[i,j]=this_corr[i,j]*this_vol[i]*this_vol[j]
        df_corr_mat.loc[d, :] = this_corr
    print (df_corr_mat.loc[common_date[-1],:])
    return df_corr_mat

def estimate_1d_vol_with(weight,var_cov_mat):
    pass

if __name__=='__main__':

    SU = su()

    import_dir = r'rates_table2.csv'
    import_dir2 = r'rates_table.csv'

    for (i, j) in itertools.product(list(range(2)), repeat=2):
        print (i,j)

    df = pd.read_csv(import_dir,header=0,index_col=0)
    df.index = pd.to_datetime(df.index)
    #df.plot(grid=True, figsize = (10,5))
    df2 = pd.read_csv(import_dir2, header=0, index_col=0)
    df2.index = pd.to_datetime(df2.index)


    df_usd = SU.remove_outlier(df.iloc[:,[0]],n=1000)
    df_cad = SU.remove_outlier(df.iloc[:,[1]])
    df_usd_vol = SU.remove_outlier(df.iloc[:,[2]],n=4)
    df_cad_vol = SU.remove_outlier(df.iloc[:,[3]])
    df_usd_vol2 = SU.remove_outlier(df2.iloc[:,[2]],n=3)
    df_cad_vol2 = SU.remove_outlier(df2.iloc[:, [3]], n=3)
    df_cad_vol2.plot()

    df_usd_realized_vol = df_usd.dropna().diff(1).rolling(window=63).std()*np.sqrt(252)

    # df_usd.plot()
    # plt.show()
    # df_usd_vol.plot()
    # plt.show()
    # df_usd_realized_vol.plot()
    # plt.show()

    # pd.concat([df_usd_realized_vol,df_usd_vol],axis=1).dropna().plot(grid=True,figsize = (10,5),secondary_y = [df_usd_realized_vol.columns[0]])
    # pd.concat([df_usd_realized_vol, df_usd_vol2], axis=1).dropna().plot(grid=True, figsize=(10, 5),
                                                                       #secondary_y=[df_usd_realized_vol.columns[0]])
    # df_comb = pd.concat([df_usd_realized_vol,df_usd_vol2],axis=1).dropna()
    # df_comb['ratio'] = df_comb.iloc[:,1]/df_comb.iloc[:,0]/100
    # df_comb[['ratio']].plot(grid=True,figsize = (10,5))
    #
    # print (df_comb.tail(5))

    df_asset_group = pd.concat([df_usd,df_cad],axis=1)
    # df_asset_group.plot()

    # print (corr_mat.loc['2019-10-28',:].values)
    df_iv_group = pd.concat([df_usd_vol2,df_cad_vol2],axis=1)
    # df_iv_group.plot()

    corr_mat = estimate_var_cov_mat(df_asset_group, df_iv_group=df_iv_group,rolling_long=252,rolling_short=63)