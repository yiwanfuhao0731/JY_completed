import pandas as pd
import glob
import os
import numpy as np
import matplotlib.pyplot as plt

def get_data():
    all_file = glob.glob(r"D:\yangs\Third_yr_grad_English\relevant_file\library\GS research\indicators\*.xlsx")
    df_result = pd.DataFrame()
    for f in all_file[:]:
        try:
            df = pd.read_excel(f,sheetname='Fiscal Impulse',index_col=0,header=0)
            df_this_total = df.loc[['Total Federal +State/Local'],:].transpose()
            df_this_total.reset_index(inplace=True,drop=True)
            #print (df_this_total)
            df_this_total.columns = [os.path.basename(f)]
            df_result = pd.concat([df_result,df_this_total],axis=1)
        except:
            pass

    df_result.to_csv(r'D:\yangs\Third_yr_grad_English\relevant_file\library\GS research\indicators\all_data.csv')

def get_continuous_quarter():
    import_dir = r"D:\yangs\Third_yr_grad_English\relevant_file\library\GS research\indicators\GS fiscal impulse full.xlsx"
    df = pd.read_excel(import_dir,index_col=0,header=0)
    df.index = pd.to_datetime(df.index)
    df.columns = pd.to_datetime(df.columns)
    df_new = df.copy()
    df_new.loc[:,:]=np.nan
    df_new['average_next_2 quarter'] = np.nan
    all_col = sorted(df.columns.tolist())
    for i in df.index[:]:
        next_2_q = [q for q in all_col if q>=i][:2]
        next_2_q_avg = df.loc[i,next_2_q].sum()
        df_new.loc[i,'average_next_2 quarter'] = next_2_q_avg
    df_continuous = df_new[['average_next_2 quarter']]
    df_fwd_looking = df.iloc[[-1],:].transpose()
    data_range = pd.date_range('2010-1-1','2022-12-31')
    df_continuous = df_continuous.reindex(data_range)
    df_fwd_looking = df_fwd_looking.reindex(data_range)
    df_continuous = df_continuous.fillna(method='ffill')
    df_fwd_looking = df_fwd_looking.fillna(method='ffill')
    pd.concat([df_continuous,df_fwd_looking],axis=1).plot()
    plt.show()
    return df_new[['average_next_2 quarter']]

df = get_continuous_quarter()
df.to_csv(r'D:\yangs\Third_yr_grad_English\relevant_file\library\GS research\indicators\continuous_next_2_q.csv')