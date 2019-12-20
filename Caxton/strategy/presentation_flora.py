import pandas as pd
from matplotlib import pyplot as plt
import numpy as np

import_dir1 = r"d:\yangs\Third_yr_grad_English\relevant_file\workspace_zy\Caxton\strategy\rates_table.csv"
import_dir2 = r"D:\yangs\Third_yr_grad_English\relevant_file\presentation\US GDP.csv"
import_dir3 = r"d:\yangs\Third_yr_grad_English\relevant_file\presentation\US Core Inflation.csv"
import_dir4 = r"d:\yangs\Third_yr_grad_English\relevant_file\presentation\USA CU.csv"

df_gdp = pd.read_csv(import_dir2,header = 0,index_col=0)
df_gdp.index = pd.to_datetime(df_gdp.index)
df_gdp.columns = ['USA GDP yoy']

df_gdp = df_gdp.pct_change(4)*100
# df_gdp.loc['1995':].plot(figsize = (10,6),grid = True)

df_rate = pd.read_csv(import_dir1,header=0,index_col=0).iloc[:,[0]]
df_rate.index = pd.to_datetime(df_rate.index)
df_rate.columns = ['USA 5Y Rate']
#df_rate.loc['1995':].plot(figsize = (10,6),grid=True)

df_cpi = pd.read_csv(import_dir3,header=0,index_col=0)
df_cpi.index = pd.to_datetime(df_cpi.index)
df_cpi.columns = ['USA core inflation (trimmed mean)']
#df_cpi.plot()
#plt.show()

df_cu = pd.read_csv(import_dir4,header=0,index_col=0)
df_cu.index = pd.to_datetime(df_cu.index)
df_cu.columns = ['USA capacity utilization']

df_comb = pd.concat([df_gdp,df_rate],axis=1)
df_comb.sort_index(inplace=True)
df_comb.fillna(method='ffill',inplace=True)
#df_comb.loc['2005':,:].plot(grid=True,color=('blue','red'),figsize=(10,6),title='USA GDP yoy vs USA 5-year rate') #secondary_y = ['USA 5Y Rate'],
#plt.show()
df_comb2 = pd.concat([df_cpi,df_rate],axis=1).sort_index().fillna(method='ffill')
#df_comb2.loc['2005':,:].plot(grid=True,color=('blue','red'),figsize=(10,6),title='USA core inflation vs USA 5-year rate') #secondary_y = ['USA 5Y Rate'],
#plt.show()
df_comb3 = pd.concat([df_cu,df_rate],axis=1).sort_index().fillna(method='ffill')
df_comb3.loc['2005':,:].plot(grid=True,color=('blue','red'),figsize=(10,6),title='USA capacity utilization vs USA 5-year rate',secondary_y = ['USA 5Y Rate'],ylim=(74,84)) #secondary_y = ['USA 5Y Rate'],
plt.show()
