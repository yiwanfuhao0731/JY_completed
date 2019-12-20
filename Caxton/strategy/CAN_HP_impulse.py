'''
Created on 2019年6月8日

@author: user
'''
import statsmodels.api as sm
from statsmodels.tsa.tsatools import add_trend
import pandas as pd
import matplotlib.pyplot as plt

from statsmodels.tsa.api import VAR, SVAR
import matplotlib.pyplot as plt
import numpy as np

import_dir = r"d:\yangs\Third_yr_grad_English\relevant_file\eclipse\workspace_zy\Caxton\strategy\CAN_HP.xlsx"
df_hp = pd.read_excel(import_dir,usecols=[0,1],index_col=0,header=0)

import_dir = r"d:\yangs\Third_yr_grad_English\relevant_file\eclipse\workspace_zy\Caxton\strategy\CAN_GDP.xlsx"
df_gdp = pd.read_excel(import_dir,usecols=[0,1],index_col=0,header=0)

import_dir = r"d:\yangs\Third_yr_grad_English\relevant_file\eclipse\workspace_zy\Caxton\strategy\CAN_Credit.xlsx"
df_credit = pd.read_excel(import_dir,usecols=[0,1,2],index_col=0,header=0)

import_dir = r"d:\yangs\Third_yr_grad_English\relevant_file\eclipse\workspace_zy\Caxton\strategy\CAN_PR.xlsx"
df_pr = pd.read_excel(import_dir,usecols=[0,1,2],index_col=0,header=0)

# impulse on hp
df_hp.index = pd.to_datetime(df_hp.index)
df_gdp.index = pd.to_datetime(df_gdp.index)
df_credit.index = pd.to_datetime(df_credit.index)
df_pr.index = pd.to_datetime(df_pr.index)

df_gdp['growth'] = df_gdp.iloc[:,0].pct_change(12)
df_hp['diff1'] = df_hp.iloc[:,0].pct_change(12)
df_hp['HP_diff_12x12'] = df_hp['diff1'].diff(12)
# d_trend the policy rate
df_pr = df_pr.diff(12)

df_credit['credit_diff12'] = df_credit['credit_ngdp'].diff(12)
df_credit['Credit_diff_12x12'] = df_credit['credit_diff12'].diff(12) 

#data = pd.merge(df_gdp[['growth']],df_hp[['HP_diff_12x12']],left_index=True,right_index=True,how='outer')
#data = pd.merge(data,df_credit[['Credit_diff_12x12']],left_index=True,right_index=True,how='outer')
data = pd.merge(df_gdp[['growth']],df_pr[['PR']],left_index=True,right_index=True,how='outer')
data.loc['2000-01-01':].plot(grid=True)
plt.show()
#data = pd.merge(data,df_pr[['PR']],left_index=True,right_index=True,how='outer')

data = data.dropna()
data = data.loc['2010-01-01':'2020-01-01']
print (data.head())
print (data.tail())

# structural var
#A = np.array([[1, 0,0], ['E', 1,0],['E', 'E',1]])
#B = np.array([['E', 0,0], [0, 'E',0],[0, 0,'E']])
A = np.array([[1, 0], ['E', 1]])
B = np.array([['E', 0], [0, 'E']])

mymodel = SVAR(data, svar_type='AB', A=A,B=B)
res = mymodel.fit(maxlags=24, maxiter=10000, maxfun=10000, solver='bfgs')
#res.irf(periods=30).plot(impulse='realgdp', plot_stderr=False)

print (res.summary())
print (res.A)
print (res.coefs)

res.irf(periods=48).plot(plot_stderr=False)
plt.show()
trans_param = {}
def growth():
    trans_param['param1']=1
    trans_param['param2']=2
    z_ = 'z1'

def growth2():
    trans_param['param3']=1
    trans_param['param4']=2
    z_ = 'z2'

def growth3():
    trans_param['param5']=1
    trans_param['param6']=2
    z_ = 'z3'

def growth4():
    trans_param['param7']=1
    trans_param['param8']=2
    z_ = 'z4'