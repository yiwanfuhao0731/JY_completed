'''
Created on 2019年6月2日

@author: user
'''
# this is the model for structural var analysis and impulsive responsive function analysis
import statsmodels.api as sm
from statsmodels.tsa.tsatools import add_trend
import pandas as pd
import matplotlib.pyplot as plt

from statsmodels.tsa.api import VAR, SVAR
import matplotlib.pyplot as plt
import numpy as np

import_dir = r"C:\Users\user\Documents\STRUCTURAL VAR RAW.xlsx"
df = pd.read_excel(import_dir,sheet_name=r'structural var',usecols=[0,1,2,3,4],index_col=0,header=0)

print (df.head())
# simple AR model
data = df.loc[:,['y1','y2']].values

# structural var
A = np.array([[1, 'E'], [0, 1]])
B = np.array([['E', 0], [0, 'E']])


mymodel = SVAR(data, svar_type='AB',A=A, B=B)
res = mymodel.fit(maxlags=12, maxiter=10000, maxfun=10000, solver='bfgs')
#res.irf(periods=30).plot(impulse='realgdp', plot_stderr=False)

print (res.summary())
print (res.A)
print (res.coefs)

res.irf(periods=30).plot(plot_stderr=False)
plt.show()


