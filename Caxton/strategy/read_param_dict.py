'''
Created on 2019年8月7日

@author: user
'''
# read from xlsx to a dictionary of the tree struct
import pandas as pd

import_dir = r"d:\yangs\Third_yr_grad_English\relevant_file\eclipse\workspace_zy\Caxton\strategy\param_dict.xlsx"

df_param = pd.read_excel(import_dir,index_col=None)

#df_param.fillna('invalid_name',inplace=True)
df_param.set_index('key',inplace=True)
#print(df_param)

CONPONENT_INFO_DICT = dict()
for i in df_param.index:
    k_list = df_param.loc[i,:].dropna().index.tolist()
    v_list = df_param.loc[i,:].dropna().tolist()
    CONPONENT_INFO_DICT[i] = {k:v for k,v in zip(k_list,v_list)}

print (CONPONENT_INFO_DICT)