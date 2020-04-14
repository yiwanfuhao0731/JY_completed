import pandas as pd
import numpy as np

import_dir = r'd:\yangs\Third_yr_grad_English\relevant_file\workspace_zy\JY_completed\Caxton\EDB\Andrew email ticker.csv'
df = pd.read_csv(import_dir,index_col=False)
mask = pd.isnull(df['list of data alert'])
df = df.loc[~mask,:]
df.to_csv(r'd:\yangs\Third_yr_grad_English\relevant_file\workspace_zy\JY_completed\Caxton\EDB\Andrew email ticker new.csv')