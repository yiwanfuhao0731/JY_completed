import pandas as pd
from statsmodels.tsa.seasonal import seasonal_decompose
import sys
import os
print ('hello world')


output_filename = r'C:\yang\Third_yr_grad_English\relevant_file\bookkeeping\tax return\2019-2020\recept.zip'
dir_name = r'C:\yang\Third_yr_grad_English\relevant_file\bookkeeping\tax return\2019-2020\receipt'
import shutil
shutil.make_archive(output_filename, 'zip', dir_name)