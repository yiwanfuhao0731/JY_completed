'''
Created on 2019年3月23日

@author: user
'''
import datetime as dt

import pandas as pd

def get_risk_reporter_eod(trader_name, asof_date, risk_spec=None):
    if risk_spec is None:
        risk_spec = 'Default'

    fn = '//composers/DFS/traders/QAGSpreadsheets/%s/EODRISK/%s/%s_RISK_%s.csv' % (
        trader_name, risk_spec, risk_spec,asof_date.strftime('%Y%m%d'))

    return pd.read_csv(fn)

 