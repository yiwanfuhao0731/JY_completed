'''
Created on 2019年3月23日

@author: user
'''
import json
import os

import pandas as pd
import requests

_md_service_url = 'http://service01.marketdata.a.prod.us-east-1.aws.caxton.com:8604/MarketDataService/'
_headers = {'content-type': 'application/json'}


def get_vol_surfaces(pricing_source, tickers, start_date, end_date):
    """
    :description: fetch vol surfaces from cax database
    :param str pricing_source: pricing source of the _vol surface, e.g. BARCAP, SOCGEN, BAML etc.
    :param list tickers: a list of _vol surface tickers, e.g. SX5E, USDNORM, USDJPY IV etc.
    :param start_date: a date object that supports strftime method
    :param end_date: a date object that supports strftime method
    :return: pandas DataFrame
    """

    vol_func = 'vols/flat'
    payload = {
        'tickers': tickers,
        'pricingSource': pricing_source,
        'startDate': start_date.strftime('%Y%m%d'),
        'endDate': end_date.strftime('%Y%m%d')
    }
    url = os.path.join(_md_service_url, vol_func)
    response = requests.post(url, data=json.dumps(payload), headers=_headers)
    json_data = response.json()
    df = pd.read_json(json_data[u'GetVolSurfacesResult'], orient='records')
    return df

 