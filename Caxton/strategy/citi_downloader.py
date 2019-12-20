# get the 3m expired, 5y tenor interest rate swap implied volatility from citi
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
import panormus.data.bo.econ as econ
import Analytics.series_utils as su
import Analytics.wfcreate as wf
import os
import panormus.data.open_data as open_data
from datetime import datetime, timedelta, date
import pandas as pd
from panormus.quant.alib import utils as quant_utils
from panormus.quant.market_date import trading_date_list
from Analytics.series_utils import date_utils_collection as s_util
import panormus.data.citi_velocity as cv

SU = s_util()

WKDIR = os.path.dirname(os.path.realpath(__file__))
PROJ_DIR = os.path.join(WKDIR, "..")

SU = su.date_utils_collection()
conn = econ.get_instance_using_credentials()
new_wf = wf.swf()
from matplotlib import pyplot as plt

s_date = datetime(year=1980, month=1, day=1).date()
e_date = datetime.today() - timedelta(days=1)

iso_dict = {
    'USA': 'USD',
    'CAN': 'CAD',
    'GBR': 'GBP',
    'AUS': 'AUD',
    'EUR': 'EUR',
    'CHE': 'CHF',
    'JPN': 'JPY',
    'NOR': 'NOK',
    'NZD': 'NZD',
    'SWE': 'SEK'
}

citi_token = {'id': '6657c27e-569e-4569-828d-b2e76a1387f7',
              'secret': 'G4mM5jH4dA5oM7uK5fL3pC3yR7iG5vH5wM7sY0tT4gV5kP0eV6'}


def create_folder(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)


def trading_dates(iso):
    sd = date(1980, 1, 1)
    ed = date.today()

    valid_trading_days = trading_date_list(sd, ed, iso_dict[iso])
    return [pd.to_datetime(d) for d in valid_trading_days]


def citi_request_3m_libor(iso_list=['USA', 'EUR', 'AUS', 'CAN', 'CHE', 'GBR', 'JPN', 'NOR', 'NZD', 'SWE']):
    citi_client = cv.CitiClient(id=citi_token['id'], secret=citi_token['secret'])
    start_date = date(1980, 1, 1)
    end_date = date(2050, 1, 1)
    frequency = 'DAILY'
    price_points = 'C'  # close price
    for iso in iso_list:
        tag_format = 'RATES.SWAP.{iso}.PAR.3M'
        new_col_name = '{}_IRS_3M_LIBOR'
        tags = [tag_format.format(iso=iso_dict[iso])]

        df = citi_client.get_hist_data(tags=tags,
                                       frequency=frequency,
                                       start_date=start_date,
                                       end_date=end_date
                                       )

        t_date = trading_dates(iso)
        df = df.reindex(t_date)
        df.columns = [new_col_name.format(iso)]

        export_path = os.path.join(PROJ_DIR, r"SCI/Market_data/{}_3m_irs_citi_libor.csv".format(iso))
        df.to_csv(export_path)

