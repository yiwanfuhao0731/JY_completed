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
from Analytics.abstract_sig import abstract_eri as abs_eri
from panormus.utils.cache import cache_response


class signal3(abs_eri):
    def __init__(self):
        super(signal3, self).__init__()
        self.add_data_info()
        self.add_dir_info()

    def add_data_info(self):
        self.data_ID = 'data_0006'
        self.Short_Name = 'DATA_RATES_3M_5Y_IMP_VOL'
        self.Description = '''
                            return the 3m implied volatility of 5 year swap for 4 countries
                            '''
        self.exec_path = __file__

    @cache_response('DATA_RATES_3M_5Y_IMP_VOL', 'disk_8h', skip_first_arg=True)
    def get_data_dict(self):
        self.new_wf = wf.swf()
        self.new_wf = wf.swf()

        self.s_date = datetime(year=1980, month=1, day=1).date()
        self.e_date = datetime.today() - timedelta(days=1)

        self.iso_dict = {
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

        self.citi_token = {'id': '6657c27e-569e-4569-828d-b2e76a1387f7',
                           'secret': 'G4mM5jH4dA5oM7uK5fL3pC3yR7iG5vH5wM7sY0tT4gV5kP0eV6'}
        result_dict = {}
        result_dict.update(self.citi_request_3m_5y_tenor())
        return result_dict

    def trading_dates(self, iso):
        sd = date(1980, 1, 1)
        ed = date.today()

        valid_trading_days = trading_date_list(sd, ed, self.iso_dict[iso])
        return [pd.to_datetime(d) for d in valid_trading_days]

    @cache_response('DATA_RATES_3M_5Y_IMP_VOL_citi_request_3m_5y_tenor', 'disk_8h', skip_first_arg=True)
    def citi_request_3m_5y_tenor(self, iso_list=['USA', 'EUR', 'AUS', 'CAN', 'CHE', 'GBR', 'JPN', 'NOR', 'SWE']):
        citi_client = cv.CitiClient(id=self.citi_token['id'], secret=self.citi_token['secret'])
        start_date = date(1980, 1, 1)
        end_date = date(2050, 1, 1)
        frequency = 'DAILY'
        price_points = 'C'  # close price
        result_dict = {}
        for iso in iso_list:
            tag_format = 'RATES.VOL.{iso}.ATM.NORMAL.ANNUAL.3M.5Y'
            new_col_name = '{}_IV_3M_5Y_TENOR'
            tags = [tag_format.format(iso=self.iso_dict[iso])]

            df = citi_client.get_hist_data(tags=tags,
                                           frequency=frequency,
                                           start_date=start_date,
                                           end_date=end_date
                                           )

            t_date = self.trading_dates(iso)
            df = df.reindex(t_date)
            df.columns = [new_col_name.format(iso)]

            export_path = os.path.join(self.PROJ_DIR,
                                       r"SCI/Market_data/Implied_Volatility/3m_5y/{}_3m_5y_iv_citi.csv".format(iso))
            df.to_csv(export_path)
            result_dict[self.Short_Name + '/' + "{}_3m_5y_iv_citi".format(iso)] = df
        return result_dict


if __name__ == '__main__':
    sig = signal3()
    data = sig.get_data_dict()
    print(data.keys())

