# estimate the total spot and carry returns
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), ".."))
import panormus.data.bo.econ as econ
import Analytics.series_utils as su
import Analytics.wfcreate as wf
from Analytics.finance import calc_duration
import os
import panormus.data.open_data as open_data
from datetime import datetime, timedelta
import pandas as pd
from Analytics.abstract_sig import abstract_eri as abs_eri
from panormus.utils.cache import cache_response


class signal3(abs_eri):
    def __init__(self):
        super(signal3, self).__init__()
        self.add_data_info()
        self.add_dir_info()

    def add_data_info(self):
        self.data_ID = 'data_0003'
        self.Short_Name = 'DATA_RATES_10Y_SWAP_OIS'
        self.Description = '''
                            return the 10 year the ois swap for 4 countries
                            '''
        self.exec_path = __file__

    @cache_response('DATA_RATES_10Y_SWAP_OIS', 'disk_8h', skip_first_arg=True)
    def get_data_dict(self, **kwargs):
        self.create_folder(os.path.join(self.PROJ_DIR, r"SCI/Market_data"))
        self.new_wf = wf.swf()
        self.s_date = datetime(year=1980, month=1, day=1).date()
        self.e_date = datetime.today() - timedelta(days=1)
        self.HOLIDAY = {
            'USA': 'USD',
            'CAN': 'CAD',
            'GBR': 'GBP',
            'AUS': 'AUD',
        }
        result_dict = {}
        result_dict.update(self.get_10y_data('USA', 'USD.OIS', 'USA_IRS_10Y', 'USA_POLICY_RATE', 'nyclose'))
        result_dict.update(self.get_10y_data('CAN', 'CAD.OIS', 'CAN_IRS_10Y', 'CAN_POLICY_RATE', 'nyclose'))
        result_dict.update(self.get_10y_data('GBR', 'GBP.OIS', 'GBR_IRS_10Y', 'GBR_POLICY_RATE', 'lonclose'))
        result_dict.update(self.get_10y_data('AUS', 'AUD.OIS', 'AUS_IRS_10Y', 'AUS_POLICY_RATE', 'tkyclose'))

        return result_dict

    def get_10y_data(self, iso='USA', ois_code='USD.OIS', libor_swap_ticker='USA_IRS_10Y',
                     policy_rate_ticker='USA_POLICY_RATE', cut_override='nyclose'):
        # USA OIS
        df = open_data.get_swap_rates(self.s_date, self.e_date, ois_code, ['0d'], ['10y'], cut_override=cut_override)
        df = df['0d']['10y'].to_frame() * 100
        df.columns = [iso + '_IRS_10Y']
        df.index = [t.replace(tzinfo=None) for t in df.index]
        # USA IRS and policy rate
        ticker = [policy_rate_ticker, libor_swap_ticker]
        self.new_wf.importts(ticker, filetype='EconDB', to_freq='bday')
        df_3ml = self.new_wf.df[libor_swap_ticker].copy()

        # splice together
        common_date = df.dropna().index.intersection(df_3ml.dropna().index)[0]
        scale2 = df_3ml.loc[common_date, :].iloc[0] - df.loc[common_date, :].iloc[0]
        df_3ml = df_3ml - scale2
        df_3ml = df_3ml.loc[:common_date, :]
        df_3ml.columns = df.columns
        df = pd.concat([df_3ml.dropna(), df.dropna()], axis=0)
        if iso + '_IRS_10Y' in self.new_wf.df.keys():
            del self.new_wf.df[iso + '_IRS_10Y']
        self.new_wf.add_df(iso + '_IRS_10Y', df)

        df = self.new_wf.df[iso + '_IRS_10Y'].copy()
        _values = df.iloc[:, 0].values
        _values = [calc_duration(i, 10) for i in _values]
        df['_duration'] = _values

        df_pr = self.new_wf.df[policy_rate_ticker].copy()
        df['delta'] = df[iso + '_IRS_10Y'].diff(1) / 100
        df['total_ret_daily'] = -df['delta'] * df['_duration'] + (
                df[iso + '_IRS_10Y'] - df_pr[policy_rate_ticker]) / 252 / 100
        df['total_ret_daily'] = 1 + df['total_ret_daily']
        first_idx = df[['total_ret_daily']].first_valid_index()
        df.iloc[df.index.get_loc(first_idx) - 1].loc['total_ret_daily'] = 100
        df[iso + '_10y_TRI'] = df['total_ret_daily'].cumprod()
        df[[iso + '_10y_TRI']].to_csv(os.path.join(self.PROJ_DIR, r"SCI/Market_data/", iso + "_10y_TRI.csv"))
        df[[iso + '_IRS_10Y']].to_csv(os.path.join(self.PROJ_DIR, r"SCI/Market_data/", iso + "_10y_IRS_OIS.csv"))
        return {
            self.Short_Name + '/' + iso + "_10y_TRI": df[[iso + '_10y_TRI']],
            self.Short_Name + '/' + iso + "_10y_IRS_OIS": df[[iso + '_IRS_10Y']]
        }


if __name__ == '__main__':
    sig = signal3()
    data = sig.get_data_dict()
    print(data.keys())

