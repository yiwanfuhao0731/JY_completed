import pandas as pd
import datetime as dt
import re

from panormus.data import citi_velocity as citi_velocity
# from panormus.data import citi_velocity_old as citi_velocity

# utils_pvk imports
import utils_pvk.lib_date_fns as date_fns
import utils_pvk.lib_data_fns as data_fns
import utils_pvk.lib_string_fns as string_fns


CITI_EOD_CACHE_FOLDER = "H:\\python_local\\storage\\CITI\\"


class CitiVelocityHist:
    def __init__(self):
        self.citi_client = citi_velocity.CitiClient('abc1fe56-7d84-41ac-a915-311e5020ccf4',
                                                    'Y1jX1qC3mS5jH5wF7aR1dG2qJ4wB6fS2pX0gU1kB2wS4vG1tV1')

    def get_hist_data(self, tags, start_date=None, end_date=None):
        if start_date is None:
            start_date = dt.date(1900, 1, 1)

        if end_date is None:
            end_date = dt.date(2100, 1, 1)

        def chunks(l, n=100):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i + n]

        data_list = []
        for chunk in chunks(tags, 100):
            hist_data = self.citi_client.get_hist_data(
                tags=chunk,
                start_date=start_date, end_date=end_date
            )
            data_list.append(hist_data)

        data_out = pd.concat(data_list, axis=1, sort=True)
        return data_out


def get(tickers, start_date=None, end_date=None, live=True, raise_on_missing_symbol=True, get_new_data=True,
        use_filecache=True, read_last_modified=True, **kwargs):
    """get jpmdq data, use cache and various options. no postprocessing of result"""

    if isinstance(tickers, str):
        tickers = [tickers]

    tickers = [s.upper() for s in tickers]

    def get_single_cached(ticker):
        load_fn = lambda start_date_x, end_date_x: get_uncached(ticker, start_date_x, end_date_x, **kwargs)

        ticker_filename = CITI_EOD_CACHE_FOLDER + re.sub(r""":\/""", "-", ticker) + ".pickle"

        if use_filecache:
            return data_fns.cached_get(load_fn, ticker_filename, start_date, end_date, not live,
                                       raise_on_missing_symbol, get_new_data, read_last_modified=read_last_modified)
        else:
            return load_fn(start_date, end_date)

    listdata = [get_single_cached(i_ticker) for i_ticker in tickers]
    dfdata = pd.concat(listdata, axis=1, sort=False)
    return dfdata


def get_uncached(tags, start_date=None, end_date=None, **kwargs):
    if start_date is None:
        start_date = dt.datetime(1900, 1, 1)
    if end_date is None:
        end_date = dt.datetime(2099, 12, 31)

    if isinstance(tags, str):
        tags = [tags]

    tags = [s.upper() for s in tags]

    conn = CitiVelocityHist()

    print(tags)
    data = conn.get_hist_data(tags, start_date, end_date)

    # data.index = [date_fns.from_str(x) for x in data.index]
    cols = []
    for col in data.columns:
        if isinstance(col, str):
            cols.append(col)
        else:
            cols.append(col[1])
    data.columns = cols
# data2 = data
    if kwargs.get('cut_weekends', False):
        if len(data.index) > 0:
            data = data[data.index.dayofweek < 5]

    return data


if __name__ == "__main__":
    tags = ['RATES.VOL.EUR.ATM.NORMAL.DAILY.1M.3M', 'CREDIT.CDS.APLINC.SNRFOR.USD.MR14.5Y.BLENDED']

    # ccy = 'USD'
    # expiry = '1m'
    # tenor = '10y'
    # tickers_citi = {"premium":   f"RATES.VOL.{ccy}.ATM.FWDPREMIUM.{expiry}.{tenor}",
    #                 "vol":       f"RATES.VOL.{ccy}.ATM.NORMAL.ANNUAL.{expiry}.{tenor}",
    #                 "fwd_rate":  f"RATES.SWAP.{ccy}.FWD.{expiry}.{tenor}",
    #                 "spot_rate": f"RATES.SWAP.{ccy}.PAR.{tenor}",
    #                 }
    # tags = tickers_citi.values()
    tags = ["RATES.VOL.USD.ATM.FWDPREMIUM.1M.10Y"]
    start_date = dt.date(2017, 1, 1)
    end_date = dt.date.today()
    # data = get(tags, start_date, end_date)
    data = get_uncached(tags, start_date, end_date)
    data.head()
    print('done')