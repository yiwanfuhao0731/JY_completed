import pandas as pd
import datetime as dt
import re
import pickle

import panormus.data.jpmdq as jpmdq
# import panormus.data.jpmdqold as jpmdqold

# utils_pvk imports
import utils_pvk.lib_date_fns as date_fns
import utils_pvk.lib_data_fns as data_fns
import utils_pvk.lib_string_fns as string_fns

JPMDQ_EOD_CACHE_FOLDER = "H:\\python_local\\storage\\JPMDQ\\"
JPMDQ_USER = "vantklooster_api"
JPMDQ_PW = "JdfIjdieDjfiejD78131"


def get(tickers, start_date=None, end_date=None, live=True, raise_on_missing_symbol=True, get_new_data=True,
        use_filecache=True, read_last_modified=True, force_full_refresh=False, **kwargs):
    """get jpmdq data, use cache and various options. no postprocessing of result"""

    if isinstance(tickers, str):
        tickers = [tickers]
    # if end_date is None:
    #     end_date = dt.datetime.today()

    def get_single_cached(ticker):
        load_fn = lambda start_date_x, end_date_x: get_uncached(ticker, start_date_x, end_date_x, **kwargs)

        ticker_filename = JPMDQ_EOD_CACHE_FOLDER + re.sub("[/\\\\:*]", "-", ticker) + ".pickle"

        if use_filecache:
            return data_fns.cached_get(load_fn, ticker_filename, start_date, end_date, not live,
                                       raise_on_missing_symbol, get_new_data, read_last_modified=read_last_modified,
                                       force_full_refresh=force_full_refresh)
        else:
            return load_fn(start_date, end_date)

    listdata = [get_single_cached(i_ticker) for i_ticker in tickers]
    dfdata = pd.concat(listdata, axis=1, sort=False)

    return dfdata


# def get_uncached(tickers, start_date=None, end_date=None, **kwargs):
#     """
#     :description: Get JPM dataquery data
#     :param str expr: dataquery expression
#     :param str sd: YYYYMMDD
#     :param str ed: YYYYMMDD
#     :param str user: jpmdq api credentials
#     :param str password: jpmdq api credentials
#     :param kwargs: cal, freq, conv, na
#     :return: dictionary with keys 'df' and 'errorMessage'
#     """
#     # tickers = tickers[:3]
#     if start_date is None or date_fns.to_datetime(start_date) < dt.datetime(1971, 1, 1):
#         start_date = dt.datetime(1971, 1, 1)
#
#     if end_date is None:
#         end_date = dt.datetime(2047, 12, 31)
#
#     start_date_str = date_fns.to_str(start_date, "yyyymmdd")
#     end_date_str = date_fns.to_str(end_date, "yyyymmdd")
#
#     drop_errors = kwargs.pop("drop_errors", True)
#     errors = pd.Series()
#
#     def __get_fn(ticker):
#         print("jpmdq get: {}".format(ticker))  # ticker = "induce_error"
#         # dat = jpmdq.fetch_df(ticker, start_date_str, end_date_str, JPMDQ_USER, JPMDQ_PW, **kwargs)
#         # data_out = (jpmdq.JpmdqClient(JPMDQ_USER, JPMDQ_PW)
#         #                  .fetch_df(ticker, start_date_str, end_date_str, **kwargs))
#         dat = jpmdqold.fetch_df(ticker, start_date_str, end_date_str, JPMDQ_USER, JPMDQ_PW, **kwargs)
#         data_out = dat['df']
#
#         if dat['err_raw'] is not None:
#             errors[ticker] = dat['err_raw']
#             return pd.DataFrame(columns=[ticker])
#         elif data_out is None:
#             return pd.DataFrame(columns=[ticker])
#         else:
#             if string_fns.is_number(data_out.index[0]):
#                 formatstring = '%Y%m%d'
#             elif data_out.index[0][4] == "-" and data_out.index[0][7] == "-":
#                 formatstring = '%Y-%m-%d'
#
#             data_out.index = [date_fns.from_str(s, formatstring) for s in data_out.index]
#
#             if drop_errors:
#                 data_out_ex_errors = data_out[data_out < 1e38].dropna()
#
#             data_cleaned = jpmdq_clean_data(ticker, data_out_ex_errors)
#
#             return data_cleaned
#
#     if isinstance(tickers, str):
#         data_out = __get_fn(tickers)
#     else:
#         dat = [__get_fn(i_ticker) for i_ticker in tickers]
#         data_out = pd.concat(dat, axis=1, join='outer')
#
#     if len(errors) > 0:
#         errors.to_csv("jpmdq_error_log.csv")
#         from cl_email import Email
#         old_width = pd.get_option('display.max_colwidth')
#         pd.set_option('display.max_colwidth', -1)
#         errors_html = pd.Series([str(v) for v in errors.values], index=errors.index).to_frame().to_html()
#         pd.set_option('display.max_colwidth', old_width)
#         Email(subject="errors while loading from JPM DataQuery", htmlbody=errors_html).send()
#
#     if kwargs.get('cut_weekends', True):
#         if len(data_out.index) > 0:
#             data_out = data_out[data_out.index.dayofweek < 5]
#     # dat_out.iloc[-1]
#
#
#     return data_out


def get_uncached(tickers, start_date=None, end_date=None, **kwargs):
    """
    :description: Get JPM dataquery data
    :param str expr: dataquery expression
    :param str sd: YYYYMMDD
    :param str ed: YYYYMMDD
    :param str user: jpmdq api credentials
    :param str password: jpmdq api credentials
    :param kwargs: cal, freq, conv, na
    :return: dictionary with keys 'df' and 'errorMessage'
    """
    # tickers = tickers[:3]
    if start_date is None or date_fns.to_datetime(start_date) < dt.datetime(1971, 1, 1):
        start_date = dt.datetime(1971, 1, 1)

    if end_date is None:
        end_date = dt.datetime(2047, 12, 31)

    start_date_str = date_fns.to_str(start_date, "yyyymmdd")
    end_date_str = date_fns.to_str(end_date, "yyyymmdd")

    def __get_fn(ticker):
        print("jpmdq get: {}".format(ticker))  # ticker = "induce_error"

        data_out = (jpmdq.JpmdqClient(JPMDQ_USER, JPMDQ_PW)
                         .fetch_df(ticker, start_date_str, end_date_str, **kwargs))

        data_out.index = [date_fns.to_datetime(x) for x in data_out.index]

        data_cleaned = jpmdq_clean_data(ticker, data_out)

        return data_cleaned

    if isinstance(tickers, str):
        data_out = __get_fn(tickers)
    else:
        dat = [__get_fn(i_ticker) for i_ticker in tickers]
        data_out = pd.concat(dat, axis=1, join='outer')

    if kwargs.get('cut_weekends', True):
        if len(data_out.index) > 0:
            data_out = data_out[data_out.index.dayofweek < 5]

    return data_out


def jpmdq_clean_data(ticker, data):

    # USD
    if ticker.lower()[:23] == 'DB(CCV,CRVSWAPMMKT,usd,'.lower():
        data = data.drop([dt.datetime(1994, 4, 8), dt.datetime(1991, 7, 11)], errors='ignore')

    # GBP
    if ticker.lower()[:23] == 'DB(CCV,CRVSWAPMMKT,gbp,'.lower():
        data = data.drop([dt.datetime(1988, 4, 11), dt.datetime(1988, 11, 14)], errors='ignore')

    # AUD
    if ticker.lower()[:25] == 'DB(CCV,CRVSWAPMMKT_6,aud,'.lower():
        data = data.drop([dt.datetime(1991, 7, 10), dt.datetime(1991, 7, 11)], errors='ignore')

    # CAD
    if ticker.lower()[:27] == 'DB(CCV,CRVSWAPMMKT,cad,10Y,'.lower():
        data = data.drop([dt.datetime(1993, 5, 3), dt.datetime(1993, 12, 28)], errors='ignore')

    if ticker.lower()[:27] == 'DB(CCV,CRVSWAPMMKT,cad,30Y,'.lower():
        data = data.drop([dt.datetime(1992, 12, 24),
                          dt.datetime(1993, 1, 12),
                          dt.datetime(1993, 5, 3),
                          dt.datetime(1993, 8, 4),
                          dt.datetime(1993, 8, 6),
                          dt.datetime(1993, 8, 11),
                          dt.datetime(1993, 8, 19),
                          dt.datetime(1993, 8, 27),
                          dt.datetime(1993, 8, 30),
                          dt.datetime(1993, 9, 9),
                          dt.datetime(1993, 12, 28)
                          ], errors='ignore')

    if ticker.lower()[:27] == 'DB(CCV,CRVSWAPMMKT,cad,10Y,'.lower() and \
            ticker.lower()[-11:] == 'AM_MOD_DUR)'.lower():
        data = data.drop([dt.datetime(1993, 5, 3), dt.datetime(1993, 12, 28),
                          dt.datetime(1995, 4, 17), dt.datetime(1995, 5, 8),
                          dt.datetime(1995, 8, 28), dt.datetime(1996, 4, 8),
                          dt.datetime(1996, 5, 6)], errors='ignore')

    # NOK
    if ticker.lower()[:23] == 'DB(CCV,CRVSWAPMMKT,nok,'.lower():
        data = data[dt.datetime(1999, 6, 4):]

    # NZD
    if ticker.lower()[:25] == 'DB(CCV,CRVSWAPMMKT_3,nzd,'.lower():
        data = data[dt.datetime(1996, 10, 21):]

    # SEK
    if ticker.lower()[:23] == 'DB(CCV,CRVSWAPMMKT,sek,'.lower():
        data = data.drop([dt.datetime(1991, 9, 23), ], errors='ignore')

    return data


def get_jpm_swap_base(ccy):
    # use jpmdq tickers
    if ccy.lower() == "aud":
        base_jpmdq_ticker = "DB(CCV,CRVSWAPMMKT_6,{},{},{},{})"
    elif ccy.lower() == "nzd":
        base_jpmdq_ticker = "DB(CCV,CRVSWAPMMKT_3,{},{},{},{})"
    else:
        base_jpmdq_ticker = "DB(CCV,CRVSWAPMMKT,{},{},{},{})"

    return base_jpmdq_ticker.lower()


def tck_cms_curve_vol(ccy, tenor_a, tenor_b, expiry):
    base = {'usd': 'FDER,YCSO', 'eur': 'COV,YCSO,eur'}[ccy]
    return f'DB({base},{tenor_a}x{tenor_b},{expiry},S,ATMF,0,BPVOL)*SQRT(252)'


def tck_swaption_vol(ccy, tenor, expiry):
    if ccy.lower() == 'usd':
        ticker = f'DB(FDER,SWAPTION,{tenor.zfill(3)},{expiry.zfill(3)},3PT,Receiver,ATMF,0,BPVOL)*SQRT(252)'
    else:
        ticker = f'DB(COV,VOLSWAPTION,{ccy},{tenor.zfill(3)},{expiry.zfill(3)},PAYER,VOLBPVOL)'
    return ticker


def tck_swap_rate(ccy, tenor, expiry, metric='RT_MID'):
    return f'DB(CCV,CRVSWAPMMKT,{ccy},{tenor},{expiry},{metric})'


if __name__ == "__main__":

    import lib_ticker_fns as tck
    tenors= ['10y', '20y', '30y']
    tickers = [tck.jpm_swap_rate('eur', x) for x in tenors]

    data_out = (jpmdq.JpmdqClient(JPMDQ_USER, JPMDQ_PW)
                .fetch_df(tickers, dt.datetime(1980, 1, 1), dt.datetime(2049, 6, 14)))

    data = data_out.dropna(axis=0, how='all')

    data.columns = tenors

    fly10s20s30s = data['20y'] - data['10y'] / 2 - data['30y'] / 2
    crv20s30s = data['20y'] - data['30y']
    fly10s20s30s[dt.date(2000, 1, 1):].plot()
    crv20s30s[dt.date(2000, 1, 1):].plot()

    fly10s20s30s[dt.date(2000, 1, 1):].plot()

    base_eur = "DB(CCV,CRVSWAPMMKT_3,EUR,03M,{},RT_MID)"
    base_jpy = "DB(CCV,CRVSWAPMMKT_3,JPY,03M,{},RT_MID)"

    tenors = ["", "01M", "03M", "06M", "09M", "01Y", "02Y", "03Y", "04Y", "05Y", "06Y", "07Y", "08Y", "09Y", "10Y", "15Y", "20Y"]
    tenors = ["", "01M"]
    tickers_eur = [base_eur.format(s) for s in tenors]
    tickers_jpy = [base_eur.format(s) for s in tenors]


    start_date = dt.datetime(1971, 1, 1)
    end_date = dt.datetime.today()

    print("loading eur")
    dat_eur = get(tickers_eur, start_date, end_date)
    print("loading jpy")
    dat_jpy = get(tickers_jpy, start_date, end_date)

    s = get("DB(CCV,CRVSWAPMMKT,USD,10Y,,AM_MOD_DUR)", use_filecache=False)

    dat = get(source_tickers, start_date, end_date)
    print(dat_eur)