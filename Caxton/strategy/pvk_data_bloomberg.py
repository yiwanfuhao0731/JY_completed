import pdblp
import blpapi
import pandas as pd
import re
import datetime as dt
import os
import json
import sqlite3

# utils_pvk imports
from utils_pvk.cl_email import Email
import utils_pvk.lib_date_fns as date_fns
import utils_pvk.lib_data_fns as data_fns

BDH_EOD_CACHE_FOLDER = "H:\\python_local\\storage\\BBG_EoD\\"
ILLEGAL_CHARS = [":", "/", "\/", "\""]


eri_tickers = {
    "ERI Global": 'CGERGLOB Index',
    "ERI DM": 'CGERGLDM Index',
    "ERI EM": 'CGERGLEM Index',
    "ERI US": 'CGERUSA1 Index',
    "ERI EU ex UK": 'CGEREUXU Index',
    "ERI UK": 'CGERUKUK Index',
    "ERI Japan": 'CGERJAPN Index',
    "ERI Pacific ex Japan": 'CGERAPXJ Index',

    "ERI Global Financials": 'CGERGLFN Index',
    "ERI EM Financials": 'CGEREMFN Index',
    "ERI US Financials": 'CGERUSFN Index',
    "ERI EU ex UK Financials": 'CGEREXFN Index',
    "ERI UK Financials": 'CGERUKFN Index',
    "ERI Japan Financials": 'CGERJPFN Index',
}

def bbg_static(bbg_tickers, field):
    return get_bdp_static(bbg_tickers, field).loc[:, 'value'].values


def get_bbg(tickers, fields, start_date=None, end_date=None, live=True, raise_on_missing_symbol=True, get_new_data=True,
            use_filecache=True, ovrds=None, write=True, storage_location=None, read_last_modified=True,
            apply_cutoff=True, **_):
    """get bloomberg data, use cache and various options. no postprocessing of result

            ovrds: list of tuples
            List of tuples where each tuple corresponds to the override
            field and value
    """
    if start_date is None:
        start_date = dt.datetime(1900, 1, 1)
    # use_filecache=False
    if end_date is None:
        # end_date = dt.datetime.today()
        end_date = dt.datetime(2100, 1, 1)

    def get(x_ticker, x_field, i_end_date):

        if x_ticker.lower() in [s.lower() for s in eri_tickers.values()] and i_end_date > dt.datetime.today():
            i_end_date = dt.datetime.today()

        dat_out = __bdhx(x_ticker, x_field, start_date, i_end_date, not live, raise_on_missing_symbol, get_new_data,
                         use_filecache, ovrds=ovrds, write=write, storage_location=storage_location,
                         read_last_modified=read_last_modified, apply_cutoff=apply_cutoff)
        return dat_out

    # zip ticker/field pairs, either can be string or list
    if isinstance(fields, str):
        if isinstance(tickers, str):
            params = [(tickers.lower(), fields.lower())]
        else:
            params = [(i_ticker.lower(), fields.lower()) for i_ticker in tickers]
    else:
        if isinstance(tickers, str):
            params = [(tickers.lower(), i_field.lower()) for i_field in fields]
        else:
            params = [(i_ticker.lower(), i_fields.lower()) for i_ticker, i_fields in zip(tickers, fields)]

    listdata = [get(i_ticker, i_field, end_date) for i_ticker, i_field in params]
    dfdata = pd.concat(listdata, axis=1, sort=False)
    return dfdata


def get_bdp_static(tickers, fields, use_filecache=True, get_new_data=True, drop_existing_nan=True,
                   **_):
    """get bloomberg data, use cache and various options. no postprocessing of result"""

    def get(x_ticker, x_field):
        return __bdpx(x_ticker, x_field, use_filecache, get_new_data, drop_existing_nan)

    # zip ticker/field pairs, either can be string or list
    if isinstance(fields, str):
        if isinstance(tickers, str):
            params = [(tickers.lower(), fields.lower())]
        else:
            params = [(i_ticker.lower(), fields.lower()) for i_ticker in tickers]
   else:
        if isinstance(tickers, str):
            params = [(tickers.lower(), i_field.lower()) for i_field in fields]
        else:
            params = [(i_ticker.lower(), i_fields.lower()) for i_ticker, i_fields in zip(tickers, fields)]

    listdata = [get(i_ticker, i_field) for i_ticker, i_field in params]
    dfdata = pd.concat(listdata, axis=0, sort=False)

    return dfdata


# def sql_store(dfdata_new, dbpath, table_name):


# def get_bdp_static_2(tickers, field, use_filecache=True, get_new_data=True,
#                      **_):
#     """get bloomberg data, use cache and various options. no postprocessing of result"""
#
#     dbpath = BDH_EOD_CACHE_FOLDER + "static_fields.db"
#     table_name = "static_fields"
#
#     if use_filecache:
#         conn = sqlite3.connect(dbpath)
#         c = conn.cursor()
#         sql = "SELECT ticker, {} FROM {}".format(field, table_name)
#         dfdata_stored = c.execute(sql).fetchall()
#         dfdata_new = __bdp(tickers_new, field)
#         dfdata = pd.concat(dfdata_new, dfdata_stored)
#     else:
#         dfdata = __bdp(tickers, field)
#
#     if use_filecache and not dfdata_new.empty:
#         sql_success = sql_store(dfdata_new, dbpath, 'static_fields')
#         if not sql_success:
#             raise ValueError("Error storing SQL data. Db not updated")
#
#     dfdata = pd.concat(listdata, axis=0, sort=False)
#     return dfdata


def get_bds(ticker, field, **kwargs):
    """load bbg data through the (ported) bdh function - no use of bbg options
    (this should be handled in wrapper function)"""

    use_filecache = kwargs.pop("use_filecache", True)
    get_new_data = kwargs.pop("get_new_data", True)

    # step 1 - load existing data and compare
    base = ticker+"-"+field+"-"+json.dumps(kwargs)

    for char in ILLEGAL_CHARS:
        base = re.sub(char, "-", base)

    ticker_filename = BDH_EOD_CACHE_FOLDER + base + ".pickle"

    if os.path.isfile(ticker_filename) and use_filecache:
        return pd.read_pickle(ticker_filename)
    else:
        if get_new_data:
            data = __bds(ticker, field, **kwargs)
        else:
            data = pd.DataFrame()
        if use_filecache:
            data.to_pickle(ticker_filename)
        return data


def __bdhx(ticker, field, start_date, end_date, exclude_data_asof_today, raise_on_missing_symbol, get_new_data=True,
           use_filecache=True, ovrds=None, write=True, storage_location=None, read_last_modified=True,
           apply_cutoff=True):
    """cashed get of bloomberg data"""

    # step 1 - load existing data and compare
    base = ticker + "-" + field

    if ovrds is not None:
        for ovrd in ovrds:
            base += "-" + str(ovrd[0]) + str(ovrd[1])

    for char in ILLEGAL_CHARS:
        base = re.sub(char, "-", base)

    if not storage_location:
        ticker_filename = BDH_EOD_CACHE_FOLDER + base + ".pickle"
    else:
        ticker_filename = storage_location + base + ".pickle"

    load_fn = lambda start_date_x, end_date_x: __bdh(ticker, field, start_date_x, end_date_x, ovrds=ovrds)

    if use_filecache:
        return data_fns.cached_get(load_fn, ticker_filename, start_date, end_date, exclude_data_asof_today,
                                   raise_on_missing_symbol, get_new_data, write=write,
                                   read_last_modified=read_last_modified, apply_cutoff=apply_cutoff)
    else:
        return load_fn(start_date, end_date)


def __bdpx(ticker, field, use_filecache=True, get_new_data=True, drop_existing_nan=True):
    """cashed get of bloomberg data"""

    # step 1 - load existing data and compare
    base = ticker+"-"+field+"-bdp"

    for char in ILLEGAL_CHARS:
        base = re.sub(char, "-", base)

    ticker_filename = BDH_EOD_CACHE_FOLDER + base + ".pickle"

    if os.path.isfile(ticker_filename) and use_filecache:
        data = pd.read_pickle(ticker_filename)
    else:
        data = None

    if drop_existing_nan:
        condition = data is not None and not data.empty and not pd.isnull(data['value'].values[0])
    else:
        condition = data is not None

    if condition:
        return data
    else:
        if get_new_data:
            data = __bdp(ticker, field)
        else:
            data = pd.DataFrame()
        if use_filecache and not data.empty:
            data.to_pickle(ticker_filename)
        return data


def __bdh(ticker, field, start_date, end_date="", ovrds=None):
    """load bbg data through the (ported) bdh function - no use of bbg options
    (this should be handled in wrapper function)"""

    global BBG_CONN

    print("bbg_get: {}, {}".format(ticker, field))

    # parse data
    # field = "EQY_DVD_YLD_IND"
    if field.upper() in ("EQY_DVD_YLD_IND") and date_fns.to_datetime(start_date) < dt.datetime(1930, 1, 1):
        start_date_str = "19300101"
    else:
        start_date_str = date_fns.to_str(start_date, "bbg")
    end_date_str = date_fns.to_str(end_date, "bbg")

    # get data through existing bbg connection object

    try:
        con = BBG_CONN
        bbg_data = con.bdh(ticker, field, start_date_str, end_date_str, ovrds=ovrds)
        print("success")
    except NameError:
        BBG_CONN = pdblp.BCon(debug=False, port=8194, timeout=30000)
        con = BBG_CONN
        con.start()
        bbg_data = con.bdh(ticker, field, start_date_str, end_date_str, ovrds=ovrds)
        print("defined and started bbg")
    except AttributeError:
        con.start()
        bbg_data = con.bdh(ticker, field, start_date_str, end_date_str, ovrds=ovrds)
        print("started bbg")
    except blpapi.InvalidStateException:
        Email(subject="BLOOMBERG RESTART ATTEMPT", htmlbody="Attempting to restart Bloomberg").send()
        try:
            con.stop()
        except:
            print('no bbg connection to stop')

        BBG_CONN = pdblp.BCon(debug=False, port=8194, timeout=30000)
        con = BBG_CONN
        con.start()
        bbg_data = con.bdh(ticker, field, start_date_str, end_date_str, ovrds=ovrds)
        print("started bbg")
        Email(subject="BLOOMBERG RESTARTED SUCCESSFULLY", htmlbody="BLOOMBERG RESTARTED SUCCESSFULLY").send()
    # con.stop()

    if type(ticker) is not list:
        ticker = [ticker]

    columns = [i_ticker + "-" + field for i_ticker in ticker]
    if len(bbg_data) == 0:
        bbg_data = pd.DataFrame(columns=columns)
    else:
        bbg_data.columns = columns

    return bbg_data


def get_hist_bdp(tickers, field, start_date=None, end_date=None):
    """load time-series datapoint through bdp and store as a time-series (e.g. for CDS spreads)"""

    # get data through existing bbg connection object
    bbg_data = __bdp(tickers, field)

    def dummy_data_fn(start_date, end_date, ticker):
        if date_fns.to_date(start_date) <= dt.date.today() - 1 and date_fns.to_date(end_date) >= dt.date.today():
            return pd.Series([bbg_data[ticker]], index=[dt.date.today()])
        else:
            return pd.Series()

    data = pd.DataFrame(columns=tickers)
    for ticker in tickers:
        # step 1 - load existing data and compare
        base = ticker + "-" + field + "-bdp"

        for char in ILLEGAL_CHARS:
            base = re.sub(char, "-", base)

        full_path = BDH_EOD_CACHE_FOLDER + base + ".pickle"

        get_data_fn = lambda s, e: dummy_data_fn(s, e, ticker)

        data = data_fns.cached_get(get_data_fn, full_path, start_date, end_date,
                                   exclude_data_asof_today=False, raise_on_missing_symbol=False, get_new_data=True)

        data[:, ticker] = data

    return data


def __bdp(tickers, fields, ovrds=None):
    """load bbg data through the (ported) bdh function - no use of bbg options
    (this should be handled in wrapper function)"""

    # get data through existing bbg connection object
    con = __get_conn()
    bbg_data = con.ref(tickers, fields, ovrds=ovrds)

    return bbg_data


def __bds(ticker, field, **overrides):
    """load bbg data through the (ported) bdh function - no use of bbg options
    (this should be handled in wrapper function)"""

    # get data through existing bbg connection object
    overrides_list = [(key, val) for key, val in overrides.items()]
    con = __get_conn()
    bbg_data = con.bulkref(ticker, field, overrides_list)

    return bbg_data


def __get_conn():
    global BBG_CONN_VARIABLE

    def __conn_exists():
        try:
            if BBG_CONN_VARIABLE is None:
                return False
            else:
                return True
        except:
            return False

    if not __conn_exists():
        BBG_CONN_VARIABLE = pdblp.BCon(debug=False, port=8194, timeout=15000)
        BBG_CONN_VARIABLE.start()
    return BBG_CONN_VARIABLE


if __name__ == "__main__":
    # __bds('SPX Index', 'INDX_MWEIGHT_HIST', END_DT='19991231')

    data = get_bds('SPX Index', 'INDX_MWEIGHT_HIST', END_DT='19991231')

    import datetime
    ticker = ["SPY US Equity", "NKY Equity"]
    field = "PX_LAST"
    start_date = datetime.date(2018, 1, 1)
    end_date = datetime.date(2018, 5, 17)

    # data = get_bbg(ticker, field, start_date, end_date)
    data = get_bdp_static(ticker, field)
    data = get_bds(ticker, field)

    print(data)