'''
Created on 2019年3月23日

@author: user
'''
"""
Submit queries to the CAX web service
"""
import datetime as dt
import json
import os
import re
import socket

import pandas as pd
import requests
from panormus.utils import chrono
from panormus.utils.simple_func_decorators import docstring_parameter

# always use new york to avoid timezone issues for daily frequency data
_cax_rest_url = 'http://cpaappdev28:9963/CAXAddInService/caxrest'
_headers = {'Content-type': 'application/json'}

__username = os.getenv('username')
_date_format = '%Y-%m-%d'


def cax(func_name, data_type, tickers=None, fields=None, start_date=None, end_date=None, options=None):
    """
    :description: generic cax function using the same function cax add-in uses and has the same signature
    :param str func_name: name of the function
    :param str data_type: data type of the function
    :param list tickers: tickers to fetch
    :param list fields: fields to fetch
    :param start_date: start date for the time series as an object that support strftime function
    :param end_date: end date for the time series  as an object that support strftime function
    :param str options: additional options
    :return: json object
    """
    s = ''
    if start_date:
        s = start_date.strftime(_date_format)
    e = ''
    if end_date:
        e = end_date.strftime(_date_format)
    f = lambda x: '' if not x else '~'.join(str(i) for i in x)

    host = 'CNYWIT18'
    ip = '10.10.100.122'

    try:
        host = socket.gethostname()
        ip = socket.gethostbyname(host)
    except:
        pass

    message = '=Cax("{}"#"{}"#"{}"#"{}"#"{}"#"{}"#"{}")' \
        .format(func_name, data_type, f(tickers), f(fields), s, e, options)
    payload = {
        'clientMessage': {
            "IPAddress": ip,
            "UserName": __username,
            "Originator": "EXCEL_ADDIN#{}#{}#{}".format(__username, host, ip),
            "MessageText": message
        }
    }
    res = requests.post(_cax_rest_url, json.dumps(payload), headers=_headers)
    data = res.json()
    try:
        data = data['CAXRestResult']
    except KeyError:
        pass
    return data


@docstring_parameter(sorted(chrono.OFFICE_TZ_DICT.keys()))
def cax_df(
        func_name, data_type,
        tickers=None, fields=None,
        start_date=None, end_date=None,
        options=None, to_tz=None
):
    """
    :description: generic cax function using the same function cax add-in uses and has the same signature
    :param str func_name: name of the function
    :param str data_type: data type of the function
    :param list tickers: tickers to fetch
    :param list|None fields: fields to fetch
    :param start_date: start date as an object that support strftime function
   :param end_date: end date as an object that support strftime function
    :param str options: additional cax options in '[opt1]=[val2]&[opt2]=[val2]...' format
    :param str|None to_tz: convert dates to this timezone. Use None ONLY for time-less dates (daily freq). \
     First to_tz is checked in caxton office dictionary: {0} \
     Otherwise to_tz is checked in pytz timezone dictionary.
    :return: pandas DataFrame
    """
    raw_data = cax(func_name, data_type, tickers, fields, start_date, end_date, options)

    if raw_data['DataArray'] is None:
        if raw_data['ErrorMessage'] is not None:
            error_message = re.sub(r"\n", "; ", raw_data['ErrorMessage'])
            tickers = "-".join(tickers)
            try:
                fields = "-".join(fields)
            except TypeError:
                fields = "err"
            raise ValueError("Error loading CAX Data: \nTickers: {}\nFields: {}\nError:\n{}".format(
                tickers, fields, error_message))
        else:
            return pd.DataFrame(columns=fields)

    da = [[__parse_date(d, to_tz) for d in a] for a in raw_data['DataArray']]
    if data_type == 'EOD':
        headers = ['Date']
        headers.extend(tickers)
    else:
        headers = raw_data[u'ExcelColumnHeaders']
    data_dict = [{z[0]: z[1] for z in zip(headers, a)} for a in da]
    df = pd.DataFrame(data_dict)
    return df


def get_epnl(start=None, end=None, options=None):
    """
    :description: queries end of day epnl snapshots
    :param start: start date of the data
    :param end: end date of the data
    :param str options: additional cax options in '[opt1]=[val2]&[opt2]=[val2]...' format
    :return: pandas DataFrame of the epnl data
    """
    # raw_data = cax('GetEPNL', 'DATA', start_date=start, end_date=end, options=options)
    # da = [[_parse_date(d) for d in a] for a in raw_data['DataArray']]
    # headers = raw_data[u'ExcelColumnHeaders']
    # data_dict = [{z[0]: z[1] for z in zip(headers, a)} for a in da]
    # df = pd.DataFrame(data_dict)
    # return df
    return cax_df(
        func_name='GetEPNL',
        data_type='DATA',
        start_date=start,
        end_date=end,
        options=options,
        to_tz=None,
    )


@docstring_parameter(sorted(chrono.OFFICE_TZ_DICT.keys()))
def get_bar_data(tickers, start, end, options='source=BBG', fields=None, to_tz='UTC'):
    """
    :description: a wrapper function for Bar data functionality in cax
    :param list[str] tickers: input tickers
    :param dt.datetime start: timezone-aware datetime
    :param dt.datetime end: timezone-aware datetime
    :param str options: additional cax options in '[opt1]=[val2]&[opt2]=[val2]...' format. \
     When requesting BAR data, a source must be defined! Source=BBG by default.
    :param list[str] fields: fields to return. None for all fields.
    :param str|None to_tz: convert dates to this timezone. Use None ONLY for time-less dates (daily freq). \
     First to_tz is checked in caxton office dictionary: {0} \
     Otherwise to_tz is checked in pytz timezone dictionary.
    :return: pandas DataFrame
    """
    bad_opt_error = 'Options must contains a source setting. Default is source=BBG'
    if not options:
        raise ValueError(bad_opt_error)
    if 'source=' not in options.lower():
        raise ValueError(bad_opt_error)

    df = cax_df(
        func_name='GetData',
        data_type='BAR',
        tickers=tickers,
        start_date=start,
        end_date=end,
        options=options,
        fields=fields,
        to_tz=to_tz
    )

    column_order = [
        u'TIMESTAMP_IN_UTC', u'TICKER', u'OPEN', u'LOW', u'HIGH', u'CLOSE', u'VOLUME', u'NUM_OF_EVENTS'
    ]

    # only keep column_order elements that are actually in the df header
    column_order_selected = [col for col in column_order if col in list(df)]

    # only keep timestamps that were actually requested
    idx_dates_selected = (df['TIMESTAMP_IN_UTC'] >= start) & (df['TIMESTAMP_IN_UTC'] <= end)
    df = df.loc[idx_dates_selected, column_order_selected]

    return df


@docstring_parameter(sorted(chrono.OFFICE_TZ_DICT.keys()))
def get_bar_data_by_source(
        tickers, start, end,
        pricing_source='BBG', bar_size=1, bar_event=None,
        fields=None, to_tz='UTC'
):
    """
    :description: a wrapper function for Bar data functionality in cax
    :param list tickers: tickers to fetch
    :param dt.datetime start: timezone-aware datetime
    :param dt.datetime end: timezone-aware datetime
    :param str pricing_source: pricing source, e.g. SOCGEN, BBG, BARCAP, etc.
    :param int bar_size: data frequency in minutes, e.g. 1, 10
    :param str bar_event: bar event type, e.g. TRADE
    :param list fields: fields to return
    :param str|None to_tz: convert dates to this timezone. Use None ONLY for time-less dates (daily freq). \
     First to_tz is checked in caxton office dictionary: {0} \
     Otherwise to_tz is checked in pytz timezone dictionary.
    :return: pandas DataFrame
    """
    options = 'source={}&owner=system&barSize={}&barShowZero=true&headers=TRUE'.format(
        pricing_source, bar_size
    )
    if bar_event:
        options = '{}&barEvent={}'.format(options, bar_event)

    return get_bar_data(tickers, start, end, options, fields, to_tz=to_tz)


def get_eod_tickers(data_source):
    """
    :description: get end of day tickers for which cax database has historical data for a given source
    :param str data_source: data source
    :return: pandas DataFrame
    """
    options = 'source={}'.format(data_source)
    df = cax_df(func_name='GetTickers', data_type='EOD', options=options)
    return df


def get_eod_data(tickers, start, end, data_source, fields=None):
    """
    :description: fetch end of day data from cax
    :param list tickers:
    :param start: start date as an object that support strftime function
    :param end: end date as an object that support strftime function
    :param str data_source: data source
    :param list fields: fields to return
    :return: pandas DataFrame
    """
    options = 'source={}'.format(data_source)
    df = cax_df(
        func_name='GetData', data_type='EOD',
        tickers=tickers, fields=fields,
        start_date=start, end_date=end,
        options=options)
    df.set_index(keys='Date', inplace=True)
    return df


def __df_with_parsed_dates(json_data, to_tz=None):
    da = [[__parse_date(d, to_tz) for d in a] for a in json_data['DataArray']]
    headers = json_data[u'ExcelColumnHeaders']
    data_dict = [{z[0]: z[1] for z in zip(headers, a)} for a in da]
    df = pd.DataFrame(data_dict)
    return df


def get_epnl_1d(start=None, end=None, options=None):
    """
    :description: fetch intraday epnl snapshots
    :param start: start date as an object that support strftime function
    :param end: end date as an object that support strftime function
    :param str options: additional cax options in '[opt1]=[val2]&[opt2]=[val2]...' format
    :return: pandas DataFrame of the epnl data
    """
    raw_data = cax('GetEPNL1D', 'DATA', start_date=start, end_date=end, options=options)
    da = [[__parse_date(d) for d in a] for a in raw_data['DataArray']]
    headers = raw_data[u'ExcelColumnHeaders']
    data_dict = [{z[0]: z[1] for z in zip(headers, a)} for a in da]
    df = pd.DataFrame(data_dict)
    return df


def get_vol_surface(ticker, source, source_type='', start_date=None, end_date=None, options=''):
    """
    :description: fetch volatility surfaces from cax
    :param str ticker: _vol surface ticker
    :param start_date: start date as an object that support strftime function
    :param end_date: end date as an object that support strftime function
    :param str source: data source
    :param str source_type: data source type
    :param str options: additional cax options in '[opt1]=[val2]&[opt2]=[val2]...' format
    :return: _vol surfaces for the ticker, source, and source_type spanning given date range
    """
    source_options = 'source=' + source
    source_options += '&sourceType=' + source_type if source_type else ''
    if options:
        options = source_options + '&' + options
    else:
       options = source_options

    df = cax_df(
        func_name='GetVolSurface', data_type='VOLS',
        tickers=[ticker],
        start_date=start_date, end_date=end_date,
        options=options,
    )

    return df


@docstring_parameter(sorted(chrono.OFFICE_TZ_DICT.keys()))
def get_open_data(observables, start_date, end_date, drop_na=False, to_tz='UTC'):
    """
    This is NOT recommended. Use :py:mod:`panormus.data.open_data` instead.

    :description: fetch data from opendata using cax.
    :param list observables: list of observables to fetch. Observables must be in '|' delimited strings
    :param start_date: start date as an object that support strftime function
    :param end_date: end date as an object that support strftime function
    :param bool drop_na: Drop NA values if set to True
    :param str|None to_tz: convert dates to this timezone. Use None ONLY for time-less dates (daily freq). \
     First to_tz is checked in caxton office dictionary: {0} \
     Otherwise to_tz is checked in pytz timezone dictionary.
    :return:
    """
    func_name = "GetData"
    data_type = "EOD"
    options = "headers=TRUE&source=opendata&datasource=caxton&domain=rates&" \
              "keytype=observable&owner=ebiri&dropNA={}".format(drop_na)
    df = cax_df(
        func_name=func_name, data_type=data_type,
        tickers=observables, fields=None,
        start_date=start_date, end_date=end_date,
        options=options, to_tz=to_tz
    )
    return df


_date_matcher = re.compile('/Date\((?P<date>\d+)(?P<offset_hours>[+-]\d{2})(?P<offset_mins>\d{2})?\)', re.IGNORECASE)


@docstring_parameter(sorted(chrono.OFFICE_TZ_DICT.keys()))
def __parse_date(dt_str, to_tz=None):
    '''
    :param str dt_str: date string
    :param str|None to_tz: convert dates to this timezone. Use None ONLY for time-less dates (daily freq). \
     First to_tz is checked in caxton office dictionary: {0} \
     Otherwise to_tz is checked in pytz timezone dictionary.
    :return: timezone-aware datetime in timezone indicated by to_tz
    '''
    if not dt_str:
        return dt_str
    if not isinstance(dt_str, str):
        return dt_str

    m = _date_matcher.match(dt_str)
    if not m:
        return dt_str
    milliseconds = int(m.group('date'))

    dtdt = dt.datetime.utcfromtimestamp(milliseconds / 1000)
    if to_tz:
        # Interpret posix time into utc (they are always utc when ignoring the timezone tail).
        # Times we receive are formatted as POSIX time with tz tail: xxxxxxxxx-0400 for NYC time.
        dtdt = chrono.datetime_localize(dtdt, 'UTC')
        dtdt = chrono.datetime_change_tz(dtdt, to_tz)

    else:
        # EOD data is generally stored as YYYYMMDD 00:00:00 from any time zone. \
        # The cax db will store this as YYYYMMDD 04:00:00 - 0400 for example. So UTC plus an offset. \
        # We want to reapply the offset to recover YYYYMMDD 00:00:00 so we don't get the wrong day in EOD data.
        o_h = int(m.group('offset_hours'))
        o_m = int(m.group('offset_mins'))
        dtdt += dt.timedelta(hours=o_h) + dt.timedelta(minutes=o_m)

    return dtdt


def get_calendar_by_code(calendar_code):
    """
    :description: get an event calendar by its calendar code, e.g. central bank meetings, holidays for \
    a specific country etc.
    :param str calendar_code: calendar code to fetch
    :return: pandas DataFrame
    """
    if not calendar_code:
        return None
    # CAX("GetCalendarByCenter", "CALENDAR","UKM","DATE", , ,"headers=true&owner=asingh&orientation=H")
    option_val = 'headers=true&owner=asingh&orientation=H'
    raw_data = cax(
        func_name='GetCalendarByCenter',
        data_type='CALENDAR',
        tickers=[calendar_code],
        fields=['DATE'],
        options=option_val)
    df = __df_with_parsed_dates(raw_data)
    return df

 