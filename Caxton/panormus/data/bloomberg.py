'''
Created on 2019年3月23日

@author: user
'''
"""
This module requires the Bloomberg vendor installation:
  pip install --index-url=https://bloomberg.bintray.com/pip/simple blpapi
"""
import datetime as dt

import blpapi
import numpy as np
import pandas as pd

import panormus.data.bloomberg_config as bbgc

__host = 'localhost'
__port = 8194


def get_historical_data(
        tickers, fields, start, end,
        calendar_code_override=None,
        currency_code=None,
        non_trading_day_fill_option=None,
        non_trading_day_fill_method=None,
        periodicity_adjustment=bbgc.PeriodicityAdjustment.ACTUAL.name,
        periodicity=bbgc.PeriodicitySelection.DAILY.name,
        max_data_points=None,
        pricing_option=None,
        adjustment_split=None,
        adjustment_normal=None,
        adjustment_abnormal=None,
        overrides=None
):
    """
    :description: fetch historical data from bloomberg using desktop api. please refer to the api documentation \
    panormus.data.reference.BLPAPI-Core-Developer-Guide.pdf
    :param tickers: an enumerable of tickers
    :param fields: an enumerable of fields
    :param start: start date as an object that supports strftime method (Date, DateTime, Timestamp, etc)
    :param end: start date as an object that supports strftime method (Date, DateTime, Timestamp, etc)
    :param str calendar_code_override:
    :param str currency_code:
    :param str non_trading_day_fill_option:
    :param str non_trading_day_fill_method:
    :param str periodicity: a member of panormus.data.bloomberg_config.PeriodicitySelection enum as a string
    :param str periodicity_adjustment: a member of panormus.data.bloomberg_config.PeriodicityAdjustment enum as a string
    :param int max_data_points: maximum points to return
    :param str pricing_option: pricing option
    :param overrides: a dictionary of overrides where keys are override fields and values are override values
    :return: DataFrame
    """
    # Fill SessionOptions
    session_options = blpapi.SessionOptions()

    session_options.setServerHost(__host)
    session_options.setServerPort(__port)

    # Create a Session
    session = blpapi.Session(session_options)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

    ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("HistoricalDataRequest")

    # append securities to request
    for t in tickers:
        request.append("securities", t)
    # append fields to request
    for f in fields:
        request.append("fields", f)

    start_str = start.strftime('%Y%m%d')
    end_str = end.strftime('%Y%m%d')

    # set period and query periodicity

    request.set(bbgc.start_date, start_str)
    request.set(bbgc.end_date, end_str)

    if calendar_code_override:
        request.set(bbgc.calendar_code_override, calendar_code_override)
    if currency_code:
        request.set(bbgc.currency_code, currency_code)
    if non_trading_day_fill_option:
        request.set(bbgc.non_trading_day_fill_option, non_trading_day_fill_option)
    if non_trading_day_fill_method:
        request.set(bbgc.non_trading_day_fill_method, non_trading_day_fill_method)
    if periodicity:
        request.set(bbgc.periodicity_selection, periodicity)
    if periodicity_adjustment:
        request.set(bbgc.periodicity_adjustment, periodicity_adjustment)
    if max_data_points:
        request.set(bbgc.max_data_points, max_data_points)
    if pricing_option:
        request.set(bbgc.pricing_option, pricing_option)
    if adjustment_split is not None:
        request.set(bbgc.adjustment_split, adjustment_split)
    if adjustment_normal is not None:
        request.set(bbgc.adjustment_normal, adjustment_normal)
    if adjustment_abnormal is not None:
        request.set(bbgc.adjustment_abnormal, adjustment_abnormal)

    if overrides:
        req_overrides = request.getElement(bbgc.overrides)
        for k, v in overrides.items():
            req_override = req_overrides.appendElement()
            req_override.setElement(bbgc.field_id, k)
            req_override.setElement(bbgc.value_fld, v)

    # print("Sending Request:", request)
    session.sendRequest(request)
    d = {'date': [],
         'ticker': []}
    for f in fields:
        d[f] = []
    try:
        # Process received events
        while True:
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement('securityData'):
                    data_arr = msg.getElement('securityData')
                    # sec_name = data_arr.getElementAsString('security')
                    n = data_arr.numValues()
                    for i in range(n):
                        # sec_data = data_arr.getValueAsElement(i)
                        ticker = data_arr.getElementAsString("security")
                        field_data_array = data_arr.getElement(bbgc.FIELD_DATA)
                        field_data_list = \
                            [field_data_array.getValueAsElement(i) for i in range(0, field_data_array.numValues())]
                        for fd in field_data_list:
                            d['date'].append(fd.getElementAsDatetime(bbgc.DATE))
                            d['ticker'].append(ticker)
                            for f in fields:
                                # noinspection PyBroadException
                                try:
                                    v = fd.getElementValue(f)
                                    d[f].append(v)
                                except Exception:
                                    d[f].append(np.NaN)
            # Response completely received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        df = pd.DataFrame(d)
        return df

    finally:
        # Stop the session
        session.stop()


def get_ref_data(tickers, fields, overrides=None):
    """
    :description: fetch reference data from bloomberg using desktop api
    :param tickers: an enumerable of tickers
    :param fields: an enumerable of fields
    :param overrides: a dictionary of overrides where keys are override fields and values are override values
    :return: pandas DataFrame
    """
    # Fill SessionOptions
    session_options = blpapi.SessionOptions()
    session_options.setServerHost(__host)
    session_options.setServerPort(__port)

    # Create a Session
    session = blpapi.Session(session_options)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return None

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return None

    ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("ReferenceDataRequest")

    # append securities to request
    for t in tickers:
        request.append("securities", t)
    # append fields to request
    for f in fields:
        request.append("fields", f)
    # Append overrides to request
    if overrides:
        req_overrides = request.getElement(bbgc.overrides)
        for k, v in overrides.items():
            req_override = req_overrides.appendElement()
            req_override.setElement(bbgc.field_id, k)
            req_override.setElement(bbgc.value_fld, v)

    # print("Sending Request:", request)
    session.sendRequest(request)
    result = []
    try:
        # Process received events
        while True:
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement('securityData'):
                    data_arr = msg.getElement('securityData')
                    n = data_arr.numValues()
                    for i in range(n):
                        sec_data = data_arr.getValueAsElement(i)
                        ticker = sec_data.getElementAsString("security")
                        d = {'ticker': ticker}
                        field_data = sec_data.getElement("fieldData")
                        for f in fields:
                            # noinspection PyBroadException
                            try:
                                d[f] = field_data.getElementValue(f)
                            except Exception:
                                d[f] = None
                        result.append(d)
            # Response completely received, so we could exit
            if ev.eventType() == blpapi.Event.RESPONSE:
                break
        df = pd.DataFrame(result)
        return df
    finally:
        # Stop the session
        session.stop()


def get_bulk_data(ticker, field, overrides=None):
    """
    :description: fetch bulk data from bloomberg using desktop api
    :param str ticker: ticker
    :param str field: field
    :param dict overrides: a dictionary of overrides where keys are override fields and values are override values
    :return: pandas DataFrame
    """
    # Fill SessionOptions
    session_options = blpapi.SessionOptions()
    session_options.setServerHost(__host)
    session_options.setServerPort(__port)

    # Create a Session
    session = blpapi.Session(session_options)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/refdata"):
        print("Failed to open //blp/refdata")
        return

   ref_data_service = session.getService("//blp/refdata")
    request = ref_data_service.createRequest("ReferenceDataRequest")

    # append securities to request
    request.append("securities", ticker)
    # append fields to request
    request.append("fields", field)
    # Append overrides to request
    if overrides is not None:
        overrideOuter = request.getElement('overrides')
        for k in overrides:
            override1 = overrideOuter.appendElement()
            override1.setElement('fieldId', k)
            override1.setElement('value', overrides[k])

    session.sendRequest(request)
    data = dict()

    try:
        # Process received events
        while True:
            # We provide timeout to give the chance to Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:
                if msg.hasElement('securityData'):
                    data_arr = msg.getElement('securityData')
                    for i in range(data_arr.numValues()):
                        field_data = data_arr.getValue(i).getElement("fieldData").getElement(field)
                        for i, row in enumerate(field_data.values()):
                            for j in range(row.numElements()):
                                e = row.getElement(j)
                                k = str(e.name())
                                v = e.getValue()
                                if k not in data:
                                    data[k] = list()

                                data[k].append(v)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                break
    finally:
        # Stop the session
        session.stop()

    return pd.DataFrame.from_dict(data)


def bsrch(domain):
    """
    :description: This function uses the Bloomberg API to retrieve 'bsrch' (Bloomberg SRCH Data) queries. \
    Returns list of tickers.
    :param str domain: A character string with the name of the domain to execute. It can be a user defined SRCH screen,\
    commodity screen or one of the variety of Bloomberg examples. All domains are in the format \
    <domain>:<search_name>. Example "COMDTY:NGFLOW"
    :return: pandas DataFrame of tickers
    """
    # Fill SessionOptions
    session_options = blpapi.SessionOptions()
    session_options.setServerHost(__host)
    session_options.setServerPort(__port)

    # Create a Session
    session = blpapi.Session(session_options)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService("//blp/exrsvc"):
        print("Failed to open //blp/exrsvc")
        return

    exr_service = session.getService("//blp/exrsvc")
    request = exr_service.createRequest("ExcelGetGridRequest")

    request.set("Domain", domain)
    session.sendRequest(request)
    data = []
    try:
        # Process received events
        while True:
            event = session.nextEvent(500)
            if event.eventType() == blpapi.Event.RESPONSE or \
                    event.eventType() == blpapi.Event.PARTIAL_RESPONSE:
                for msg in event:
                    for v in msg.getElement("DataRecords").values():
                        for f in v.getElement("DataFields").values():
                            data.append(f.getElementAsString("StringValue"))
            if event.eventType() == blpapi.Event.RESPONSE:
                break
    finally:
        # Stop the session
        session.stop()

    return pd.DataFrame(data)


def get_last_cb_meeting(ticker, months, today):
    """
    description: Determine most recent central bank meeting from `today`, given a valid \
    central bank ticker.
    :rtype: dt.datetime
    """
    dates = [dt.datetime(int(e[:4]), int(e[5:7]), int(e[8:10])) for e in
             get_bulk_data(ticker, 'ECO_FUTURE_RELEASE_DATE_LIST').iloc[:, 0]]
    return [e for e in dates if e < today and e.month in months][-1]
   

 