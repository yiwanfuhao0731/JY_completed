pvk date_fns.py



import datetime as dt
import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
import traceback
import sys

import QuantLib as ql


DATE_FORMATS = {
    # Excel date time formats
    'dd/mm/yy':          lambda d: d.strftime('%d/%m/%y'),
    'mm/dd/yy':          lambda d: d.strftime('%m/%d/%y'),
    'dd m yy':           lambda d: "{} {} {}".format(d.strftime('%d', d.month), d.strftime('%y')),
    'd mm yy':           lambda d: "{} {} {}".format(d.day, d.strftime('%m'), d.strftime('%y')),
    'd mmm yy':          lambda d: "{} {} {}".format(d.day, d.strftime('%b'), d.strftime('%y')),
    'd mmm yyyy':        lambda d: "{} {} {}".format(d.day, d.strftime('%b'), d.strftime('%Y')),
    'd mmmm yy':         lambda d: "{} {} {}".format(d.day, d.strftime('%B'), d.strftime('%y')),
    'd mmmm yyy':        lambda d: "{} {} {}".format(d.day, d.strftime('%B'), d.strftime('%Y')),
    'd mmmm yyyy':       lambda d: "{} {} {}".format(d.day, d.strftime('%B'), d.strftime('%Y')),
    'dd/mm/yy hh:mm':    lambda d: d.strftime('%d/%m/%y %H:%M:%S'),
    'dd/mm/yy hh:mm:ss': lambda d: d.strftime('%d/%m/%y %H:%M:%S'),
    'dd-mmm-yy hh:mm':   lambda d: d.strftime('%d-%b-%y %H:%M'),
    'dd-mmm-yyyy hh:mm': lambda d: d.strftime('%d-%b-%Y %H:%M'),
    'dd-mm-yy_hh-mm':    lambda d: d.strftime('%d-%m-%Y_%H-%M'),
    'yyyy-mm-dd_hh-mm':  lambda d: d.strftime('%Y-%m-%d_%H-%M'),
    'yy-mm-dd_hh-mm':    lambda d: d.strftime('%y-%m-%d_%H-%M'),
    'yyyymmddHhhMmm':    lambda d: d.strftime('%Y%m%dH%HM%M'),
    'hh:mm':             lambda d: d.strftime('%H:%M'),
    'hh:mm:ss':          lambda d: d.strftime('%H:%M:%S'),
    'hh:mm:ss.000':      lambda d: d.strftime('%H:%M:%S'),
    'yyyymmdd':          lambda d: d.strftime('%Y%m%d'),
    'yyyy-mm-dd':        lambda d: d.strftime('%Y-%m-%d'),
    'dd-mmm-yy':         lambda d: d.strftime('%d-%b-%y'),
    'dd-mmm-yyyy':       lambda d: d.strftime('%d-%b-%Y'),
    # Custom names
    'utc':               lambda d: d.strftime('%Y-%m-%d'),
    'bbg':               lambda d: d.strftime('%Y%m%d'),
    'cax':               lambda d: d.strftime('%d/%m/%Y'),
    }

DATE_FORMATS_READ = {
    # Excel date time formats
    'dd/mm/yy':             '%d/%m/%y',
    'mm/dd/yy':             '%m/%d/%y',
    'mm/dd/yyyy':           '%m/%d/%Y',
    'dd/mm/yy hh:mm':       '%d/%m/%y %H:%M:%S',
    'dd/mm/yy hh:mm:ss':    '%d/%m/%y %H:%M:%S',
    'dd-mm-yy hh:mm:ss':    '%d-%m-%y %H:%M:%S',
    'yyyy-mm-dd hh:mm:ss':  '%Y-%m-%d %H:%M:%S',

    'hh:mm':                '%H:%M:%S',
    'hh:mm:ss':             '%H:%M:%S',
    'hh:mm:ss.000':         '%H:%M:%S',
    'yyyymmdd':             '%Y%m%d',
    'yyyy-mm-dd':           '%Y-%m-%d',
    'dd-mm-yy':             '%d-%m-%y',
    'yy-mm-dd':             '%d-%m-%y',

    # Custom names
    'utc':                  '%Y-%m-%d',
    'bbg':                  '%Y%m%d',
    'cax':                  '%d/%m/%Y',
    }


def to_str(date, format="yyyy-mm-dd"):
    if date is None:
        return None

    formatfn = DATE_FORMATS.get(format, lambda d: d.strftime(format))

    if isinstance(date, dt.date):
        if pd.isnull(date):
            return "-"
        else:
            return formatfn(date)

    elif isinstance(date, dt.datetime):
        raise ValueError("Passed a date.datetime object, this object is reserved for intra-day data, " +
                         "which is not yet supported")
    elif isinstance(date, str):
        return date
    elif pd.isnull(date):
        return "-"
    else:
        raise ValueError("Input of type \'{}\' not supported at this moment.".format(type(date)))


def from_str(date, format='%Y%m%d', raiseonerror=False):
    # format = ["%m-%d-%Y", "%d/%m/%Y"]
    def convert(date, format):
        try:
            format = DATE_FORMATS_READ.get(format, format)
            # format = "%m-%d-%Y"
            return dt.datetime.strptime(str(date), format)
        except ValueError:
            return sys.exc_info()

    if isinstance(format, str):
        output = convert(date, format)
    else:
        for f in format:
            output = convert(date, f)
            if isinstance(output, dt.datetime):
                break

    if raiseonerror and not isinstance(output, dt.datetime):
        raise output[1]
    elif isinstance(output, dt.datetime):
        return output
    else:
        return None


def to_date(date):
    if date is None:
        return date
    else:
        return dt.date(date.year, date.month, date.day)


def to_datetime(date):
    """Convert a date to datetime.datetime"""
    if isinstance(date, dt.datetime):
        return date
    elif isinstance(date, ql.Date):
        return dt.datetime(date.year(), date.month(), date.dayOfMonth())
    else:
        return dt.datetime(date.year, date.month, date.day)


def is_date(date):
    if isinstance(date, dt.datetime):
        return True
    elif isinstance(date, ql.Date):
        return True
    elif hasattr(date, 'year'):
        return True
    else:
        return False


def to_qldate(date):
    """Convert a date to ql.Date"""
    if isinstance(date, ql.Date):
        return date
    else:
        return ql.Date(date.day, date.month, date.year)


def to_relativedelta(str_rd):
    """convert string to relativedelta
    input e.g. 3m, 5y, 12w
    """

    if str_rd[-1] == "m":
        return relativedelta(months=int(str_rd[:-1]))
    elif str_rd[-1] == "y":
        return relativedelta(years=int(str_rd[:-1]))
    elif str_rd[-1] == "w":
        return relativedelta(weeks=int(str_rd[:-1]))


def get_calendar(start_date, end_date, interval, only_weekdays=True):
    """
    return a list of dates between start and end date
    :param start_date:
    :param end_date:
    :param interval: either dt.timedelta or "1b", "4m" etc.
    :return: list of dates
    """
    if isinstance(interval, dt.timedelta):
        step = interval
    else:
        if interval[-1] == "b":
            step = dt.timedelta(days=int(interval[:-1]))
            only_weekdays = True
        elif interval[-1] == "m":
            # step = dt.timedelta(minutes=int(interval[:-1]))
            step = relativedelta(months=int(interval[:-1]))
        elif interval[-1] == "w":
            step = relativedelta(weeks=int(interval[:-1]))
        else:
            raise ValueError("Interval: \'{}\' not recognized.".format(interval))

    dates_out = []
    i_date = start_date
    while i_date <= end_date:
        if not only_weekdays or i_date.weekday() < 5:
            dates_out.append(i_date)
        i_date += step

    return dates_out


def next_imm(i_dt):
    ql_dt = to_qldate(i_dt)
    return ql.IMM.nextDate(ql_dt)


def advance_calendar(date, period, calendar, convention='fol'):
    """advance the day according to a calendar"""

    if period == '':
        return date

    # get the QuantLib date objects
    calendar = {'us': ql.UnitedStates(),
                'eu': ql.TARGET()}[calendar]
    qlperiod = ql.Period(int(period[:-1]), {'b': ql.Days,
                                            'w': ql.Weeks,
                                            'm': ql.Months,
                                            'y': ql.Years}[period[-1]])
    convention = {'modfol': ql.ModifiedFollowing,
                  'fol': ql.Following,
                  'pre': ql.Preceding,
                  'modpre': ql.ModifiedPreceding,
                  'unadj': ql.Unadjusted
                  }[convention]

    # convert the date
    # date_ql_in = ql.Date(date.day, date.month, date.year)
    date_ql_out = calendar.advance(to_qldate(date), qlperiod, convention)

    # convert back to the original type
    date_out = type(date)(date_ql_out.year(), date_ql_out.month(), date_ql_out.dayOfMonth())

    return date_out


def period_to_yearfrac_approx(per):
    """approximate string to period conversion"""
    if per == '':
        return 0
    letter = per[-1]
    counter = int(per[:-1])
    fractions = {'b': 252, 'd': 365, 'w': 52, 'm': 12, 'y': 1}
    return counter / fractions[letter]