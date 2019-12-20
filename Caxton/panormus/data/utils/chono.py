"""

Date and time utilities with minimal dependencies. Especially useful for intraday data and timezones.



See :py:mod:`market_date` if you want to adjust dates using holiday calendars or market conventions.

"""

import datetime as dt

from math import log10

import numpy as np

import pytz

from panormus.utils.simple_func_decorators import docstring_parameter

OFFICE_TZ_DICT = {

    'NYC': 'US/Eastern',

    'LON': 'Europe/London',

    'HCM': 'Asia/Ho_Chi_Minh',

    'SYD': 'Australia/Sydney',

    'SGD': 'Asia/Singapore',

    'UTC': 'UTC',

}

DAY_COUNT_CONV_DICT = {

    'bus251': {

        'd': 1,

        'w': 5,

        'm': 21,

        'y': 251,

    },

    'bus252': {

        'd': 1,

        'w': 5,

        'm': 21,

        'y': 252,

    },

    'bus260': {

        'd': 1,

        'w': 5,

        'm': 65 / 3.,

        'y': 260,

    },

    'cal360': {

        'd': 1,

        'w': 7,

        'm': 30,

        'y': 360,

    },

    'cal365': {

        'd': 1,

        'w': 7,

        'm': 365 / 12,

        'y': 365,

    }

}


@docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
def now(drop_time=False, tz_str='NYC'):
    '''

    :description: Get current time.

    :param bool drop_time: Determines return type. False: datetime.datetime. True: datetime.date.

    :param str tz_str: A pytz timezone string or CAX 3-letter office code: {0}.

    :return: a timezone-aware datetime with timezone set using office.

    '''

    tz = OFFICE_TZ_DICT.get(tz_str.upper(), tz_str)

    now = datetime_localize(dt.datetime.utcnow(), 'UTC').astimezone(pytz.timezone(tz))

    if drop_time:
        now = now.date()

    return now


def convert_datetime_to_trading_session(exact_dt, open_hour, tz):
    """

    :description: convert intraday tick to its trading session based market's open hour and timezone. \

    This is needed because the date within tick datetime may not equal to the trading session day

    :param datetime.datetime exact_dt:

    :param float open_hour: hour of the day when market opens. Negative values for markets that open the night before.

    :param str tz: string for the timezone of the market.

    :return: datetime.date of trading session

    """

    market_open_shift = dt.timedelta(hours=open_hour)

    session_date = datetime_change_tz(exact_dt - market_open_shift, to_tz=tz).date()

    return session_date


@docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
def datetime_change_tz(start_date, to_tz):
    '''

    :description: Shifts a given datetime to the target time zone.

    :param dt.datetime start_date: datetime with timezone awareness.

    :param str to_tz: A pytz timezone string or CAX 3-letter office code: {0}.

    :return: a timezone-aware datetime with timezone set using to_office.

    '''

    to_tz = OFFICE_TZ_DICT.get(to_tz.upper(), to_tz)

    end_date = start_date.astimezone(pytz.timezone(to_tz))

    return end_date


@docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
def datetime_localize(start_date, tz_str):
    '''

    :description: You can't pass a pytz timezone into the datetime constructor or datetime.replace method.\

    Use this method to safely make a date timezone aware.

    :param dt.datetime start_date: datetime (tz naive) to localize to given timezone

    :param str tz_str: A pytz timezone code or one of the CAX 3-letter office codes: {0}.

    :return: a timezone-aware datetime with the same time, now associated the office timezone

    '''

    return pytz.timezone(OFFICE_TZ_DICT.get(tz_str.upper(), tz_str)).localize(start_date)


def time_range(start_time, end_time, step_hours=0, step_minutes=0, step_seconds=0, step_microseconds=0):
    '''

    :description: Generate a range of times between start_time and end_time (inclusive) with given step sizes. \

    Multiple step size inputs (e.g. step_hours=1 and step_minutes=1) will both be applied at each step.

    :param start_time: First time in range. A datetime.time or a colon-delimited string as HH:MM, HH:MM:SS, HH:MM:SS:MS

    :param end_time: Last time in range. A datetime.time or a colon-delimited string as HH:MM, HH:MM:SS, HH:MM:SS:MS

    :param step_hours: time step size

    :param step_minutes: time step size

    :param step_seconds: time step size

    :param step_microseconds: time step size

    :return: list of times

    '''

    if start_time == end_time:
        return [start_time]

    if all(step == 0 for step in [step_hours, step_minutes, step_seconds, step_microseconds]):
        raise ValueError('At least one nonzero step period must be provided.')

    if isinstance(start_time, str):

        st = dt.time(*[int(tm) for tm in start_time.split(':')])

    else:

        st = start_time

    if isinstance(end_time, str):

        et = dt.time(*[int(tm) for tm in end_time.split(':')])

    else:

        et = end_time

    time_list = []

    tdelta = dt.timedelta(

        hours=step_hours, minutes=step_minutes, seconds=step_seconds, microseconds=step_microseconds)

    loop_dt = dt.datetime.combine(dt.datetime.today(), st)

    end_dt = dt.datetime.combine(dt.datetime.today(), et)

    while loop_dt <= end_dt:
        time_list += [loop_dt.time()]

        loop_dt += tdelta

    return time_list


def iterate_time(

        start_hour, start_minute,

        end_hour, end_minute,

        step_minutes=1,

):
    """

    :description: iterates between two times by step minutes.

    :param int start_hour: hour of start time

    :param int start_minute: minute of start time

    :param int end_hour:  hour of end time

    :param int end_minute: minute of end time

    :param int step_minutes: minutes to step from start time to end time

    :return: yields one value at a time for iteration.

    """

    if step_minutes == 0:
        raise ValueError("Step minutes cannot be zero to avoid infinite loop.")

    start_time = dt.time(hour=start_hour, minute=start_minute)

    end_time = dt.time(hour=end_hour, minute=end_minute)

    step_td = dt.timedelta(minutes=step_minutes)

    # loop condition must correspond to sign of step timedelta (pos/neg)

    if step_td > dt.timedelta():

        f = lambda x: x <= end_time

    else:

        f = lambda x: x >= end_time

    dummy_date = dt.date(2018, 6, 1)

    current = start_time

    while f(current):
        yield current

        # Can't add a timedelta to a time, so use a fixed dummy date to facilitate datetime adjustment.

        current = (dt.datetime.combine(dummy_date, current) + step_td).time()


def convert_date_to_excel_ordinal(year, month, day):
    '''

    :description: convert a date to an excel ordinal date

    :param int year:

    :param int month:

    :param int day:

    :return: integer

    '''

    EXCEL_JULIAN_OFFSET = 693594

    date_to_convert = dt.date(year, month, day)

    return date_to_convert.toordinal() - EXCEL_JULIAN_OFFSET


def get_quarter(month):
    """

    Determine quarter of the year.



    :rtype: int

    """

    return (month - 1) // 3 + 1


def get_year_quarter(date, quarter_offset=0):
    """

    :description: given date and a quarter offset, return the year and quarter as tuple

    :param date: a date with year and month fields

    :param int quarter_offset: quarter offset from quarter of input date

    :return: tuple of integers (year, quarter)

    """

    if type(quarter_offset) != int:
        raise ValueError('quarter_offset must be integer')

    year = date.year

    quarter = get_quarter(date.month)

    if quarter_offset != 0:
        adj_quarter = quarter - 1 + quarter_offset

        year += adj_quarter // 4

        quarter = (adj_quarter % 4) + 1

    return (year, quarter)


def parse_date_num(date_num):
    """

    :description: Parse date numbers like 20180501.0

    :param float|int date_num:

    :return: dt.date

    """

    return parse_datetime_numeric(date_num).date()


def parse_datetime_numeric(date_num):
    """

    :description: Parse datetime numbers like 201805011430.0

    :param float|int date_num:

    :return: dt.datetime

    """

    date_remaining = int(date_num)

    date_len = int(log10(date_remaining)) + 1

    if date_len > 14:
        raise ValueError(f'Sub-second date numbers are not supported: {date_num}')

    date_parts = []

    for i in range((date_len - 4) // 2):
        date_parts.append(date_remaining % 100)

        date_remaining = date_remaining // 100

    date_parts = [date_remaining] + list(reversed(date_parts))

    res = dt.datetime(*date_parts)

    return res


@docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
def combine_date_with_time(date, time, time_tz=None, to_tz=None):
    """

    :description: Combine date with time. Optionally provide the timezone of the given time using time_tz to \

    localize datetime. Then optionally convert to a desired timezone using to_tz.

    :param dt.date date:

    :param dt.time time:

    :param str|None time_tz: Localize time to this timezone. A pytz timezone string or CAX 3-letter office code: {0}.

    :param str|None to_tz: Convert to this timezone after localizing. Same rules as time_tz.

    :return: dt.datetime

    """

    ret_dt = dt.datetime.combine(date, time)

    if time_tz:
        ret_dt = datetime_localize(ret_dt, time_tz)

    if to_tz:
        ret_dt = datetime_change_tz(ret_dt, to_tz)

    return ret_dt


def iterate_year_quarter(

        start_year, start_quarter,

        end_year, end_quarter,

        step_quarters=1

):
    """

    :description: iteratively yields 2 tuples of (year, quarter)

    :param int start_year: positive integer

    :param int start_quarter: 1 thorugh 3

    :param int end_year: positive integer

    :param int end_quarter: 1 thorugh 3

    :param int step_quarters: positive integer

    :rtype: collections.Iterable[(int, int)]

    """

    step_quarters = int(step_quarters)

    # Convert start and end to YYYYQ representation for arithmetic

    start_val = int(start_year) * 10 + int(start_quarter)

    end_val = int(end_year) * 10 + int(end_quarter)

    # Convert step quaters into a YYYYQ representation for arithmetic

    step_val = 10 * (step_quarters // 4) + (step_quarters % 4)

    if step_quarters <= 0:
        raise ValueError("Step quarters must be a positive integer.")

    if end_val < start_val:
        raise ValueError(f"Start yearquarter must be earlier than end yearquarter. start: {start_val}, end: {end_val}")

    if (start_quarter < 1) | (start_quarter > 4):
        raise ValueError(

            f"Invalid start quarter: {start_quarter}. Start quarter must be an integers valued 1 through 4.")

    if (end_quarter < 1) | (end_quarter > 4):
        raise ValueError(f"Invalid end quarter: {end_quarter}. End quarter must be an integers valued 1 through 4.")

    # Iterate through quarters

    current = start_val

    while current <= end_val:
        yield (current // 10, current % 10)

        # Step forward

        current += step_val

        # Roll extra quarters into the years position

        current += 6 * (((current - 1) % 10) // 4)


def period_index_quarterly_to_date(series):
    """

    :rtype: np.ndarray

    """

    return np.array([dt.date(e.year, e.quarter * 3, 1) for e in series])


def period_index_monthly_to_date(series):
    """

    :rtype: np.ndarray

    """

    return np.array([dt.date(e.year, e.month, 1) for e in series])


def period_str_add(period_str_list, return_units, convention='bus251'):
    '''

    :description: add period strings as converted day counts and returns integer number in return units. \

     Rounds toward zero to determine integer

    :param period_str_list: list of periods strings, e.g. ['1m', '3m', ...]

    :param return_units: string in ['d', 'w', 'm', 'y']

    :param str convention: key for DAY_COUNT_CONV_DICT

    :return: dictionary with keys 'period_str': e.g. '1m', '3m', etc and 'period_num': e.g. 1, 3, etc.

    '''

    if not period_str_list:
        return None

    n = 0

    for p in period_str_list:
        n += period_str_to_busdays(p, convention)

    return busdays_to_period_str(n, return_units, convention)


def busdays_to_period_str(n, return_units, convention='bus251'):
    '''

    :description: convert number of business days to integer number of return_units. Rounds toward zero.

    :param n: business day count

    :param return_units: string in ['d', 'w', 'm', 'y']

    :param str convention: key for DAY_COUNT_CONV_DICT

    :return: dictionary with keys 'period_str': e.g. '1m', '3m', etc and 'period_num': e.g. 1, 3, etc.

    '''

    period_num = int(round(n / DAY_COUNT_CONV_DICT[convention.lower()][return_units], 0))

    period_str = str(period_num) + return_units

    return {

        'period_str': period_str,

        'period_num': period_num,

    }


def period_str_to_busdays(s, convention='bus251'):
    '''

    :description: convert a date adjustment string to the equivalent number of business days \

     given a convention that maps letters to business days.

    :param s: one of ['1d', '2w', '3m', '2y'] etc

    :param str convention: key for DAY_COUNT_CONV_DICT

    :return: integer number of business days

    '''

    bd_map = DAY_COUNT_CONV_DICT[convention.lower()]

    q = s[-1].lower()

    m = float(s[:-1])

    return int(bd_map[q] * m)


def period_to_dates_as_first_of_month(dates):
    """

    :rtype: np.ndarray

    """

    return np.array([dt.date(e.year, e.month, 1) for e in dates])


def add_months(year, month, step=1):
    """



    :param int year:

    :param int month:

    :param int step:

    :rtype: (int, int)

    :return: stepped year and month

    """

    new_month = month + step

    year_adj = (new_month - 1) // 12

    new_month = (new_month - 1) % 12 + 1

    return year + year_adj, new_month