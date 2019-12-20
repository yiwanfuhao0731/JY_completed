'''
Created on 2019年3月23日

@author: user
'''
"""
This module requires the Haver vendor installation:
  pip install Haver --extra-index-url http://www.haver.com/Python --trusted-host www.haver.com
"""
import Haver
from panormus.utils.simple_func_decorators import hide_warnings

__db_path = '//mozart/Home/Economics/dlx/data'

Haver.path(__db_path)


def get_db_meta(db_name):
    """
    :description: get meta data information for a given database suc as available \
    series, their description, date range, etc
    :param str db_name: database name
    :return: dictionary
    """
    meta = Haver.metadata(database=db_name)
    return meta


@hide_warnings([DeprecationWarning, UserWarning, FutureWarning])
def get_data(
        codes, database=None,
        startdate=None, enddate=None,
        frequency=None, aggmode='strict',
        eop=True, dates=False, uselimits=True
):
    """
    :description: fetch data from haver database
    :param codes: an enumerable of haver codes
    :param str database: name of the haver database that contains data for given codes
    :param startdate: (str or datetime.date) – Specifies the starting date of the query. If it is specified as str, \
    it is converted to a datetime.date object using the input format yyyy-MM-dd, e.g. (‘2005-12-31).
    :param enddate: (str or datetime.date) – Specifies the ending date of the query. Analogous comments \
    as for startdate apply.
    :param frequency: (str or list or tuple of str with one element) – Specifies the target frequency of the query. \
    One of ‘daily’, ‘weekly’, ‘monthly’, ‘quarterly’, ‘annual’, or ‘yearly’, or any abbreviation thereof, down
    to a single character.
    :param aggmode: (str or list or tuple of str with one element) – The aggregation mode of the query.
    One of ‘strict’, ‘relaxed’, or ‘force’, or any abbreviation therof.
    :param dates: Determines whether time periods are shown as dates (e.g. ‘2000-03-31’ instead of ‘2000Q1’). \
    Data with frequencies other than weekly are displayed with respect to time periods. Weekly data are shown as dates.\
    The date shown for weekly data corresponds to the anchor weekday of the series or DataFrame
    :param bool eop: Determines whether dates are recorded beginning-of-period or end-of-period. Only relevant \
    if dates=True. Default: True.
    :param bool uselimits: If False, Haver.data() ignores settings regarding query limits. Default: True.
    :return: pandas DataFrame
    """
    df = Haver.data(
        codes=codes,
        database=database,
        startdate=startdate,
        enddate=enddate,
        frequency=frequency,
        aggmode=aggmode,
        eop=eop,
        dates=dates,
        uselimits=uselimits
    )
    return df

 