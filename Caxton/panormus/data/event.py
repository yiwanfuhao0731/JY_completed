'''
Created on 2019年3月23日

@author: user
'''
import datetime as dt
import os
import pandas as pd

from panormus.utils.chrono import (datetime_localize, datetime_change_tz, OFFICE_TZ_DICT)
from panormus.utils.simple_func_decorators import docstring_parameter
from panormus.utils.filesystem import find_filenames


class EventClient(object):
    """
    Loads event dates and future event dates on demand.
    """
    def __init__(
            self,
            event_dir=r'\\mozart\Caxton-Shared\QAG\data\eco_release',
            exact_event_dir=r'\\mozart\Caxton-Shared\QAG\data\eco_release_exact',
            future_event_dir=r'\\mozart\Caxton-Shared\QAG\data\future_eco_release',
            file_suffix='.csv',
            date_fmt='%Y-%m-%d',
            datetime_fmt='%Y-%m-%d %H:%M:%S',
            datetime_tz='UTC',
            date_col_name='datetime',
    ):
        """
        :description: makes client instance and reads file names from event directories.
        :param str event_dir: directory containing event files with dates only
        :param str exact_event_dir: directory containing event files with datetimes
        :param str future_event_dir: directory containing future event files
        :param str file_suffix: suffix for event files (file extension)
        :param str date_fmt: format for parsing event dates
        :param str datetime_fmt: format for parsing event datetime
        :param str datetime_tz: timezone for parsing event datetimes
        :param str date_col_name: column name for parsing dates and datetimes across all event files.
        """
        self.event_dir = event_dir
        self.exact_event_dir = exact_event_dir
        self.future_event_dir = future_event_dir
        self.file_suffix = file_suffix
        self.date_fmt = date_fmt
        self.datetime_fmt = datetime_fmt
        self.datetime_tz = datetime_tz
        self.date_col_name = date_col_name
        self.refresh_file_lists()

    def refresh_file_lists(self):
        """
        Read event directories to refresh event name lists.
        """
        self.events = [
            fn[:-1 * len(self.file_suffix)]
            for fn in sorted(find_filenames(self.event_dir, fn_suffix=self.file_suffix))
        ]
        self.exact_events = [
            fn[:-1 * len(self.file_suffix)]
            for fn in sorted(find_filenames(self.exact_event_dir, fn_suffix=self.file_suffix))
        ]
        self.future_events = [
            fn[:-1 * len(self.file_suffix)]
            for fn in sorted(find_filenames(self.future_event_dir, fn_suffix=self.file_suffix))
        ]

    def list_event_names(self):
        return self.events

    def list_exact_event_names(self):
        return self.exact_events

    def list_future_event_names(self):
        return self.future_events

    def get_event_dates(self, event_name):
        """
        :description: get past event dates.
        :param str event_name: event name. Use list_event_names to see choices.
        :return: list[dt.date]
        """
        fn = os.path.join(self.event_dir, event_name + self.file_suffix)
        dt_parser = lambda x: dt.datetime.strptime(x, self.date_fmt)
        ser = pd.read_csv(
            fn, parse_dates=[self.date_col_name], date_parser=dt_parser
        )[self.date_col_name].dt.date
        return ser.tolist()

    @docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
    def get_exact_event_dates(self, event_name, to_tz):
        """
        :description: get exact past event datetimes and convert to desired timezone.
        :param str event_name: event name. Use list_exact_event_names to see choices.
        :param str to_tz: A pytz timezone string or CAX 3-letter office code: {0}.
        :return: list[dt.datetime]
        """
        fn = os.path.join(self.exact_event_dir, event_name + self.file_suffix)
        if not to_tz:
            to_tz = self.datetime_tz

        dt_parser = lambda x: datetime_change_tz(datetime_localize(
            dt.datetime.strptime(x, self.datetime_fmt), self.datetime_tz
       ), to_tz=to_tz)
        ser = pd.read_csv(
            fn, parse_dates=[self.date_col_name], date_parser=dt_parser
        )[self.date_col_name]
        return ser.dt.to_pydatetime().tolist()

    @docstring_parameter(sorted(OFFICE_TZ_DICT.keys()))
    def get_future_event_datetimes(self, event_name, to_tz=None):
        """
        :description: get future event datetimes and convert to desired timezone.
        :param str event_name: event name. Use list_future_event_names to see choices.
        :param str to_tz: A pytz timezone string or CAX 3-letter office code: {0}.
        :return: list[dt.datetime]
        """
        fn = os.path.join(self.future_event_dir, event_name + self.file_suffix)
        if not to_tz:
            to_tz = self.datetime_tz

        dt_parser = lambda x: datetime_change_tz(datetime_localize(
            dt.datetime.strptime(x, self.datetime_fmt), self.datetime_tz
        ), to_tz=to_tz)
        ser = pd.read_csv(
            fn, parse_dates=[self.date_col_name], date_parser=dt_parser
        )[self.date_col_name]
        return ser.dt.to_pydatetime().tolist()

 