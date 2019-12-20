"""

This module provides methods for managing automated jobs.

"""

import argparse

import traceback

import datetime as dt

from panormus.config.settings import JOB_EMAIL_DEFAULT

from panormus.utils import chrono, message

_date_fmt = '%Y-%m-%d'

_date_fmt_example = 'YYYY-MM-DD'


def timed_print(str):
    print(f'{chrono.now()} -- ' + str)


def make_parser(job_desc):
    parser = argparse.ArgumentParser(

        description=job_desc

    )

    return parser


def make_single_date_parser(job_desc, ad_default=None):
    parser = make_parser(job_desc)

    parser.add_argument(

        '--ad', '--asof_date', dest='asof_date', default=ad_default, type=str, required=False,

        help=f'asof date as {_date_fmt_example}')

    return parser


def make_date_parser(job_desc, lookback_default=7, sd_default=None, ed_default=None):
    parser = make_parser(job_desc)

    parser.add_argument(

        '--sd', '--start_date', dest='start_date', default=sd_default, type=str, required=False,

        help=f'start date as {_date_fmt_example}')

    parser.add_argument(

        '--ed', '--end_date', dest='end_date', default=ed_default, type=str, required=False,

        help=f'end date as {_date_fmt_example}')

    parser.add_argument(

        '--lb', '--lookback', dest='lookback_cal_days', default=lookback_default, type=int, required=False,

        help='get start_date by looking back this many days from end_date if start_date is not given')

    return parser


def add_email_arg(parser, flags=('--email', '--to', '--email_to'), dest='email_to', default=JOB_EMAIL_DEFAULT):
    if isinstance(flags, str):

        parser.add_argument(

            flags, dest=dest, default=default, type=str, required=False,

            help='Comma separated string. Use "quotes" if your string contains spaces.')

    else:

        parser.add_argument(

            *flags, dest=dest, default=default, type=str, required=False,

            help='Comma separated string. Use "quotes" if your string contains spaces.')


def parse_kwargs(parser):
    """

    :description: process a parser into kwargs dict

    :param parser: a parser from argparse

    """

    return vars(parser.parse_args())


def parse_kwargs_with_single_date(parser, arg_date_fmt=_date_fmt):
    """

    :description: process a parser into kwargs dict, determining start_date and end_date

    :param parser: a parser from argparse

    :param arg_date_fmt: command line date format

    :return: dict

    """

    kwargs = parse_kwargs(parser)

    if kwargs.get('asof_date', None):

        kwargs['asof_date'] = dt.datetime.strptime(kwargs['asof_date'], arg_date_fmt).date()

    else:

        kwargs['asof_date'] = chrono.now().date()

    return kwargs


def parse_kwargs_with_dates(parser, arg_date_fmt=_date_fmt):
    """

    :description: process a parser into kwargs dict, determining start_date and end_date

    :param parser: a parser from argparse

    :param arg_date_fmt: command line date format

    :return: dict

    """

    kwargs = parse_kwargs(parser)

    # Extract date args from kwargs and determine start_date, end_date

    lookback_cal_days = int(kwargs.pop('lookback_cal_days', 0))

    if kwargs.get('end_date', None):

        kwargs['end_date'] = dt.datetime.strptime(kwargs['end_date'], arg_date_fmt).date()

    else:

        kwargs['end_date'] = chrono.now().date()

    if kwargs.get('start_date', None):

        kwargs['start_date'] = dt.datetime.strptime(kwargs['start_date'], arg_date_fmt).date()

    else:

        kwargs['start_date'] = kwargs['end_date'] - dt.timedelta(days=lookback_cal_days)

    return kwargs


class email_job_errors:

    def __init__(self, job_name, email_to=message.FO_SUPPORT, email_failure=True, email_success=False):

        """

        :param str job_name: name of job for log message and error email subject

        :param str email_to: email address for job notifications

        :param bool email_failure: send email when failures occurs

        :param bool email_success: send email upon job completion

        """

        self.job_name = job_name

        self.email_to = email_to

        self.email_failure = email_failure

        self.email_success = email_success

    def __call__(self, f):

        """

        If there are decorator arguments, __call__() is only called

        once, as part of the decoration process! You can only give

        it a single argument, which is the function object.

        """

        def wrapped_f(*args, **kwargs):

            '''

            Arguments passed to f are received here. This wrapped function replaces f.

            '''

            try:

                timed_print(

                    f'Running job {self.job_name} with' + \
 \
                    f'\nargs:\n{args}\nkwargs:\n{str(kwargs.items())}'

                )

                f(*args, **kwargs)

                timed_print(f'Finished {self.job_name}')

                if self.email_to and self.email_success:
                    timed_print(f'Sending success notification to {self.email_to}')

                    message.send_email_html(

                        subject='Completed ' + self.job_name,

                        body_html='<br>',

                        to=self.email_to,

                    )

                    timed_print(f'Sent success notification')



            except KeyboardInterrupt:

                raise

            except SystemExit:

                raise

            except Exception:

                if self.email_to and self.email_failure:
                    timed_print(f'Sending failure notification to {self.email_to}')

                    message.send_email_html(

                        subject='Error running ' + self.job_name,

                        body_html=traceback.format_exc(),

                        to=self.email_to,

                    )

                    timed_print(f'Sent failure notification')

                raise

        wrapped_f.__doc__ = f.__doc__

        return wrapped_f