'''
Created on 2019年3月23日

@author: user
'''
import base64
import json
import requests
import time
from urllib3.exceptions import InsecureRequestWarning
from urllib.parse import quote_plus

from numpy import nan
import pandas as pd

from panormus.data.bo.db_engine import get_credentials
from panormus.utils.chrono import parse_date_num
from panormus.utils.simple_func_decorators import hide_warning

_jpm_root = 'https://markets-api.jpmorgan.com'
_api_url = _jpm_root + '/research/dataquery/DQDataJSONP'
_cert_fn = r'\\composers\dfs\traders\QAGSpreadsheets\certificates\jpmdq\CAXTON.COM.cer'
_key_fn = r'\\composers\dfs\traders\QAGSpreadsheets\certificates\jpmdq\caxton.com.key'


def _make_basic_header(user, password):
    basic_creds = base64.b64encode((user + ':' + password).encode('ascii', 'ignore')).decode('utf-8')
    return {
        "Authorization": "Basic " + basic_creds
    }


def _jsonify_and_extract_series_dict(res):
    """
    :description: parse raw response from jpmdq into json, and extract dictionary of series
    :param res: raw result
    :return: dict[pd.Series]
    """
    try:
        res_dict = json.loads(res)
    except json.decoder.JSONDecodeError:
        raise JpmdqException(f'Response could not be parsed as json: {res}')

    if res_dict['errorMessage']:
        raise JpmdqException(res_dict['errorMessage'])

    ser_dict = {
        item['expression']: pd.Series(item['plotPoints'], index=res_dict['date'])
        for item in res_dict['seriesarr']
    }
    return ser_dict


def get_client_using_credentials(
        user,
        api_type='jpmdq',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='api_creds.yml'
):
    """
    :description: constructs a JpmdqClient by pulling credentials from a yaml file.
    :param str user: caxton user to pull credentials for
    :param str api_type: citi_velocity, jpmdq, etc
    :param str file_dir: directory of credentials file
    :param str file_name: credentials file name
    :return: JpmdqClient
    """
    multi_creds = get_credentials(for_key=api_type, file_dir=file_dir, file_name=file_name)
    creds = multi_creds[user]
    return JpmdqClient(user=creds['user'], password=creds['password'])


class JpmdqException(Exception):
    """
    Class for exceptions while parsing JP Morgan dataquery api responses
    """

    def __init__(self, message):
        super().__init__(message)


class JpmdqClient(object):
    """
    JP Morgan dataquery client
    """

    def __init__(self, user, password, cert_fn=_cert_fn, key_fn=_key_fn, api_url=_api_url):
        """
        :param str user: jpmdq api credentials
        :param str password: jpmdq api credentials
        :param str cert_fn: path to certificate file
        :param str key_fn: path to key file
        :param str api_url: jpm dataquery url
        """
        self.basic_header = _make_basic_header(user, password)
        self.cert_fn = cert_fn
        self.key_fn = key_fn
        self.api_url = api_url

    def update_credentials(self, user, password):
        self.basic_header = _make_basic_header(user, password)

    @hide_warning(InsecureRequestWarning)
    def fetch_raw(self, expr, sd, ed, **kwargs):
        """
        :description: retrieve data from jpm dataquery. Optional url parameters can be passed as kwargs. \
        IMPORTANT LIMITATIONS! You can only perform 2 queries per second and only 10 expressions per query!
        :param str|list[str] expr: dataquery expression(s). You can only request 10 tickers at a time!
        :param str|dt.date sd: YYYYMMDD or date that can be formatted with strftime
        :param str|dt.date ed: YYYYMMDD or date that can be formatted with strftime
        :param kwargs: extra url arguments such as cal, freq, conv, na
        :return: raw response string
        """
        # Convert multiple expressions into single string
        if not isinstance(expr, str):
            expr = '&expr='.join([quote_plus(e) for e in expr])

        if not isinstance(sd, str):
            sd = sd.strftime('%Y%m%d')

        if not isinstance(ed, str):
            ed = ed.strftime('%Y%m%d')

        url_params = {'expr': expr, 'sd': sd, 'ed': ed}
        url_params.update(kwargs)
        url = self.api_url + "?" + "&".join([k + '=' + url_params[k] for k in url_params])
        res = requests.get(url=url, cert=(self.cert_fn, self.key_fn), headers=self.basic_header, verify=False).content
        return res

    def fetch_raw_iter_chunks(self, expr, sd, ed, **kwargs):
        """
        :description: retrieve data from jpm dataquery. Optional url parameters can be passed as kwargs. \
        For list of expressions longer than 10, iterate over chunks of 10 with pauses in between.
        :param str|list[str] expr: dataquery expression(s). You can only request 10 tickers at a time!
        :param str|list[str] expr: dataquery expression(s). You can only request 10 tickers at a time!
        :param str|dt.date sd: YYYYMMDD or date that can be formatted with strftime
        :param str|dt.date ed: YYYYMMDD or date that can be formatted with strftime
        :param kwargs: extra url arguments such as cal, freq, conv, na
        :return: raw response string
        """
        sleep_seconds = 0.501  # only 2 queries allowed per second. 0.5s is too precise for gap time.
        chunk_size = 10
        expr_len = len(expr)

        if isinstance(expr, str):
            yield self.fetch_raw(expr, sd, ed, **kwargs)
            return

        for i, start_ix in enumerate(range(0, expr_len, chunk_size)):
            end_ix = start_ix + chunk_size
            print(f'Querying chunk {i + 1}')
            yield self.fetch_raw(expr[start_ix:end_ix], sd, ed, **kwargs)

            if end_ix < expr_len:
                print('Sleeping')
                time.sleep(sleep_seconds)

        print('Done')

    def fetch_df(self, expr, sd, ed, **kwargs):
        """
        :description: retrieve data from jpm dataquery. Optional url parameters can be passed as kwargs. \
        For list of expressions longer than 10, iterate over chunks of 10 with pauses in between.
        :param str|list[str] expr: dataquery expression(s). You can only request 10 tickers at a time!
        :param str|list[str] expr: dataquery expression(s). You can only request 10 tickers at a time!
        :param str|dt.date sd: YYYYMMDD or date that can be formatted with strftime
        :param str|dt.date ed: YYYYMMDD or date that can be formatted with strftime
        :param kwargs: extra url arguments such as cal, freq, conv, na
        :return: dataframe
        """
        ser_dict = {}
        for res in self.fetch_raw_iter_chunks(expr, sd, ed, **kwargs):
            # Interpret JSON as dict. Note that xml is returned in some error instances
            ser_dict.update(_jsonify_and_extract_series_dict(res))

        # Parse dataframe
        df = pd.DataFrame(ser_dict)

        # convert date index
        df.index = list(map(parse_date_num, df.index))

        # Reorder columns
        if not isinstance(expr, str):
            df = df.reindex(columns=expr)

        df.sort_index(inplace=True)

        # Replace maxint (2^128) values with NaN. JPM uses this for empty values!
        df.replace(340282346638528859811704183484516925440, nan, inplace=True)

        return df

 