'''
Created on 2019年3月23日

@author: user
'''
import datetime as dt
import http.client
import json
import os
import traceback as tb
import zipfile

import pandas as pd

from panormus.data.bo.db_engine import get_credentials
from panormus.utils import chrono
from panormus.utils.simple_func_decorators import docstring_parameter

_api_root_url = "api.citivelocity.com"
FREQUENCY_CHOICES = ['MONTHLY', 'WEEKLY', 'DAILY', 'HOURLY', 'MI10', 'MI01']


def chunks(lst, n):
    """
    Yield successive n-sized chunks from list

    :param list lst: list of things to chunk
    :param int n: chunk size
    :rtype: list
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def map_citi_df_to_open_data_format(df, column_map_dict):
    """
    Rename and reshape dataframe for open data export

    :param pd.DataFrame df: citi result
    :param dict column_map_dict: keys are citi tags, values are open data tuples (item, attr, cut, source)
    :rtype: pd.DataFrame
    """
    open_data_df = df.rename(columns=column_map_dict)
    open_data_df.columns = pd.MultiIndex.from_tuples(list(open_data_df.columns))
    open_data_df = open_data_df.stack(level=[0, 1, 2, 3]).reset_index()
    open_data_df.columns = ['timestamp', 'item', 'attribute', 'cut', 'source', 'value']
    export_order = ['item', 'attribute', 'cut', 'source', 'timestamp', 'value']
    return open_data_df[export_order]


def _post_get_json(conn, url, payload, headers):
    if not isinstance(payload, str):
        payload = json.dumps(payload)

    conn.request(method="POST", url=url, body=payload, headers=headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data)


def _get_json_using_token(conn, url, token):
    headers = {
        'accept': "application/json",
        'authorization': f'Bearer {token}'
    }
    conn.request("GET", url=url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    return json.loads(data)


def get_client_using_credentials(
        user,
        api_type='citi_velocity',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='api_creds.yml'
):
    """
    Constructs a CitiClient by pulling credentials from panormus admin location.

    :param str user: caxton user to pull credentials for
    :param str api_type: citi_velocity, jpmdq, etc
    :param str file_dir: directory of credentials file
    :param str file_name: credentials file name
    :return:
    """
    multi_creds = get_credentials(for_key=api_type, file_dir=file_dir, file_name=file_name)
    creds = multi_creds[user]
    return CitiClient(id=creds['client_id'], secret=creds['client_secret'])


class CitiClient(object):
    """
    Citi velocity client. Must provide your client_id and client_secret which you can get from citi velocity website.
    """

    def __init__(
            self, id, secret,
            api_root_url=_api_root_url
    ):
        self.id = id
        self.secret = secret
        self.conn = http.client.HTTPSConnection(api_root_url)
        self.refresh_token()

    def __del__(self):
        try:
            self.conn.close()
        except:
            pass

    def refresh_token(self):
        token_url = "/markets/cv/api/oauth2/token"
        payload_fmt = "grant_type=client_credentials&client_id={id}&client_secret={secret}&scope=/api"
        payload = payload_fmt.format(id=self.id, secret=self.secret)
        headers = {
            'content-type': "application/x-www-form-urlencoded",
            'accept': "application/json"
        }
        token_obj = _post_get_json(self.conn, url=token_url, payload=payload, headers=headers)
        self.token = token_obj['access_token']

    def get_api_status(self):
        url = f'/markets/analytics/chartingbe/rest/external/authed/ibe/apistatus?client_id={self.id}'
        status = _get_json_using_token(self.conn, url, self.token)
        return status

    def search_tags(self, prefix=None, regex=None):
        """
        :description: search tags in citi velocity. Prefix must be provided.
        :param str prefix: tag prefix. Must contain at least the first two fields \
        (“Category” and “Sub Category” in the Data Browser). The trailing ‘.’ is optional.
        :param str|None regex: An regular expression which further restricts the list of tags \
        matching the specified prefix. The regex is matched against the entire tag. \
        Begin the regex with ‘.*’ to avoid repeating the prefix. The regex follows Java conventions.
        :return: list[str] tags matching search criteria
        """
        if not prefix and not regex:
            return ['']

        payload = {'prefix': prefix}
        if regex:
            payload['regex'] = regex

        headers = {
            'content-type': "application/json",
            'accept': "application/json",
            'authorization': f"Bearer {self.token}"
        }
        url = f'/markets/analytics/chartingbe/rest/external/authed/taglisting?client_id={self.id}'
        return _post_get_json(self.conn, url=url, payload=payload, headers=headers)

    def get_metadata(self, tags):
        """
        :description: get static reference data
        :param list[str] tags: tags to retrieve metadata for
        :return: dict of parsed json, with "status" and "body" keys.
        """
        payload = {'tags': tags}
        headers = {
            'content-type': "application/json",
            'accept': "application/json",
            'authorization': f"Bearer {self.token}"
        }
        url = f'/markets/analytics/chartingbe/rest/external/authed/metadata?client_id={self.id}'
        return _post_get_json(self.conn, url=url, payload=payload, headers=headers)

    @docstring_parameter(FREQUENCY_CHOICES)
    def get_hist_data(self, tags, start_date, end_date, frequency='DAILY'):
        """
        :description: fetch historical data
        :param list[str] tags:
        :param datetime.date start_date:
        :param datetime.date end_date:
        :param str frequency: one of {0}
        :return: pandas.DataFrame with dates on index and tags on columns
        """
        response_dict = {}
        for chunk_num, tag_chunk in enumerate(chunks(list(tags), 100)):
            print(f'Querying chunk {chunk_num}')
            raw_res = self.get_hist_data_raw(
                tags=tag_chunk, start_date=start_date, end_date=end_date, frequency=frequency)
            response_body = raw_res['body']
            response_dict.update(response_body)

        def process_item(item):
            if item['type'] == 'SERIES':
                return pd.Series(item['c'], index=item['x'])
            else:
                return None

        df = pd.DataFrame({
            key: process_item(response_dict[key])
            for key in response_dict
        })
        df = df.reindex(columns=tags, copy=False)
        df.index = list(map(chrono.parse_datetime_numeric, df.index))

        return df

    def get_hist_data_raw(self, tags, start_date, end_date, frequency):
        """
        :description: Fetch historical data
        :param list[str] tags:
        :param datetime.date start_date:
        :param datetime.date end_date:
        :param str frequency: one of {0}
        :return: dict of parsed json
        """
        payload = {
            'tags': tags,
            'frequency': frequency,
            "startTime": 0,
            "endTime": 2359,
            'startDate': start_date.strftime('%Y%m%d'),
        }

        if end_date:
            payload['endDate'] = end_date.strftime('%Y%m%d')

        headers = {
            'content-type': "application/json",
            'accept': "application/json",
            'authorization': f"Bearer {self.token}"
        }
        url = f'/markets/analytics/chartingbe/rest/external/authed/data?client_id={self.id}'
        return _post_get_json(self.conn, url=url, payload=payload, headers=headers)

    def bulk_submit_request(self, tags, frequency, start_date, price_points):
        payload = {
            'tags': tags,
            'frequency': frequency,
            'startDate': start_date.strftime('%Y%m%d'),
            'pricePoints': price_points
        }

        headers = {
            'content-type': "application/json",
            'accept': "application/json",
            'authorization': f"Bearer {self.token}"
        }
        url = f'/markets/analytics/chartingbe/rest/external/authed/ibe/submitjob?client_id={self.id}'
        return _post_get_json(self.conn, url=url, payload=payload, headers=headers)

    def check_job_status(self, job_id):
        job_status_loc = '/markets/analytics/chartingbe/rest/external/authed/ibe/jobstatus?client_id={id}&jobId={job}'
        url = job_status_loc.format(id=self.id, job=job_id)
        job_status = _get_json_using_token(self.conn, url, self.token)
        return job_status

    def download_file(self, job_id, file_path):
        headers = {
            'accept': "application/json",
            'authorization': f"Bearer {self.token}"
        }
        job_status_loc = '/markets/analytics/chartingbe/rest/external/authed/ibe/download?client_id={id}&jobId={job}'
        url = job_status_loc.format(id=self.id, job=job_id)
        self.conn.request("GET", url, headers=headers)
        res = self.conn.getresponse()
        data = res.read()
        file_names = []
        try:
            f_name = self.__save_ref_file(data, tag='data_job_{}'.format(job_id), f_type='zip', is_byte=True)
            file_names.append(f_name)
            file_dir = os.path.dirname(file_path)
            if not file_dir:
                print('creating directory {}'.format(file_dir))
                os.makedirs(file_dir)

            if file_path:
                with open(file_path, 'wb') as f:
                    f.write(data)
                file_names.append(file_path)
        except Exception as e:
            print('failed to save files. returning data only')
            tb.print_exc()

        return data, file_names

    def read_bulk_data_file(self, file_path, tags=None):
        f = zipfile.ZipFile(file_path)
        files = [t.filename for t in f.filelist]
        series_dict = {}
        if not tags:
            tags = [fn.replace('.txt', '') for fn in files if fn != 'summary.txt']

        for t in tags:
            fname = '{}.txt'.format(t)
            if fname in files:
                times = []
                values = []
                with f.open(fname) as zf:
                    next(zf)  # skip header line
                    for line in zf:
                        parts = line.decode('utf-8').strip().split('\t')
                        times.append(parts[0])
                        values.append(parts[1])

                series_dict[t] = pd.Series(values, index=times)

        df = pd.DataFrame(series_dict)[tags]
        return df

    def __save_ref_file(
            self, data, tag, f_type=None, is_byte=False, directory='//mozart/Caxton-Shared/QAG/data/citi_bulk'
    ):

        ts = dt.datetime.now().strftime('%Y%m%d_%H%M%S.%f')
        f_name = '{}_{}'.format(tag, ts)
        if f_type:
            f_name = '{}.{}'.format(f_name, f_type)

        f_name = os.path.join(directory, f_name)

        if not os.path.exists(directory):
            print('creating directory {}'.format(directory))
            os.makedirs(directory)

        w = 'wb' if is_byte else 'w'

        with open(f_name, w) as f:
            f.write(data)
        return f_name

