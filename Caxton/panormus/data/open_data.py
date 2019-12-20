'''
Created on 2019年3月23日

@author: user
'''
import json
import os

import requests
import datetime
import pandas as pd

from panormus.data import open_data_config as dodc
from panormus.utils import chrono


class OpenDataClient:
    __search_result_limit = 10 ** 6

    def __init__(self, root, user, domain, key):
        self.user = user
        self.domain = domain
        self.apikey = key
        self.domainDict = {
            'Domain': self.domain,
            'UserName': self.user,
            'ApiKey': self.apikey
        }

        if root:
            self.service_root = root
        else:
            self.service_root = \
                'http://service01.marketdata.a.prod.us-east-1.aws.caxton.com:8604/MarketDataService/opendata/{0}'

        self.base_headers = {'Content-type': 'application/json'}

    def add_key(self, source, name, propertyTypes, create_props=True):
        data = {'name': name, 'dataSource': source, 'propertyTypes': propertyTypes,
                'createPropertyTypes': create_props}
        result = self.submit_request('addkey', data)['AddKeyResult']
        return result

    def add_properties(self, type, value):
        data = {'type': type, 'names': value}
        result = self.submit_request('addproperties', data)
        return result

    def set_value(self, data_source, key_type, key, value, timestamp, optype):
        data = {'dataSource': data_source,
                'keyType': key_type,
                'key': self.to_dictionary(key),
                'value': value,
                'timeStamp': timestamp,
                'opType': optype}
        result = self.submit_request('setvalue', data)
        return result

    def set_value_range(self, data_source, key_type, key, values, times, optype):
        data = {'dataSource': data_source,
                'keyType': key_type,
                'key': self.to_dictionary(key),
                'values': values,
                'timeStamps': times,
                'opType': optype}

        result = self.submit_request('setvaluerange', data)
        return result

    def get_keys(self, domain, data_source):
        url = self.service_root.format('getdatakeys/{}/{}'.format(domain, data_source))
        response = requests.get(url)
        if response.status_code != 200:
            raise EnvironmentError(response.text())
        return response.json()['GetKeysResult']

    def get_data(self, data_source, key_type, keys, frm, to):
        data = {
            'dataSource': data_source,
            'keyType': key_type,
            'keys': [self.to_dictionary(key) for key in keys],
            'from': frm,
            'to': to
        }

        result = self.submit_request('getdata', data)
        json_res = result['GetDataResult']
        for i, set in enumerate(json_res):
            set['TimeStamp'] = [self.json_date_as_datetime(t) for t in set['TimeStamp']]
        return json_res

    def search_observables(self, item, attribute, cut, source, limit):
        if limit > OpenDataClient.__search_result_limit:
            raise Exception('specified search result limit {} is greater than the threshold {}'
                            .format(limit, OpenDataClient.__search_result_limit))
        data = {
            'items': item,
            'attributes': attribute,
            'cuts': cut,
            'sources': source,
            'maxLimit': limit,
        }
        result = self.submit_request('search_observables', data)
        json_res = result['SearchObservablesResult']

        result = {}
        result['observable_list'] = \
            [(d['Item'], d['Attribute'], d['Cut'], d['Source'])
             for d in json_res['Observables']]

        result['is_observable_list_truncated'] = json_res['IsTruncated']
        result['number_matching_observables'] = json_res['Found']

        return result

    def submit_request(self, method, data):
        url = self.service_root.format(method)
        data['domain'] = self.domainDict
        response = requests.post(url, json.dumps(data), headers=self.base_headers)

        if response.status_code != 200:
            raise EnvironmentError(response)
        return response.json()

    @staticmethod
    def to_dictionary(key_dictionary):
        dict = []
        for k in key_dictionary:
            dict.append({'Key': k, 'Value': key_dictionary[k]})
        return dict

    @staticmethod
    def json_date_as_datetime(jd):
        """
        json datetime conversion methods resolving the time from .net epoch based serialization

        :param str jd: jason date
        :return: datetime
        """
        sign = jd[-7]
        if sign not in '-+' or len(jd) == 13:
            millisecs = int(jd[6:-2])
        else:
            millisecs = int(jd[6:-7])
            hh = int(jd[-7:-4])
            mm = int(jd[-4:-2])
            if sign == '-': mm = -mm
            millisecs += (hh * 60 + mm) * 60000
        return datetime.datetime(1970, 1, 1) \
               + datetime.timedelta(microseconds=millisecs * 1000)

    @staticmethod
    def datetime_as_iso(dt):
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")  # truncates

    @staticmethod
    def datetime_as_iso_ms(dt):  # with millisecs as fraction
        return dt.strftime("%Y-%m-%dT%H:%M:%S.%%03dZ") \
               % (dt.microsecond // 1000)  # truncate


class ObservableClient:
    """
    Observable client is a wrapper on OpenDataClient which allows access to the
    OpenData service of Caxton's MarketDataService
    """
    KEY_TYPE = 'observable'
    DEFAULT_SOURCE = 'caxton'
    DATE_FORMAT = '%Y%m%d%H%M%S'
    KEY_SEPARATOR = '|'

    def __init__(self, user, domain, api_key=None):
        """
        :description: Initiates the observable client
        :param user: username
        :param domain: target domain
        :param api_key: api key that will be used to verify access of the user to the specified domain
        """
        self.client = OpenDataClient(None, user, domain, api_key)
        self.user = user
        self.domain = domain
        self.api_key = api_key

    def get_keys(self):
        keys = self.client.get_keys(self.domain, self.DEFAULT_SOURCE)
        return keys

    def save_data(self, value_set, optype='IU'):
        """
        :param optype:
        :param value_set:
        :return:
        """
        for s in value_set:
            (item, source, attribute, cut) = s.key
            key = {'item': item, 'attribute': attribute, 'cut': cut}
            self.client.set_value_range(source, self.KEY_TYPE, key, s.values, s.times, optype)

    def save_frame(self, frame, frame_format='S', optype='IU'):
        """
        :param optype:
        :param frame:
        :param frame_format:
        :return:
        """
        value_set = []
        if frame_format == 'S':
            grouped = frame.groupby(['item', 'source', 'attribute', 'cut'])
            for g in grouped:
                valid_data = g[1][pd.notnull(g[1]['value'])]
                value_set.append({
                    'key': g[0],
                    'values': valid_data['value'],
                    'times': valid_data['timestamp']
                })

        elif frame_format == 'T' or frame_format == 'I':
            if frame_format == 'I':
                frame = frame.transpose()

            for c in frame.columns:
                key = self.__parse_keys([c], 'S')
                valid_data = frame[pd.notnull(frame[c])]
                value_set.append({
                    'key': key,
                    'values': valid_data[c],
                    'times': valid_data.index
                })

        return self.save_data(value_set, optype)

    def fetch_data(self, keys, start, end, key_format='T', result_format='S'):
        """
        :description: Calls market data service to fetch data for given keys in the given range \
        and returns a pandas dataframe in the specified format
        :param keys: list of keys
        :param start: start date
        :param end: end date
        :param key_format: format of the keys in the given list. Available formats {'S': string, 'T': tuple} \
        'T', tuple format: (item, source, attribute, cut), \
        'S', string format: "[item]__[attribute]__[cut]__[source]"
        :param result_format: format of returned data frame. \
        Available formats {'S': standard, 'T': tuple columns, 'I': inverted}
        :return: pandas dataframe in the specified format
        """
        parsed_keys = self.__parse_keys(keys, key_format)
        if isinstance(start, str):
            start = pd.to_datetime(start)
        if isinstance(end, str):
            end = pd.to_datetime(end)

        frm = to = None
        if start:
            frm = start.strftime(self.DATE_FORMAT)
        if end:
            to = end.strftime(self.DATE_FORMAT)

        raw_data = self.client.get_data(self.DEFAULT_SOURCE, self.KEY_TYPE, parsed_keys, frm, to)
        return self.__convert_raw_to_frame(raw_data, result_format)

    def search_observables(self, item, attribute, cut, source, limit):
        """
        :description: list of observable tuples
        """
        if not (item or attribute or cut or source):
            raise Exception('at least one parameter must be specified')

        if isinstance(item, str):
            item = [item]
        if isinstance(attribute, str):
            attribute = [attribute]
        if isinstance(cut, str):
            cut = [cut]
        if isinstance(source, str):
            source = [source]

        res = self.client.search_observables(item, attribute, cut, source, limit)
        return res

    def __transmit_call(
            self, keys, start_date=None, end_date=None,
            observable_style='mul', return_records=False, key_format='T'
    ):
        result_format = 'S'
        if observable_style == 'str':
            result_format = 'T'
        if not return_records:
            result_format = result_format + 'F'
        return self.fetch_data(keys, start_date, end_date, key_format, result_format)

    def df_for_observable_tuples(
            self, obs_tup_list, start_date=None, end_date=None, observable_style='mul', return_records=False
    ):
        return self.__transmit_call(obs_tup_list, start_date, end_date, observable_style, return_records, 'T')

    def df_for_observable_strings(
            self, obs_str_list, start_date, end_date, observable_style='mul', return_records=False
    ):
        return self.__transmit_call(obs_str_list, start_date, end_date, observable_style, return_records, 'S')

    @staticmethod
    def __parse_keys(keys, key_format='T'):
        """
        :description: Parse the given shorthand key list to a list of key dictionaries grouped by data source \
        This is the format MarketDataService OpenData rest endpoint is expecting the keys
        :param keys:
        :param key_format: format of the keys in the given set. Pass 'T' for tuple and 'S' for concatenated string \
        tuple format is (item, source, attribute, cut), string format is "[item]__[source]__[attribute]__[cut]"
        :return: list of key dictionaries grouped by data source
        """
        keyList = []
        key_format = key_format.upper()
        for k in keys:
            if key_format == 'T':
                (item, attribute, cut, source) = k
            elif key_format == 'S':
                (item, attribute, cut, source) = tuple(filter(None, k.split(ObservableClient.KEY_SEPARATOR)))
            else:
                raise ValueError('Parameter invalid: key_format')

            key = {
                'item': item,
                'attribute': attribute,
                'cut': cut,
                'source': source
            }
            keyList.append(key)

        return keyList

    @staticmethod
    def __convert_raw_to_frame(raw_data, result_format):
        """
        :description: Converts the raw_data into a pandas dataframe in the given format
        :param raw_data: input data in list of dictionaries format
        :param result_format: a format specifier for the resulting frame
        :return: pandas dataframe in the desired format
        """
        frames = []
        for r in raw_data:
            (item, attribute, cut, source) = ObservableClient.__get_key_tuple(r['Key'])
            times = r['TimeStamp']
            values = r['Value']
            cnt = len(r['TimeStamp'])
            key_str = ObservableClient.KEY_SEPARATOR.join([item, attribute, cut, source])
            if result_format in ['S', 'SF', 'TF']:
                df = pd.DataFrame({
                    'item': [item] * cnt,
                    'attribute': [attribute] * cnt,
                    'cut': [cut] * cnt,
                   'source': [source] * cnt,
                    'timestamp': pd.to_datetime(times, utc=True),
                    'value': values
                })
            elif result_format in ['T']:
                df = pd.DataFrame({
                    'observable': key_str,
                    'timestamp': pd.to_datetime(times, utc=True),
                    'value': values
                })
            else:
                raise ValueError('Parameter invalid: result_format')

            frames.append(df)
        frames = [f for f in frames if not f.empty]
        if len(frames) > 0:
            df = pd.concat(frames, axis=0, join='outer', join_axes=None, ignore_index=True)
        else:
            return pd.DataFrame()

        if result_format in ['SF', 'TF']:
            df = df.set_index(['item', 'attribute', 'cut', 'source', 'timestamp']) \
                     .loc[:, 'value'] \
                .unstack(['item',
                          'attribute',
                          'cut',
                          'source']) \
                .sort_index(axis=0) \
                .sort_index(axis=1)

        if result_format == 'TF':
            df.columns = ['|'.join(c) for c in df.columns]
            df.columns.names = ['observable']

        return df

    @staticmethod
    def __get_key_tuple(key_parts):
        (item, attribute, cut, source) = [None] * 4
        for p in key_parts:
            if p['Key'] == 'item':
                item = p['Value']
            elif p['Key'] == 'attribute':
                attribute = p['Value']
            elif p['Key'] == 'cut':
                cut = p['Value']
            elif p['Key'] == 'source':
                source = p['Value']
        return item, attribute, cut, source


__client_instance = ObservableClient('ebiri', 'rates', None)


def df_for_observable_tuples(
        obs_tup_list,
        start_date=None, end_date=None,
        observable_style='mul', return_records=False
):
    """
    :description: fetch data from open data
    :param list obs_tup_list: observables as tuples
    :param start_date: (str or datetime) query start date.if str, it needs to be in YYYYMMDD format
    :param end_date: (str or datetime) query end date. if str, it needs to be in YYYYMMDD format
    :param observable_style: strings 'mul' or 'str', 'str' for observable as a '|' separated string, 'mul' for \
    observable as a tuple
    :param bool return_records: True for observables as rows, False for observables as column headers
    :return: pandas DataFrame
    """
    return __client_instance.df_for_observable_tuples(
        obs_tup_list,
        start_date,
        end_date,
        observable_style,
        return_records)


def df_for_observable_strings(
        obs_str_list,
        start_date=None, end_date=None,
        observable_style='mul',
        return_records=False
):
    """
    :description: fetch data from open data via MarketDataService
    :param list obs_str_list: observables as '|' separated strings
    :param start_date: (str or datetime) query start date.if str, it needs to be in YYYYMMDD format
    :param end_date: (str or datetime) query end date. if str, it needs to be in YYYYMMDD format
    :param observable_style: strings 'mul' or 'str', 'str' for observable as a '|' separated string, 'mul' for \
    observable as a tuple
    :param bool return_records: True for observables as rows, False for observables as column headers
    :return: pandas DataFrame
    """
    return __client_instance.df_for_observable_strings(
        obs_str_list,
        start_date,
        end_date,
        observable_style,
        return_records)


def search_observables(item=None, attribute=None, cut=None, source=None, limit=10000):
    """
    :description: Searches observables. Parameters are either strings or string arrays. \
    in case of an array input, all members will be matched in the same value, \
    e.g. if items = ['gbp','10y'], both 'gbp' and '10y' must be present in the \
    item of the observable, same rule applies to attribute, cut and source \
    maximum limit allowed is 1,000,000.
    :param str|None item: item search string
    :param str|None attribute: attribute search string
    :param str|None cut: cut search string
    :param str|None source: source search string
    :param int limit: max number of search results to be returned
    :return: a dictionary
    """
    return __client_instance.search_observables(
        item,
        attribute,
        cut,
        source,
        limit
    )


def pull_bond_attributes(types, isins, start_date, end_date, attributes_to_pull=None):
    if attributes_to_pull is None:
        attributes_to_pull = dodc.OD_BOND_DEFAULT_ATTRIBUTES

    ticker_list = []
    mapping_list = []
    for t, i in zip(types, isins):
        for a in attributes_to_pull:
            ticker = dodc.build_od_bond_ticker(t, i, a)
            ticker_list.append(ticker)
            mapping_list.append([ticker, t, i])

    mapping_df = pd.DataFrame(mapping_list, columns=['ticker', 'type', 'isin']).set_index('ticker')

    od_df = df_for_observable_strings(ticker_list, start_date, end_date, return_records=True)
    od_df['ticker'] = od_df['item'] + '|' + od_df['attribute'] + '|' + od_df['cut'] + '|' + od_df['source']

    drop_cols = ['ticker', 'item', 'source', 'cut']

    return mapping_df.join(od_df.set_index('ticker')).reset_index().drop(drop_cols, axis=1)


def write_df_to_od(
        df, file_tag='od_insert', write_index=False,
        od_dir='//mozart/Caxton-Shared/QAG/OpenData/Rates/'
):
    '''
    :description: write a dataframe to open data. Columns must be ordered properly!
    :param df: A dataframe with columns ordered as [Item, Attribute, Cut, Source, Timestamp, Value]
    :param str file_tag: A name to prepend to the csv file written to open data. File name will be timestamped.
    :param bool write_index: set True if "Item" values are in the index. Otherwise False.
    :param str od_dir: directory of open data file drops
    :return: str full path of file which was dropped.
    '''
    file_name = file_tag + '_' + chrono.now().strftime('%Y%m%d.%H.%M.%S.%f') + '.csv'
    full_file_path = os.path.join(od_dir, file_name)
    date_col_num = 3 if write_index else 4
    df.iloc[:, date_col_num] = df.iloc[:, date_col_num].apply(datetime_to_od_str)
    df.to_csv(full_file_path, index=write_index)

    return full_file_path


def datetime_to_od_str(date):
    '''
    :description: convert datetimes or timestamps to open data formatted strings. \
     If a string is given, it is returned as-is.
    :param date: dt.datetime, dt.date, pd.Timestamp, or np.datetime64
    :return: formatted str
    '''
    # Do not process strings
    if isinstance(date, str):
        return date

    # Try strftime method. If no method, assuming numpy.datetime64 or similar
    try:
        return date.strftime('%Y%m%d %H:%M:%S')
    except AttributeError:
        return pd.Timestamp(date).strftime('%Y%m%d %H:%M:%S')

 