import pandas as pd
import numpy as np
import panormus.data.bo.econ as econ
from datetime import datetime, timedelta
import os
import pickle
from panormus.data import haver
import panormus.data.citi_velocity as cv
from datetime import datetime, timedelta, date
from panormus.utils.cache import cache_response, clear_cache

conn = econ.get_instance_using_credentials()


@cache_response('_conn_get_clean', 'disk_8h')
def _conn_get_clean(ticker):
    df_raw = conn.get(ticker)
    df_list = []
    if len(df_raw.columns) < 0.01:
        df_list.append(df_raw)
    for col in df_raw.columns:
        df = df_raw[[col]]
        if len(df.index) > 0.01:
            test_index = [d.date() for d in df.loc[:'2019-8-19', :].index[-1000:]]
            if len(test_index) != len(set(test_index)):
                new_index = [d.replace(hour=0, minute=0, second=0) for d in df.index]
                df.index = new_index
                df.dropna(inplace=True)
                df = df[~df.index.duplicated(keep='last')]
        df_list.append(df)

    return pd.concat(df_list, axis=1)


@cache_response('_haver_get', 'disk_8h')
def _haver_get(ticker):
    return haver.get_data(ticker)


@cache_response('_citi_get', 'disk_8h')
def _citi_get(ticker):
    citi_token = {'id': '6657c27e-569e-4569-828d-b2e76a1387f7',
                  'secret': 'G4mM5jH4dA5oM7uK5fL3pC3yR7iG5vH5wM7sY0tT4gV5kP0eV6'}
    citi_client = cv.CitiClient(id=citi_token['id'], secret=citi_token['secret'])
    start_date = date(1980, 1, 1)
    end_date = date(2050, 1, 1)
    frequency = 'DAILY'
    price_points = 'C'  # close price
    new_col_name = ticker
    tags = [ticker]

    df = citi_client.get_hist_data(tags=tags,
                                   frequency=frequency,
                                   start_date=start_date,
                                   end_date=end_date
                                   )
    return df.dropna()


def _clear_cache():
    clear_cache('_conn_get_clean', 'disk_8h')
    clear_cache('_haver_get', 'disk_8h')


class new_downloader:
    def __init__(self):
        self.WKDIR = os.path.dirname(os.path.realpath(__file__))
        self.PROJ_DIR = os.path.join(self.WKDIR, "..")
        self.H5_LOCALDB_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", 'local_db.h5')
        # file to set pickle file
        self.SET_DATA_PICKLE_FILE = os.path.join(self.PROJ_DIR, "offline_data", 'offline_data.pickle')

    def grab_pre_run_result(self, data_dict):
        self.pre_run_result = data_dict

    def get_market_data(self, MASTER_INPUT_DIR, Short_Name, export_backup=None, exec_path=None):
        print('running...', exec_path)
        print('start downloading data...')
        result_dict = {}
        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)
        if not export_backup in [None]:
            self.dump_param_to_txt(import_df, export_backup)
        import_df.replace(np.nan, 'invalid_name', inplace=True)
        iso_list = import_df['ISO'].values.tolist()

        # start download the data
        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

        all_ser_list = []
        all_load_fn_list = []
        all_location_list = []
        all_suffix_list = []

        for i in range(number_of_series_col):
            i = str(int(i) + int(1))
            series_col = ('').join(['series', i])
            load_fn_col = ('').join(['load_fn', i])
            location_col = ('').join(['location', i])
            suffix_col = ('').join(['in_file_suf_fix', i])

            ser_list = import_df[series_col].values.tolist()
            all_ser_list = all_ser_list + ser_list

            load_fn_list = import_df[load_fn_col].values.tolist()
            all_load_fn_list = all_load_fn_list + load_fn_list

            location_list = import_df[location_col].values.tolist()
            all_location_list = all_location_list + location_list

            suffix_list = import_df[suffix_col].values.tolist()
            all_suffix_list = all_suffix_list + suffix_list

        for f in list(set(all_load_fn_list)):
            is_f = [True if z == f else False for z in all_load_fn_list]
            this_ser, this_loc, this_suf = self.mask_list(all_ser_list, is_f), self.mask_list(all_location_list,
                                                                                              is_f), self.mask_list(
                all_suffix_list, is_f)

            if f == 'csv':
                result_dict.update(self._import_csv_bulk(this_ser, this_loc, this_suf))
            if f == 'EconDB':
                result_dict.update(self._import_conn_bulk(this_ser, this_suf))
            if f == 'h5_local_db':
                result_dict.update(self._import_h5_local_db_bulk(this_ser, this_loc, this_suf))
        if f == 'haver':
            result_dict.update(self._import_haver_bulk(this_ser, this_suf))
        if f == 'citivelocity':
            result_dict.update(self._import_citi_bulk(this_ser, this_suf))
        if f == 'pre_run_result':
            result_dict.update(self._use_pre_run_result(this_ser, this_loc, this_suf, self.pre_run_result))

        print('done downloading data...')
        return result_dict

    def set_market_data(self, MASTER_INPUT_DIR, Short_Name, export_backup=None, exec_path=None):
        print('running...', exec_path)
        print('start downloading data...')
        result_dict = {}
        import_df = pd.read_excel(MASTER_INPUT_DIR, sheet_name=Short_Name, index_col=False, header=0)
        if not export_backup in [None]:
            self.dump_param_to_txt(import_df, export_backup)
        import_df.replace(np.nan, 'invalid_name', inplace=True)
        iso_list = import_df['ISO'].values.tolist()

        # start download the data
        number_of_series_col = sum('series' in i for i in import_df.columns.tolist())

        all_ser_list = []
        all_load_fn_list = []
        all_location_list = []
        all_suffix_list = []

        for i in range(number_of_series_col):
            i = str(int(i) + int(1))
            series_col = ('').join(['series', i])
            load_fn_col = ('').join(['load_fn', i])
            location_col = ('').join(['location', i])
            suffix_col = ('').join(['in_file_suf_fix', i])

            ser_list = import_df[series_col].values.tolist()
            all_ser_list = all_ser_list + ser_list

            load_fn_list = import_df[load_fn_col].values.tolist()
            all_load_fn_list = all_load_fn_list + load_fn_list

            location_list = import_df[location_col].values.tolist()
            all_location_list = all_location_list + location_list

            suffix_list = import_df[suffix_col].values.tolist()
            all_suffix_list = all_suffix_list + suffix_list

        for f in list(set(all_load_fn_list)):
            is_f = [True if z == f else False for z in all_load_fn_list]
            this_ser, this_loc, this_suf = self.mask_list(all_ser_list, is_f), self.mask_list(all_location_list,
                                                                                              is_f), self.mask_list(
                all_suffix_list, is_f)

            if f == 'csv':
                result_dict.update(self._import_csv_bulk(this_ser, this_loc, this_suf))
            if f == 'h5_local_db':
                result_dict.update(self._import_h5_local_db_bulk(this_ser, this_loc, this_suf))
            if f == 'pre_run_result':
                result_dict.update(self._use_pre_run_result(this_ser, this_loc, this_suf, self.pre_run_result))
            if f in ['EconDB', 'haver']:
                file = open(self.SET_DATA_PICKLE_FILE, 'rb')
                pickle_data = pickle.load(file)
                result_dict.update(self._use_pickle_to_set(this_ser, this_suf, pickle_data))

        print('done downloading data...')
        return result_dict

    def dump_param_to_txt(self, df, txt_path, type='data_ticker'):
        if type in ['data_ticker']:
            col = []
            for c in df.columns:
                if 'Unnamed' in c:
                    break
                else:
                    col = col + [c]
            df = df.loc[:, col]
            last_valid = df.iloc[:, 0].last_valid_index()
            df = df.loc[df.index <= last_valid, :]
            df.to_csv(txt_path)

    def _import_csv_bulk(self, ser_list, loc_list, suf_list):
        result_dict = {}
        for s, l, suf in zip(ser_list, loc_list, suf_list):
            if 'panel' in s:
                assert l != 'invalid_name', 'sorry, the loc is invalid'
                try:
                    result_dict[suf] = pd.read_csv(l, index_col=0, header=0)
                except:
                    loc = os.path.join(self.PROJ_DIR, l)
                    result_dict[suf] = pd.read_csv(loc, index_col=0, header=0)
                continue
            else:
                assert l != 'invalid_name', 'sorry, the loc is invalid'
                if not os.path.isfile(l):
                    try:
                        loc = os.path.join(self.PROJ_DIR, l)
                        df = pd.read_csv(loc, index_col=0, header=0)
                    except:
                        result_dict[suf] = pd.DataFrame()
                        continue
                else:
                    df = pd.read_csv(l, index_col=0, header=0)
                    df.index = pd.to_datetime(df.index)
                if len(df.index) < 0.001 or (s not in df.columns.tolist()):
                    result_dict[suf] = pd.DataFrame()
                    continue
                df.index = pd.to_datetime(df.index)
                result_dict[suf] = df.loc[:, [s]]
                continue
        return result_dict

    def _import_h5_local_db_bulk(self, ser_list, loc_list, suf_list):
        result_dict = {}
        if os.path.isfile(self.H5_LOCALDB_DIR):
            h5_store = pd.HDFStore(self.H5_LOCALDB_DIR)
            key_list = h5_store.keys()

            for s, l, suf in zip(ser_list, loc_list, suf_list):
                if l in key_list:
                    # check the last update time
                    # if datetime.strptime(h5_store.get_storer(l).attrs.last_update,'%Y-%m-%d %H:%M').date() < datetime.now().date():
                    if 1 == 0:
                        raise ValueError(l + 'has not been updated today!')
                    else:
                        if s in h5_store.get(l).columns:
                            result_dict[suf] = h5_store.get(l).loc[:, [s]]
                        else:
                            raise ValueError(
                                s + ' is not in key : ' + l + ' Actually the columns are : ' + h5_store.get(l).columns)
                else:
                    raise ValueError(l + 'is not in H5 local DB keys!')
        else:
            raise ValueError(self.H5_LOCALDB_DIR + ' is not exist.')
        return result_dict

    def _import_conn_bulk(self, ser_list, suf_list):
        result_dict = {}
        df = _conn_get_clean(ser_list)
        for s, suf in zip(ser_list, suf_list):
            if s == 'invalid_name':
                result_dict[suf] = pd.DataFrame()
                continue
            elif s not in df.columns:
                result_dict[suf] = pd.DataFrame()
                continue
            else:
                result_dict[suf] = df.loc[:, [s]].dropna()
        return result_dict

    def _import_haver_bulk(self, ser_list, suf_list):
        result_dict = {}
        for s, suf in zip(ser_list, suf_list):
            if s == 'invalid_name':
                result_dict[suf] = pd.DataFrame()
                continue
            try:
                df = _haver_get([s])
            except:
                print(' not able to download from haver : ' + s)
                raise ValueError
            try:
                df.index = df.index.to_timestamp()
            except:
                print(df)
                raise ValueError('df.index can not be converted into pd.Timestamp')
            result_dict[suf] = df
            continue
        return result_dict

    def _import_citi_bulk(self, ser_list, suf_list):
        result_dict = {}
        for s, suf in zip(ser_list, suf_list):
            if s == 'invalid_name':
                result_dict[suf] = pd.DataFrame()
                continue
            try:
                df = _citi_get(s)
            except:
                print(' not able to download from citi : ' + s)
                raise ValueError
            result_dict[suf] = df
            continue
        return result_dict

    def _use_pre_run_result(self, ser_list, loc_list, suf_list, pre_run_result):
        result_dict = {}
        key_list = pre_run_result.keys()
        for s, l, suf in zip(ser_list, loc_list, suf_list):
            if l in key_list:
                if s in pre_run_result[l].columns:
                    result_dict[suf] = pre_run_result[l].loc[:, [s]]
                else:
                    raise ValueError(l + ' is not in the pre-run result keys')
            else:
                result_dict[suf] = pd.DataFrame()
        return result_dict

    def _use_pickle_to_set(self, ser_list, suf_list, pickle_data_dict):
        result_dict = {}
        for s, suf in zip(ser_list, suf_list):
            if s == 'invalid_name':
                result_dict[suf] = pd.DataFrame()
                continue
            if s in pickle_data_dict.keys():
                result_dict[suf] = pickle_data_dict[s]
                continue
            else:
                print(' not able to load from pickled file : ' + s)
                raise ValueError
        return result_dict

    @staticmethod
    def mask_list(l, m):
        return [z[0] for z in zip(l, m) if z[1]]

