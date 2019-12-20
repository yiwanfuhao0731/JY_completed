import pandas as pd
import numpy as np
import datetime as dt
import os.path
import hashlib
import os
import shutil
import json
import pathlib

# utils_pvk imports
import utils_pvk.lib_date_fns as date_fns

PATH_LIVECACHE = "C:\\cache\\livecache\\"
PATH_FN_CACHE = "C:\\cache\\fn_cache\\"


def h5store(filename, df, **kwargs):
    store = pd.HDFStore(filename)
    store.put('mydata', df)
    store.get_storer('mydata').attrs.metadata = kwargs
    store.close()


def h5load(filename):
    store = pd.HDFStore(filename)
    data = store['mydata']
    metadata = store.get_storer('mydata').attrs.metadata
    store.close()
    return data, metadata


def cached_get(get_data_fn, full_path, start_date, end_date=dt.datetime.today(),
               exclude_data_asof_today=True, raise_on_missing_symbol=False, get_new_data=True, write=True,
               read_last_modified=True, force_full_refresh=False, apply_cutoff=True, **_):
    """
    reads output from load_fn, but caches to disk such that data is only loaded once

    load_fn should take only 2 parameters: start_date and end_date
    """

    if end_date is None:
        end_date = dt.datetime.today()

    if start_date is None:
        start_date = dt.datetime(1900, 1, 1)

    if isinstance(end_date, dt.date):
        end_date = dt.datetime(end_date.year, end_date.month, end_date.day, 23, 59, 59)

    # step 1 - load existing data and compare
    if exclude_data_asof_today and end_date >= pd.to_datetime(dt.date.today() - pd.tseries.offsets.BDay(1)):
        yesterday = dt.date.today() - pd.tseries.offsets.BDay(1)
        end_date = dt.datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)

    path, file = os.path.split(full_path)
    filename, file_extension = os.path.splitext(file)
    if file.lower() not in os.listdir(path) and os.path.isfile(full_path):
        os.rename(full_path, path + "\\" + filename + "_renamed_not_lcase" + file_extension)

    if force_full_refresh and os.path.isfile(full_path):
        os.remove(full_path)

    if os.path.isfile(full_path):
        current = pd.read_pickle(full_path)
        file_exists = True

        if 'no_data' in current.columns:
            current.drop('no_data', axis=1, inplace=True)

        current.columns = [s.lower() for s in current.columns]

        if pd.read_pickle(full_path).empty or isinstance(current.index[0], dt.date):
            is_daily = True
        else:
            hours = np.all(np.array([t.hour for t in current.index[-1000:]]) == 0)
            mins = np.all(np.array([t.minute for t in current.index[-1000:]]) == 0)
            secs = np.all(np.array([t.second for t in current.index[-1000:]]) == 0)
            is_daily = hours and mins and secs

        if read_last_modified and exclude_data_asof_today:
            start_date_newdata = dt.datetime.fromtimestamp(os.path.getmtime(full_path))
        else:
            if pd.read_pickle(full_path).empty:
                start_date_newdata = dt.date(1900, 1, 1)
            elif type(current.index[-1]) in (dt.date, ) or is_daily:
                start_date_newdata = current.index[-1] + pd.tseries.offsets.BDay(1)
            elif type(current.index[-1]) in (dt.datetime, pd._libs.tslibs.timestamps.Timestamp):
                start_date_newdata = current.index[-1] + dt.timedelta(seconds=60)
            else:
                raise ValueError("Index var type: {} not supported at this time".format(type(current.index[-1])))

        if pd.read_pickle(full_path).empty:
            pass
    else:
        file_exists = False
        start_date_newdata = dt.date(1900, 1, 1)

    # step 2 - get new data, if necessary
    searched_for_new_data = False
    if pd.to_datetime(start_date_newdata) <= end_date and get_new_data:
        try:
            newdata = get_data_fn(start_date_newdata, end_date)

            newdata.columns = [s.lower() for s in newdata.columns]

            idx_datetime = np.array([date_fns.to_datetime(i_d) for i_d in newdata.index])

            newdata = newdata.iloc[idx_datetime >= date_fns.to_datetime(start_date_newdata)]
            searched_for_new_data = True
            if len(newdata.index) == 0:
                updated = False
            else:
                updated = True
        except ValueError as e:
            if str(e) == "Length mismatch: Expected axis has 0 elements, new values have 4 elements":
                updated = False
            else:
                raise
    else:
        updated = False

    if file_exists and updated and not current.empty:
        cur_s = current.iloc[:, 0]
        new_s = newdata.loc[cur_s.index[-1]:, :].iloc[:, 0].drop(cur_s.index[-1], errors='ignore')
        output = pd.concat([cur_s, new_s], axis=0).to_frame()

    elif updated and (not file_exists or current.empty):
        output = newdata
    elif file_exists and not updated:
        output = current
    else:
        if raise_on_missing_symbol:
            raise ValueError("Data non-existent in database or bbg".format(ticker, field,
                         date_fns.to_str(start_date), date_fns.to_str(end_date)))
        else:
            output = pd.DataFrame(columns=['no_data'])

    output = output[~output.index.duplicated(keep='first')]
    output.sort_index(inplace=True)

    # store to HDF, never store today's mark to file as (for bbg) it can be a live mark, not (necessarily) a close mark
    if write:
        if updated:
            if isinstance(output.index[0], pd.datetime):
                if apply_cutoff:
                    cutoff_tmp = (pd.datetime.today() - pd.tseries.offsets.BDay(1))
                    last_date_store = dt.datetime(cutoff_tmp.year, cutoff_tmp.month, cutoff_tmp.day, 23, 59, 59)
                    storedf = output.loc[:last_date_store, :]#
                else:
                    storedf = output.copy()
                output = output.loc[start_date:]
            elif isinstance(output.index[0], dt.date):
                first_date_output = dt.date(start_date.year, start_date.month, start_date.day)
                if apply_cutoff:
                    last_date_store = (dt.date.today() - pd.tseries.offsets.BDay(1))
                    cutoff = dt.date(last_date_store.year, last_date_store.month, last_date_store.day)
                    storedf = output.loc[:cutoff, :]
                else:
                    storedf = output.copy()
                output = output.loc[first_date_output:]
            else:
                raise ValueError("Timeseries 'output' index type: '{}' not supported at this time.".
                                 format(type(output.index[0])))
            storedf.to_pickle(full_path.lower())
        else:
            # update last written date:
            if searched_for_new_data:
                if file_exists:
                    pd.read_pickle(full_path.lower()).to_pickle(full_path.lower())
                else:
                    pd.DataFrame().to_pickle(full_path.lower())

            if output.empty and len(output.columns) == 0:
                return pd.DataFrame(columns=['no_data'])
            if not output.empty:
                if isinstance(output.index[0], pd.datetime):
                    start_date = date_fns.to_datetime(start_date)
                    end_date = date_fns.to_datetime(end_date)
                elif isinstance(output.index[0], dt.date):
                    start_date = date_fns.to_date(start_date)
                    end_date = date_fns.to_date(end_date)
                else:
                    raise ValueError("Timeseries 'output' index type: '{}' not supported at this time.".
                                     format(type(output.index[0])))
                output = output.loc[start_date:end_date]
    return output


def clean_temp_folders(folders=[PATH_LIVECACHE, ]):
    print("cleaning temporary folders")

    for folder in folders:
        for the_file in os.listdir(folder):
            file_path = os.path.join(folder, the_file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)


def naive_livecache_get(get_data_fn, timeout_secs=10*60, force_reload=False, unique_string=""):
    """naive live cache of a function
    function output needs to be h5 store-able
    it will only load the function if previous run of function is older than timeout"""

    full_path = PATH_LIVECACHE + "\\livecache_hash_" + str(hash(get_data_fn.__code__)) + unique_string

    if not force_reload and os.path.isfile(full_path):
        last_updated = dt.datetime.fromtimestamp(os.path.getmtime(full_path))
        age = (dt.datetime.today() - last_updated).seconds

        if age < timeout_secs:
            # load existing data
            data, _ = h5load(full_path)
            return data

    # otherwise, load data
    data = get_data_fn()
    h5store(full_path, data)
    return data


def param_livecache_get(get_data_fn, params, timeout_secs=10*60, force_reload=False):
    """naive live cache of a function, it will only load the function"""

    paramsx = params.copy()
    if isinstance(paramsx, list):
        for i in range(len(paramsx)):
            elem = paramsx[i]
            if date_fns.is_date(elem):
                paramsx[i] = date_fns.to_str(date_fns.to_date(elem))

    strhash = hashlib.sha1(json.dumps(paramsx, sort_keys=True).encode('utf-8')).hexdigest()
    full_path = PATH_LIVECACHE + "\\livecache_hash_" + strhash

    if not force_reload and os.path.isfile(full_path):
        last_updated = dt.datetime.fromtimestamp(os.path.getmtime(full_path))
        age = (dt.datetime.today() - last_updated).seconds

        if age < timeout_secs:
            # load existing data
            data, _ = h5load(full_path)
            return data

    # otherwise, load data
    data = get_data_fn()
    h5store(full_path, data)
    return data


def cached_fn_get_naive(fn, fn_args=[], fn_kwargs={}, timeout_secs=24*3600, force_reload=False):
    """
    Cached get of a function.
    :description: Cache function output. Function should always return the same output for the set of parameters.
    Function output must be pickle serializable.

    :param fn: function to be loaded
    :param fn_args: function positional arguments
    :param fn_kwargs: function named arguments
    :param timeout_secs: cache timeout in seconds (will reload if this time is exceeded)
    :param force_reload: force refresh of the cached output

    :return: function output

    """

    # make sure start and end date are date type (i.e. do not contain seconds etc.)
    argsx = fn_args.copy()
    if isinstance(argsx, list):
        for i in range(len(argsx)):
            elem = argsx[i]
            if date_fns.is_date(elem):
                argsx[i] = date_fns.to_str(date_fns.to_date(elem))

    kwargsx = fn_kwargs.copy()
    if isinstance(kwargsx, dict):
        for k in kwargsx.keys():
            elem = kwargsx[k]
            if date_fns.is_date(elem):
                kwargsx[k] = date_fns.to_str(date_fns.to_date(elem))

    params = {'args': argsx, 'kwargs': kwargsx}

    strhash = hashlib.sha1(json.dumps(params, sort_keys=True).encode('utf-8')).hexdigest()
    full_path = PATH_FN_CACHE + "\\livecache_hash_" + strhash

    pathlib.Path(PATH_FN_CACHE).mkdir(parents=True, exist_ok=True)

    # If cache file exists, load and return it
    if not force_reload and os.path.isfile(full_path):
        last_updated = dt.datetime.fromtimestamp(os.path.getmtime(full_path))
        age = (dt.datetime.today() - last_updated).seconds

        if age < timeout_secs:
            # load existing data
            data, _ = h5load(full_path)
            return data

    # Otherwise, run the function, store the output, and return it
    data = fn(*fn_args, **fn_kwargs)
    h5store(full_path, data)
    return data


if __name__ == "__main__":
    import datetime as dt
    args = ["SPX Index", "PX_LAST", dt.date(2018, 1, 1), dt.date.today()]

    import data_bloomberg

    from panormus.data.bbg import BbgClient

    ticker = "SPX Index"
    field = "PX_LAST"
    start_date = dt.datetime(2018, 1, 1)
    end_date = dt.datetime.today()
    x = BbgClient().get_historical_data([ticker], [field], start_date, end_date)

    x.pivot(index='date', columns='ticker', values='PX_LAST')
    data_bloomberg.__bdh(ticker, field, start_date, end_date)


    def load_bbg(ticker, field, start_date, end_date):
        return BbgClient().get_historical_data([ticker], [field], start_date, end_date)


    start = dt.datetime.today()
    xz = load_bbg(ticker, field, dt.date(2018, 1, 2), end_date)
    print(dt.datetime.today() - start)
    cached_fn_get_naive(data_bloomberg.__bdh, [ticker, field, start_date, end_date])