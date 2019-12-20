from itertools import islice

import os

import sys

import threading

import pandas as pd

from panormus.config.settings import TEMP_STORAGE_DIR

from panormus.utils import chrono


def find_filenames(parent_path='', fn_prefix='', fn_suffix='.csv'):
    '''

    :description: Find files under a parent path based on some search criteria

    :param str parent_path: parent directory to find the files

    :param str fn_prefix: file name prefix to limit the search

    :param str fn_suffix: file name suffix to limit the search

    :return: a list containing the name of the files based on the search criteria

    '''

    filenames = os.listdir(parent_path)

    return [filename for filename in filenames if filename.endswith(fn_suffix) and filename.startswith(fn_prefix)]


def concat_files_to_df(

        parent_path='',

        fn_prefix='', fn_suffix='.csv',

        index_col=None, header='infer',

        axis=0, sort=False,

        **kwargs_pd_read_csv

):
    """

    :description: Locate files on disk and concatenate them (tall or wide) to a dataframe

    :param str parent_path: parent directory to search for fn_prefix and fn_suffix

    :param str fn_prefix: file name start

   :param str fn_suffix: file name ending

    :param str index_col: pandas.read_csv() argument

    :param int|str header: pandas.read_csv() argument. int to indicate header line in file or 'infer'

    :param axis: pandas.concat() argument. 0 to stack tall, 1 to stack wide

    :param bool sort: pandas.concat() argument

    :return: dataframe

    """

    file_names = find_filenames(

        parent_path=parent_path,

        fn_prefix=fn_prefix,

        fn_suffix=fn_suffix,

    )

    df = pd.DataFrame()

    for fn in file_names:
        full_path = os.path.join(parent_path, fn)

        df = pd.concat(

            [df, pd.read_csv(full_path, index_col=index_col, header=header, **kwargs_pd_read_csv)],

            axis=axis, sort=sort

        )

    return df


def stack_files(

        parent_path='', fn_prefix='', fn_suffix='.csv', header_lines=0,

        fullpath_output='', skip_line_equal_to='\n'

):
    '''

    :description: Finds files in a parent directory and stacks them together (row stacking) into a new file.

    :param parent_path: Directory containing files. Full path with ending slash.

    :param fn_prefix: Optional prefix to identify target files.

    :param fn_suffix: Optional suffix to identify target files. Default '.csv'.

    :param header_lines: Number of header lines to skip when stacking files 2->end.

    :param fullpath_output: Optional full path to output file.

    :return: None

    '''

    # Default formula for fullpath_output

    out_fn = 'merged_' + fn_prefix if len(fn_prefix) > 0 else 'merged'

    if fullpath_output == '': fullpath_output = ''.join(

        [parent_path, out_fn, fn_suffix]

    )

    # Find matching files to merge

    filenames = find_filenames(parent_path=parent_path, fn_prefix=fn_prefix, fn_suffix=fn_suffix)

    def copy_lines_to_file(fin, fout, skip_line_equal_to):

        for line in fin:

            if line != skip_line_equal_to:
                fout.write(line)

    # Open output file for appending

    with open(fullpath_output, 'a') as fout:

        # Output first file to get copy of headers

        with open(''.join([parent_path, filenames[0]])) as fin:
            copy_lines_to_file(fin, fout, skip_line_equal_to)

        # Output remaining files

        for filename in filenames[1:]:
            with open(''.join([parent_path, filename])) as fin:
                # Output each line after skipping header_lines

                copy_lines_to_file(islice(fin, header_lines, None), fout, skip_line_equal_to)


def ser_write_or_append(ser, file_name, ascending=False):
    """

    Writes series values (ignores index) to new file or appends to existing file \

    assuming existing file contains a single column of data with a header.



    :param pd.Series ser: values to append to file

    :param str file_name: full path to target file

    :param bool ascending: sort values ascending or descending

    :rtype: None

    """

    if os.path.exists(file_name):
        orig_ser = pd.read_csv(file_name).iloc[:, 0]

        ser.name = orig_ser.name

        ser = orig_ser.append(ser, ignore_index=True).drop_duplicates()

    ser.sort_values(ascending=ascending).to_csv(file_name, index=False, header=True)


def df_write_or_append(df, file_name, ascending_index=False, orig_index_transform=None, prioritize_new=False):
    """

    Writes dataframe to new file or appends to existing file. \

    Assuming first column of existing file is the index, and all column names already agree.



    :param pd.DataFrame df: values to append to file

    :param str file_name: full path to target file

    :param bool ascending_index: sort index ascending or descending

    :param None|func orig_index_transform: optional function to transform the original df index\

    before combining data. Commonly needed for data indices.

   :param bool prioritize_new: prioritize new dataframe values over old file values. \

    If true, use combine_first on df instead of orig_df.

    :rtype: None

    """

    df_out = df.copy()

    if os.path.exists(file_name):

        orig_df = pd.read_csv(file_name, index_col=0, header=list(range(df.columns.nlevels)))

        if orig_index_transform is not None:
            orig_df.index = orig_index_transform(orig_df.index)

        df_out = df_out.combine_first(orig_df) if prioritize_new else orig_df.combine_first(df_out)

    df_out.sort_index(ascending=ascending_index).to_csv(file_name, index=True, header=True)


def get_temp_storage_directory(subfolder=None):
    """

    return folder path for temp storage location. create folder if it does not exists



    :param str|None subfolder: the subfolder within temporary storage directory

    """

    directory = os.path.join(TEMP_STORAGE_DIR, subfolder) if subfolder else TEMP_STORAGE_DIR

    if not os.path.exists(directory):
        os.makedirs(directory)

    return directory


def get_temp_filename(name="python_temp", extension="tmp", subfolder="python_files", override_path=None):
    """

    return filename for a plot. \

    output will be in the form: path/name_threadID_yyyy-mm-dd_hh-mm-ss-ffffff.extension



    :param str name: base name of the file

    :param str extension: extension of the file

    :param str|None subfolder: the subfolder within temporary storage directory

    :param str|None override_path: if not None, use this full directory path instead of the temp location

    """

    time = chrono.now().strftime("%Y-%m-%d_%H-%M-%S-%f")

    full_path = override_path or get_temp_storage_directory(subfolder)

    tid = threading.get_ident()

    filename = os.path.join(full_path, "{}_{}_{}.{}".format(name, tid, time, extension))

    # If file already exists with this exact timestamp, modify the base name and try again.

    if os.path.exists(filename):
        return get_temp_filename(

            name=name + '1', extension=extension, subfolder=subfolder, override_path=override_path)

    return filename


class add_path:
    """

    Context manager to temporarily add a project directory to the python search path.



    Usage:

    with add_path(package_dir):

        import subpackage.file as file



    """

    def __init__(self, path):

        """ :param path: package directory from which to import"""

        self.path = path

    def __enter__(self):

        sys.path.insert(0, self.path)

    def __exit__(self, exc_type, exc_value, traceback):

        try:

            sys.path.remove(self.path)

        except ValueError:

            pass  # catch package already being removed from path within the with block