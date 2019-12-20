'''
Created on 2019年3月23日

@author: user
'''
import datetime as dt
import os
import pandas as pd

from panormus.utils import chrono
from panormus.data.bo.db_engine import (MySqlEngine, get_credentials)


def get_instance_using_credentials(
        for_key='md_obj_ro',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='creds.yml',
        read_only=True
):
    """
    :description: creates an connector from a yml file containing user and password
    :param str for_key: root key in yml file. user and password keys should be children of root key.
    :param str file_dir: credentials file location
    :param str file_name: credentials file name
    :param bool read_only: connect to read-only replica?
    :return: client instance
    """
    creds = get_credentials(for_key=for_key, file_dir=file_dir, file_name=file_name)
    host = 'md_obj-ro.mysql.pm.a.dev.use1.aws.caxton.com' if read_only else 'md_obj.mysql.pm.a.dev.use1.aws.caxton.com'
    return MdObj(user=creds['user'], password=creds['password'], host=host)


class MdObj(MySqlEngine):
    """
    Create a connector to market data object database
    """

    def __init__(
            self, user, password,
            db_name='MD_OBJ',
            host='md_obj-ro.mysql.pm.a.dev.use1.aws.caxton.com',
            port=3306,
            connector='mysql+pymysql',
            pool_recycle=1800,
            verifyServerCertificate=False,
    ):
        """
        :param str user: database user
        :param str password: password
        :param str db_name: database name within host
        :param str host: host address
        :param int port: port number
        :param str connector: connection driver for sql alchemy
        :param int pool_recycle: connection pool recycle period
        """
        self._DATE_STR_FMT = '%Y-%m-%d %H:%M:%S'
        super().__init__(
            user=user, password=password,
            db_name=db_name, host=host, port=port,
            connector=connector, pool_recycle=pool_recycle,
            connect_args={'ssl': {'check_hostname': verifyServerCertificate}}
        )

    def _coerce_date_arg(self, date_arg):
        '''
        :description: Coerces date argument to desired format for sql query
        :param dt.date|dt.datetime|str date_arg: a date argument to process
        :return: date string as 'null' or formatted like _DATE_STR_FMT
        '''
        if date_arg is None:
            date_arg = 'null'
        elif not isinstance(date_arg, str):
            date_arg = date_arg.strftime(self._DATE_STR_FMT)

        return date_arg

   def insert_csv(self, full_path):
        """
        :description: Updates tblGen with strFile, an Nx5 CSV (type, item, cut, timestamp, val) without headers. \
         Any records that already exist (key & dt) will have values updated. \
         Any records that don't exist will be inserted.
        :param str full_path: full path to csv file
        :return:
        """
        assert os.path.isfile(full_path), 'File not found: ' + full_path
        status = False
        try:
            # Upload data into a temporary table
            res = self.engine.execute('DROP TEMPORARY TABLE IF EXISTS tblTmpObjInsert')
            res = self.engine.execute(
                'CREATE TEMPORARY TABLE tblTmpObjInsert (' + \
                'type      VARCHAR(500) NOT NULL, ' + \
                'item      VARCHAR(500) NOT NULL, ' + \
                'cut       VARCHAR(500) NOT NULL, ' + \
                'timeStamp DATETIME     NOT NULL, ' + \
                'value     MEDIUMTEXT   NOT NULL);')
            res_load = self.engine.execute(
                'LOAD DATA LOCAL INFILE "' + full_path.replace('\\', '\\\\') + '" ' + \
                'INTO TABLE tblTmpObjInsert ' + \
                'FIELDS TERMINATED BY ",";')

            # Load temporary table into official normalized tables
            res_insert = self.engine.execute('CALL SP_MD_OBJ_VALUE_INSERT("tblTmpObjInsert");')
            if res_insert.cursor._rows[0][0] == 'Success':
                status = True
            else:
                print(res_insert)

        except Exception as msg:
            print(msg)
            status = False

        return status

    def insert_df(self, df, stage_dir=None, clear_staging=True):
        """
        :description: Updates tblGen with df, an Nx3 DataFrame (key, dt, val). \
         Dates need not have times on them. If they do, times will be treated as UTC. \
         Any records that already exist (key & dt) will be updated. \
         Any records that don't exist will be inserted.
        :param pd.DataFrame df: Nx3 dataframe (key, dt, val)
        :param str|None stage_dir: Optional directory to put staging csv files. Defaults to environment temp directory.
        :param bool clear_staging: Delete staging csv file after writing to database
        :return: bool status for success (true) or failure (false)
        """
        assert type(df) in [pd.DataFrame], 'df must be a DataFrame.'
        assert df.shape[1] == 5, 'df must have 5 columns: type, item, cut, timestamp, value'
        if stage_dir is None:
            stage_dir = os.environ['TEMP']

        str_file = 'cax_obj_staging_%s.csv' % chrono.now().strftime('%Y-%m-%d.%H.%M.%S')
        full_path = os.path.join(stage_dir, str_file)
        df.to_csv(full_path, index=False, header=False)
        bool_status = self.insert_csv(full_path)
        if clear_staging:
            try:
                os.remove(full_path)
            except:
                pass

        return bool_status

    def fetch_df(self, type, item_list, cut_list, sd, ed):
        """
        :description: retrieve dataframe of objects from cax object database
        :param str type: database type like alib_curve or vol_surface
        :param str|list[str] item_list: list of items names like USD.LIBOR.3ML
        :param str|list[str] cut_list: list of cuts like nyclose
        :param dt.datetime|str|None sd: start date. Use None for open ended start.
        :param dt.datetime|str|None ed: end date. Use None for open ended end.
        :return: pd.DataFrame with columns [item, cut, timestamp, value
        """
        if isinstance(item_list, str):
            item_list = [item_list]
        if isinstance(cut_list, str):
            cut_list = [cut_list]

        items = '&'.join(item_list)
        cuts = '&'.join(cut_list)
        sd_str = self._coerce_date_arg(sd)
        ed_str = self._coerce_date_arg(ed)

        df = self.run_query(f'CALL SP_MD_OBJ_VALUE_QUERY("{type}", "{items}", "{cuts}", "{sd_str}", "{ed_str}")')

        return df

 