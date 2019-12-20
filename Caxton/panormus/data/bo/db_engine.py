'''
Created on 2019年3月23日

@author: user
'''
import os
import yaml

from sqlalchemy import create_engine
from pandas import read_sql
import cx_Oracle

DRIVERS = {
    'SQL_SERVER_NATIVE_11': 'SQL+Server+Native+Client+11.0',
}


def get_credentials(
        for_key='',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
       file_name='creds.yml'
):
    """
    :description: get credentials for a given database
    :param str for_key: key in credentials file
    :param str file_dir: directory override for the credential yaml file
    :param str file_name: file name override for the credential yal file
    :return:
    """
    full_path = os.path.join(file_dir, file_name)
    with open(full_path, 'r') as ymlfile:
        creds = yaml.load(ymlfile)

    if for_key:
        return creds[for_key]
    else:
        return creds


class BaseEngine(object):
    def __init__(self, conn_str, **kwargs):
        self.engine = create_engine(conn_str, **kwargs)

    def __del__(self):
        try:
            self.engine.dispose()
        except:
            pass

    def run_query(self, query_str):
        """
        :description: Execute sql to return a dataframe.
        :param str query_str: sql
        :return: pandas.DataFrame
        """
        df = read_sql(query_str, self.engine)
        return df


class SqlServerEngine(BaseEngine):
    def __init__(
            self, user, password, db_name, host, port=1433,
            connector='mssql+pyodbc', driver='SQL+Server+Native+Client+11.0',
            **connect_kwargs
    ):
        conn_str = f"{connector}://{user}:{password}@{host}:{port}/{db_name}?driver={driver}"
        super().__init__(conn_str=conn_str)


class MySqlEngine(BaseEngine):
    def __init__(
            self, user, password, db_name, host, port=3306,
            connector='mysql+pymysql', pool_recycle=1800,
            **kwargs,
    ):
        conn_str = f"{connector}://{user}:{password}@{host}:{port}/{db_name}?local_infile=1"
        super().__init__(conn_str=conn_str, pool_recycle=pool_recycle, **kwargs)


class OracleEngine(BaseEngine):
    def __init__(
           self, user, password, sid_name, host, port=3306,
            connector='mysql+pymysql',
            **connect_kwargs
    ):
        dsn = cx_Oracle.makedsn(host=host, port=port, sid=sid_name)
        conn_str = f"{connector}://{user}:{password}@{dsn}"
        super().__init__(conn_str=conn_str, connect_args=connect_kwargs)

 