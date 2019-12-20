'''
Created on 2019年3月23日

@author: user
'''
from panormus.data.bo.db_engine import (MySqlEngine, get_credentials)

def get_instance_using_credentials(
        for_key='caxtondb',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='creds.yml'
):
    """
    :description: creates an connector from a yml file containing user and password
    :param str for_key: root key in yml file. user and password keys should be children of root key.
    :param str file_dir: credentials file location
    :param str file_name: credentials file name
    :return: instance of CaxtonDb
    """
    creds = get_credentials(for_key=for_key, file_dir=file_dir, file_name=file_name)
    return CaxtonDb(user=creds['user'], password=creds['password'])


class CaxtonDb(MySqlEngine):
    """
    Create a connector to caxtondb

    """

    def __init__(
            self, user, password,
            db_name='caxtondb',
            host='mysql.marketdata.a.prod.us-east-1.aws.caxton.com',
            port=3306,
            connector='mysql+pymysql',
            pool_recycle=1800,
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
            connector=connector, pool_recycle=pool_recycle
        )

 