'''
Created on 2019年3月23日

@author: user
'''
from panormus.data.bo.db_engine import (get_credentials, OracleEngine)


def get_instance_using_credentials(
        for_key='foap_nrg',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='creds.yml'
):
    """
    :description: creates a connector from a yml file containing user and password
    :param str for_key: root key in yml file. user and password keys should be children of root key.
    :param str file_dir: credentials file location
    :param str file_name: credentials file name
    :return: client instance
    """
    creds = get_credentials(for_key=for_key, file_dir=file_dir, file_name=file_name)
    return FOAP(user=creds['user'], password=creds['password'])


class FOAP(OracleEngine):
    """
    Connect to FOAP database
    """

    def __init__(
            self, user, password,
            sid_name="FOAP", host="cnjfoap01", port=1601, connector='oracle'
    ):
        """
        Connect to OPDS database
        :param str user:
        :param str password:
        :param str host: name of host machine
        :param int port: database port
        :param str connector: sql alchemy connector type
        """
        super().__init__(
            user=user, password=password,
            sid_name=sid_name, host=host, port=port,
            connector=connector
        )

 