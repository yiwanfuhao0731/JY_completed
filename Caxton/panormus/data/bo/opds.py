'''
Created on 2019年3月23日

@author: user
'''
from panormus.data.bo.db_engine import (get_credentials, OracleEngine)

def get_instance_using_credentials(
        for_key='opds_odstrade',
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
    return OPDS(user=creds['user'], password=creds['password'])

class OPDS(OracleEngine):
    """
    Connect to OPDS database
    """
    def __init__(
           self, user, password, sid_name="OPDS", host="CAX31", port=1521,
            connector='oracle'
    ):
        """
        Connect to OPDS database
        :param str user:
        :param str password:
        :param str sid_name: schema name
        :param str host: name of host machine
        :param int port: database port
        :param str connector: sql alchemy connector type
        """
        super().__init__(
            user=user, password=password,
            sid_name=sid_name, host=host, port=port,
            connector=connector
        )



    def get_option_types(self, inst_ids):
        """
        :description: get option exercise st
        :param list[str] inst_ids: ods inst_ids to query for exercise style
        :return:
        """
        template = '''select
                        INST_ID,
                        OPT_EXERCISE_TYPE_CODE
                      from ODSTRADE.OPTION_INSTRUMENT
                      where INST_ID in ({})'''
        ids = []
        ids.extend(inst_ids)
        id_filter = ','.join(["'{}'".format(i) for i in inst_ids])
        query = template.format(id_filter)
        result = self.run_query(query)

        return result

 