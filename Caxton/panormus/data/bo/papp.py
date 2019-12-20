'''
Created on 2019年3月23日

@author: user
'''
import os
from panormus.data.bo.db_engine import (get_credentials, OracleEngine)
from panormus.utils.ref_data import TRADER_USER_ID_DICT


def get_instance_using_credentials(
        for_key='papp_odstrade',
        file_dir=r'\\composers\dfs\traders\QAGSpreadsheets\panormus',
        file_name='creds.yml'
):
    """
    :description: creates a connector db from a yml file containing user and password
    :param str for_key: root key in yml file. user and password keys should be children of root key.
    :param str file_dir: credentials file location
    :param str file_name: credentials file name
    :return: client instance
    """
    creds = get_credentials(for_key=for_key, file_dir=file_dir, file_name=file_name)
    return PAPP(user=creds['user'], password=creds['password'])


class PAPP(OracleEngine):
    """
    Connect to PAPP database
    """

    def __init__(
            self, user, password,
            sid_name="PAPP", host="CAX31", port=1523, connector='oracle'
    ):
        """
        Connect to PAPP database
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

    def load_repo_additional_details(self, trader, incl_repo_rate=False,
                                     folder_dir=r'\\composers\dfs\traders\QAGSpreadsheets',
                                     keep_header=False):

        if incl_repo_rate:
            template = '''SELECT ots.INSTRUMENT_IDENTIFIER as BO_TRADE_ID, 
                    fsa.CNTRPRTY_ID, fsa.NAME, fsa.SUB_ACCT_ID,
                    TO_CHAR(ots.TRADE_DATE, 'DD-Mon-YYYY'), 
                    TO_CHAR(ots.SETTLE_DATE, 'DD-Mon-YYYY'),
                    frs.RESET_RATE
                    FROM OPEN_TRADE_STATUS ots 
                    JOIN TRADER_SUB_OBJECTIVE tso ON ots.FUND_IDENTIFIER = tso.FUND_ID AND 
                    ots.TRADING_SUBOBJECTIVE = tso.TRADING_SUB_OBJECTIVE 
                    JOIN TRADE t ON t.TRADE_ID = ots.TRADE_IDENTIFIER 
                    JOIN FUND_SUB_ACCOUNT fsa ON fsa.FUND_ID = t.FUND_ID AND fsa.SUB_ACCT_ID = t.EXECUTING_BROKER_SUB_ACCT_ID 
                    JOIN ODSTRADE.FLOATING_RATE_SCHEDULE frs ON ots.INSTRUMENT_IDENTIFIER = frs.INST_ID
                    WHERE t.MX_GROUP IN ('REPO','RREPO') 
                    AND fsa.CNTRPRTY_ID != 'INTERNAL'
                    AND tso.TRADER_ID = '{}' 
                    AND ots.INSTRUMENT_IDENTIFIER NOT LIKE 'R %' 
                    order by BO_TRADE_ID'''
        else:
            template = '''SELECT ots.INSTRUMENT_IDENTIFIER as BO_TRADE_ID, 
            fsa.CNTRPRTY_ID, fsa.NAME, fsa.SUB_ACCT_ID,
            TO_CHAR(ots.TRADE_DATE, 'DD-Mon-YYYY'), 
            TO_CHAR(ots.SETTLE_DATE, 'DD-Mon-YYYY')
            FROM OPEN_TRADE_STATUS ots 
            JOIN TRADER_SUB_OBJECTIVE tso ON ots.FUND_IDENTIFIER = tso.FUND_ID AND 
            ots.TRADING_SUBOBJECTIVE = tso.TRADING_SUB_OBJECTIVE 
            JOIN TRADE t ON t.TRADE_ID = ots.TRADE_IDENTIFIER 
            JOIN FUND_SUB_ACCOUNT fsa ON fsa.FUND_ID = t.FUND_ID AND fsa.SUB_ACCT_ID = t.EXECUTING_BROKER_SUB_ACCT_ID 
            WHERE t.MX_GROUP IN ('REPO','RREPO') 
            AND tso.TRADER_ID = '{}' 
            AND ots.INSTRUMENT_IDENTIFIER NOT LIKE 'R %' 
            order by BO_TRADE_ID'''

        query = template.format(trader)
        result = self.run_query(query)
        trader_name = TRADER_USER_ID_DICT[trader]['USER_ID']

        if not incl_repo_rate:
            file_name = 'repoadditionaldetails.csv'
        else:
            file_name = 'repoadditionaldetails2.csv'

        file_path = os.path.join(
            folder_dir, trader_name,
            file_name)

        result.to_csv(file_path, index=False, header=keep_header)

        return file_path

    def load_position_ladder(self, trader, mxGroup, asofStr,
                             folder_dir=r'\\composers\dfs\traders\QAGSpreadsheets', keep_header=False):
        fromStr = '''FROM TRADE t JOIN TRADER_SUB_OBJECTIVE tso ON t.FUND_ID = 
        tso.FUND_ID AND t.TRADING_SUB_OBJECTIVE = tso.TRADING_SUB_OBJECTIVE '''

        whereStr = '''WHERE t.MX_GROUP = '{}' AND tso.TRADER_ID = '{}' '''.format(mxGroup, trader)

        strQuery = '''SELECT * FROM ( 
        SELECT 'SETTLED:' || t.MX_INSTRUMENT || ':' || tso.TRADING_SUB_OBJECTIVE_NAME as StrategyIdx, 'SETTLED' as SETTLEMENT_STATUS,
        t.MX_INSTRUMENT as ISIN, tso.TRADING_SUB_OBJECTIVE_NAME as STRATEGY, 
        TO_CHAR(MAX(t.TRADE_DATE), 'DD-Mon-YYYY') as TRADE_DATE, TO_CHAR(MAX(t.SETTLE_DATE), 'DD-Mon-YYYY') as SETTLE_DATE,  
        SUM(CASE WHEN t.TRADE_TYPE_CODE = 'B' THEN 1 ELSE -1 END * t.NOTIONAL) as QUANTITY 
        {}
        {}  
        AND t.SETTLE_DATE <= '{}' 
        AND t.TRADE_BATCH_PROCESS_TYPE_CODE = 'PROC'
        GROUP BY 'SETTLED', t.MX_INSTRUMENT, tso.TRADING_SUB_OBJECTIVE_NAME
        UNION
        SELECT 'UNSETTLED:' || t.MX_INSTRUMENT || ':' || tso.TRADING_SUB_OBJECTIVE_NAME, 'UNSETTLED',
        t.MX_INSTRUMENT as ISIN, tso.TRADING_SUB_OBJECTIVE_NAME, TO_CHAR(t.TRADE_DATE, 'DD-Mon-YYYY'), TO_CHAR(t.SETTLE_DATE, 'DD-Mon-YYYY'),
        SUM(CASE WHEN t.TRADE_TYPE_CODE = 'B' THEN 1 ELSE -1 END * t.NOTIONAL)
        {}
        {}  
        AND t.SETTLE_DATE > '{}' 
        AND t.TRADE_BATCH_PROCESS_TYPE_CODE = 'PROC'
        GROUP BY 'UNSETTLED', t.MX_INSTRUMENT, tso.TRADING_SUB_OBJECTIVE_NAME, t.TRADE_DATE, t.SETTLE_DATE
        UNION
        SELECT t.MX_INSTRUMENT || ':' || tso.TRADING_SUB_OBJECTIVE_NAME, 'ALL',
        t.MX_INSTRUMENT, tso.TRADING_SUB_OBJECTIVE_NAME,
        TO_CHAR(MAX(t.TRADE_DATE), 'DD-Mon-YYYY'), TO_CHAR(MAX(t.SETTLE_DATE), 'DD-Mon-YYYY'), SUM(CASE WHEN t.TRADE_TYPE_CODE = 'B' THEN 1 ELSE -1 END * t.NOTIONAL)
        {} 
        {}
        AND t.TRADE_BATCH_PROCESS_TYPE_CODE = 'PROC'  
        GROUP BY t.MX_INSTRUMENT, tso.TRADING_SUB_OBJECTIVE_NAME) x 
        WHERE x.QUANTITY <> 0 ORDER BY x.SETTLEMENT_STATUS, x.StrategyIdx '''. \
            format(fromStr, whereStr, asofStr, fromStr, whereStr, asofStr, fromStr, whereStr)

        print(strQuery)
        result = self.run_query(strQuery)
        trader_name = TRADER_USER_ID_DICT[trader]['USER_ID']

        file_path = os.path.join(
            folder_dir, trader_name,
            'positionladder.csv')

        result.to_csv(file_path, index=False, header=keep_header)

        return file_path


if __name__ == '__main__':
    papp_db = get_instance_using_credentials()

    papp_db.load_position_ladder('LL', 'BOND', '08-nov-2018')
    papp_db.load_repo_additional_details('LL')

 