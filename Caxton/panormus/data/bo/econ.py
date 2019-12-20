'''
Created on 2019年3月23日

@author: user
'''
import datetime as dt

from pandas import DataFrame

from panormus.data.bo.db_engine import (MySqlEngine)


def get_instance_using_credentials():
    """
    :description: creates an connector public read-only credentials
    :return: client instance
    """

    return EconDb(user='ECON_RO', password='RpZi7Mie5oRSEjyp')


class EconDb(MySqlEngine):
    """
    Create a connector to economics database

    """

    def __init__(
            self, user, password,
            db_name='ECON',
            host='econ-ro.mysql.pm.a.dev.use1.aws.caxton.com',
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
        super().__init__(
            user=user, password=password,
            db_name=db_name, host=host, port=port,
            connector=connector, pool_recycle=pool_recycle,
            connect_args={'ssl': {'check_hostname': verifyServerCertificate}}
        )

    def get(self,
            tickers,
            start_date=dt.datetime(1500, 1, 1), end_date=dt.datetime(2099, 12, 31),
            vintage='last', output='data', query_raw_db=False
            ):
        """
        Get time series for tickers in the Economic Database

        :param str|list[str] tickers: EDB tags \
        (see: https://tableau.a.dev.use1.aws.caxton.com/#/views/Haver_Extract/Haver?:iid=1)
        :param dt.datetime|None start_date: Optional start date
        :param dt.datetime|None end_date: Optional end date
        :param str vintage: 'last' for the last revision and 'first' for the first known unrevised value
        :param str output: 'data' for the data or 'timestamp' for the timestamps of the data
        :param bool query_raw_db: False to query helper tables, True to query the raw DB directly
        :rtype: DataFrame
        """
        # parse data
        if start_date is None:
            start_date = dt.datetime(1500, 1, 1)
        if end_date is None:
            end_date = dt.datetime(2099, 12, 31)

        if isinstance(tickers, str):
            tickers = [tickers]

        if vintage.lower() in ('last', 'revised'):
            if query_raw_db:
                select = "Attribute, TimeStamp, Value, MAX(KnownAsOf)"
                db = "EDB"
                groupby = " GROUP BY Attribute, TimeStamp"
            else:
                select = "*"
                db = "LastEDB"
                groupby = ""
        elif vintage.lower() in ('first', 'unrevised'):
            if query_raw_db:
                select = "Attribute, TimeStamp, Value, MIN(KnownAsOf)"
                db = "EDB"
                groupby = " GROUP BY Attribute, TimeStamp"
            else:
                select = "*"
                db = "FirstEDB"
                groupby = ""
        else:
            raise ValueError(f"Vintage option '{vintage}' not valid")

        if output.lower() == 'data':
            col = 'Value'
        elif output.lower() == 'timestamp':
            col = 'KnownAsOf'

        sd_str = "\'" + start_date.strftime("%Y-%m-%d") + "\'"
        ed_str = "\'" + end_date.strftime("%Y-%m-%d") + "\'"
        tags_str = ",".join(["\'" + tag + "\'" for tag in tickers])
        # tags_str = 'USA_PMI_SERVICES_SA'
        sql_string = (
            f"SELECT {select} FROM ECON.{db} WHERE Attribute IN ({tags_str}) "
            f"AND TimeStamp >= {sd_str} AND TimeStamp <= {ed_str}{groupby}"
        )
        # 'USA_SBO_NFIB_INDEX_SA' in df_raw['Attribute']
        # run sql query
        df_raw = self.run_query(sql_string)
        df_out = df_raw.pivot(index='TimeStamp', columns='Attribute', values=col)
        # df_out.plot()
        return df_out
 