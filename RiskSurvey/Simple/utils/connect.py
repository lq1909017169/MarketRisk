from Simple.utils.log import log
from Simple.utils.utils import apply_map_callback, get_id, get_current_time
from abc import ABC, abstractmethod
from configparser import ConfigParser
import warnings
import jaydebeapi
import pandas as pd
import os

warnings.filterwarnings('ignore')


# 发送post请求

# def http_post(url, params):
#     result = requests.post(url=url, params=params)
#     if result.status_code == 200:
#         response = json.loads(result.text)
#         log.info(f"java response: {response}")
#         success = response['success']
#         if success is True and 'file_path' in response:
#             file_path = response['file_path']
#             return 'success', file_path
#         elif success is True:
#             return 'success'
#         else:
#             return 'failed'


class Connect(ABC):

    def __init__(self) -> None:
        self.conn = None
        self.cursor = None

    @abstractmethod
    def to_df(self, sql):
        pass

    @abstractmethod
    def query_one(self, table, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            log.error(f"{exc_val}: {exc_type}")
        else:
            pass

    @staticmethod
    def verify(data):
        if data:
            return data
        else:
            log.error("get data of database, error")

    @abstractmethod
    def save(self, table, df, partition_key):
        pass

    @abstractmethod
    def clean_table(self, table, partition_key):
        pass

    @abstractmethod
    def execute(self, sql):
        pass


class OceanBaseConnect(Connect):

    def __init__(self, ):
        super().__init__()
        config = ConfigParser()
        config.read(os.path.split(os.path.abspath(__file__))[0] + '/setting.ini', encoding='utf-8')
        ob_connect = {k: v for k, v in config.items('OB_CONNECT')}
        url, user, password, driver, jar = ob_connect['url'], ob_connect['user'], ob_connect['password'], ob_connect[
            'driver'], ob_connect['jar']
        jar = os.path.join(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'driver'), jar)
        log.info(f"create database connect，database params ：{url, user, password, driver, jar}")
        self.connect = jaydebeapi.connect(driver, url, [user, password], jar)
        self.cursor = self.connect.cursor()

    def to_df(self, sql) -> pd.DataFrame:
        return pd.read_sql(sql, self.connect)

    def query_one(self, table, *args, **kwargs):
        select = "SELECT {columns} FROM {table} {condition}"
        columns = "(" + ",".join(args) + ")" if args else '*'
        condition = 'WHERE ' + ' AND '.join(["{}='{}'".format(k, v) for k, v in kwargs.items()]) if kwargs else ''
        sql = select.format_map({'table': table.__table_name__, 'columns': columns, 'condition': condition})
        return self.to_df(sql).applymap(apply_map_callback).to_dict('records')

    @staticmethod
    def batch_index(rows: int, batch_number: int = 2000):
        index = []
        r = 0
        while r + batch_number < rows:
            left = r + batch_number
            index.append((r, left))
            r = left
        if r < rows:
            index.append((r, rows))
        return index

    @staticmethod
    def sql_one_insert(series, table, columns, self):
        into = "insert into {table} ({keys}) values {values}"
        keys = ','.join(columns)
        values = f"""('{"','".join(tuple(map(str, series.values.tolist())))}')"""
        into = into.format_map({
            'table': table,
            'keys': keys,
            'values': values
        })
        try:
            self.cursor.execute(into)
        except BaseException as e:
            log.error(f"table{table}_insert sql_{into}: {e}")

    def save(self, table, df, partition_key):
        today = get_current_time()
        df['ID'] = df.apply(lambda _: get_id(), axis=1)
        df['ENTRY_DATE'] = df.apply(lambda _: today, axis=1)
        log.debug(f'{table} insert into DB data, rows__{df.shape[0]}')

        # file_path = os.path.join(os.path.dirname(__file__), f'{table}.csv')
        # df.fillna('').to_csv(file_path, sep='^', index=False, header=False)
        # columns = df.columns
        # insert_sql = f"INSERT INTO {table}({','.join(columns)})VALUES({','.join(['?' for _ in columns])})"
        # log.info(f'insert sql: {insert_sql}')
        # params = {
        #     'sql_content': insert_sql,
        #     'file_path': file_path
        # }
        # url = 'http://localhost:9088/ob-tool/data/batchInsert'
        # success = http_post(url, params)
        # log.info(f'success: {success}')

        # df.fillna('').apply(self.sql_one_insert, args=(table, df.columns, self), axis=1)

        index_list = self.batch_index(df.shape[0])
        for i in index_list:
            sql = self.create_sql(table, df[i[0]: i[1]].fillna(''))
            self.cursor.execute(sql)

    def clean_table(self, table, partition_key):
        sql = f"delete from {table} where PARTITION_KEY='{partition_key}'"
        log.info(f"delete table <{table}>, PARTITION_KEY={partition_key} sql -> {sql}")
        self.cursor.execute(sql)

    @staticmethod
    def callback(series):
        values = "'" + "','".join(tuple(map(str, series.values.tolist()))) + "'"
        return f"({values})"

    def create_sql(self, table, data):
        into = "insert into {table} ({keys}) values {values}"
        keys = ','.join(data.columns)
        values = ','.join(data.apply(self.callback, axis=1).values)
        return into.format_map({
            'table': table,
            'keys': keys,
            'values': values
        })

    def execute(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            log.error(f"{exc_val}: {exc_type}")
        else:
            pass

    def __del__(self):
        pass
        # self.cursor.close()
