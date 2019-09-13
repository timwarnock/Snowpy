#!/usr/bin/env python3

import snowflake.connector
import configparser
import getpass
from collections import defaultdict

config = configparser.ConfigParser()
config.read('Snowpy.ini')

_user_pw = {}
_db_schema_conn_pool = defaultdict(dict)


def connect(db):
    _DATABASE = db
    _USER = config[db]['user']
    _ACCOUNT = config[db]['account']
    _WAREHOUSE = config[db]['warehouse']
    _SCHEMA = config[db]['schema']
    if _USER not in _user_pw:
        _user_pw[_USER] = getpass.getpass(f"Snowflake Password for {_USER}: ")
    _PASSWORD = _user_pw[_USER]
    conn = snowflake.connector.connect(
        user=_USER,
        password=_PASSWORD,
        account=_ACCOUNT,
        warehouse=_WAREHOUSE,
        database=_DATABASE,
        schema=_SCHEMA
    )
    return conn


def _get_conn_by_db_schema(db, schema):
    if db not in _db_schema_conn_pool or schema not in _db_schema_conn_pool[db]:
        conn = connect(db)
        conn.cursor().execute(f''' USE DATABASE {db} ''')
        conn.cursor().execute(f''' USE SCHEMA {schema} ''')
        _db_schema_conn_pool[db][schema] = conn
    return _db_schema_conn_pool[db][schema]


def query(sql, params=None, db=None, schema=None):
    if db is None:
        db = config['DEFAULT']['database']
    if schema is None:
        schema = config[db]['schema']
    conn = _get_conn_by_db_schema(db, schema)
    return conn.cursor().execute(sql, params)


def DB(dbname):
    return _DB(dbname)


class _DB:
    def __init__(self, db):
        self.db = db
        self.conn = _get_conn_by_db_schema(db, config[db]['schema'])

    def query(self, sql, params=None):
        return query(sql, params, self.db, config[self.db]['schema'])

    def Schema(self, schema):
        return _Schema(self.db, schema)


class _Schema:
    def __init__(self, db, schema):
        self.db = db
        self.schema = schema
        self.conn = _get_conn_by_db_schema(db, schema)

    def query(self, sql, params):
        return query(sql, params, self.db, self.schema)

    def Table(self, table):
        return _Table(self.db, self.schema, table)


class _Table:
    def __init__(self, db, schema, table):
        self.db = db
        self.schema = schema
        self.table = table
        self.conn = _get_conn_by_db_schema(db, schema)
