#!/usr/bin/env python3

import snowflake.connector
import configparser
import getpass
import os
from pathlib import Path
from collections import defaultdict

config = configparser.ConfigParser()
config.read(os.path.join(Path.home(), '.snowsql', 'config'))

_user_pw = {}
_db_schema_conn_pool = defaultdict(lambda: defaultdict(dict))
_db_schema_info = defaultdict(dict)


def __getattr__(name: str):
    if 'connections.'+ name in config:
        return _Profile(name)
    raise AttributeError(f"module {__name__} has no attribute {name}")


def connections():
    return [x[12:] for x in config.sections() if x.startswith('connections.')]


def _connect(profile):
    ini_section = 'connections.' + profile
    _DATABASE = config[ini_section]['dbname']
    _USER = config[ini_section]['username']
    _ROLE = config[ini_section]['rolename']
    _ACCOUNT = config[ini_section]['accountname']
    _WAREHOUSE = config[ini_section]['warehousename']
    _SCHEMA = config[ini_section]['schemaname']
    if _USER not in _user_pw:
        _user_pw[_USER] = getpass.getpass(f"Snowflake Password for {_USER}: ")
    _PASSWORD = _user_pw[_USER]
    conn = snowflake.connector.connect(
        user=_USER,
        password=_PASSWORD,
        role=_ROLE,
        account=_ACCOUNT,
        warehouse=_WAREHOUSE,
        database=_DATABASE,
        schema=_SCHEMA
    )
    return conn


def _get_conn_by_db_schema(profile, db, schema, role):
    if profile not in _db_schema_conn_pool \
            or db not in _db_schema_conn_pool[profile] \
            or schema not in _db_schema_conn_pool[profile][db]:
        conn = _connect(profile)
        _use_db(conn, db)
        _use_schema(conn, schema)
        _use_role(conn, role)
        _db_schema_conn_pool[profile][db][schema] = conn
    return _db_schema_conn_pool[profile][db][schema]


def _use_db(conn, db):
    if db is not None:
        conn.cursor().execute(f''' USE DATABASE {db} ''')


def _use_role(conn, role):
    if role is not None:
        conn.cursor().execute(f''' USE ROLE {role} ''')
    return conn


def _use_schema(conn, schema):
    if schema is not None:
        conn.cursor().execute(f''' USE SCHEMA {schema} ''')
    return conn


def _query(profile, sql, params=None, db=None, schema=None, role=None):
    if db is None:
        db = config['connections.'+profile]['dbname']
    conn = _get_conn_by_db_schema(profile, db, schema, role)
    return conn.cursor().execute(sql, params)


class _Profile:
    def __init__(self, profile):
        self._profile = profile

    def query(self, sql, params=None):
        return _query(self._profile, sql, params)

    def use_role(self, role):
        conn = _get_conn_by_db_schema(self._profile, None, None, None)
        _use_role(conn, role)
        _db_schema_info[self._profile] = {}

    def __getattr__(self, item):
        if self._profile not in _db_schema_info:
            raw = self.query('select distinct database_name from information_schema.databases').fetchall()
            for r in raw:
                _db_schema_info[self._profile][r[0]] = False
        if item == 'databases':
            return [x for x in _db_schema_info[self._profile].keys()]
        elif item in _db_schema_info[self._profile]:
            return _DB(self._profile, item)
        raise AttributeError(f"{__name__}.{self._profile} has no database {item}")


class _DB:
    def __init__(self, profile, db):
        self._profile = profile
        self._db = db

    def query(self, sql, params=None):
        return _query(self._profile, sql, params, self._db)

    def use_role(self, role):
        conn = _get_conn_by_db_schema(self._profile, self._db, None, None)
        _use_role(conn, role)
        _db_schema_info[self._profile][self._db] = {}

    def __getattr__(self, item):
        if not _db_schema_info[self._profile][self._db]:
            _db_schema_info[self._profile][self._db] = defaultdict(dict)
            raw = self.query('select distinct schema_name from information_schema.schemata').fetchall()
            for r in raw:
                _db_schema_info[self._profile][self._db][r[0]] = False
        if item == 'schemas':
            return [x for x in _db_schema_info[self._profile][self._db].keys()]
        elif item in _db_schema_info[self._profile][self._db]:
            return _Schema(self._profile, self._db, item)
        raise AttributeError(f"{__name__}.{self._profile}.{self._db} has no schema {item}")


class _Schema:
    def __init__(self, profile, db, schema):
        self._profile = profile
        self._db = db
        self._schema = schema

    def query(self, sql, params=None):
        return _query(self._profile, sql, params, self._db, self._schema)

    def use_role(self, role):
        conn = _get_conn_by_db_schema(self._profile, self._db, self._schema, None)
        _use_role(conn, role)
        _db_schema_info[self._profile][self._db][self._schema] = {}

    def __getattr__(self, item):
        if not _db_schema_info[self._profile][self._db][self._schema]:
            _db_schema_info[self._profile][self._db][self._schema] = defaultdict(list)
            raw = self.query('''select distinct table_name 
                from information_schema.tables t where t.table_schema = %s''', self._schema).fetchall()
            for r in raw:
                _db_schema_info[self._profile][self._db][self._schema][r[0]] = []
        if item == 'tables':
            return [x for x in _db_schema_info[self._profile][self._db][self._schema].keys()]
        elif item in _db_schema_info[self._profile][self._db][self._schema]:
            return _Table(self._profile, self._db, self._schema, item)
        raise AttributeError(f"{__name__}.{self._profile}.{self._db}.{self._schema} has no table {item}")


class _Table:
    def __init__(self, profile, db, schema, table, cols=None, predicates=[], predicate=None):
        self._profile = profile
        self._db = db
        self._schema = schema
        self._table = table
        if not cols:
            cols = self._fetch_columns()
        self._cols = cols
        self._predicates = predicates
        if predicate is not None:
            self._predicates.append(predicate)

    def _fetch_columns(self):
        if not _db_schema_info[self._profile][self._db][self._schema][self._table]:
            raw = _query(self._profile, ''' select column_name from information_schema.columns 
                where table_schema = %s and table_name = %s
                order by ordinal_position;''', (self._schema, self._table), self._db, self._schema).fetchall()
            _db_schema_info[self._profile][self._db][self._schema][self._table] = [r[0] for r in raw]
        return _db_schema_info[self._profile][self._db][self._schema][self._table]

    def cols(self, *cols):
        return _Table(self._profile, self._db, self._schema, self._table, cols, self._predicates.copy())

    def filter(self, predicate):
        return _Table(self._profile, self._db, self._schema, self._table, self._cols, self._predicates.copy(), predicate)

    def fetchall(self):
        where = ''
        if self._predicates:
            where = "where " + " and ".join(self._predicates)
        columns = ','.join(self._cols)
        return _query(self._profile, f'select {columns} from {self._table} {where}', None, self._db, self._schema).fetchall()

    def sum(self, column):
        where = ''
        if self._predicates:
            where = "where " + " and ".join(self._predicates)
        raw = _query(self._profile, f'select sum({column}) from {self._table} {where}', None, self._db, self._schema).fetchall()
        return raw[0][0]

    def __getattr__(self, item):
        if item == 'columns':
            return self._cols
        raise AttributeError(
            f"{__name__}.{self._profile}.{self._db}.{self._schema}.{self._table} [colgroup] has no column {item}")

    def __len__(self):
        where = ''
        if self._predicates:
            where = "where " + " and ".join(self._predicates)
        raw = _query(self._profile, f'select count(*) from {self._table} {where}', None, self._db, self._schema).fetchall()
        return raw[0][0]

    def __getitem__(self, key):
        columns = ','.join(self._cols)
        col1 = self._cols[0]
        rowid = key + 1
        where = ''
        if self._predicates:
            where = "where " + " and ".join(self._predicates)
        raw = _query(self._profile, f'''select {columns} from (
            select row_number() over (order by {col1}) as rowid, {columns} from {self._table} {where}
            ) where rowid = {rowid}''', None, self._db, self._schema).fetchall()
        return dict(zip(self._cols, raw[0]))
