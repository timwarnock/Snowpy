# Snowpy
Pythonic access to Snowflake data. In other words, access Snowflake objects (schemas, tables, data) as local Python objects, e.g.,
```python
>>> import Snowpy
>>> table1 = Snowpy.dev.DB.myschema.table1
>>> len(table1)
9099
>>> table1.columns
['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
>>> mydata[5]['C1']
37.4
```

## Connect to Snowflake
Snowpy will use your snowsql config (e.g., ~/.snowsql/config), e.g.,
```ini
[connections.dev]
accountname = dev.us-east-1.company
username = TWARNOCK
dbname = DB
schemaname = USER_TWARNOCK
warehousename = WH_L1
rolename = SNOW_TWARNOCK_ROLE

[connections.qa]
accountname = qa.us-east-1.company
username = TWARNOCK
dbname = DB
schemaname = USER_TWARNOCK
warehousename = WH_L1
rolename = SNOW_TWARNOCK_ROLE

[connections.prod]
accountname = prod.us-east-1.company
username = TWARNOCK
dbname = DB
schemaname = USER_TWARNOCK
warehousename = WH_L1
rolename = SNOW_TWARNOCK_ROLE

```

*NOTE* do not store your user password in your snowsql config. Snowpy will prompt you for your password.

## Usage

You can call SQL queries easily from a profile, a DB, or a schema.
```python
>>> Snowpy.dev.query('select count(*) from DB.myschema.table1').fetchall()
[(9099,)]
>>> 
>>> Snowpy.dev.DB.query('select count(*) from myschema.table1').fetchall()
[(9099,)]
>>> 
>>> myschema = Snowpy.dev.DB.myschema
myschema.query('select count(*) from table1').fetchall()
[(9099,)]
```

And you can slice and dice your Snowflake data as if it were local Python objects.
```python
>>> Snowpy.connections
['dev', 'qa', 'prod']
>>> 
>>> dev = Snowpy.dev.databases
['DB', 'SB']
>>> dev.SB.schemas
['myschema', 'otherschema']
>>> myschema = dev.SB.myschema
>>> myschema.tables
['table1', 'table2']
>>> 
>>> myschema.table1.columns
['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
>>> 
>>> len(myschema.table1)
9099
>>> 
>>> mydata = myschema.table1.cols('C1', 'C2', 'C3')
>>> len(mydata)
9099
>>> mydata.columns
['C1', 'C2', 'C3']
>>> 
>>> mydata[5]
{'C1': 37.4, 'C2': -129.3, 'C3': 0.003}
>>> 
>>> mydata[5]['C1']
37.4
>>> 
>>> 
>>> mydata.sum('C1')
3249.12
>>> 
>>> gt0 = mydata.filter("C1 > 0")
>>> gt0.sum('C1')
113286.5
>>> 
>>> ## or all in one line
>>> Snowpy.dev.SB.myschema.filter('C1 > 0').sum('C1')
113286.5
```

