# Snowpy
Pythonic access to Snowflake data. In other words, access Snowflake objects (schemas, tables, data) as local Python objects, e.g.,
```python
>>> import Snowpy
>>> table1 = Snowpy.devDB.myschema.table1
>>> len(table1)
9099
>>> table1.columns
['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9']
>>> mydata[5].C1
37.4
```

You can call SQL queries easily from a DB or schema.
```python
>>> Snowpy.devDB.query('select count(*) from myschema.table1').fetchall()
[(9099,)]
>>> 
>>> myschema = Snowpy.devDB.myschema
myschema.query('select count(*) from table1').fetchall()
[(9099,)]
```

And in most cases you can slice and dice your Snowflake data as if it were local Python objects.
```python
>>> Snowpy.databases
['devDB', 'qaDB', 'prodDB']
>>> 
>>> devDB = Snowpy.devDB
>>> devDB.schemas
['myschema', 'otherschema']
>>> myschema = devDB.myschema
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
>>> mydata[5].C1
37.4
>>> 
>>> everyother = mydata[0::2]
>>> len(everyother)
4550
>>> 
>>> everyother.sum('C1')
3249.12
>>> 
>>> everyother_gt0 = everyother.filter("C1 > 0")
>>> everyother_gt0.sum('C1')
113286.5
>>> 
>>> ## or all in one line
>>> Snowpy.devDB.myschema.test1[0::2].filter('C1 > 0').sum('C1')
113286.5
```
