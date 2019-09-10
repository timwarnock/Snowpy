# Snowpy
Pythonic access to Snowflake data

```python
>>> import Snow
>>> mydb = Snow.DB('MYDB')
>>> mydata = mydb.Schema('MYSCHEMA').Table('mydata')
>>> 
>>> mydata.columns
['C1', 'C2', 'C3']
>>> 
>>> len(mydata)
9099
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
```
