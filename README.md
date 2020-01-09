# onetsdb
A Uniform Interface for Timeseries Database Python Library. Will Support MongoDB, InfluxDB, SQLITE3 etc.

Install
===============
```
 pip install onetsdb
```

Usage
===============

```python

from onetsdb import connect, TSDBPoint

# tsdb = connect('mongodb://localhost/tsdb')
# tsdb = connect('influxdb://localhost/tsdb')
# tsdb = connect('sqlite3://localhost/tmp/tsdb.sqlite3')  # file: with/tmp/tsdb.sqlite3,
tsdb = connect('sqlite3://localhost/file::memory:')  # with memory sqlite3

tsdb.register_table('device', {
    'tags': {   # Tags can be a filter for querying
        'devid': 'string',  # Device ID
    },
    'fields': {
        'temp': 'float',  # Temperature value
        'humi': 'float',  # humidity value
    }
})
tsdb.write_point('device', TSDBPoint(data={'devid': 'A1', 'temp': 23.5, 'humi': 45.5}))
tsdb.write_point('device', TSDBPoint(data={'devid': 'A2', 'temp': 20.2, 'humi': 35}))
tsdb.write_point('device', TSDBPoint(data={'devid': 'A1', 'temp': 24.5, 'humi': 50}))

print tsdb.query('device').filter(devid='A1').all()


```


[Click to view more information!](https://github.com/sintrb/pbus)