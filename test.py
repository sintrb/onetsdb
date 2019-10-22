# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''
from __future__ import print_function
from onetsdb import connect, TSDBPoint
import datetime

# tsdb = connect('mongodb://localhost/tsdb')
tsdb = connect('influxdb://localhost/tsdb')
import random

table = 'test'

tsdb.register_table(table, {
    'tags': {
        'name': 'string',
        'group': 'int',
    },
    'fields': {
        'v': 'int',
        'i': 'float',
        'x': 'int',
    }
})

st = datetime.datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0)
st = datetime.datetime.now()
print(st)
for i in range(0, 10):
    tm = st + datetime.timedelta(seconds=i)
    data = {
        'v': int(random.random() * 10),
        'i': int(random.random() * 100) / 100.0,
        'name': random.choice(['a', 'b', 'c']),
        'x': i,
    }
    pt = TSDBPoint(time=tm, data=data)
    # print(pt)
    tsdb.write_point(table, pt)
    # break
# print('count', tsdb.query(table).count(), tsdb.query(table).time_range(st, st + datetime.timedelta(seconds=5)).count())
print(list(tsdb.query(table).filter()))
print('------')
print(list(tsdb.query(table).filter(name='a').order_by('v')))
print('------')
print(list(tsdb.query(table).filter(name='b').order_by('-v')))
print('------')
print(tsdb.query(table).first())
print(tsdb.query(table).last())
print('------')
print(list(tsdb.query(table)[0:3]))
print('------N')
print(list(tsdb.query(table)[2:3]))
print(list(tsdb.query(table)[-2:]))
print('------B')
print(tsdb.query(table)[3])
print(tsdb.query(table)[-2])
print('------')
print(tsdb.query(table).delete())
print(tsdb.drop_table(table))
