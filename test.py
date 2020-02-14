# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''
from __future__ import print_function
from onetsdb import connect, TSDBPoint
import datetime


def do_test(con, table='test', count=100):
    import random, time

    tsdb = connect(con)

    class SpeedIt(object):
        _start = 0

        def start(self):
            self._last = self._start = time.time()
            self._counter = 0

        def feed(self):
            self._counter += 1
            if (time.time() - self._last) > 10:
                self.display()

        def display(self):
            off = time.time() - self._start
            print('time cost', off, 'count', self._counter, 'speed', self._counter / off, '/ s')
            self._last = time.time()

        def stop(self):
            self.display()

    print('test table=%s with uri=%s' % (table, uri))

    tsdb.drop_table(table)
    # tsdb.register_table(table, {
    #     'tags': {
    #         'name': 'string',
    #     },
    #     'fields': {
    #         'v': 'int',
    #     }
    # })

    tsdb.register_table(table, {
        'tags': {
            'name': 'string',
            'group': 'int',
            'x': 'int',
        },
        'fields': {
            'v': 'int',
            'y': 'float',
            'z': 'float',
            't': 'float',
        }
    })

    st = datetime.datetime(year=2019, month=1, day=1, hour=0, minute=0, second=0)
    tsdb.query(table).delete()
    sp = SpeedIt()
    print('insert...')
    sp.start()
    for i in range(0, count):
        tm = st + datetime.timedelta(seconds=i)
        data = {
            'v': int(random.random() * 10),
            'i': int(random.random() * 100) / 100.0,
            'name': random.choice(['a', 'b', 'c']),
            'group': i,
            'x': i,
        }
        sp.feed()
        pt = TSDBPoint(time=tm, data=data)
        tsdb.write_point(table, pt)
        # break
    sp.display()
    print('query...')
    # exit()
    # print('count', tsdb.query(table).count(), tsdb.query(table).time_range(st, st + datetime.timedelta(seconds=5)).count())
    # print(tsdb.query(table).filter().count())
    assert tsdb.query(table).filter().count() == count
    assert tsdb.query(table).filter(x=2).count() == 1
    # print(tsdb.query(table).filter().first().data)
    assert tsdb.query(table).filter().first().data['x'] == 0
    assert tsdb.query(table).filter()[3].data['x'] == 3
    assert tsdb.query(table).filter()[4].data['x'] == 4
    assert tsdb.query(table).filter()[-1].data['x'] == count - 1
    assert tsdb.query(table).filter()[-2].data['x'] == count - 2
    assert tsdb.query(table).filter(x=7).first().data['x'] == 7
    assert tsdb.query(table).filter(name='a').first().data['name'] == 'a'
    assert len(list(tsdb.query(table)[0:3])) == 3

    assert tsdb.query(table).filter().time_range(st, st + datetime.timedelta(seconds=5)).count() == 6
    assert tsdb.query(table).filter().time_range(st, st).first().time == st

    assert tsdb.query(table).filter(x=2).first() != None
    tsdb.query(table).filter(x=2).delete()
    assert tsdb.query(table).filter().count() == count - 1
    assert tsdb.query(table).filter(x=2).first() == None

    # tsdb.query(table).delete()
    # assert tsdb.query(table).filter().count() == 0

    assert len(list(tsdb.query(table))) == count - 1

    tsdb.drop_table(table)
    tsdb.close()
    sp.stop()


if __name__ == '__main__':
    import sys

    uris = [
        'mongodb://localhost/tsdb',
        'mongodb://172.16.1.211/tsdb',
        'influxdb://localhost/tsdb',
        'influxdb://172.16.1.211/?db=tsdb5',
        'sqlite3://localhost/?db=/tmp/tt.sqlite3',
        'sqlite3://localhost/tmp/tsdb.sqlite3',
        'sqlite3://localhost/file::memory:',
    ]

    if len(sys.argv) >= 1:
        uris = [sys.argv[1]]
    table = 'test'
    count = 100
    if len(sys.argv) > 2:
        table = sys.argv[2]
    if len(sys.argv) > 3:
        count = int(sys.argv[3])
    for uri in uris:
        print('++++++ begin %s +++++++++' % uri)
        do_test(uri, table, count)
        print('------  end %s  ---------' % uri)
        print()
