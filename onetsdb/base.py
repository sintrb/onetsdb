# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''

from __future__ import print_function

NotImplemented = NotImplementedError()


class TSDBException(Exception):
    pass


class TSDBPoint(object):
    time = 0
    data = {}

    def __init__(self, time=None, data=None):
        # if not time:
        #     import datetime
        #     time = datetime.datetime.now()
        self.time = time
        self.data = data

    def __unicode__(self):
        return "TSDBPoint(%s %s)" % (self.time, self.data)

    def __str__(self):
        return self.__unicode__()

    def __repr__(self):
        return self.__str__()


class TSDBBase(object):
    def write_points(self, table, points):
        '''
        write points
        '''
        raise NotImplemented

    def write_point(self, table, point):
        '''
        write point
        '''
        return self.write_points(table, [point])

    def query(self, table, **kwargs):
        q = TSDBQuery(self, table)
        if kwargs:
            q = q.filter(**kwargs)
        return q

    def fetch_with_query(self, query):
        raise NotImplemented

    def delete_with_query(self, query):
        raise NotImplemented

    def count_with_query(self, query):
        raise NotImplemented

    def first_with_query(self, query):
        raise NotImplemented

    def last_with_query(self, query):
        raise NotImplemented

    def getitem_with_query(self, query, item):
        raise NotImplemented

    def register_table(self, table, options):
        raise NotImplemented

    def drop_table(self, table):
        raise NotImplemented

    def close(self):
        raise NotImplemented

    def commit(self):
        raise NotImplemented


class TSDBQuery(object):
    def __init__(self, tsdb, table, options=None):
        self.tsdb = tsdb
        self.table = table
        if options == None:
            options = {}
        self.options = options

    def copy(self):
        import copy
        q = self.__class__(self.tsdb, self.table, copy.deepcopy(self.options))
        return q

    def filter(self, **kwargs):
        q = self.copy()
        if 'filter' not in q.options:
            q.options['filter'] = {}
        q.options['filter'].update(kwargs)
        return q

    def order_by(self, *args):
        q = self.copy()
        q.options['order_by'] = args
        return q

    def time_range(self, start=None, end=None):
        q = self.copy()
        q.options['time_start'] = start
        q.options['time_end'] = end
        return q

    def all(self):
        return list(self)

    def __iter__(self):
        return self.tsdb.fetch_with_query(self)

    def __getitem__(self, item):
        return self.tsdb.getitem_with_query(self, item)

    def delete(self):
        return self.tsdb.delete_with_query(self)

    def count(self):
        return self.tsdb.count_with_query(self)

    def first(self):
        return self.tsdb.first_with_query(self)

    def last(self):
        return self.tsdb.last_with_query(self)

    def all(self):
        return list(self)


def connect(uri):
    '''
    Connect to bus server with uri.
    :param uri: the bus server uri, example: mqtt://localhost:1883 , redis://user:secret@localhost:6379/0
    :return: Bus instance
    '''
    try:
        import urlparse as parse
    except:
        from urllib import parse
    res = parse.urlparse(uri)
    param = parse.parse_qs(res.query)
    dbname = res.path
    if param.get('db'):
        dbname = param['db'][0]
    if not dbname.strip():
        dbname = 'tsdb'
    if res.scheme == 'mongodb':
        # MongoDB
        from pymongo import MongoClient
        from .mongo import MongoTSDB
        client = MongoClient(host=res.hostname, port=int(res.port or 27017), username=res.username or None, password=res.password or None)
        dbname = dbname.strip('/')
        db = client.get_database(dbname)
        tsdb = MongoTSDB(db)
    elif res.scheme == 'influxdb':
        # InfluxDB
        from influxdb import InfluxDBClient
        from .influx import InfluxDB, InfluxTSDB
        client = InfluxDBClient(host=res.hostname, port=int(res.port or 8086), username=res.username or None, password=res.password or None)
        dbname = dbname.strip('/')
        db = InfluxDB(dbname, client)
        tsdb = InfluxTSDB(db)
    elif res.scheme == 'sqlite3':
        # sqlite3
        import sqlite3
        from .sqlite import SqliteTSDB
        if 'file::memory:' in dbname:
            # momery db
            dbname = 'file::memory:'
        con = sqlite3.connect(dbname, check_same_thread=False)
        tsdb = SqliteTSDB(con)
    else:
        raise TSDBException('Unknow uri: %s' % uri)
    return tsdb
