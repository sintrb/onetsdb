# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''
from __future__ import print_function
import datetime, time
from .base import TSDBPoint, TSDBBase

TIME_FIELD = 'time'
TIME_ALTZONE = time.altzone


class InfluxDB(object):
    def __init__(self, dbname, client):
        self.client = client
        self.dbname = dbname
        self.client.create_database(dbname)


class InfluxTSDB(TSDBBase):
    '''
    Wrapper for InfluxDB
    '''

    def __init__(self, db=None):
        self.db = db
        self._table_define = {}

    def _get_table_define(self, table):
        return self._table_define.get(table)

    def register_table(self, table, options):
        # if options.get('fields'):
        #     countk = list(options['fields'].keys())[0]
        # else:
        #     countk = '_count'
        options['_count_field'] = '__count__'
        options['_field_map'] = {
            k: v for k, v in list(options.get('tags', {}).items()) + list(options.get('fields', {}).items())
        }
        self._table_define[table] = options

    def _to_db_time(self, tm):
        if isinstance(tm, datetime.datetime):
            import time
            tm = int(time.mktime(tm.timetuple()) * 1000000 + tm.microsecond) * 1000
        return tm

    def _to_point_time(self, tm):
        import six
        if isinstance(tm, int):
            return datetime.datetime.fromtimestamp(tm)
        if isinstance(tm, six.string_types):
            try:
                return datetime.datetime.strptime(tm, '%Y-%m-%dT%H:%M:%S.%fZ') - datetime.timedelta(seconds=TIME_ALTZONE)
            except:
                return datetime.datetime.strptime(tm, '%Y-%m-%dT%H:%M:%SZ') - datetime.timedelta(seconds=TIME_ALTZONE)

    def _point_to_db_data(self, point, table):
        td = self._get_table_define(table)
        data = {
            'measurement': table,
            'time': self._to_db_time(point.time),
            'tags': {

            },
            'fields': {
                td['_count_field']: 1
            },
        }
        if point.data:
            tags = td.get('tags') if td else None
            for k, v in point.data.items():
                if k == TIME_FIELD:
                    continue
                if tags and k in tags:
                    data['tags'][k] = v
                else:
                    data['fields'][k] = v
        return data

    def write_points(self, table, points):
        pts = []
        for p in points:
            if p.time == None:
                p.time = datetime.datetime.now()
            pts.append(self._point_to_db_data(p, table))
        if pts:
            self.db.client.write_points(pts, database=self.db.dbname)
        return len(pts)

    def _get_where_ql_with_query(self, query):
        import six
        td = self._get_table_define(query.table)
        where = {}
        options = query.options
        if options.get('filter'):
            for k, v in options.get('filter').items():
                if k in td.get('tags', {}) and not isinstance(v, six.string_types):
                    # tags
                    v = str(v)
                where[k] = ('=', v)
        if options.get('time_start'):
            where['time'] = ('>=', self._to_db_time(options['time_start']))
        if options.get('time_end'):
            where['time'] = ('<=', self._to_db_time(options['time_end']))
        if where:
            wql = ' AND '.join(['"%s" %s %s' % (k, v[0], "'%s'" % v[1] if isinstance(v[1], six.string_types) else v[1]) for k, v in where.items()])
            return wql

    def _create_influxql_with_query(self, query, fields=None):
        if fields == None:
            fields = '*'
        q = 'SELECT %s FROM "%s"' % (fields, query.table)
        w = self._get_where_ql_with_query(query)
        if w:
            q += ' WHERE %s' % w
        # if self.timezone:
        #     q += " tz('%s')" % self.timezone
        return q

    def _exec_influxql(self, ql):
        # print('--->exec influxql:', ql)
        return self.db.client.query(ql, database=self.db.dbname)

    def _db_data_to_point(self, data, define):
        pdata = {}
        fieldmap = define['_field_map']
        for k, v in data.items():
            if k == TIME_FIELD or k == define['_count_field']:
                continue
            t = fieldmap.get(k)
            if t:
                if t == 'int':
                    v = int(v)
                elif t == 'float':
                    v = float(v)
                elif t == 'string':
                    v = str(v)
            pdata[k] = v
        pt = TSDBPoint(time=self._to_point_time(data.get(TIME_FIELD)), data=pdata)
        return pt

    def _fetch_with_resultset(self, resultset, query):
        td = self._get_table_define(query.table)
        for d in resultset.get_points():
            yield self._db_data_to_point(d, td)

    def fetch_with_query(self, query):
        return self._fetch_with_resultset(self._exec_influxql(self._create_influxql_with_query(query, fields='*')))

    def count_with_query(self, query):
        key = self._get_table_define(query.table)['_count_field']
        rs = self._exec_influxql(self._create_influxql_with_query(query, fields='COUNT("%s") as "%s"' % (key, key)))
        count = 0
        for r in rs.get_points():
            count = r[key]
            break
        return count

    def delete_with_query(self, query):
        q = 'DELETE FROM "%s"' % (query.table)
        w = self._get_where_ql_with_query(query)
        if w:
            q += ' WHERE %s' % w
        self._exec_influxql(q)

    def first_with_query(self, query):
        return self.getitem_with_query(query, 0)

    def last_with_query(self, query):
        return self.getitem_with_query(query, -1)

    def getitem_with_query(self, query, item):
        q = self._create_influxql_with_query(query, fields='*')
        if type(item) == slice:
            if item.start < 0:
                count = self.count_with_query(query)
                item = slice(max(0, item.start + count), count)
            q += ' LIMIT %d OFFSET %d' % (item.stop - item.start, item.start)
            return self._fetch_with_resultset(self._exec_influxql(q), query)
        else:
            if item < 0:
                count = self.count_with_query(query)
                item = max(0, item + count)
            q += ' LIMIT %d OFFSET %d' % (1, item)
            for pt in self._fetch_with_resultset(self._exec_influxql(q), query):
                return pt

    def drop_table(self, table):
        self._exec_influxql('DROP MEASUREMENT "%s"' % table)

    def close(self):
        pass
