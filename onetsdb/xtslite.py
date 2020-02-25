# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''
from __future__ import print_function
import datetime, time
from .base import TSDBPoint, TSDBBase

TIME_FIELD = 'time'
TIME_ALTZONE = time.altzone


class TsliteTSDB(TSDBBase):
    '''
    Wrapper for tslite
    '''

    def __init__(self, db=None):
        self.db = db

    def register_table(self, table, options):
        tb = self.db.get_table(table)
        fields = []
        fields += [
            {'name': f, 'type': t, 'index': True}
            for f, t in options.get('tags', {}).items()
        ]
        fields += [
            {'name': f, 'type': t, 'index': False}
            for f, t in options.get('fields', {}).items()
        ]
        tb.define({
            'field_lock': True,
            'fields': fields
        })

    def _to_db_time(self, tm):
        if isinstance(tm, datetime.datetime):
            tm = time.mktime(tm.timetuple()) + tm.microsecond / 1000000.0
        return tm

    def _to_point_time(self, tm):
        if isinstance(tm, float) or isinstance(tm, int):
            return datetime.datetime.fromtimestamp(tm)
        return tm

    def _point_to_db_data(self, point):
        data = {
            TIME_FIELD: self._to_db_time(point.time),
        }
        data.update(point.data)
        return data

    def _db_data_to_point(self, data):
        if data != None:
            pdata = {}
            for k, v in data.items():
                if k == TIME_FIELD:
                    continue
                pdata[k] = v
            pt = TSDBPoint(time=self._to_point_time(data.get(TIME_FIELD)), data=pdata)
            return pt

    def write_points(self, table, points):
        pts = []
        for p in points:
            if p.time == None:
                p.time = datetime.datetime.now()
            pts.append(self._point_to_db_data(p))
        if pts:
            self.db.get_table(table).write_datas(pts)
        return len(pts)

    def _get_cursor_with_query(self, query):
        tab = self.db.get_table(query.table)
        return tab.query(start=self._to_db_time(query.options['time_start']) if query.options.get('time_start') else None,
                         end=self._to_db_time(query.options['time_end']) if query.options.get('time_end') else None,
                         eqs=query.options.get('filter'))

    def fetch_with_query(self, query):
        cursor = self._get_cursor_with_query(query)
        for r in cursor:
            yield self._db_data_to_point(r)

    def count_with_query(self, query):
        return self._get_cursor_with_query(query).count()

    def delete_with_query(self, query):
        raise NotImplemented

    def first_with_query(self, query):
        return self._db_data_to_point(self._get_cursor_with_query(query).first())

    def last_with_query(self, query):
        return self._db_data_to_point(self._get_cursor_with_query(query).last())

    def getitem_with_query(self, query, item):
        cursor = self._get_cursor_with_query(query)
        if type(item) == slice:
            def f():
                if type(item) == slice:
                    for d in cursor[item]:
                        yield self._db_data_to_point(d)

            return f()
        else:
            return self._db_data_to_point(cursor[item])

    def drop_table(self, table):
        self.db.drop_table(table)

    def close(self):
        self.db.close()

    def commit(self):
        self.db.commit()
