# -*- coding: UTF-8 -*
'''
Created on 2019-10-22
'''
from __future__ import print_function
import datetime
from .base import str_types, try_parset_datetime_str
from .base import TSDBException, TSDBPoint, TSDBBase
from .base import Max, Min, Mean, Sum, Count, First, Last

TIME_FIELD = '_time'
MONGOID_FIELD = '_id'


class MongoTSDB(TSDBBase):
    '''
    Wrapper for mongodb
    '''

    def __init__(self, db=None):
        self.db = db

    def _get_collection(self, name):
        return self.db.get_collection(name)

    def _to_db_time(self, tm):
        if isinstance(tm, datetime.datetime):
            return tm

    def _to_point_time(self, tm):
        if isinstance(tm, datetime.datetime):
            return tm
        elif isinstance(tm, str_types):
            return try_parset_datetime_str(tm)

    def _point_to_db_data(self, point):
        data = {
            TIME_FIELD: self._to_db_time(point.time),
        }
        if point.data:
            data.update(point.data)
        return data

    def _db_data_to_point(self, data):
        pt = TSDBPoint(time=self._to_point_time(data.get(TIME_FIELD)))
        pt.data = {
            k: v for k, v in data.items() if k not in [TIME_FIELD, MONGOID_FIELD]
        }
        return pt

    def write_points(self, table, points):
        '''
        write points
        '''
        col = self._get_collection(table)
        count = 0
        for p in points:
            if p.time == None:
                p.time = datetime.datetime.now()
            col.save(self._point_to_db_data(p))
            count += 1
        return count

    def _get_filter(self, query):
        ft = {}
        options = query.options
        if options.get('filter'):
            ft.update(options.get('filter'))
        tmft = {}
        if options.get('time_start'):
            tmft['$gte'] = self._to_db_time(options['time_start'])
        if options.get('time_end'):
            tmft['$lte'] = self._to_db_time(options['time_end'])
        if tmft:
            ft[TIME_FIELD] = tmft
        return ft

    def _get_aggregate_exp(self, ag):
        if isinstance(ag, Sum):
            return {'$sum': '$%s' % ag.field}
        elif isinstance(ag, Count):
            return {'$sum': 1}
        elif isinstance(ag, Max):
            return {'$max': '$%s' % ag.field}
        elif isinstance(ag, Min):
            return {'$min': '$%s' % ag.field}
        elif isinstance(ag, Mean):
            return {'$avg': '$%s' % ag.field}
        elif isinstance(ag, Mean):
            return {'$avg': '$%s' % ag.field}
        elif isinstance(ag, First):
            return {'$first': '$%s' % ag.field}
        elif isinstance(ag, Last):
            return {'$last': '$%s' % ag.field}
        raise TSDBException('Unknown Aggregate: %s' % ag)

    def _get_value_pipes(self, query):
        options = query.options
        ps = []
        tg = options.get('time_group')
        if options.get('values'):
            vd = {v.field: 1 for k, v in options['values'].items() if v.field and isinstance(v.field, str_types)}
            tgf = {
                'year': "%Y-01-01",
                'month': "%Y-%m-01",
                'day': "%Y-%m-%d",
                'hour': "%Y-%m-%d %H:00:00",
                'minute': "%Y-%m-%d %H:%M:00",
            }[tg]
            if tg:
                vd[TIME_FIELD] = {'$dateToString': {'format': tgf, 'date': "$" + TIME_FIELD}}
            ps.append({'$project': vd})
        if tg:
            gd = {
                MONGOID_FIELD: "$" + TIME_FIELD,
                TIME_FIELD: {"$first": '$' + TIME_FIELD},
            }
            for k, v in options['values'].items():
                e = self._get_aggregate_exp(v)
                gd[k] = e
            ps.append({'$group': gd})
        return ps

    def _get_cursor_with_query(self, query):
        from pymongo import ASCENDING
        col = self._get_collection(query.table)
        ft = self._get_filter(query)
        vs = self._get_value_pipes(query)
        if not vs:
            cursor = col.find(ft)
            cursor = cursor.sort(TIME_FIELD, ASCENDING)
        else:
            ps = []
            if ft:
                ps.append({'$match': ft})
            ps += vs
            ps.append({'$sort': {TIME_FIELD: ASCENDING}})
            cursor = col.aggregate(ps)
        return cursor

    def _fetch_with_cursor(self, cursor):
        for d in cursor:
            yield self._db_data_to_point(d)

    def fetch_with_query(self, query):
        cursor = self._get_cursor_with_query(query)
        return self._fetch_with_cursor(cursor)

    def first_with_query(self, query):
        cursor = self._get_cursor_with_query(query)
        try:
            return self._db_data_to_point(cursor[0])
        except IndexError:
            return None

    def last_with_query(self, query):
        cursor = self._get_cursor_with_query(query)
        try:
            count = cursor.count()
            return self._db_data_to_point(cursor[count - 1]) if count else None
        except IndexError:
            return None

    def delete_with_query(self, query):
        col = self._get_collection(query.table)
        ft = self._get_filter(query)
        col.delete_many(ft)

    def count_with_query(self, query):
        col = self._get_collection(query.table)
        ft = self._get_filter(query)
        return col.find(ft).count()

    def register_table(self, table, options):
        col = self._get_collection(table)
        col.create_index(TIME_FIELD)
        for k in options.get('tags', {}).keys():
            col.create_index(k)

    def getitem_with_query(self, query, item):
        cursor = self._get_cursor_with_query(query)
        if type(item) == slice:
            if item.start < 0:
                count = cursor.count()
                item = slice(max(0, item.start + count), count)
            return self._fetch_with_cursor(cursor[item])
        else:
            if item < 0:
                count = cursor.count()
                item = max(0, item + count)
            return self._db_data_to_point(cursor[item])

    def drop_table(self, table):
        self._get_collection(table).drop()

    def close(self):
        pass

    def commit(self):
        pass
