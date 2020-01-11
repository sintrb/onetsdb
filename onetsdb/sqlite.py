# -*- coding: UTF-8 -*
'''
Created on 2019-12-16
'''
from __future__ import print_function

import datetime
import sqlite3

from .base import TSDBPoint, TSDBBase

TIME_FIELD = '_time'


class SqliteTSDB(TSDBBase):
    '''
    Wrapper for sqlite3
    '''

    def __init__(self, con=sqlite3.Connection):
        self.con = con
        self._table_define = {}

    def _execute(self, sql, *args):
        # nsql = sql % args
        # print('sql', sql, args if args else '')
        return self.con.execute(sql, args)

    def _get_table_define(self, table):
        if table not in self._table_define:
            self._table_define[table] = self._get_table_define_from_db(table)
        return self._table_define[table]

    def _get_table_fields(self, table):
        td = self._get_table_define(table)
        fields = [TIME_FIELD]
        for k in td.get('tags', {}):
            fields.append(k)
        for k in td.get('fields', {}):
            fields.append(k)
        fields.sort()
        return fields

    def _set_table_define(self, table, define):
        self._table_define[table] = define

    def _to_db_field_type(self, tp):
        return {
            # 'string': 'CHAR(255)',
        }.get(tp, tp).upper()

    def _from_db_field_type(self, tp):
        return {
            # 'string': 'CHAR(255)',
        }.get(tp, tp).lower()

    def _is_exists_table(self, table):
        r = self._execute('SELECT COUNT(*) FROM sqlite_master WHERE type = "table" AND tbl_name = ?', table).fetchmany(1)
        return r[0][0]

    def _get_table_define_from_db(self, table):
        if self._is_exists_table(table):
            # 存在表
            tags = {}
            fields = {}
            for r in self._execute('SELECT * FROM sqlite_master WHERE type = "index" AND tbl_name = ?', table):
                if not r[4]:
                    continue
                tk = r[1]
                k = tk.replace('%s_' % table, '', 1)
                tags[k] = None
            for r in self._execute('PRAGMA table_info(`%s`)' % table):
                k = r[1]
                t = r[2]
                if k == TIME_FIELD:
                    continue
                if k in tags:
                    tags[k] = self._from_db_field_type(t)
                else:
                    fields[k] = self._from_db_field_type(t)
            return {
                'tags': tags,
                'fields': fields
            }
        else:
            # 创建表
            return None

    def _define_to_dict(self, define):
        newdef = {
            k: {'index': True, 'type': v} for k, v in define['tags'].items()
        }
        newdef.update({
            k: {'index': False, 'type': v} for k, v in define['fields'].items()
        })
        return newdef

    def register_table(self, table, options):
        define = self._get_table_define(table)
        options.setdefault('tags', {})
        options.setdefault('fields', {})
        if not define:
            # 需创建表
            sql = 'CREATE TABLE %s (`%s` DATETIME PRIMARY KEY NOT NULL);' % (table, TIME_FIELD)
            self._execute(sql)
            define = {'tags': {}, 'fields': {}}
        # 无需创建
        rawdef = self._define_to_dict(define).copy()
        newdef = self._define_to_dict(options)
        for k, d in newdef.items():
            rd = rawdef.pop(k, None)
            if rd and rd['type'] == d['type'] and rd['index'] == d['index']:
                continue
            if not rd:
                # 不存在之前的
                self._execute('ALTER TABLE `%s` add `%s` %s;' % (table, k, self._to_db_field_type(d['type'])))
                rd = {'index': False, 'type': d['type']}
            if rd['type'] != d['type']:
                # 修改类型
                pass

            if rd['index'] != d['index']:
                if rd['index']:
                    # 删除索引
                    self._execute('DROP INDEX `%s_%s`;' % (table, k))
                else:
                    # 创建索引
                    self._execute('CREATE INDEX `%s_%s` on `%s`(`%s`);' % (table, k, table, k))
        for k, d in rawdef.items():
            # 删除
            # 暂无法删除
            if d['index']:
                options['tags'][k] = d['type']
            else:
                options['fields'][k] = d['type']
                # self._execute('ALTER TABLE `%s` DROP COLUMN `%s`;' % (table, k))
        define.update(options)
        self._set_table_define(table, define)
        self.commit()

    def _to_db_time(self, tm):
        if isinstance(tm, datetime.datetime):
            tm = tm.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return tm

    def _to_point_time(self, tm):
        if not isinstance(tm, datetime.datetime):
            tm = datetime.datetime.strptime(tm, '%Y-%m-%dT%H:%M:%S.%fZ')
        return tm

    def write_points(self, table, points):
        td = self._get_table_define(table)
        tmd = self._define_to_dict(td)
        for p in points:
            if p.time == None:
                p.time = datetime.datetime.now()
            # pts.append(self._point_to_db_data(p, table))
            vals = [(TIME_FIELD, self._to_db_time(p.time))]
            for k, v in p.data.items():
                if k not in tmd:
                    continue
                vals.append((k, v), )
            self._execute('INSERT INTO `%s` (%s) VALUES (%s);' % (table,
                                                                  ','.join(['`%s`' % r[0] for r in vals]),
                                                                  ','.join('?' * len(vals))), *[r[1] for r in vals])
        if points:
            self.con.commit()
        return len(points)

    def _get_where_sql_with_query(self, query):
        where = {}
        options = query.options
        if options.get('filter'):
            for k, v in options.get('filter').items():
                where[k] = ('=', v)
        if options.get('time_start'):
            where[TIME_FIELD] = ('>=', self._to_db_time(options['time_start']))
        if options.get('time_end'):
            where[TIME_FIELD] = ('<=', self._to_db_time(options['time_end']))
        if where:
            import six
            wql = ' AND '.join(['"%s" %s %s' % (k, v[0], "'%s'" % v[1] if isinstance(v[1], six.string_types) else v[1]) for k, v in where.items()])
            return wql

    def _create_sql_with_query(self, query, fields=None):
        if not fields or fields == '*':
            fields = ','.join(['`%s`' % f for f in self._get_table_fields(query.table)])
        q = 'SELECT %s FROM %s' % (fields, query.table)
        w = self._get_where_sql_with_query(query)
        if w:
            q += ' WHERE %s' % w
        return q

    def _db_data_to_point(self, data):
        pt = TSDBPoint(time=self._to_point_time(data.get(TIME_FIELD)))
        pt.data = {
            k: v for k, v in data.items() if k != TIME_FIELD
        }
        return pt

    def _fetch_with_queryset(self, query, resultset):
        fields = self._get_table_fields(query.table)
        for r in resultset:
            d = dict(zip(fields, r))
            yield self._db_data_to_point(d)

    def fetch_with_query(self, query):
        return self._fetch_with_queryset(query, self._execute(self._create_sql_with_query(query)))

    def count_with_query(self, query):
        rs = self._execute(self._create_sql_with_query(query, fields='COUNT(`%s`)' % (TIME_FIELD))).fetchmany(1)
        count = rs[0][0]
        return count

    def delete_with_query(self, query):
        q = 'DELETE FROM %s' % (query.table)
        w = self._get_where_sql_with_query(query)
        if w:
            q += ' WHERE %s' % w
        self._execute(q)

    def first_with_query(self, query):
        return self.getitem_with_query(query, 0)

    def last_with_query(self, query):
        return self.getitem_with_query(query, -1)

    def getitem_with_query(self, query, item):
        q = self._create_sql_with_query(query)
        if type(item) == slice:
            if item.start < 0:
                count = self.count_with_query(query)
                item = slice(max(0, item.start + count), count)
            q += ' LIMIT %d OFFSET %d' % (item.stop - item.start, item.start)
            return self._fetch_with_queryset(query, self._execute(q))
        else:
            if item < 0:
                count = self.count_with_query(query)
                item = max(0, item + count)
            q += ' LIMIT %d OFFSET %d' % (1, item)
            for pt in self._fetch_with_queryset(query, self._execute(q)):
                return pt

    def drop_table(self, table):
        try:
            self._execute('DROP TABLE `%s`' % table)
        except:
            pass
        self.commit()

    def close(self):
        self.con.close()

    def commit(self):
        self.con.commit()
