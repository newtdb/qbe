import contextlib
import json
from newt.db.search import read_only_cursor
import re
import six


is_identifier = re.compile(r'\w+$').match
is_access = re.compile(r"state\s*(->\s*(\d+|'\w+')\s*)+$").match
is_paranthesized = re.compile("\w*[(].+[)]$").match

class Convertible(object):

    def convert(self, v):
        return v

class match(Convertible):

    def __init__(self, name, convert=None):
        self.name = name
        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        query = self.convert(query)
        return cursor.mogrify('(state @> %s::jsonb)',
                              (json.dumps({self.name: query}),))

class Search(Convertible):

    def order_by(self, cursor, query):
        return self.expr.encode('ascii')

class scalar(Search):

    def __init__(self, expr, type=None, convert=None):
        if is_identifier(expr):
            expr = 'state ->> %r' % expr

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))

        if not is_paranthesized(expr):
            expr = '(' + expr + ')'

        if type:
            expr = '%s::%s' % (expr, type)

        self.expr = expr
        d = dict(expr=expr)
        self._range = '((%(expr)s >= %%s) and (%(expr)s <= %%s))' % d
        self._eq =    '(%(expr)s = %%s)'                          % d
        self._ge =    '(%(expr)s >= %%s)'                         % d
        self._le =    '(%(expr)s <= %%s)'                         % d

        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        if not isinstance(query, tuple):
            return cursor.mogrify(self._eq, (self.convert(query),))

        min, max = query
        if min is None:
            max = self.convert(max)
            return cursor.mogrify(self._le, (max,))
        elif max is None:
            min = self.convert(min)
            return cursor.mogrify(self._ge, (min,))
        else:
            min = self.convert(min)
            max = self.convert(max)
            return cursor.mogrify(self._range, (min, max))

    def index_sql(self, name):
        expr = self.expr
        if not is_paranthesized(expr):
            expr = '(' + expr + ')'
        return "CREATE INDEX CONCURRENTLY newt_%s_idx ON newt (%s)" % (
            name, expr)

class text_array(Search):

    def __init__(self, expr, convert=None):
        if is_identifier(expr):
            expr = "(state -> %r)" % expr
        elif not is_paranthesized(expr):
            expr = '(' + expr + ')'

        self.expr = expr
        self._any = self.expr + ' && %s'

        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        query = self.convert(query)
        return cursor.mogrify(self._any, (query,))

    def index_sql(self, name):
        return ("CREATE INDEX CONCURRENTLY newt_%s_idx ON newt USING GIN (%s)" %
                (name, self.expr))

class prefix(Search):

    def __init__(self, expr, delimiter=None, convert=None):
        if is_identifier(expr):
            expr = 'state -> %r' % expr

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))
            if delimiter:
                expr = "(%s) || '%s'" % (expr, delimiter)

        if not is_paranthesized(expr):
            expr = '(' + expr + ')'

        self.expr = expr

        self._like = "(%s like %%s || '%s%%%%')" % (expr, delimiter or '')

        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        query = self.convert(query)
        return cursor.mogrify(self._like, (query,))

    def index_sql(self, name):
        return (
            "CREATE INDEX CONCURRENTLY newt_%s_idx"
            " ON newt (%s text_pattern_ops)" %
            (name, self.expr))

class fulltext(Search):

    def __init__(self, expr, config,
                 parser=None,
                 weights=(.1, .2, .4, 1.0),
                 convert=None
                 ):
        if is_identifier(expr):
            expr = "state -> %r" % expr

        config = repr(config) + ', '

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))
            expr = 'to_tsvector(%s%s)' % (config, expr)
        elif not is_paranthesized(expr):
            expr = '(' + expr + ')'

        self.expr = expr

        self._search = "%s @@ to_tsquery(%s%%s)" % (expr, config)
        d, c, b, a = weights
        self._order = (
            "ts_rank_cd(array[%g, %g, %g, %g], %s, to_tsquery(%s%%s))" % (
                d, c, b, a, expr, config))

        self.parser = parser
        self.weights = weights

        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        if self.parser is not None:
            query = self.parser(query)
        return cursor.mogrify(self._search, (query,))

    def order_by(self, cursor, query):
        if self.parser is not None:
            query = self.parser(query)
        return cursor.mogrify(self._order, (query,))

    def index_sql(self, name):
        return (
            "CREATE INDEX CONCURRENTLY newt_%s_idx ON newt USING GIN (%s)" %
            (name, self.expr))

class sql(Convertible):

    def __init__(self, cond, order=None, convert=None):
        self.cond = cond
        self.order = order

        if convert is not None:
            self.convert = convert

    def __call__(self, cursor, query):
        query = self.convert(query)
        return cursor.mogrify(self.cond, (query,))

    def order_by(self, cursor, query):
        if self.order:
            return cursor.mogrify(self.order, (query,))

class QBE(dict):

    def sql(self, conn, query, order_by=()):
        with contextlib.closing(read_only_cursor(conn)) as cursor:
            result = []
            wheres = [self[name](cursor, q) for name, q in query.items()]
            if wheres:
                result.append(
                    b' AND\n  '.join(
                        self[name](cursor, q)
                        for name, q in sorted(query.items())
                        )
                    )
            else:
                result.append(b'true')


            if order_by:
                if isinstance(order_by, str):
                    order_by = (order_by,)
                result.append(
                    b'\nORDER BY ' +
                    b',\n  '.join(
                        self[item].order_by(cursor, query.get(item))
                        if isinstance(item, str) else
                        self[item[0]].order_by(cursor, query.get(item[0]))
                        + (b' DESC' if item[1] else b'')
                        for item in order_by
                        ))

        return b''.join(result)

    def index_sql(self, *names):
        return [self[name].index_sql(name)
                for name in sorted(names or self)
                if hasattr(self[name], 'index_sql')
                ]
