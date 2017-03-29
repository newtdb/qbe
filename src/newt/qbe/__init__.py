import contextlib
from newt.db.search import read_only_cursor
import persistent
import persistent.mapping
import re
import six


is_identifier = re.compile(r'\w+$').match
is_access = re.compile(r"state\s*(->\s*(\d+|'\w+')\s*)+$").match

class Search(persistent.Persistent):

    def order_by(self, cursor, query):
        return self.expr.encode('ascii')

class scalar(Search):

    def __init__(self, expr, type=''):
        if is_identifier(expr):
            expr = 'state ->> %r' % expr

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))


        expr = '(' + expr + ')'
        if type:
            expr = '%s::%s' % (expr, type)
        self.expr = expr
        d = dict(expr=expr)
        self._range = '((%(expr)s >= %%s) and (%(expr)s <= %%s))' % d
        self._eq =    '(%(expr)s = %%s)'                          % d
        self._ge =    '(%(expr)s >= %%s)'                         % d
        self._le =    '(%(expr)s <= %%s)'                         % d

    def __call__(self, cursor, query):
        if not isinstance(query, tuple):
            return cursor.mogrify(self._eq, (query,))

        min, max = query
        if min is None:
            return cursor.mogrify(self._le, (max,))
        elif max is None:
            return cursor.mogrify(self._ge, (min,))
        else:
            return cursor.mogrify(self._range, (min, max))

class array(Search):

    def __init__(self, expr, type=''):
        if is_identifier(expr):
            expr = "state -> %r" % expr

        if is_access(expr):
            expr = (
                "array(select value%s from jsonb_array_elements_text(%s))" % (
                    '::' + type if type else '',
                    expr
                    )
                )
        else:
            expr = '(' + expr + ')'

        self.expr = expr
        self._any = self.expr + ' && %s'

    def __call__(self, cursor, query):
        return cursor.mogrify(self._any, (query,))

class prefix(Search):

    def __init__(self, expr, delimiter=''):
        if is_identifier(expr):
            expr = 'state ->> %r' % expr

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))

        expr = '(' + expr + ')'
        self.expr = expr

        self._like = "(%s like %%s || '%s%%%%')" % (expr, delimiter)

    def __call__(self, cursor, query):
        return cursor.mogrify(self._like, (query,))

class fulltext(Search):

    def __init__(self, expr,
                 config=None,
                 parser=None,
                 weights=(.1, .2, .4, 1.0),
                 ):
        if is_identifier(expr):
            expr = "state -> %r" % expr

        config = repr(config) + ', ' if config else ''

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))
            expr = 'to_tsvector(%s%s)' % (config, expr)
        else:
            expr = '(' + expr + ')'

        self.expr = expr

        self._search = "%s @@ to_tsquery(%s%%s)" % (expr, config)
        d, c, b, a = weights
        self._order = "ts_rank_cd({%g, %g, %g, %g}, %s, to_tsquery(%s%%s))" % (
            d, c, b, a, expr, config)

        self.parser = parser
        self.weights = weights

    def __call__(self, cursor, query):
        if self.parser is not None:
            query = self.parser(query)
        return cursor.mogrify(self._search, (query,))

    def order_by(self, cursor, query):
        if self.parser is not None:
            query = self.parser(query)
        return cursor.mogrify(self._order, (query,))

class QBE(persistent.mapping.PersistentMapping):

    def sql(self, query, order_by=None, desc=False):
        with contextlib.closing(read_only_cursor(self._p_jar)) as cursor:
            result = []
            wheres = [self[name](cursor, q) for name, q in query.items()]
            if wheres:
                result.append(
                    b' AND '.join(
                        self[name](cursor, q)
                        for name, q in sorted(query.items())
                        )
                    )
            if order_by:
                result.append(b' ORDER BY ' +
                              self[order_by].order_by(cursor, query[order_by]))
                if desc:
                    result.append(b' DESC')

        return b''.join(result)
