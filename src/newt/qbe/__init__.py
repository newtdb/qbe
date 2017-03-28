import contextlib
import persistent.mapping
import re
import six

is_identifier = re.compile(r'\w+$').match
is_access = re.compile(r"state\s*(->\s*(\w+|'\w+')\s*)+$").match

class scalar:

    def __init__(self, expr, type=''):
        if is_identifier(expr):
            expr = 'state ->> %r' % expr
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

class array:

    def __init__(self, expr, type=''):
        if is_identifier(expr):
            expr = "state -> " + expr

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

class prefix:

    def __init__(self, expr, delimiter=''):
        if is_identifier(expr):
            expr = 'state ->> %r' % expr
        expr = '(' + expr + ')'
        self.expr = expr

        self._like = "%s like %%s || '%s%%%%'" % (expr, delimiter)

    def __call__(self, cursor, query):
        return cursor.mogrify(self._like, query)

class fulltext:

    def __init__(self, expr, config=None, parser=None):
        if identifier(expr):
            expr = "state -> " + expr

        if is_access(expr):
            expr = '>>'.join(expr.rsplit('>', 1))
            expr = 'to_tsvector(%s%s)' % (repr(config) + ', ' if config else '',
                                          expr)
        else:
            expr = '(' + expr + ')'

        self.expr = expr

        self._search = '%s @@ %%s' % expr

    def __call__(self, cursor, query):
        if self.parser is not None:
            query = self.parser(query)
        return cursor.mogrify(self._search, query)

class QBE(persistent.mapping.PersistentMapping):

    def sql(self, query, order_by=None, desc=False):
        with contextlib.closing(self._p_jar._storage.ex_cursor()) as cursor:
            result = [b'SELECT * FROM newt']
            wheres = [self[name](cursor, q) for name, q in query.items()]
            if wheres:
                result.append(b' WHERE ')
                result.append(
                    b' AND '.join(
                        self[name](cursor, q) for name, q in query.items()
                        )
                    )
            if order_by:
                result.append(b' ORDER BY ' + self[order_by].expr)
                if desc:
                    result.append(b' DESC')

        return b''.join(result)
