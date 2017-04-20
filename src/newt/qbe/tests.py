import os
import unittest
import newt.db.tests.base

class QBETests(newt.db.tests.base.TestCase):

    maxDiff = None

    def setUp(self):
        super(QBETests, self).setUp()
        import newt.qbe
        self.conn = newt.db.connection(self.dsn)
        self.conn.root.qbe = self.qbe = newt.qbe.QBE()
        self.conn.commit()
        from newt.db.search import read_only_cursor
        self.cursor = read_only_cursor(self.conn)

    def tearDown(self):
        self.cursor.close()
        self.conn.close()
        super(QBETests, self).tearDown()

    def test_scalar(self):
        from newt.qbe import scalar
        self.qbe['x'] = scalar('x')
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual("CREATE INDEX CONCURRENTLY newt_foo_idx"
                         " ON newt ((state ->> 'x'))",
                         self.qbe['x'].index_sql('foo'))


        from newt.qbe import scalar
        self.qbe['x'] = scalar("state -> 'x'")
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual("CREATE INDEX CONCURRENTLY newt_foo_idx"
                         " ON newt ((state ->> 'x'))",
                         self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = scalar('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))
        self.assertEqual(b"(state ->> 'x')::int",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx"
            " ON newt (((state ->> 'x')::int))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = scalar("state ->> 'x'")
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') = 'y')", self.qbe.sql(dict(x='y')))
        self.assertEqual("CREATE INDEX CONCURRENTLY newt_foo_idx"
                         " ON newt ((state ->> 'x'))",
                         self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = scalar("state ->> 'x'", type='int')
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))

        self.assertEqual(
            b"((state ->> 'x')::int <= 1)",
            self.qbe.sql(dict(x=(None, 1))))

        self.assertEqual(
            b"((state ->> 'x')::int >= 1)",
            self.qbe.sql(dict(x=(1, None))))

        self.assertEqual(
            b"(((state ->> 'x')::int >= 1) and"
            b" ((state ->> 'x')::int <= 2))",
            self.qbe.sql(dict(x=(1,2))))
        self.assertEqual(b"(state ->> 'x')::int",
                         self.qbe['x'].order_by(self.cursor, 'y'))

    def test_array(self):
        from newt.qbe import text_array
        self.qbe['x'] = text_array('x')
        self.conn.commit()
        self.assertEqual(
            b"(state -> 'x')"
            b" ?| ARRAY['a', 'b', 'c']",
            self.qbe.sql(dict(x=['a', 'b', 'c'])))
        self.assertEqual(
            b"(state -> 'x')",
            self.qbe['x'].order_by(self.cursor, None))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx"
            " ON newt USING GIN ((state -> 'x'))",
            self.qbe['x'].index_sql('foo'))

        self.qbe["x"] = text_array("state->0-> 'z' ")
        self.conn.commit()
        self.assertEqual(b"(state->0-> 'z' ) ?| ARRAY[0, 1, 2]",
                         self.qbe.sql(dict(x=[0,1,2])))
        self.assertEqual(
            b"(state->0-> 'z' )",
            self.qbe['x'].order_by(self.cursor, None))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx"
            " ON newt USING GIN ((state->0-> 'z' ))",
            self.qbe['x'].index_sql('foo'))

    def test_prefix(self):
        from newt.qbe import prefix

        self.qbe['x'] = prefix('x')
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx ON newt "
            "((state ->> 'x') text_pattern_ops)",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = prefix('x', delimiter='/')
        self.conn.commit()
        self.assertEqual(b"(((state ->> 'x') || '/') like 'y' || '/%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"((state ->> 'x') || '/')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = prefix("state -> 0 -> 'x'")
        self.conn.commit()
        self.assertEqual(b"((state -> 0 ->> 'x') like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state -> 0 ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = prefix("path(state)")
        self.conn.commit()
        self.assertEqual(b"(path(state) like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"path(state)",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx ON newt "
            "(path(state) text_pattern_ops)",
            self.qbe['x'].index_sql('foo'))

    def test_fulltext(self):
        from newt.qbe import fulltext

        self.qbe['x'] = fulltext('x', 'klingon')
        self.conn.commit()
        self.assertEqual(
            b"to_tsvector('klingon', state ->> 'x') @@"
            b" to_tsquery('klingon', 'y')",
            self.qbe.sql(dict(x='y')))
        self.assertEqual(
            b"ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector('klingon', state ->> 'x'),"
            b" to_tsquery('klingon', 'y'))",
            self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "CREATE INDEX CONCURRENTLY newt_foo_idx ON newt USING GIN "
            "(to_tsvector('klingon', state ->> 'x'))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = fulltext('x', 'klingon', weights=(.2, .3, .5, .7))
        self.conn.commit()
        self.assertEqual(
            b"ts_rank_cd({0.2, 0.3, 0.5, 0.7},"
            b" to_tsvector('klingon', state ->> 'x'),"
            b" to_tsquery('klingon', 'y'))",
            self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = fulltext('x', 'xxx', parser=crazy_parse)
        self.conn.commit()
        self.assertEqual(
            b"to_tsvector('xxx', state ->> 'x') @@"
            b" to_tsquery('xxx', 'CRAZY y')",
            self.qbe.sql(dict(x='y')))

    def test_sql(self):
        from newt.qbe import sql

        self.qbe['x'] = sql('%s = 42', "%s")
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=42)))
        self.assertEqual(b"42", self.qbe['x'].order_by(self.cursor, 42))

        self.qbe['x'] = sql('%s = 42', b"42")
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=42)))
        self.assertEqual(b"42", self.qbe['x'].order_by(self.cursor, 42))

        self.qbe['x'] = sql('%s = 42')
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=42)))
        self.assertEqual(None, self.qbe['x'].order_by(self.cursor, 42))

    def test_qbe(self):
        from newt.qbe import prefix, text_array, fulltext

        self.qbe['path'] = prefix('path(state)', delimiter='/')
        self.qbe['can_view'] = text_array('can_view(state)')
        self.qbe['text'] = fulltext('text', 'english')
        self.conn.commit()

        self.assertEqual(
            b"can_view(state) ?| ARRAY['p1', 'g2'] AND\n"
            b"  (path(state) like '/foo' || '/%') AND\n"
            b"  to_tsvector('english', state ->> 'text') @@"
            b" to_tsquery('english', 'thursday')\n"
            b"ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector('english', state ->> 'text'),"
            b" to_tsquery('english', 'thursday'))",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by='text'),
            )

        self.assertEqual(
            b"can_view(state) ?| ARRAY['p1', 'g2'] AND\n"
            b"  (path(state) like '/foo' || '/%') AND\n"
            b"  to_tsvector('english', state ->> 'text') @@"
            b" to_tsquery('english', 'thursday')\n"
            b"ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector('english', state ->> 'text'),"
            b" to_tsquery('english', 'thursday')) DESC",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by=[('text', True)],
                         )
            )

    def test_integration(self):
        qbe = self.qbe
        from newt.qbe import scalar, text_array, prefix, fulltext, sql
        qbe['stars'] = scalar('stars', 'int')
        qbe['allowed'] = text_array('allowed')
        qbe['path'] = prefix('path', delimiter='/')
        qbe['text'] = fulltext('text', 'english')
        qbe['ends'] = sql("state ->> 'path' like '%%' || %s")

        self.conn.root()._p_jar.explicit_transactions = True
        self.conn.commit()

        from contextlib import closing
        with closing(newt.db.pg_connection(self.dsn)) as conn:
            conn.autocommit = True
            with closing(conn.cursor()) as cursor:
                for sql in qbe.index_sql():
                    cursor.execute(sql)

        from newt.db import Object
        root = self.conn.root
        root.content = (
            Object(
                stars = 5,
                allowed = ['all'],
                path = '/db/newt_review',
                text = 'the best database is newt',
                ),
            Object(
                stars = 4,
                allowed = ['victoria', 'will'],
                path = '/db/secret_review',
                text = 'newt uses ZODB',
                ),
            Object(
                stars = 3,
                allowed = ['victoria', 'paul'],
                path = '/db/summary',
                text = 'We have two newt reviews',
                ),
            Object(
                stars = 2,
                allowed = ['paul', 'mary'],
                path = '/news/friday',
                text = 'qbe is nearing release',
                ),
            )
        self.conn.commit()
        where = self.conn.where


        self.assertEqual(
            ['newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(stars=4)))],
            )

        self.assertEqual(
            ['qbe is nearing release', 'We have two newt reviews',
             'newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(stars=(None, 4)),
                                           order_by=['stars']))],
            )

        self.assertEqual(
            ['We have two newt reviews', 'newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(stars=(3, 4)),
                                           order_by=['stars']))],
            )

        self.assertEqual(
            ['We have two newt reviews', 'newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(stars=(None, 4), path='/db'),
                                           order_by=['stars']))],
            )

        self.assertEqual(
            ['newt uses ZODB', 'We have two newt reviews'],
            [o.text for o in where(qbe.sql(dict(stars=(None, 4), path='/db'),
                                           order_by=[('stars', True)]))],
            )

        self.assertEqual(
            ['We have two newt reviews', 'newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(allowed=['victoria', 'bob']),
                                           order_by=['stars']))],
            )

        self.assertEqual(
            ['qbe is nearing release', 'We have two newt reviews',
             'newt uses ZODB'],
            [o.text for o in where(qbe.sql(dict(allowed=['victoria', 'paul']),
                                           order_by=['stars']))],
            )

        self.assertEqual(
            ['newt uses ZODB', 'the best database is newt'],
            [o.text for o in where(qbe.sql(dict(ends='review'),
                                           order_by=['stars']))],
            )

def crazy_parse(q):
    return 'CRAZY ' + q

README = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'README.rst')
if os.path.exists(README):
    def test_suite():
        import doctest
        import newt.db.tests.testdocs
        import re
        from zope.testing import renormalizing, setupstack

        def setUp(test):
            newt.db.tests.testdocs.setUp(test)
            conn = newt.db.connection(test.globs['dsn'])
            conn.create_text_index('content_text', 'text')
            conn.close()

        return unittest.TestSuite((
            unittest.makeSuite(QBETests),
            doctest.DocFileSuite(
                '../../../README.rst',
                setUp=setUp,
                tearDown=setupstack.tearDown,
                checker=renormalizing.RENormalizing([
                    (re.compile("b'true'"), "'true'"),
                    ]),
                )
            ))
