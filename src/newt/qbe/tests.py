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
        self.assertEqual("create index newt_foo_idx on newt ((state ->> 'x'))",
                         self.qbe['x'].index_sql('foo'))


        from newt.qbe import scalar
        self.qbe['x'] = scalar("state -> 'x'")
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual("create index newt_foo_idx on newt ((state ->> 'x'))",
                         self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = scalar('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))
        self.assertEqual(b"(state ->> 'x')::int",
                         self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "create index newt_foo_idx on newt (((state ->> 'x')::int))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = scalar("state ->> 'x'")
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') = 'y')", self.qbe.sql(dict(x='y')))
        self.assertEqual("create index newt_foo_idx on newt ((state ->> 'x'))",
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
        from newt.qbe import array
        self.qbe['x'] = array('x')
        self.conn.commit()
        self.assertEqual(
            b"array(select value from jsonb_array_elements_text(state -> 'x'))"
            b" && ARRAY['a', 'b', 'c']",
            self.qbe.sql(dict(x=['a', 'b', 'c'])))
        self.assertEqual(
            b"array(select value from jsonb_array_elements_text(state -> 'x'))",
            self.qbe['x'].order_by(self.cursor, None))
        self.assertEqual(
            "create index newt_foo_idx on newt using gin (array("
            "select value from jsonb_array_elements_text(state -> 'x')))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = array('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"array("
            b"select value::int from jsonb_array_elements_text(state -> 'x')"
            b")"
            b" && ARRAY[0, 1, 2]",
            self.qbe.sql(dict(x=[0,1,2])))
        self.assertEqual(
            b"array("
            b"select value::int from jsonb_array_elements_text(state -> 'x')"
            b")",
            self.qbe['x'].order_by(self.cursor, None))
        self.assertEqual(
            "create index newt_foo_idx on newt using gin (array("
            "select value::int from jsonb_array_elements_text(state -> 'x')))",
            self.qbe['x'].index_sql('foo'))

        self.qbe["x"] = array("state->0-> 'z' ", type='int')
        self.conn.commit()
        self.assertEqual(
            b"array("
            b"select value::int from jsonb_array_elements_text("
            b"state->0-> 'z' "
            b")"
            b")"
            b" && ARRAY[0, 1, 2]",
            self.qbe.sql(dict(x=[0,1,2])))
        self.assertEqual(
            b"array("
            b"select value::int from jsonb_array_elements_text("
            b"state->0-> 'z' "
            b")"
            b")",
            self.qbe['x'].order_by(self.cursor, None))
        self.assertEqual(
            "create index newt_foo_idx on newt using gin (array("
            "select value::int from jsonb_array_elements_text(state->0-> 'z' )"
            "))",
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
            "create index newt_foo_idx on newt "
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
            "create index newt_foo_idx on newt "
            "(path(state) text_pattern_ops)",
            self.qbe['x'].index_sql('foo'))

    def test_fulltext(self):
        from newt.qbe import fulltext

        self.qbe['x'] = fulltext('x')
        self.conn.commit()
        self.assertEqual(b"to_tsvector(state ->> 'x') @@ to_tsquery('y')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(
            b"ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector(state ->> 'x'),"
            b" to_tsquery('y'))",
            self.qbe['x'].order_by(self.cursor, 'y'))
        self.assertEqual(
            "create index newt_foo_idx on newt using gin "
            "(to_tsvector(state ->> 'x'))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = fulltext('x', config='klingon')
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
            "create index newt_foo_idx on newt using gin "
            "(to_tsvector('klingon', state ->> 'x'))",
            self.qbe['x'].index_sql('foo'))

        self.qbe['x'] = fulltext('x', config='klingon',
                                 weights=(.2, .3, .5, .7))
        self.conn.commit()
        self.assertEqual(
            b"ts_rank_cd({0.2, 0.3, 0.5, 0.7},"
            b" to_tsvector('klingon', state ->> 'x'),"
            b" to_tsquery('klingon', 'y'))",
            self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = fulltext('x', parser=crazy_parse)
        self.conn.commit()
        self.assertEqual(b"to_tsvector(state ->> 'x') @@ to_tsquery('CRAZY y')",
                         self.qbe.sql(dict(x='y')))

    def test_sql(self):
        from newt.qbe import sql

        self.qbe['x'] = sql('%s = 42', "%s")
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=(42,))))
        self.assertEqual(b"42", self.qbe['x'].order_by(self.cursor, (42,)))

        self.qbe['x'] = sql('%s = 42', b"42")
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=(42,))))
        self.assertEqual(b"42", self.qbe['x'].order_by(self.cursor, (42,)))

        self.qbe['x'] = sql('%s = 42')
        self.conn.commit()
        self.assertEqual(b"42 = 42", self.qbe.sql(dict(x=(42,))))
        self.assertEqual(None, self.qbe['x'].order_by(self.cursor, (42,)))

    def test_qbe(self):
        from newt.qbe import prefix, array, fulltext

        self.qbe['path'] = prefix('path(state)', delimiter='/')
        self.qbe['can_view'] = array('can_view(state)')
        self.qbe['text'] = fulltext('text')
        self.conn.commit()

        self.assertEqual(
            b"can_view(state) && ARRAY['p1', 'g2'] AND\n"
            b"  (path(state) like '/foo' || '/%') AND\n"
            b"  to_tsvector(state ->> 'text') @@ to_tsquery('thursday')\n"
            b"ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector(state ->> 'text'),"
            b" to_tsquery('thursday'))",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by='text'),
            )

        self.assertEqual(
            b"can_view(state) && ARRAY['p1', 'g2'] AND\n"
            b"  (path(state) like '/foo' || '/%') AND\n"
            b"  to_tsvector(state ->> 'text') @@ to_tsquery('thursday')\n"
            b"ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector(state ->> 'text'),"
            b" to_tsquery('thursday')) DESC",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by=[('text', True)],
                         )
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
