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

        from newt.qbe import scalar
        self.qbe['x'] = scalar("state -> 'x'")
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = scalar('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))
        self.assertEqual(b"(state ->> 'x')::int",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = scalar("state ->> 'x'")
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') = 'y')", self.qbe.sql(dict(x='y')))

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

    def test_prefix(self):
        from newt.qbe import prefix

        self.qbe['x'] = prefix('x')
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = prefix('x', delimiter='/')
        self.conn.commit()
        self.assertEqual(b"((state ->> 'x') like 'y' || '/%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = prefix("state -> 0 -> 'x'")
        self.conn.commit()
        self.assertEqual(b"((state -> 0 ->> 'x') like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(state -> 0 ->> 'x')",
                         self.qbe['x'].order_by(self.cursor, 'y'))

        self.qbe['x'] = prefix("path(state)")
        self.conn.commit()
        self.assertEqual(b"((path(state)) like 'y' || '%')",
                         self.qbe.sql(dict(x='y')))
        self.assertEqual(b"(path(state))",
                         self.qbe['x'].order_by(self.cursor, 'y'))

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

    def test_qbe(self):
        from newt.qbe import prefix, array, fulltext

        self.qbe['path'] = prefix('path(state)', delimiter='/')
        self.qbe['can_view'] = array('can_view(state)')
        self.qbe['text'] = fulltext('text')
        self.conn.commit()

        self.assertEqual(
            b"(can_view(state)) && ARRAY['p1', 'g2'] AND"
            b" ((path(state)) like '/foo' || '/%') AND"
            b" to_tsvector(state ->> 'text') @@ to_tsquery('thursday')"
            b" ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector(state ->> 'text'),"
            b" to_tsquery('thursday'))",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by='text'),
            )

        self.assertEqual(
            b"(can_view(state)) && ARRAY['p1', 'g2'] AND"
            b" ((path(state)) like '/foo' || '/%') AND"
            b" to_tsvector(state ->> 'text') @@ to_tsquery('thursday')"
            b" ORDER BY ts_rank_cd({0.1, 0.2, 0.4, 1},"
            b" to_tsvector(state ->> 'text'),"
            b" to_tsquery('thursday')) DESC",
            self.qbe.sql(dict(path="/foo",
                              can_view=['p1', 'g2'],
                              text="thursday"),
                         order_by='text', desc=True),
            )

def crazy_parse(q):
    return 'CRAZY ' + q
