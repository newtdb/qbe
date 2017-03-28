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

    def tearDown(self):
        self.conn.close()
        super(QBETests, self).tearDown()

    def test_scalar(self):
        from newt.qbe import scalar
        self.qbe['x'] = scalar('x')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))

        self.qbe['x'] = scalar('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))

        self.qbe['x'] = scalar("state ->> 'x'")
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x') = 'y')",
            self.qbe.sql(dict(x='y')))

        self.qbe['x'] = scalar("state ->> 'x'", type='int')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x')::int = 1)",
            self.qbe.sql(dict(x=1)))

        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x')::int <= 1)",
            self.qbe.sql(dict(x=(None, 1))))

        self.assertEqual(
            b"SELECT * FROM newt WHERE ((state ->> 'x')::int >= 1)",
            self.qbe.sql(dict(x=(1, None))))

        self.assertEqual(
            b"SELECT * FROM newt WHERE "
            b"(((state ->> 'x')::int >= 1) and"
            b" ((state ->> 'x')::int <= 2))",
            self.qbe.sql(dict(x=(1,2))))

    def test_array(self):
        from newt.qbe import array
        self.qbe['x'] = array('x')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE "
            b"array(select value from jsonb_array_elements_text(state -> x))"
            b" && ARRAY['a', 'b', 'c']",
            self.qbe.sql(dict(x=['a', 'b', 'c'])))

        self.qbe['x'] = array('x', type='int')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE "
            b"array("
            b"select value::int from jsonb_array_elements_text(state -> x)"
            b")"
            b" && ARRAY[0, 1, 2]",
            self.qbe.sql(dict(x=[0,1,2])))

        self.qbe["x"] = array("state->0-> 'z' ", type='int')
        self.conn.commit()
        self.assertEqual(
            b"SELECT * FROM newt WHERE "
            b"array("
            b"select value::int from jsonb_array_elements_text("
            b"state->0-> 'z' "
            b")"
            b")"
            b" && ARRAY[0, 1, 2]",
            self.qbe.sql(dict(x=[0,1,2])))
