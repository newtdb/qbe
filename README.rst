==============================
Newt Query by Example
==============================

Newt QBE is a small `Newt DB <http://www.newtdb.org>`_ add-on that
provides help building SQL ``WHERE`` clauses for searching Newt DB
databases, especially when search is driven by form input.  It also
provides some abstraction of JSONB-search-expression details.

It is **not a goal** of of newt.qbe to replace use of SQL.  PostgreSQL is
powerful and well documented.  You'll get more out of Newt DB if you
understand how to apply it when searching Newt databases.  To that end,
this package can provide some SQL construction hints.

.. contents::

Overview
=========

To set up Newt QBE, you'll create a QBE object, add some items to it,
and add it to your database:

    >>> import newt.db, newt.qbe
    >>> qbe = newt.qbe.QBE()
    >>> qbe['email'] = newt.qbe.scalar('email')
    >>> qbe['stars'] = newt.qbe.scalar("state->'rating'->'stars'", type='int')
    >>> qbe['keywords'] = newt.qbe.text_array('keywords')
    >>> qbe['path'] = newt.qbe.prefix('path', delimiter='/')
    >>> qbe['text'] = newt.qbe.fulltext('content_text(state)', 'english')
    >>> conn = newt.db.connection(dsn)
    >>> conn.root.qbe = qbe
    >>> conn.commit()

Then, you can generate SQL using the ``sql`` method, which takes a
dictionary of item names and values:

    >>> sql = qbe.sql(dict(path='/foo', text='newt'))
    >>> result = conn.where(sql)

In addition to a criteria mapping, you can supply an ``order_by``
keyword argument to specify sorting information.

The items in a QBE are search helpers.  There are several built-in
helpers:

scalar
  Search scalar data

text_array
  Search text-array data

prefix
  Search string scalars by prefix.

fulltext
  Full-text search of text fields

sql
  Searcher that uses provided SQL

You can also define your own helpers by implementing a fairly simple
API.  A search helper provides the following methods, only one of
which is required:

``__call__(cursor, query)``
  Compute the helper's contribution to s search ``WHERE`` clause.

  This required method returns a bytes SQL expression.

  The required format of the query is up to the helper.

  The helper must use the `mogrify
  <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_ method
  of the cursor argument to substitute data from the query.

``order_by(cursor, query)``
  Compute a PostgreSQL expression to be used in an ``ORDER BY`` clause.

  This required method returns a bytes SQL expression.

  This method is optional. If it is not provided, then ordering on the
  helper won't be allowed.

  The helper must use the `mogrify
  <http://initd.org/psycopg/docs/cursor.html#cursor.mogrify>`_ method
  of the cursor argument to substitute data from the query, if necessary.

``index_sql(name)``
  Return a PostgreSQL string to be used to create a corresponding index.

  This method is optional.

The constructor arguments and search criteria are specific to each helper.

QBE methods
===========

QBE objects provide the following methods:

``sql(query, order_by=())``
---------------------------

Return contents of a PostgreSQL ``WHERE`` clause (as bytes).

An SQL boolean expression is returned by combining expressions given
in the query.  (If the query is empty, then ``'true'`` is returned.)

The query argument must be a mapping object. The keys must also
exist in the QBE.  The values, who's format is helper specific, are
passed to helper's ``__call__`` methods.

The ``order_by`` argument is an iterable of ordering criteria.  The items may
be helper names or two-tuples containing helper names and descending flags.

To illustrate the usage, here are some examples using the QBE object
created in the overview section:

  >>> qbe.sql(dict())
  b'true'

  >>> print(qbe.sql(dict(text='database', path='/wiki'),
  ...               order_by=[('stars', True), 'text']).decode('ascii'))
  (((state ->> 'path') || '/') like '/wiki' || '/%') AND
    content_text(state) @@ to_tsquery('english', 'database')
  ORDER BY (state->'rating'->>'stars')::int DESC,
    ts_rank_cd({0.1, 0.2, 0.4, 1}, content_text(state), to_tsquery('english', 'database'))

``index_sql(*names)``
---------------------

Return PostgreSQL text to create indexes for the given helpers.  If no
helpers are specified, then statements for all of the helpers (that
implement the optional ``index_sql`` method) are returned).

    >>> print(qbe.index_sql())
    create index newt_email_idx on newt ((state ->> 'email'));
    create index newt_keywords_idx on newt using gin ((state -> 'keywords'));
    create index newt_path_idx on newt (((state ->> 'path') || '/') text_pattern_ops);
    create index newt_stars_idx on newt (((state->'rating'->>'stars')::int));
    create index newt_text_idx on newt using gin (content_text(state))

Built-in helpers
================

``scalar(expr, type=None)``
---------------------------

The ``scalar`` helper searches based on scalar values.  The constructor
takes an expression that yields a text result.  For convenience, if an
identifier (for example ``'email'``) is given, then it will be
computed to an expression for accessing a top-level property.  Also,
for convenience, if a simple JSON accessor expression, like::

  state -> 'x' -> 0

it will be modified to produce a text result::

  state -> 'x' ->> 0

You can supply an optional second argument giving the name of a
PostgreSQL data type to convert the text value to.

``text_array(expr)``
--------------------

The ``array`` helper searches based on text-array values. The constructor takes
an expression that yields a PostgreSQL JSONB array of text.

Searches are based on overlap. Search criteria are satisfied if
searched values have elements in common with the given query
value. For example, a query: ``['a', 'b']`` matches stored JSON
``["a", "c"]``.

For convenience, if an identifier is given, it's converted to a JSON
expression.

``prefix(expr, delimiter=None)``
--------------------------------

The ``prefix`` helper supports prefix queries against scalar text values.
This will often be used for path searches.

The constructor takes an expression that yields a text result.  As
with the scalar helper, an identifier or JSON accessor will be
converted to an expression, if necessary.

An optional second argument may be provided giving a path delimiter.
If provided, the delimiter will be included in ``like`` queries.  If
an expression is generated from an identifier or simpler JSON
accessor, then the delimiter will be included in the generated
expression as well.

``fulltext(expr, config, parer=None, weights=(.1, .2, .4, 1.0)``
---------------------------------------------------------------------

The ``fulltext`` helper supports full-text search.  The constructor
takes an expression that evaluates to a PostgreSQL `ts_vector
<https://www.postgresql.org/docs/current/static/datatype-textsearch.html#DATATYPE-TSVECTOR>`_
and the name of a `test-search configuration
<https://www.postgresql.org/docs/current/static/textsearch-intro.html#TEXTSEARCH-INTRO-CONFIGURATIONS>`_.

For convenience, if an identifier or a JSON accessor (like ``state ->
'x' -> 0``) is given, a tsvector expression is generated.

When searching, queries are provided as strings that are passed
`to_tsquery
<https://www.postgresql.org/docs/current/static/textsearch-controls.html#TEXTSEARCH-PARSING-QUERIES>`_. An
optional query parser function may be provided to transform the search
queries.

If a text helper is used for ordering, the `ts_rank_cd function
<https://www.postgresql.org/docs/current/static/textsearch-controls.html#TEXTSEARCH-RANKING>`_
will be called with the supplied weights.

``sql(cond, order=None)``
-------------------------

The ``sql`` helper provides a way to encapsulate more or less arbitrary
SQL as a search helper.  The constructor takes an string SQL
expression to use when searching.  The string should contain a single
`placeholder
<http://initd.org/psycopg/docs/usage.html#passing-parameters-to-sql-queries>`_
for substituting query data.

An optional second argument provides an SQL expression to use for
ordering.

Status
======

This project is in an early stage of development.  The built-in
helpers cover common cases.  Initial helpers are convenient for the
initial application for which this is being developed.  It's easy to
imagine future enhancements.  Contributions and suggestions are
welcome, especially when motivated by specific needs.

It's worth noting that the ``sql`` helper can cover a lot of gaps.
For example the initial applications needs to search against
PostgreSQL arrays returned from functions, rather than JSON arrays.
This is easily handled by the ``sql`` helper::

  sql("allowed_to_view(state) && %s")
