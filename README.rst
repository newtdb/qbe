==============================
Newt Query by Example
==============================

Newt QBE is a small Newt add-on that provides help buildouting SQL
where clauses for searching Newt, especially when search is driven
by form input.  It also provides some abstraction of JSONB-search-expression
details.

It is **not a goal** of of newt.qbe to replace use of SQL.  PostgreSQL is
powerful and well documented.  You'll get more out of newt if you
understand how to apply it when searching Newt datbases.  To that end,
this package can provide some SQL construction hints.

Overview
=========

To set up Newt QBE, you'll create a QBE object, add some items to it,
and add it to your database:

    >>> import newt.db, newt.qbe
    >>> qbe = newt.qbe.QBE()
    >>> qbe['email'] = newt.qbe.scalar('email')
    >>> qbe['stars'] = newt.qbe.scalar("state->'rating'->'stars'", type='int')
    >>> qbe['keywords'] = newt.qbe.array('keywords')
    >>> qbe['path'] = newt.qbe.prefix('path', delimiter='/')
    >>> qbe['text'] = newt.qbe.text('content_text(state)')
    >>> conn = newt.db.connection(dsn)
    >>> conn.root.qbe = qbe
    >>> conn.commit()

Then, you can generate sql using the sql method, which takes a
dictionary of item names and values:

    >>> sql = qbe.sql(dict(path='/foo', text='newt')
    >>> result = conn.where(sql)

In addition to a criteria mapping, you can supply ``order_by`` and desc
keywords to specify sorting information.  Order by specified the name
of a helper and ``desc`` is a boolean value that must be ``True`` or
``False``.  It defaults to ``False``.

The items in a qbe are search helpers.  There are serveral built-in
helpers:

scalar
  Search scalar data

array
  Search array data

prefix
  Search string scalars by prefix.

text
  Full-text search of text fields

You can also define your own helpers by implementing a fairly simple API.

The constructor arguments and search criteria are specific to each helper.

