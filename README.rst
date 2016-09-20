DBQuery: make database queries easy
===================================

.. image:: https://api.travis-ci.org/merry-bits/DBQuery.svg?branch=master
    :target: https://travis-ci.org/merry-bits/DBQuery?branch=master

A comfortable database configuration and query wrapper for the Python DB-API.


Example
-------

Sample code for connecting to an existing SQLite database and
printing some rows from a table named world:

.. code-block:: python

    >>> import dbquery
    >>> db = dbquery.SQLiteDB('test.db')
    >>> get_hello = db.Select('SELECT hello FROM world WHERE id=?')
    >>> for hello_id in (123, 456):
    ...     rows = get_hello(hello_id)
    ...     print(rows)  # list of row tuples
    ...
    [('hello',)]
    [('another hello',)]

Using ``SelectOne`` instead of ``Select`` this can be simplified even further:

.. code-block:: python

    >>> get_one_hello = db.SelectOne('SELECT hello FROM world WHERE id=?')
    >>> for hello_id in (123, 456):
    ...     hello = get_one_hello(hello_id)
    ...     print(hello)  # content of the hello column
    ...
    hello
    another hello


Set up a database for the example code
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

    $ sqlite3 test.db
    sqlite> CREATE TABLE world (id INTEGER, hello VARCHAR);
    sqlite> INSERT INTO world VALUES (123, 'hello'), (456, 'another hello');

With this you can use ``test.db`` as database in the above examples. Just be
sure you call python from the same directory as where the database file is.


Supported databases
-------------------

* SQLite
* PostgreSQL (requires the presence of
  `Psycopg2 <http://initd.org/psycopg/>`_)


Installation
============

.. code-block:: bash

    $ pip install dbquery


Documentation
=============

The `Python DB-API <https://www.python.org/dev/peps/pep-0249/>`_ specifies
connections and cursors for executing SQL. DBQuery is designed to hide this
complexity when it is not needed. Instead it provides a DB and a Query class
for executing SQL. ``DB`` (or one of its sub classes like ``SQLiteDB``) saves
the connection information and provides access to the ``Query`` classes which
use this to execute the provided SQL.

This way a it is possible work with SQL queries as if they where functions:

.. code-block:: python

    >>> db = dbquery.db.DB(<CONFIGURATION>)
    >>> get_user = db.SelectOne(
    ...    "Select email, first_name FROM users WHERE user_id=?")
    >>> email, first_name = get_user(123)

What is more, if the connection to a database gets lost DBQuery can
automatically try to reconnect up to a specified count of retries:

.. code-block:: python

    >>> db = dbquery.db.DB(configuration, retry=3)  # retry to connect 3 times


Configuration
-------------

The exact behavior depends on the actual DB implementation for a specific
database. In general all configuration parameters are passed to the DB
constructor. Usually a connection to the database will not be opened until the
first query is made


SQLiteDB
^^^^^^^^

``database, **kwds`` parameters of the SQLiteDB constructor will be passed on
the the SQLite connect function.


PostgreSQL
^^^^^^^^^^

Accepts either the DSN string or configuration parameters for the Psqycopg2
connect function as keyword parameters.


Transaction
-----------

The DB instance acts as a context manager for starting a connection on
entering the context and committing the queries in between in exit. If an
exception happens a ``rollback`` call will be made instead.

**Note**: ``SQLiteDB`` does not implement this feature.


Query
-----

Executes a SQL query without being interested in any result. It is the base
class for all other queries.

Overwrite ``_produce_return`` if you are interested in creating your own class
that does something with the cursor that executed the query.


QueryCursor
^^^^^^^^^^^

Executes the given SQL then returns the curser for direct access. Use within
a context. Exiting the context will close the cursor.

For example perform a `fetchone`:

.. code-block:: python

    >>> get_first_name_cursor = db.QueryCursor(
    ...     "SELECT first_name FROM users where id=?")
    >>> with get_first_name_cursor(123) as cursor:
    ...     print(cursor.fetchone())
    ...
    ('Foo',)
    >>>


Manipulation
^^^^^^^^^^^^

Use this to execute any ``INSERT``, ``UPDATE`` and similar queries when the
``rowcount`` of the cursor should be returned. It is possible to automatically
check the value of the row count by setting the ``rowcount`` parameter. If the
resulting row count does not match the provided one a ManipulationCheckError
will be raised.

This can be used to for example make sure that only one row was updated by a
query:

.. code-block:: python

    >>> update_user_name = db.Manipulation(
    ...    "UPDATE users SET first_name=? WHERE id=?", rowcount=1)
    >>> with db:  # start a new transaction, does not work with SQLiteDB!
    ...    update_user_name("new_name", 123)  # roll back if rowcount != 1
    ...
    1
    >>>


Select
^^^^^^

Returns the result of ``fetchall()``, making it ideal for SELECT queries.


SelectOne
^^^^^^^^^

Checks that only one row is returned by the specified query. Returns ``None``
otherwise. If the result row contains only one column then only that columns
value will be returned:

.. code-block:: python

    >>> get_first_name = db.SelectOne(
    ...     "SELECT first_name FROM users where id=?")
    >>> get_first_name(123)
    'Foo'
    >>>


SelectIterator
^^^^^^^^^^^^^^

Select rows and precess them in chunks. For this purpose ``SelectIterator``
requeires a callback function together with the SQL. This callback will at
query time be called with a generator which produces all the rows from the
query result, directly streamed from the DB, in blocks of specified size
(``arraysize``).

It is possible to specify additional parameters for the callback function, if
needed.

.. code-block:: python

    >>> def callback(row_generator):
    ...     for row in row_generator:
    ...         print(row)
    ...
    >>> get_first_names = db.SelectIterator(
    ...     "SELECT first_name FROM users", callback)
    >>> get_first_names()
    ('Foo',)
    >>>


Changelog
=========


v0.4.1
------

* Added `SelectIterator` iteration `QueryCursor` which allows direct cursor
  access


v0.4.0
------

* Added `SelectIterator`
