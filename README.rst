DBQuery: makes database queries easy
===================================

.. image:: https://api.travis-ci.org/merry-bits/DBQuery.svg?branch=master
    :target: https://travis-ci.org/merry-bits/DBQuery?branch=master

A comfortable database configuration and query wrapper for the Python DB-API.


Example
-------

Sample code for connecting to an existing SQLite database and
printing some rows from a table named world:

.. code-block:: python

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

    >>> import dbquery
    >>> db = dbquery.SQLiteDB(":memory")  # or whatever your database is
    >>> get_user = db.SelectOne(
    ...    "SELECT email, first_name FROM users WHERE user_id=?")
    >>> email, first_name = get_user(123)

What is more, if the connection to a database gets lost DBQuery can
automatically try to reconnect up to a specified count of retries:

.. code-block:: python

    >>> db = dbquery.db.DB(configuration, retry=3)  # retry to connect 3 times

**Note**: Per default a ``DB`` connection does not attempt any retry at all!


The two ways of working with a ``DB`` object
--------------------------------------------

DBQuery allows for two different usage scenarios. The first one (as shown in
the examples above) uses a global ``DB`` object:

.. code-block:: python

    >>> import dbquery
    >>> db = dbquery.SQLiteDB(":memory")
    >>> say_hello = db.SelectOne("select 'hello'")
    >>> say_hello()
    'hello'

The other scenario works through injection and allows to define queries
without a ``DB`` object. The configuration gets added at runtime. Assuming for
example that ``handle_request`` is a function that should do something useful
like handling a HTTP request then a query could be used like this:

.. code-block:: python

    >>> import dbquery
    >>> # Define the query:
    >>> say_hello_query = dbquery.SelectOne("select 'hello'")
    >>> # Create a "resource" using DBMixin which will add any query to self
    >>> # and inject the DB connection object.
    >>> class HelloResource(dbquery.DBMixin):
    ...   say_hello = say_hello_query  # make the query usable for an instance
    ...   def handle_request(self, *request_parameters):
    ...     print(self.say_hello())
    ... 
    >>> # Now create a resource instance, giving it a DB connection object and
    >>> # "make" a request, executing the query:
    >>> resource = HelloResource(db=dbquery.SQLiteDB(":memory"))
    >>> resource.handle_request()
    hello

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

**Note**: ``SQLiteDB`` does not implement this feature, yet.

Example:

.. code-block:: python

    >>> db = ...
    >>> query1 = db.Manipulation("UPDATE world SET hello='HELLO'")
    >>> query2 = db.Manipulation("UPDATE world SET hello='H E L L O'")
    >>> with db as transaction:
    ...   query1()
    ...   query2()
    ...   transaction.abort_transaction()
    ...   print("not reached")
    2
    2
    >>> get_one_hello(456)
    'another hello'

Query
-----

Executes a SQL query without being interested in any result. It is the base
class for all other queries.

Overwrite ``_produce_return`` if you are interested in creating your own class
that does something with the cursor that executed the query.


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
    ...    "UPDTAE users SET name=%s WHERE id=%s", rowcount=1)
    >>> with db:  # start a new transaction, does not work with SQLiteDB!
    ...    update_user_name("new_name", 123)  # roll back if rowcount != 1


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
    >>> first_name = get_first_name(123)

