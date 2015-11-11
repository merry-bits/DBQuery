# DBQuery

Simplify your database access.

A comfortable database configuration and query wrapper for the Python DB-API.


## Example

Sample code for connecting to an existing SQLite database and
printing some rows from a table named world:

```Python
    from dbquery import SQLiteDB
    
    db = SQLiteDB('<MY DATABASE>')
    get_hello = db.Select('SELECT hello FROM world WHERE id=?')
    for i in (123, 456):
        rows = get_hello(i)
        row_0 = rows[0]
        hello = row_0[0]
        print(hello)
```

The example can be simplified by assuming that each SQL execution only returns
exactly one row:

```Python
    from dbquery import SQLiteDB
    
    db = SQLiteDB('<MY DATABASE>')
    get_hello = db.SelectOne('SELECT hello FROM world WHERE id=?')
    for i in (123, 456):
        hello = get_hello(i)
        print(hello)
```


### Set up a database for the example code


```
    $ sqlite3 test.db
    sqlite> CREATE TABLE world (id INTEGER, hello VARCHAR);
    sqlite> INSERT INTO world VALUES (123, 'a'), (456, 'b');
```

With data you can use `test.db` as database in the above examples. Just be
sure you call python from the same directory as where the database file is.


## Supported databases

- SQLite
- PostgreSQL (requires the presence of [Psycopg2](http://initd.org/psycopg/)


# Documentation

The [Python DB-API](https://www.python.org/dev/peps/pep-0249/) specifies
connections and cursors for executing SQL. DBQuery is designed to hide this
complexity when it is not needed. Instead it provides a DB and a Query class
for executing SQL. The DB (or one of its sub classes) save the connection
information and provide access to the Query classes with use this to execute
the provided SQL.

This way a it is possible to handle SQL queries as callable functions:
```python
    db = DB(configuration)
    get_user = SelectOne(
        "Select email, first_name FROM users WHERE user_id=?")
    email, first_name = get_user(123)
```

What is more, if the connection to a database gets lost DBQuery can
automatically try to reconnect up to a specified count of retries:
```python
    db = DB(configuration, retry=3)  # retry 3 time to reconnect
```


## Configuration

The exact behavior depends on the actual DB implementation for a specific
database. In general all configuration parameters are passed to the DB
constructor. Usually a connection to the database will not be opened until the
first query is made


### SQLiteDB

`database, **kwds` parameters of the SQLiteDB constructor will be passed on
the the SQLite connect function.


## PostgreSQL

Accepts either the DSN string or configuration parameters for the Psqycopg2
connect function as keyword parameters.


## Query

Execute a SQL query without being interested in any result. It is the base
class for all other queries. Overwrite `_produce_return` if you are
interested in creating your own class that does something with the cursor that
executed the query.


### Manipulation

Use this to execute any INSERT, UPDATE and similar queries when the rowcount
of the cursor should be returned. It is possible to automatically check the
value of the row count by setting the rowcount parameter. If the resulting
row count does not match the provided one a ManipulationCheckError will be
raised.

This can be used to for example make sure that only one row was updated by a
query:
```python
    update_user_name = db.Manipulation(
        "UPDTAE users SET name=%s WHERE id=%s", rowcount=1)
    with db:  # start a new transaction, does not work with SQLiteDB!
        update_user_name("new_name", 123)  # does a roll back if rowcount != 1
```

### Select

Returns the result of `fetchall()` thus making it ideal for SELECT queries. 


### SelectOne

Check that only one row is returned by the specified query. Returns `None`
otherwise. If the result row contains only one column then only that columns
value will be returned:
```python
    get_first_name = db.SelectOne("SELECT first_name FROM users where id=?")
    first_name = get_first_name(123) 
```


### Transaction

The DB instance acts as a context manager for starting a connection on
entering the context and committing the queries in between in exit. If an
exception happens a `rollback` call will be made instead.

`SQLiteDB` does not implement this feature.
