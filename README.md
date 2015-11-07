# dbaccess

Simplify your database access.

A comfortable database configuration and access wrapper for the Python DB-API.


## Example

Sample code for connectig to an existing SQLite database and
printing some rows from a table named world:

```Python
    from dbaccess import SQLiteDB
    
    db = SQLiteDB(0, '<MY DATABASE>')
    select_hello = db.Select('SELECT hello FROM world WHERE id=?')
    for i in (123, 456):
        rows = select_hello(i)
        row_0 = rows[0]
        hello = row_0[0]
        print(hello)
```

The example can be simplified by assuming that each SQL execution only returns
exactly one row:

```Python
    from dbaccess import SQLiteDB
    
    db = SQLiteDB(0, '<MY DATABASE>')
    select_hello = db.SelectOne('SELECT hello FROM world WHERE id=?')
    for i in (123, 456):
        hello = select_hello(i)
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


# Documentation


## Configuration


## Query


### Manipulation


### Select


### SelectOne


### Transaction


# Get involved


## Todo


### Make it into a package


### Add RowObject


### Add support for other databases
