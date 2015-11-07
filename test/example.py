# -*- coding: utf-8 -*-
"""
DBAccess example., using SQLite.

Shows the use of set_configuration, Manipulation, Select and SelectOne.
"""
from random import randrange

from dbaccess import SQLiteDB


if __name__ == '__main__':
    # Create a SQLite connection to an in-memory database.
    db = SQLiteDB(':memory:')

    # Create a Manipulation which can create a table.
    create_world_table = db.Manipulation('CREATE TABLE world (hello INTEGER)')
    # Execute the SQL.
    create_world_table()

    # Create a new Manipulation which can insert data.
    insert_into_world = db.Manipulation('INSERT INTO world VALUES (?)')
    # Insert some random values.
    for _ in range(3):
        row_count = insert_into_world(randrange(100))
        print('Inserted rows: ', row_count)

    # Create a Select which can read those values.
    select_hello = db.Select('SELECT hello FROM world')
    # Print values.
    for hello_value in select_hello():
        print(hello_value[0])

    # Count the rows.
    print('Row count: ', db.SelectOne('SELECT count(*) FROM world')())
