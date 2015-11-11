# -*- coding: utf-8 -*-
from sqlite3 import connect, OperationalError as SQL3OperationalError

from .db import DB


class SQLiteDB(DB):
    """ SQLite DB class.
    From https://docs.python.org/3/library/sqlite3.html
    Needs at least:
     - 'database': database file/URI

    No support for transactions.
    No support for read_obj.
    """

    OperationalError = SQL3OperationalError

    def __init__(self, database, retry=0, **kwds):
        super(SQLiteDB, self).__init__(retry=retry)
        self._database = database
        self._kwds = kwds
        self._connection = None

    def _connect(self):
        """Try to create a connection to the database if not yet connected.
        """
        if self._connection is not None:
            raise RuntimeError('Close connection first.')
        self._connection = connect(self._database, **self._kwds)
        self._connection.isolation_level = None  # auto commit

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass  # ignore
            self._connection = None

    @DB.connected
    def execute(self, sql, params, produce_return):
        c = self._connection.execute(sql, params)
        return produce_return(c)

    def show(self, sql, params):
        """SQLite does not provide a function for showing the actual, formated
        SQL. This function just returns the SQL and the parameters as a string.
        """
        return '{} {}'.format(sql, params)
