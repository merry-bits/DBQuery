# -*- coding: utf-8 -*-
from psycopg2 import connect, OperationalError as PGOperationalError

from .db import DB
from .query import SelectOne


class _NextVal(SelectOne):

    def __init__(self, db, sequence):
        super(_NextVal, self).__init__(
            db, 'SELECT nextval(\'{}\')'.format(sequence), None)


class PostgresDB(DB):
    """ PostgreSQL DB class using a single psycopg2 connection.

    Use either a 'dsn' connection string or keyword parameter to define the
    connection (from the psycopg2 documentation):
        database – the database name (only as keyword argument)
        user – user name used to authenticate
        password – password used to authenticate
        host – database host address (defaults to UNIX socket if not provided)
        port – connection port number (defaults to 5432 if not provided)
    """

    OperationalError = PGOperationalError

    def __init__(self, dsn=None, retry=0, **kwds):
        super(PostgresDB, self).__init__(retry=retry)
        self._kwds = kwds or {}
        if dsn:
            self._kwds["dsn"] = dsn
        self._connection = None

    def _connect(self):
        if self._connection is not None:
            raise RuntimeError("Connection still exists.")
        self._connection = connect(**self._kwds)
        self._connection.set_session(autocommit=True)

    def close(self):
        if self._connection is not None:
            try:
                self._connection.close()
            except Exception:
                pass  # ignore
            self._connection = None

    @DB.connected
    def execute(self, sql, params, return_function=None):
        with self._connection.cursor() as c:
            c.execute(sql, params)
            if return_function:
                return return_function(c)

    @DB.connected
    def show(self, sql, params):
        with self._connection.cursor() as cursor:
            return cursor.mogrify(sql, params).decode(
                self._connection.encoding)

    def NextVal(self, sequence):
        return _NextVal(self, sequence)

    @DB.connected
    def _begin(self):
        self._connection.autocommit = False

    def _commit(self):
        if self._connection is None:
            raise RuntimeError("Connection lost, can not commit!")
        self._connection.commit()
        self._connection.autocommit = True

    def _rollback(self):
        if self._connection is None:
            raise RuntimeError("Connection lost, can not roll back!")
        self._connection.rollback()
        self._connection.autocommit = True
