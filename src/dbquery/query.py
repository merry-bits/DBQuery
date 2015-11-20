# -*- coding: utf-8 -*-
""" Query classes.

Simplify SQL execution, using a configured DB class.
"""
from logging import getLogger

from .log_msg import LogMsg


_LOG = getLogger(__name__)


def to_dict_formatter(row, cursor):
    """ Take a row and use the column names from cursor to turn the row into a
    dictionary.

    Note: converts column names to lower-case!

    :param row: one database row, sequence of column values
    :type row: (value, ...)
    :param cursor: the cursor which was used to make the query
    :type cursor: DB-API cursor object
    """
    # Empty row? Return.
    if not row:
        return row
    # No cursor? Raise runtime error.
    if cursor is None or cursor.description is None:
        raise RuntimeError("No DB-API cursor or description available.")

    # Give each value the appropriate column name within in the resulting
    # dictionary.
    column_names = (d[0] for d in cursor.description)  # 0 is the name
    return {name: value for value, name in zip(row, column_names)}


class Query(object):
    """ Base class for other SQL query classes.

    Only use Query directly if you need access to the DB-API cursor.
    """
    def __init__(self, db, sql):
        """
        :param db: The DB class to communicate with the database.
        :type db: db_access.db.DB
        :param sql: The SQL to execute.
        :type sql: str
        """
        self._db = db
        self._sql = sql  # save the SQL for later execution
        # The following variables will be filled in when a connection is
        # obtained by accessing the connection property.
        self.OperationalError = db.OperationalError
        self._connection = None

    def _produce_return(self, cursor):
        """ Gets called with the cursor on which the query was executed.

        Its return value will be the return value for the __call__ function.

        Do not return the cursor, since it might get closed after the function
        returns!

        :type cursor: DB-API cursor
        :return: Return value for __call__
        """
        return None

    def __call__(self, *args, **kwds):
        """ Execute the SQL with given parameters.

        Can only use either positional arguments or keyword arguments but not
        both at the same time. Positional arguments win.

        :rtype: Result of the _produce_return call.
        """
        # Use either args or kwds as parameter for the SQL execution. Mixing
        # does not work.
        params = args
        if not params:
            params = kwds

        # Try to execute the SQL through the slected connection.
        # If the connection is down try several times to open a new one.
        retry_count = 1
        while 1:  # either return or raise
            try:
                # Execute and return.
                return self._db.execute(
                    self._sql, params, self._produce_return)
            except self._db.OperationalError:
                # Usually means a connection problem, log and try to connect
                # again.
                if retry_count < self._db.retry:
                    _LOG.warning(
                        LogMsg(
                            "DB connection {} failed (retry {}).",
                            self._db, retry_count),
                        exc_info=1)
                    retry_count += 1
                    # Make sure that a new connection is established by closing
                    # the current (damaged or closed) one.
                    self._db.close()
                else:
                    # No (more) retry, raise the exception.
                    raise

    def show(self, *args, **kwds):
        """ Show how the SQL looks like when executed by the DB.

        This might not be supported by all connection types.
        For example: PostgreSQL does support it, SQLite does not.

        :rtype: str
        """
        # Same as in __call__, arguments win over keywords
        arg = args
        if not arg:
            arg = kwds
        return self._db.show(self._sql, arg)


class Select(Query):
    """ Useful for executing SELECT statements.

    Performs a DB-API fetchall and returns its row list when called.

    If a row formatter is provided each row will be passed through it first and
    a generator instead of a sequence will be returned.
    """

    def __init__(self, db, sql, row_formatter):
        """
        :param row_formatter: function that 'formats' a row, for example into
            a dictionary. take to_dict_formatter as an example if you want to
            implement your own.
        :type row_formatter: function(tuple, Select) -> tuple
        """
        super(Select, self).__init__(db, sql)
        self._row_formatter = row_formatter

    def _produce_return(self, cursor):
        """ Get the rows from the cursor and apply the row formatter.

        :return: sequence of rows, or a generator if a row formatter has to be
            applied
        """
        results = cursor.fetchall()

        # Format rows within a generator?
        if self._row_formatter is not None:
            return (self._row_formatter(r, cursor) for r in results)

        return results


class SelectOne(Select):
    """ A Select class which returns the single row or the single column if
    the row contains only one column.

    If the query returns something other than one row the call returns None.
    """

    def _produce_return(self, cursor):
        """ Return the one result.
        """
        results = cursor.fetchmany(2)
        if len(results) != 1:
            return None

        # Return the one row, or the one column.
        row = results[0]
        if self._row_formatter is not None:
            row = self._row_formatter(row, cursor)
        elif len(row) == 1:
            row = row[0]

        return row


class ManipulationCheckError(Exception):
    """ Error marker for when a Manipulation call does not behave as expected.
    """
    pass


class Manipulation(Query):
    """ Class for executing all kinds of queries, other than select, returns
    the row count.

    Can do an automatic row count check and raises ManipulationCheckError if
    the numbers don't match.
    """
    def __init__(self, db, sql, rowcount):
        """
        :param rowcount: the expected row count for the query or None, if no
            check should be performed
        :type rowcount: int
        """
        super(Manipulation, self).__init__(db, sql)
        self._rowcount = rowcount

    def _produce_return(self, cursor):
        """ Return the rowcount property from the used cursor.

        Checks the count first, if a count was given.

        :raise ManipulationCheckError: if a row count was set but does not
            match
        """
        rowcount = cursor.rowcount

        # Check the row count?
        if self._rowcount is not None and self._rowcount != rowcount:
            raise ManipulationCheckError(
                "Count was {}, expected {}.".format(rowcount, self._rowcount))

        return rowcount
