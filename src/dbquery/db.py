# -*- coding: utf-8 -*-
from functools import wraps
from logging import getLogger

from .log_msg import LogMsg
from .query import Query, Select, SelectOne, Manipulation


_LOG = getLogger(__name__)


class _DBRollbackException(Exception):
    """Used by Connection transaction context manager.

    Caught in __exit__ and converted into a rollback call.
    """


class DBContextManagerError(Exception):
    """Raised when an transaction action is not possible:
     - abort when no transaction is in progress
     - __exit__ beyond the __enter__ level(count)
    """


class DB(object):
    """ Database class that knows how to talk to the database.

    Used by all the Query classes to execute SQL.

    Inherit and implement to provide access to your database.
    """

    OperationalError = Exception

    def __init__(self, retry=0):
        """
        :param retry: How many attempts to connect to make before giving up.
        """
        self._retry = retry
        self._orig_retry = None  # saves retry value during a transaction
        self._transaction_level = 0  # counts nested contexts

    def Query(self, sql):
        return Query(self, sql)

    def Select(self, sql, row_formatter=None):
        return Select(self, sql, row_formatter)

    def SelectOne(self, sql, row_formatter=None):
        return SelectOne(self, sql, row_formatter)

    def Manipulation(self, sql, rowcount=None):
        return Manipulation(self, sql, rowcount)

    @property
    def retry(self):
        return self._retry

    def execute(self, sql, params, produce_return):
        """ Open or reuse a connection automatically, create a cursor and
        execute the query then call produce_return to get a value to return.

        :type sql: str
        :type params: [] or {}
        :type return_function: None or function(cursor) -> response
        :return: Result of the return_function.
        """
        raise NotImplementedError()

    def close(self):
        """ Close the connection so that another call to execute will
        trigger opening or reusing a new connection.
        """
        raise NotImplementedError()

    def show(self, sql, params):
        """ If possible use the drivers mogrify function or equivalent to get
        the SQL as the server would have build it using all the parameters.

        :rtype: str
        """
        raise NotImplementedError()

    def _begin(self):
        raise NotImplementedError()

    def _commit(self):
        raise NotImplementedError()

    def _rollback(self):
        raise NotImplementedError()

    def __enter__(self):
        # On first context level start the DB transaction.
        if self._transaction_level == 0:
            self._begin()
            # Save retry value and set it to 0. If a connection fails during a
            # transaction then that error needs to end the transaction. Usually
            # a transaction can not be continued on a different (new)
            # connection!
            self._orig_retry = self._retry
            self._retry = 0
            _LOG.debug(LogMsg("BEGIN on {}.", self))

        # Count the new context.
        self._transaction_level += 1

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Decrease the context level.
        self._transaction_level -= 1

        # Check for a wrong __exit__ call.
        if self._transaction_level < 0:
            level = self._transaction_level  # needed for exception
            self._transaction_level = 0
            # Raise the exception, can not go below 0 ... would mean that
            # one more __exit__ than __enter__ call was made.
            raise DBContextManagerError(
                "Illegal transaction level reached: {}.".format(level))

        # Leaving the context entirely, commit the DB transaction.
        if self._transaction_level == 0:
            # Got an error? Roll back the transaction!
            if exc_value:
                self._rollback()
                # Log the roll back, if it was an not the abort marker
                # (_ConnectionRollbackException).
                if not isinstance(exc_value, _DBRollbackException):
                    _LOG.debug(LogMsg("ROLLBACK on {}.", self))
            else:
                self._commit()
                _LOG.debug(LogMsg("COMMIT on {}.", self))
            # Restore retry value.
            self._retry = self._orig_retry
            self._orig_retry = None

        # Do not propagate any exception if:
        # - there was no exception
        # - there was a _ConnectionRollbackException, but there is no more
        #    context to close, in which case a roll back was done and the
        #    exception (marker) was dealt with.
        return (not exc_value or
                (self._transaction_level == 0 and
                 isinstance(exc_value, _DBRollbackException)))

    def abort_transaction(self):
        if self._transaction_level <= 0:  # no transaction in progress!
            raise DBContextManagerError("No Transaction in progress.")
        # If there is a transaction in progress raise the abort marker to cause
        # a roll back.
        raise _DBRollbackException("Aborting transaction...")

    @staticmethod
    def connected(f):

        @wraps(f)
        def new_f(self, *args, **kwds):
            if self._connection is None:
                self._connect()
            return f(self, *args, **kwds)

        return new_f
