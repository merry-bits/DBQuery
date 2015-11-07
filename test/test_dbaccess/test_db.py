# -*- coding: utf-8 -*-
from unittest.case import TestCase

from dbaccess import DBContextManagerError
from dbaccess.db import DB as DBBase


_TEST_PARAM = "test_param"


class DB(DBBase):
    """ Empty database class, for testing __enter__, __exit__ from Connection.

    Counts all transaction related calls.
    """

    def __init__(self, call_super=False):
        super().__init__(0)
        self.begin_calls = 0
        self.commit_calls = 0
        self.rollback_calls = 0
        self.close_calls = 0
        self._call_super = call_super

    def _begin(self):
        self.begin_calls += 1
        if self._call_super:
            super()._begin()

    def _commit(self):
        self.commit_calls += 1
        if self._call_super:
            super()._commit()

    def _rollback(self):
        self.rollback_calls += 1
        if self._call_super:
            super()._rollback()

    def close(self):
        self.close_calls += 1
        if self._call_super:
            super().close()


class TestConnection(TestCase):
    """ Using the Connection test if ConnectionBase calls begin, commit and
    rollback correctly when used as a context manager for a DB transaction.
    """

    def _check_calls(
            self, connection, begin_calls, commit_calls, rollback_calls):
        """ Check if all functions where called as many times as expected.
        """
        self.assertEqual(connection.begin_calls, begin_calls)
        self.assertEqual(connection.commit_calls, commit_calls)
        self.assertEqual(connection.rollback_calls, rollback_calls)

    def test_context(self):
        """ One begin, one commit with a simple context.
        """
        db = DB()
        with db:
            pass
        self._check_calls(db, 1, 1, 0)

    def test_nested_context(self):
        """ One begin and one commit even in a nested context.
        """
        db = DB()
        with db:
            with db:
                pass
        self._check_calls(db, 1, 1, 0)

    def test_exception(self):
        """ Check rollback call, in an exception situation.
        """
        db = DB()
        with self.assertRaises(RuntimeError):
            with db:
                raise RuntimeError()
        self._check_calls(db, 1, 0, 1)

    def test_nested_exception(self):
        """ Check rollback call, in a nested exception situation.
        """
        db = DB()
        with self.assertRaises(RuntimeError):
            with db:
                with db:
                    raise RuntimeError()
        self._check_calls(db, 1, 0, 1)

    def test_abort(self):
        """ Test the abort function.
        """
        db = DB()
        with db:
            db.abort_transaction()
        self._check_calls(db, 1, 0, 1)

    def test_nested_abort(self):
        """ Test abort in a nested context.
        """
        db = DB()
        with db:
            with db:
                db.abort_transaction()
        self._check_calls(db, 1, 0, 1)

    def test_abort_exception(self):
        """ If no transaction is in progress, abort_transaction should raise
        an exception.
        """
        db = DB()
        with self.assertRaises(DBContextManagerError):
            db.abort_transaction()

    def test_exit_exception(self):
        """ If there is a missmatch between __enter__ and __exit__ calls an
        exception should be raised
        """
        db = DB()
        with self.assertRaises(DBContextManagerError):
            db.__exit__(None, None, None)

    def test_not_implemented(self):
        """ Just check that an error occurs when trying to use an unsupported
        feature.
        """
        db = DB(call_super=True)
        with self.assertRaises(NotImplementedError):
            db.execute(None, None, lambda x: None)
        with self.assertRaises(NotImplementedError):
            db.close()
        with self.assertRaises(NotImplementedError):
            db.show(None, None)
        with self.assertRaises(NotImplementedError):
            db._begin()
        with self.assertRaises(NotImplementedError):
            db._commit()
        with self.assertRaises(NotImplementedError):
            db._rollback()
        with self.assertRaises(NotImplementedError):
            with db:  # calls __enter__ which calls _begin
                pass
