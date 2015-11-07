# -*- coding: utf-8 -*-
from unittest import TestCase

from dbaccess import SQLiteDB


class _CloseErrorConnection():

    def close(self):
        """ Simulate an error on close.
        """
        raise Exception()


class SQLiteTest(TestCase):
    """Test the sqlite.py connection code.

    Establishes a connection to a new, empty in-memory SQLite DB before each
    test.
    """

    def setUp(self):
        self.db = SQLiteDB(":memory:")

    def test_create_insert_select(self):
        """Create a table, insert a row and read it again, the basic DB
        actions.
        """
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        self.db.Manipulation("INSERT INTO test VALUES(?)")(test_value)
        select = self.db.Select("SELECT * FROM test")
        self.assertEqual(select()[0][0], test_value)

    def test_reopen(self):
        """ Try what happens if the connection is lost (do a close) and see
        that it is reopened again.
        """
        self.db.close()
        # This should not throw an exception!
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()

    def test_show(self):
        """ Check the show test_query function.
        """
        self.assertEqual(self.db.show("test", [1, 2]), "test [1, 2]")

    def test_connect_error(self):
        """ Test that it is not possible to open a connection again, without
        closing it first.
        """
        # Do something to create the connection.
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        # Now try to create the connection again, without closing it first.
        with self.assertRaises(RuntimeError):
            self.db._connect()

    def test_close_error(self):
        """ Add a faulty close function and check that the DB instance can
        handle that.
        """
        self.db._connection = _CloseErrorConnection()
        self.db.close()  # should not raise an error!
