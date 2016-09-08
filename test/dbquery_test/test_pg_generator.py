# -*- coding: utf-8 -*-
"""
Enable the PostgreSQL tests by setting the environment variable in
_DBQUERY_POSTGRES_TEST to the Postgres DSN to be used.

Prepare a PostgreSQL test database:
  =# CREATE ROLE <user> LOGIN PASSWORD '<password>';
  =# CREATE DATABASE <database> WITH OWNER <user>;
"""
from unittest import TestCase, skipUnless
from os import getenv

from dbquery.postgres import PostgresDB
from dbquery.query import to_dict_formatter


_TEST_SCHEMA = "dbquery_test"

_DBQUERY_POSTGRES_TEST = "DBQUERY_POSTGRES_TEST"


@skipUnless(
    getenv(_DBQUERY_POSTGRES_TEST), 'PostgreSQL connection tests not enabled.')
class PostgresTestCase(TestCase):
    """ Adds the PostgreSQL connection to the TestCase class in the setUp
    method and clears up the DB in tearDown.
    """

    def setUp(self):
        """ Set up a connection and create the test schema, drop it first if
        necessary and set it as the users default.

        This of course tests if a connection can be established and if
        Manipulation works on the connections...
        """
        super(PostgresTestCase, self).setUp()
        test_dsn = getenv(_DBQUERY_POSTGRES_TEST)
        # Set up the connection.
        self.db = PostgresDB(test_dsn, retry=3)
        # Drop schema if it exists and create an empty one.
        self.db.Manipulation(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(_TEST_SCHEMA))()
        # Create schema and set it as default.
        self.db.Manipulation("CREATE SCHEMA {}".format(_TEST_SCHEMA))()
        self.db.Manipulation("SET search_path TO {}".format(_TEST_SCHEMA))()

    def tearDown(self):
        super(PostgresTestCase, self).tearDown()
        self.db.Manipulation(
            "DROP SCHEMA IF EXISTS {} CASCADE".format(_TEST_SCHEMA))()
        self.db.close()


class TestBasic(PostgresTestCase):
    """ Insert, select etc. """

    def test_create_insert_select(self):
        """ Create a table insert a row and select it.
        """
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        self.db.Manipulation("INSERT INTO test VALUES(%s)")(test_value)
        select = self.db.Select("SELECT * FROM test")
        self.assertEqual(select()[0][0], test_value)


class TestInsertMany(PostgresTestCase):
    """ Insert, select etc. """

    def test_create_insert_select(self):
        """ Create a table, insert N rows and select them.
        """
        N = 10
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (id INTEGER, val VARCHAR)")()

        for i in range(N):
            self.db.Manipulation(
                "INSERT INTO test VALUES(%s, %s)")(str(i), test_value + str(i))

        select = self.db.Select("SELECT * FROM test")

        # We need to call select() to get data.
        for i, row in enumerate(select()):
            self.assertEqual(row, (i, test_value + str(i)))

    def test_create_insert_select_gen(self):
        """ Create a table, insert N rows and select them with generator and
        callback function.
        """
        N = 10
        callback_counter = 0
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (id INTEGER, val VARCHAR)")()

        for i in range(N):
            self.db.Manipulation(
                "INSERT INTO test VALUES(%s, %s)")(str(i), test_value + str(i))

        def _callback(rowset, *args):
            """ Row elements are tuples. """
            nonlocal callback_counter
            self.assertEqual(
                rowset[0],
                (callback_counter, "hello" + str(callback_counter)))
            callback_counter += 1
            self.assertEqual(
                rowset[1],
                (callback_counter, "hello" + str(callback_counter)))
            callback_counter += 1
            self.assertEqual(args[0], "hello")
            self.assertEqual(args[1], "world")

        cb_args = ["hello", "world"]
        arraysize = 2
        select = self.db.SelectGen(
            "SELECT * FROM test", _callback, cb_args, arraysize)
        sg = select()
        self.assertEqual(sg, None)

    def test_create_insert_select_gen_row_formatter(self):
        """ Create a table, insert N rows and select them with generator and
        callback function, using the row formatter.
        """
        N = 10
        callback_counter = 0
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (id INTEGER, val VARCHAR)")()

        for i in range(N):
            self.db.Manipulation(
                "INSERT INTO test VALUES(%s, %s)")(str(i), test_value + str(i))

        def _callback(rowset, *args):
            """ Row elements are now dicts. """
            rowset = list(rowset)
            nonlocal callback_counter
            self.assertEqual(rowset[0]["id"], callback_counter)
            self.assertEqual(rowset[0]["val"], "hello" + str(callback_counter))
            callback_counter += 1
            self.assertEqual(rowset[1]["id"], callback_counter)
            self.assertEqual(rowset[1]["val"], "hello" + str(callback_counter))
            callback_counter += 1
            self.assertEqual(args[0], "hello")
            self.assertEqual(args[1], "world")

        cb_args = ["hello", "world"]
        arraysize = 2
        select = self.db.SelectGen(
            "SELECT * FROM test", _callback, cb_args, arraysize,
            to_dict_formatter)

        sg = select()
        self.assertEqual(sg, None)
