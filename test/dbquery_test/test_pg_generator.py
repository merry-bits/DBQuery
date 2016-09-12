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
    """ Insert several items and test SelectIterator. """

    def test_select_iterator(self):
        """ Create N rows, use SelectIterator and callback function.
        """
        N = 10
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (id INTEGER, val VARCHAR)")()

        for i in range(N):
            self.db.Manipulation(
                "INSERT INTO test VALUES(%s, %s)")(str(i), test_value + str(i))

        # SelectIterator calls this once with a generator of rows.
        def _callback(row_generator, *args):
            """ Handles generator which delivers individual rows. Row elements
                are tuples.
            """
            nonlocal N
            row_counter = 0
            for row in row_generator:
                self.assertEqual(
                    row,
                    (row_counter, "hello" + str(row_counter)))
                row_counter += 1
                self.assertEqual(args[0], "hello")
                self.assertEqual(args[1], "world")
            self.assertEqual(row_counter, N)

        cb_args = ["hello", "world"]
        arraysize = 2
        select = self.db.SelectIterator(
            "SELECT * FROM test", _callback, cb_args, arraysize)
        sg = select()
        self.assertEqual(sg, None)

    def test_select_iterator_row_formatter(self):
        """ Create N rows, use SelectIterator, callback function and formatter.
        """
        N = 10
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (id INTEGER, val VARCHAR)")()

        for i in range(N):
            self.db.Manipulation(
                "INSERT INTO test VALUES(%s, %s)")(str(i), test_value + str(i))

        # SelectIterator calls this once with a generator of rows.
        def _callback(row_generator, *args):
            """ Handles generator which delivers individual rows formatted with
                dict_formatter. Row elements are dicts.
            """
            row_counter = 0
            for row in row_generator:
                self.assertEqual(row["id"], row_counter)
                self.assertEqual(
                    row["val"], "hello" + str(row_counter))
                row_counter += 1
                self.assertEqual(args[0], "hello")
                self.assertEqual(args[1], "world")
            self.assertEqual(row_counter, N)

        cb_args = ["hello", "world"]
        arraysize = 2
        select = self.db.SelectIterator(
            "SELECT * FROM test", _callback, cb_args, arraysize,
            to_dict_formatter)

        sg = select()
        self.assertEqual(sg, None)
