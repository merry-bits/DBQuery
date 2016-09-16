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


class PostgresTest(PostgresTestCase):
    """ Test Postgres connection class.
    This needs a running postgres server and expects a DSN string in the
    DBQUERY_POSTGRES_TEST environment variable.
    For example:
        postgres://user:password@localhost/test_db

    The schema used by this test is: dbquery_test

    If test schema will be dropped if it exists!
    The DB user needs permission to create and tear down the test schema and
    create/drop tables within.
    """

    def test_creat_insert_select(self):
        """ Create a table insert a row and select it.
        """
        test_value = "hello"
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        self.db.Manipulation("INSERT INTO test VALUES(%s)")(test_value)
        select = self.db.Select("SELECT * FROM test")
        self.assertEqual(select()[0][0], test_value)

    def test_show(self):
        """ Test show SQL, without parameters.
        """
        test_sql = "SELECT \'hello\'"
        self.assertEqual(self.db.show(test_sql,  []), test_sql)
        self.assertEqual(self.db.show(test_sql,  None), test_sql)
        # For coverage, execute _connect line, after closing a connection.
        self.db.close()
        self.assertEqual(self.db.show(test_sql,  []), test_sql)

    def test_show_args(self):
        """ Test show SQL, with arguments.
        """
        test_sql = "SELECT * FROM WHERE id=%s"
        test_value = "test_value"
        self.assertIn(test_value, self.db.show(test_sql,  [test_value]))

    def test_show_kwds(self):
        """ Test show SQL, with arguments.
        """
        test_sql = "SELECT * FROM test WHERE id=%(test)s"
        test_value = "test_value"
        self.assertIn(test_value, self.db.show(test_sql, {"test": test_value}))

    def test_closed_commit(self):
        """ A commit on a closed connection should throw an error.
        """
        self.db.close()
        with self.assertRaises(RuntimeError):
            self.db._commit()

    def test_closed_rollback(self):
        """ A commit on a closed connection should throw an error.
        """
        self.db.close()
        with self.assertRaises(RuntimeError):
            self.db._rollback()

    def test_context(self):
        """ Test using a transaction to insert data.
        """
        test_value = "hello"
        select = self.db.Select("SELECT * FROM test")
        with self.db:
            self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
            self.db.Manipulation("INSERT INTO test VALUES(%s)")(test_value)
        self.db.close()  # execute _connect line in begin. Coverage!
        with self.db:
            self.assertEqual(select()[0][0], test_value)

    def test_context_rollback(self):
        """ Test aborting a transaction, rolling back an INSERT.
        """
        test_value = "hello"
        select = self.db.Select("SELECT * FROM test")
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        with self.db as db:
            self.db.Manipulation("INSERT INTO test VALUES(%s)")(test_value)
            db.abort_transaction()
        self.assertEqual(len(select()), 0)

    def test_context_closed(self):
        """ Test closing the connection during a transaction, should rollback
        the transaction automatically.
        """
        test_value = "hello"
        select = self.db.Select("SELECT * FROM test")
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        with self.assertRaises(RuntimeError):
            with self.db as db:
                self.db.Manipulation(
                    "INSERT INTO test VALUES(%s)")(test_value)
                db.close()  # ending the transaction now raises an error
        self.assertEqual(len(select()), 0)

    def test_context_rollback_closed(self):
        """ Test aborting the transaction while the connection is already
        closed, should be rolled back automatically.
        """
        test_value = "hello"
        select = self.db.Select("SELECT * FROM test")
        self.db.Manipulation("CREATE TABLE test (test VARCHAR)")()
        with self.assertRaises(RuntimeError):
            with self.db as db:
                self.db.Manipulation(
                    "INSERT INTO test VALUES(%s)")(test_value)
                db.close()
                db.abort_transaction()  # raises an error
        self.assertEqual(len(select()), 0)


class TestNextVal(PostgresTestCase):
    """ Test the NextVal class.
    """

    def test_next(self):
        """ Create a primary key and use NextVal to get the next id.

        A sequence name for SERIAL consists of:
            <table name>_<column name>_seq
        """
        table_name = 'test'
        column_name = 'i'
        create_sql = (
            'CREATE TABLE {}({} SERIAL PRIMARY KEY)'.format(
                table_name, column_name))
        self.db.Manipulation(create_sql)()
        n = self.db.NextVal('{}_{}_seq'.format(table_name, column_name))
        self.assertEqual(n(), 1, 'Should be the first id, 1.')
