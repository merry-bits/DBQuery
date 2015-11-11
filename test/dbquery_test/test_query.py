# -*- coding: utf-8 -*-
from unittest.case import TestCase
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from dbquery import ManipulationCheckError, to_dict_formatter
from dbquery.db import DB as DBBase


_RETRY = 2


class DB(DBBase):
    """ Empty/dummy connection class.

    The execute function behavior can be modified as needed: return a specific
    value or raise an exception.
    """

    def __init__(self):
        super(DB, self).__init__(_RETRY)
        self._raise_on_exec = False
        self._exec_cursor = None
        self.execute_calls = 0

    def set_raise_on_exec(self):
        """ Advice execute to raise a InternaLError exception.
        """
        self._raise_on_exec = True

    def set_cursor(self, cursor):
        """ Set a cursor to be used for the execute function.
        """
        self._exec_cursor = cursor

    def close(self):
        pass

    def execute(self, sql, params, produce_return):
        """ Per default calls produce_return with  (sql, params) tuple.
        Behavior can be modified with above functions.
        """
        self.execute_calls += 1
        if self._raise_on_exec:
            raise self.InternalError()
        if self._exec_cursor:
            return produce_return(self._exec_cursor)
        return produce_return((sql, params))

    def show(self, sql, params):
        """ Returns unmodified (sql, params) right back.
        """
        return sql, params


class _Cursor():
    """Represents a dummy cursor with fetchall and description.
    """

    def __init__(self, results, description, rowcount=None):
        """ Set results for the fetchall function, set the description and
        the rowcount.

        From Python DB interface:

        .description
            This read-only attribute is a sequence of 7-item sequences.

            Each of these sequences contains information describing one result
            column:

                name
                type_code
                display_size
                internal_size
                precision
                scale
                null_ok
        """
        self._results = results
        self._description = description
        self._rowcount = rowcount
        self.closed = False

    @property
    def description(self):
        if self.closed:
            raise RuntimeError("Closed cursor!")
        return self._description

    @property
    def rowcount(self):
        if self.closed:
            raise RuntimeError("Closed cursor!")
        return self._rowcount

    def fetchall(self):
        if self.closed:
            raise RuntimeError("Closed cursor!")
        return self._results

    def fetchmany(self, count):
        if self.closed:
            raise RuntimeError("Closed cursor!")
        return self._results[:count]

    def close(self):
        """ After closing the cursor should raise an exception on all attempts
        to use it further.
        """
        self.closed = True


class QueryTest(TestCase):
    """ Test the Query class.
    """

    def setUp(self):
        self.db = DB()

    def test_query(self):
        """ Test calling query with arguments.
        """
        sql_text = "some sql"
        params_list = (1, 2)
        with patch('dbquery.query.Query._produce_return') as produce_return:
            q = self.db.Query(sql_text)
            q(*params_list)
            ((sql, params), ), _ = produce_return.call_args
        self.assertEqual(sql, sql_text)
        self.assertEqual(params, params_list)

    def test_query_kwds(self):
        """ Test calling query with keyword arguments
        """
        sql_text = "some sql"
        params_list = {"a": 1}
        with patch('dbquery.query.Query._produce_return') as produce_return:
            q = self.db.Query(sql_text)
            q(**params_list)
            ((_, params), ), _ = produce_return.call_args
        self.assertEqual(params, params_list)

    def test_show(self):
        """ Check if Query calls show correctly.
        """
        sql_text = "some sql"
        params_list = (1, 2)
        q = self.db.Query(sql_text)
        sql, params = q.show(*params_list)
        self.assertEqual(sql, sql_text)
        self.assertEqual(params, params_list)

    def test_show_kwds(self):
        """ Test calling query with keyword arguments.
        """
        sql_text = "some sql"
        params_list = {"a": 1}
        q = self.db.Query(sql_text)
        _, params = q.show(**params_list)
        self.assertEqual(params, params_list)

    def test_retry(self):
        """ Tweak Connection to raise an exception on execute and check if
        retry in Query works as expected.
        """
        self.db.set_raise_on_exec()
        with self.assertRaises(self.db.OperationalError):
            with patch("dbquery.query._LOG"):  # hide log
                self.db.Query("")()
        self.assertEqual(self.db.execute_calls, _RETRY)


class SelectTest(TestCase):
    """ Test the Select class.
    """

    def setUp(self):
        self.db = DB()

    def test_(self):
        """Test a simple select call.
        """
        result = [(0, )]
        self.db.set_cursor(_Cursor(result, None))
        s = self.db.Select("")
        self.assertEqual(s(), result)

    def test_row_formatter(self):
        """Test to_dict_formatter.
        """
        result = [(0, 1)]  # one row with two columns
        col1_name = "test"  # name of first column
        col2_name = "test2"  # name of second column
        self.db.set_cursor(
            _Cursor(
                result,
                ((col1_name, None, None, None, None, None, None),
                    (col2_name, None, None, None, None, None, None))
            )
        )
        s = self.db.Select("", row_formatter=to_dict_formatter)
        # Check if the row i converted into a dictionary with "test" as the
        # one existing column.
        self.assertEqual(
            list(s()), [{"test": 0, "test2": 1}, ],
            "Result should be the same")

    def test_to_dict_formatter_empty_row(self):
        """Do give the formatter an empty row and check that an empty row is
        returned.
        """
        result = [tuple()]  # just one emptry row
        self.db.set_cursor(_Cursor(result, None))
        s = self.db.Select("", row_formatter=to_dict_formatter)
        self.assertEqual(list(s()), result, "Result should be the same")

    def test_to_dict_formatter_missing_keys(self):
        """Test giving the to_dict_formatter no keys (no row description) and
        look for the RuntimeError.
        """
        result = [(1, )]
        self.db.set_cursor(_Cursor(result, None))  # no row description
        s = self.db.Select("", row_formatter=to_dict_formatter)
        with self.assertRaises(RuntimeError):
            list(s())


class SelectOneTest(TestCase):
    """ Test SelectOne class.
    """

    def setUp(self):
        self.db = DB()

    def test_one(self):
        """ Test getting one result value, plain, not as a list or row.
        """
        value = 33
        self.db.set_cursor(_Cursor([(value, )], None))
        self.assertEqual(self.db.SelectOne("")(), value)

    def test_two(self):
        """ Test getting a single row directly, instead as in a list.
        """
        result_row = (1, 2, 3)
        self.db.set_cursor(_Cursor([result_row], None))
        self.assertEqual(self.db.SelectOne("")(), result_row)

    def test_many(self):
        """ Test getting tow rows, should return None.
        """
        self.db.set_cursor(_Cursor([(0, -1), (1, -2)], None))
        s = self.db.SelectOne("")
        self.assertEqual(s(), None)


class ManipulationTest(TestCase):
    """ Test Manipulation class.
    """

    def setUp(self):
        self.db = DB()

    def test_manipulation(self):
        """ Create a table and insert one row, check row count.
        """
        rowcount = 1
        self.db.set_cursor(_Cursor(None, None, rowcount))
        m = self.db.Manipulation("")
        self.assertEqual(m(), rowcount)

    def test_rowcount_check(self):
        """ Set an expected rowcount and try both correct and wrong count in
        cursor.
        """
        rowcount = 1
        self.db.set_cursor(_Cursor(None, None, rowcount))
        m = self.db.Manipulation("", rowcount=1)
        # This should not throw an exception.
        self.assertEqual(m(), rowcount)
        # This should.
        m = self.db.Manipulation("", rowcount=2)
        with self.assertRaises(ManipulationCheckError):
            with patch("dbquery.query._LOG"):  # hide log
                m()
