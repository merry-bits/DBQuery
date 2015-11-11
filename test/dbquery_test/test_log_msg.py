# -*- coding: utf-8 -*-
from unittest.case import TestCase

from dbquery.log_msg import LogMsg


class TestLogMsg(TestCase):
    """ Test LogMsg class.
    """

    def test_msg_only(self):
        """ Check that msg parameter is passed correctly as a string.
        """
        s = "Hello"
        m = LogMsg(s)
        self.assertEqual(str(m), s)

    def test_args(self):
        """ Test if formatting a message with arguments works.
        """
        s = "Hello {}"
        args = ["world"]
        m = LogMsg(s, *args)
        self.assertEqual(str(m), s.format(*args))

    def test_kwds(self):
        """ Check a message with keywords.
        """
        s = "Hello {w}"
        kwds = {"w": "world"}
        m = LogMsg(s, **kwds)
        self.assertEqual(str(m), s.format(**kwds))

    def test_all(self):
        """ Check a message with arguments and keywords.
        """
        s = "{} {w}"
        args = ["Hello"]
        kwds = {"w": "world"}
        m = LogMsg(s, *args, **kwds)
        self.assertEqual(str(m), s.format(*args, **kwds))
