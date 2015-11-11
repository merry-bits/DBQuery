# -*- coding: utf-8 -*-


class LogMsg():
    """ Wrapper for using Python 3 string format in logging messages.

    Save the extra parameters until the string is needed.
    """

    def __init__(self, msg='', *args, **kwds):
        self._msg = msg
        self._args = args or []
        self._kwds = kwds or {}
        self._str = None

    def __str__(self):
        """ Now format the message with given arguments using Python 3 string
        format. Create string on first call, then cache it and set other
        variables to None.
        """
        # String prepared previously?
        if self._str is None:
            self._str = self._msg
            self._msg = None  # not needed anymore
            # Format string?
            if self._args or self._kwds:
                self._str = self._str.format(*self._args, **self._kwds)
                # Not needed anymore, too.
                self._args = None
                self._kwds = None
        return self._str
