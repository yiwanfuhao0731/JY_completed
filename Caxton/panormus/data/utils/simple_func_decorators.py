import contextlib

import sys

import warnings as wn


class DummyFile(object):

    def write(self, x): pass


@contextlib.contextmanager
def nostdout():
    """

    Use this function to call another function without capturing the standard output.



    with nostdout():

        foo()

    """

    save_stdout = sys.stdout

    sys.stdout = DummyFile()

    yield

    sys.stdout = save_stdout


class hide_warning:

    def __init__(self, type):

        """

        Hide warnings of given type thrown by function.

        :param type: type of warnings to hide

        """

        self.type = type

    def __call__(self, f):

        """

        If there are decorator arguments, __call__() is only called

        once, as part of the decoration process! You can only give

        it a single argument, which is the function object.

        """

        def wrapped_f(*args, **kwargs):

            '''

            Arguments passed to f are received here. This wrapped function replaces f.

            '''

            try:

                wn.simplefilter(action='ignore', category=self.type)

                res = f(*args, **kwargs)



            finally:

                wn.resetwarnings()

            return res

        wrapped_f.__doc__ = f.__doc__

        return wrapped_f


class hide_warnings:

    def __init__(self, array_of_types):

        """

        Hide warnings of given types thrown by function.

        :param array_of_types: list of warning types to hide

        """

        self.types = array_of_types

    def __call__(self, f):

        """

        If there are decorator arguments, __call__() is only called

        once, as part of the decoration process! You can only give

        it a single argument, which is the function object.

        """

        def wrapped_f(*args, **kwargs):

            '''

            Arguments passed to f are received here. This wrapped function replaces f.

            '''

            try:

                for type in self.types:
                    wn.simplefilter(action='ignore', category=type)

                res = f(*args, **kwargs)



            finally:

                wn.resetwarnings()

            return res

        wrapped_f.__doc__ = f.__doc__

        return wrapped_f


def docstring_parameter(*sub):
    '''

    Decorator that allows static variable insertion into docstrings.

    Use positional args in docstring such as {0} and {1} that correspond

    to arguments given to this function.

    '''

    def dec(obj):
        obj.__doc__ = obj.__doc__.format(*sub)

        return obj

    return dec