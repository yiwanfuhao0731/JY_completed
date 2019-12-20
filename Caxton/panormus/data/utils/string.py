"""Common string manipulation functions"""

import re

import unicodedata


def slugify(value):
    """

    Normalizes string, converts to lowercase, removes non-alpha characters,

    and converts spaces to hyphens.



    :param value: string with potentially illegal filesystem characters

    """

    value = unicodedata.normalize('NFKD', value)

    value = re.sub('[^\w\s-]', '', value).strip().lower()

    value = re.sub('[-\s]+', '-', value)

    return value


def is_number(string):
    """

    Test if input string represents a number



    :param string: test if this string is a number

    """

    try:

        float(string)

        return True

    except (ValueError, TypeError):

        return False


def num2str(num_in, n_fixed_digits=None, decimal_precision=None):
    """

    Convert number to string.

    Precisely one of (n_fixed_digits, decimal_precision) must be not None.

    If n_fixed_digit is passed, output string will have a fixed number of **total** digits.

    If decimal precision, converts to a fixed number of decimals.



    E.g., for n_fixed_digits=6:

    - 0.00123456789 gives "0.00123"

    - 0.0000000123456789 gives "0.00000"

    - 12345678.9 gives "12345679" (on overflow, all digits left of decimal point remain)

    - 4.2 gives "4.2" (no trailing zero's)



    :param num_in: number to be converted to string

    :param n_fixed_digits: max total digits in the string

    :param decimal_precision: decimal precision in the string (digits after the . decimal separator)

    """

    if n_fixed_digits is None:

        if decimal_precision is None:
            n_fixed_digits = 4

    elif decimal_precision is not None:

        raise ValueError("Conflicting options set: " +

                         "Only one of the options: (fixed_digits, decimal_precision) can be not None")

    if not is_number(num_in):
        raise ValueError("passed value \'{}\' is not a number or convertable to a number.".format(num_in))

    if isinstance(num_in, int):

        return "{}".format(num_in)

    else:

        # return string with fixed number of total digits

        if n_fixed_digits is not None:

            string_out = str(num_in).rstrip("0").rstrip(".")

            if len(string_out) <= n_fixed_digits or "." not in string_out:

                return string_out

            else:

                n_decimals = max(n_fixed_digits - len(str(num_in).split(".")[0]), 0)

                return ("{:." + str(n_decimals) + "f}").format(num_in)



        # return string with fixed number of decimals

        elif decimal_precision is not None:

            return ("{:." + str(decimal_precision) + "f}").format(num_in)

        else:

            raise ValueError("Error in function num_to_string in lib_string_fns, please check input parsing.")