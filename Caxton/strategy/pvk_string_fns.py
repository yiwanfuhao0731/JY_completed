import pandas as pd
import numpy as np
import datetime as dt
import hashlib
import re
import unicodedata


def hash(s):
    return hashlib.sha224(s.encode('utf-8')).hexdigest()


def is_number(s):
    """test if input is a number"""
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


def num_to_string(n, n_fixed_digits=None, decimal_precision=None):

    if n_fixed_digits is None:
        if decimal_precision is None:
            n_fixed_digits = 4
    elif decimal_precision is not None:
        raise ValueError("Conflicting options set: " +
                        "Only one of the options: (fixed_digits, decimal_precision) can be not None")
    """convert number to string"""
    if not is_number(n):
        raise ValueError("passed value \'{}\' is not a number or convertable to a number.".format(n))

    if isinstance(n, int):
        return "{}".format(n)
    else:
        if n_fixed_digits is not None:
            return fixed_digits(n, n_fixed_digits)
        elif decimal_precision is not None:
            return ("{:." + str(decimal_precision) + "f}").format(n)
        else:
            raise ValueError("Error in function num_to_string in lib_string_fns, please check input parsing.")

# date to string in lib_date_fns


def fixed_digits(float_num, max_total_digits=4):
    """
    format a float to string in standard decimal notation, to have a fixed number of **total** digits

    E.g., for max_total_digits=6:
    - 0.00123456789 gives "0.00123"
    - 0.0000000123456789 gives "0.00000"
    - 12345678.9 gives "12345679" (on overflow, all digits left of decimalpoint remain)
    - 4.2 gives "4.2" (no trailing zero's)
    """
    string_out = str(float_num).rstrip("0").rstrip(".")
    if len(string_out) <= max_total_digits or "." not in string_out:
        return string_out
    else:
        n_decimals = max(max_total_digits - len(str(float_num).split(".")[0]), 0)
        try:
            x = ("{:." + str(n_decimals) + "f}").format(float_num)
        except:
            print('stop')
        return ("{:." + str(n_decimals) + "f}").format(float_num)


def recursive_replace(input_cfg, old_str, new_str):
    """apply string replace to all values in nested dict/list"""
    # check whether it's a dict, list, tuple, or scalar
    if isinstance(input_cfg, dict):
        items = input_cfg.items()
    elif isinstance(input_cfg, (list, tuple)):
        items = enumerate(input_cfg)
    else:
        # just a value, split and return
        return str(input_cfg).replace(old_str, new_str)

    # now call ourself for every value and replace in the input
    for key, value in items:
        new = recursive_replace(value, old_str, new_str)
        if new != input_cfg[key]:
            input_cfg[key] = new
    return input_cfg


def slugify(value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters,
    and converts spaces to hyphens.
    """
    value = unicodedata.normalize('NFKD', value)
    value = re.sub('[^\w\s-]', '', value).strip().lower()
    value = re.sub('[-\s]+', '-', value)
    return value