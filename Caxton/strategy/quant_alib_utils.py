from ctypes import (byref, c_bool, c_char, c_wchar, c_byte, c_ubyte, c_short, c_ushort,
                    c_int, c_uint, c_long, c_ulong, c_longlong, c_ulonglong, c_float, c_double,
                    c_longdouble, c_char_p, c_wchar_p, c_void_p)

from calendar import monthrange as cal_mr
import datetime as dt
import os
import sys
import warnings as wn

from scipy.optimize import newton

from panormus.quant import option as qo

from panormus.quant.alib import Alib_Class
from panormus.quant.alib.conventions import (
    CONV_DICT, HOLIDAY_PATH, VAL_DATE_CONV_DICT,
    EXP_DATE_CONV_DICT, CURVE_NAMES_DICT, YCF_CAL_DICT)
from panormus.config.settings import ALIB_PATH, ALIB_LOG_PATH, ALIB_CURVE_DIR

# Initialize library and error log file
if sys.platform.startswith('win'):
    from ctypes import windll

    ALib = windll.LoadLibrary(ALIB_PATH)

else:
    from ctypes import cdll

    ALib = cdll.LoadLibrary(ALIB_PATH)

ALib.ALIB_INITIALIZE()
ALib.ALIB_ERR_LOG(c_long(1))
ALib.ALIB_ERR_LOG_FILE_NAME(ALIB_LOG_PATH.encode('ascii', 'ignore'), c_long(1))

# Alib base date
ALIB_TIME_STARTS = dt.date(year=1601, month=1, day=1)
MAX_ARRAY_SIZE = 600


def free_object(alib_obj):
    try:
        ALib.ALIB_OBJECT_FREE(alib_obj.handle)
    except:
        try:
            ALib.ALIB_OBJECT_FREE(alib_obj)
        except:
            pass


def free_objects(*args):
    for alib_obj in args:
        free_object(alib_obj)


def swaptions_exp_and_tenor_ycf(d, curve_handle, holiday_oracle, expiries, tenors):
    """
    :description: Convert lists of expiries and tenors into lists of year count fractions
    :param datetime.date d:
    :param curve_handle:
    :param holiday_oracle:
    :param list[str] expiries:
    :param list[str] tenors:
    :return: dictionary of exp dates, exp ycfs, swap dates, and tenor ycfs.
    """
    exp_dates = [
        expiry_date_from_trade_date(d, e, curve_handle, holiday_oracle) for e in expiries
    ]
    exp_ycfs = [
        bus_ycf(d, ed, curve_handle, holiday_oracle) for ed in exp_dates
    ]
    swap_dates = [
        dates_from_trade_date(d, '0d', t, curve_handle, holiday_oracle) for t in tenors
    ]
    tenor_ycfs = [
        ac_day_cnt_frac(sd[0], sd[1], 'act/365') for sd in swap_dates
    ]
    return {
        'exp_dates': exp_dates, 'exp_ycfs': exp_ycfs,
        'swap_dates': swap_dates, 'tenor_ycfs': tenor_ycfs
    }


# TODO only put raw alib wrappers in here.
# Move higher-level apis to asset-specific modules, irs, date, etc.
def python_date_to_alib_Tdate(python_date):
    '''
    :description: convert any python date with month, day, and year fields into an alib date
    :param python_date: requires month, day, and year fields
    :return: alib date
    '''
    alib_date = ALib.MDY_TO_TDATE(c_int(python_date.month), c_int(python_date.day), c_int(python_date.year))
    if isinstance(alib_date, int):
        alib_date = c_long(alib_date)

    return alib_date


def alib_Tdate_to_python_date(TDate):
    m = c_long(0)
    d = c_long(0)
    y = c_long(0)
    status = ALib.TDATE_TO_MDY(TDate, byref(m), byref(d), byref(y))
    python_date = dt.date(y.value, m.value, d.value)
    return python_date


def python_date_to_alib_str_date(python_date):
    '''
    :description: convert any python date with month, day, and year fields into a string in \
    the format alib expects
    :param python_date: requires month, day, and year fields
    :return: alib string of the date
    '''
    return python_date.strftime('%m/%d/%Y')


def ascii_encode(in_str):
    '''
    :description: convert unicode string to basic string. Basic strings will be returned as-is.
    :param in_str: unicode string
    :return: basic string
    '''
    return in_str.encode('ascii', 'ignore') if isinstance(in_str, str) else in_str


def alib_obj_coerce(params, obj_type, handle):
    """
    :description: wraps alib object_coerce_from_string
    :param str params: symbol to convert for alib
    :param str obj_type: a string indicating object type, such as 'IVL'
    :param handle: pointer to fill with return value
    :return: int error status
    """
    status = ALib.ALIB_OBJECT_COERCE_FROM_STRING(
        ascii_encode(params),
        ascii_encode(obj_type),
        handle
    )
    return status


class Holidays(object):
    '''
    :description: Class instances are dictionary-like objects that store the location of alib holiday \
    files and also provide combination of holiday calendars with plus operator, such as \
    Holidays()['USD+GBP'] to combine USD and GBP holiday calendars.
    '''

    def __init__(self, *args, **kwargs):
        self._holiday_path = kwargs.pop('holiday_path', HOLIDAY_PATH)
        self._hol_dict = {}
        for a in args:
            a_path = os.path.join(self._holiday_path, a.upper() + '.hol')
            self._hol_dict[a] = ascii_encode(a_path)
        for k, v in kwargs.items():
            self._hol_dict[k] = ascii_encode(v)

    def __getitem__(self, key):
        if key in self._hol_dict:
            return self._hol_dict.__getitem__(key)
        elif '+' not in key:
            key_path = os.path.join(self._holiday_path, key.upper() + '.hol')
            self._hol_dict[key] = ascii_encode(key_path)
            return self._hol_dict[key]
        elif '+' in key:
            key_list = key.split('+')
            for k in key_list:
                if k not in self._hol_dict:
                    k_path = os.path.join(self._holiday_path, k.upper() + '.hol')
                    self._hol_dict[k] = ascii_encode(k_path)
            num_keys = len(key_list)
            returned = c_char_p()
            path_array = (c_char_p * num_keys)()
            for i, k in enumerate(key_list):
                path_array[i] = self._hol_dict[k]

            ALib.ALIB_HOLIDAYS_COMBINE(
                c_int(num_keys),
                path_array,
                ascii_encode(key),
                byref(returned)
            )
            self._hol_dict[key] = returned.value
            return self._hol_dict[key]

    def __iter__(self):
        return iter(self._hol_dict)

    def iteritems(self):
        return self._hol_dict.items()

    def add_hols(self, *args, **kwargs):
        for a in args:
            a_path = os.path.join(self._holiday_path, a.upper() + '.hol')
            self._hol_dict[a] = ascii_encode(a_path)
        for k, v in kwargs.items():
            self._hol_dict[k] = ascii_encode(v)


def ac_err_get_log(lines=20):
    """
    :description: Return the alib error log
    :param int lines: number of lines to return, default 20
    :return: string
    """
    raw_res = (c_char_p * lines)()
    failure = ALib.ALIB_ERR_GET_LOG(
        byref(raw_res)
    )
    error_log = b'\n'.join(raw_res[i] for i in range(lines) if raw_res[i])
    return error_log.decode('ascii')


def raise_val_err_w_log(s):
    """
    :description: prepends the user supplied string to the alib error log, and raises a value error
    :param string s: message
    :return: string
    """
    err_log = ac_err_get_log()
    err_str = '\n'.join([s, err_log])
    raise ValueError(err_str)


def ac_interp_lf2(xs, ys, fxys, x, y):
    """
    :description: This function performs 2-dimensional linear interpolation on real number data types. The function assumes that the two arrays are sorted in increasing order.
    :param xs: x values
    :param ys: y values
    :param fxys: matrix of f(x, y) values
    :param x: x value to interp to
    :param y: y value to interp to
    :return:
    """
    nx = len(xs)
    ny = len(ys)

    xs_c = (c_double * nx)(*[])
    for i in range(0, nx):
        xs_c[i] = c_double(xs[i])

    ys_c = (c_double * ny)(*[])
    for i in range(0, ny):
        ys_c[i] = c_double(ys[i])

    FXYS = ((c_double * nx) * ny)
    fxys_c = FXYS()
    for i in range(0, ny):
        for j in range(0, nx):
            fxys_c[i][j] = c_double(fxys[i, j])

    fout = c_double()
    failure = ALib.ALIB_INTERP_LF2(
        c_long(nx), xs_c,
        c_long(ny), ys_c,
        c_long(nx * ny), fxys_c,
        c_double(x), c_double(y),
        byref(fout))
    if failure:
        base_err_msg = '2d interp failed\n'
        base_err_msg = base_err_msg + 'x = %d\n' % x
        base_err_msg = base_err_msg + 'y = %d\n' % y
        raise_val_err_w_log(base_err_msg)
    return fout.value


def ac_interp_rate(crv, rate_end_date):
    """
    :description: Interpolate a zero rate from a zero curve
    :param crv: alib curve object
    :param rate_end_date: end date of the rate (start date will be the base date of the curve)
    :return: double
    """
    alib_end_date = python_date_to_alib_Tdate(rate_end_date)

    rate = c_double()
    failure = ALib.ALIB_INTERP_RATE_O(crv, alib_end_date, byref(rate))
    if failure:
        base_err_msg = 'Error in interping rate'
        raise_val_err_w_log(base_err_msg)
    return rate.value


def ac_ivl_adj_make(ivl_string, bus_day_int, holiday_file, bad_day_conv_str):
    """
    :description: Constructor for an Adjusted Date Interval
    :param ivl_string: alib date inteval string (e.g. 3M)
    :param bus_day_int: Whether to count business days (non-zero) or calendar days (0)
    :param holiday_file: holiday list
    :param bad_day_conv_str: bad day convention (M, F or N)
    :return: ivl adj object
    """
    alib_interval = Alib_Class.IVL()
    alib_obj_coerce(ivl_string, "IVL", byref(alib_interval))
    holiday_file = ascii_encode(holiday_file)
    bad_day_conv_str = ascii_encode(bad_day_conv_str)
    alib_ivl_adj = Alib_Class.IVL_ADJ()
    failure = ALib.ALIB_IVL_ADJ_MAKE(
        alib_interval,
        c_long(bus_day_int),
        holiday_file,
        bad_day_conv_str,
        byref(alib_ivl_adj)
    )
    free_object(alib_interval)
    if failure:
        free_object(alib_ivl_adj)
        base_err_msg = 'Error, cannot construct date adjuster'
        raise_val_err_w_log(base_err_msg)

    return alib_ivl_adj


def ac_ivl_years(ivl_string):
    """
   :description: Returns the year fraction equivalent to a specified DateInterval
    :param ivl_string: Alib interval string (e.g. 3M)
    :return: double
    """
    ycf = c_double()
    alib_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(ivl_string, "IVL", byref(alib_interval))
    failure = ALib.ALIB_IVL_YEARS(alib_interval, byref(ycf))
    free_object(alib_interval)
    if failure:
        base_err_msg = 'Could not compute interval years'
        raise_val_err_w_log(base_err_msg)

    return ycf.value


def pull_stored_zc(curve_name, asof_date, curve_base_dir=None, live_mode=False, lon1600_mode=False):
    """
    :description: Pulls a stored zero curve from the filesystem
    :param string curve_name: e.g USD.LIBOR.3M
    :param asof_date: date to pull the curve on
    :param curve_base_dir: base director for the stored curves, default is the ny quant share
    :param bool live_mode: set True when using a curve_base_dir with live curves directly in root.
    :return: alib curve object
    """
    if curve_base_dir == None:
        crv_dir = ALIB_CURVE_DIR
    else:
        crv_dir = curve_base_dir

    asof_date_str = asof_date.strftime('%Y%m%d')

    if live_mode:
        full_curve_name = curve_name + '.live.xml'
        curve_fn = os.path.join(crv_dir, full_curve_name)
        return load_zc(curve_fn)

    # For non-live mode, path down to dated subfolder.
    asof_year = asof_date.strftime('%Y')
    asof_month = asof_date.strftime('%m')
    asof_day = asof_date.strftime('%d')

    curve_point = 'lon1600' if lon1600_mode else asof_date_str

    full_curve_name = curve_name + '.' + curve_point + '.xml'
    curve_fn = os.path.join(crv_dir, asof_year, asof_month, asof_day, full_curve_name)

    return load_zc(curve_fn)


def load_zc(path):
    """
    :description: Returns a curve object from the xml file
    :param str path: fully qualified path the to curve object
    :return: alib curve object
    """
    path = ascii_encode(path)
    sbuf = c_char_p()
    crv_obj = Alib_Class.ZC()
    failure = ALib.ALIB_OBJECT_LOAD(path, byref(crv_obj), byref(sbuf))
    if failure:
        free_object(crv_obj)
        base_err_msg = 'Error while loading zerocurve from path %s' % path
        raise_val_err_w_log(base_err_msg)

    return crv_obj


def ac_zero_curve_slide(alib_curve, new_base_date):
    """
    :description: Slides a zero curve to a new base date
    :param alib_curve: original curve
    :param new_base_date: new date to slide the curve to
    :return: alib curve
    """
    alib_new_base_date = python_date_to_alib_Tdate(new_base_date)
    new_crv_obj = Alib_Class.ZC()
    failure = ALib.ALIB_ZERO_CURVE_SLIDE(
        alib_curve,
        alib_new_base_date,
        byref(new_crv_obj))
    if failure:
        free_object(new_crv_obj)
        base_err_msg = 'Error while sliding zero curve'
        raise_val_err_w_log(base_err_msg)

    return new_crv_obj


def make_single_date_stub_curve(stub_date):
    alib_stub_zero_curve = Alib_Class.ZC()
    baseDate = python_date_to_alib_Tdate(stub_date)
    cashDCC = Alib_Class.DCC()
    alib_obj_coerce("Act/360", "DCC", byref(cashDCC))
    rt = Alib_Class.RT()
    alib_obj_coerce("Annual", "RT", byref(rt))

    stubDiscDates = (c_long * 1)(*[])
    stubDiscRates = (c_double * 1)(*[])
    stubDiscDates[0] = baseDate
    stubDiscRates[0] = 0
    failure = ALib.ALIB_ZERO_CURVE_MAKE(
        c_long(1),  # numDates
        stubDiscDates,  # Dates
        stubDiscRates,  # Rates
        cashDCC,  # DCC
        ascii_encode("F"),  # interpType
        baseDate,  # baseDate
        rt,  # rateType
        byref(alib_stub_zero_curve)  # output
    )
    free_objects(cashDCC, rt)

    if failure:
        free_object(alib_stub_zero_curve)
        base_err_msg = 'Error making stub curve'
        raise_val_err_w_log(base_err_msg)

    return alib_stub_zero_curve


def ac_zero_curve3(stub_zero_curve, instrument_types, start_dates, end_dates, rates, prices, include_flags,
                   adjustment_rates, money_market_dcc, value_floating_leg, swap_day_count_conventions, swap_coupon_ivls,
                   swap_bad_day_convention, float_rate_fixed, fix_dates, fix_rates, futures_flags, futures_mm_date,
                   ir_vol_model, coupon_interpolation, zero_interpolation, extrapolation_date, holiday_file):
    """
    :description: Wrapping of the raw alib zero curve 3 function.
    :param stub_zero_curve: date or alib zero curve object
    :param instrument_types: array of alib instrument type strings
    :param start_dates: array of start dates
    :param end_dates: array of end dates
    :param rates: array of rates
    :param prices: array of prices
    :param include_flags: array of include flags
    :param adjustment_rates: unsupported, supply 0
    :param money_market_dcc: money market dcc string
    :param value_floating_leg: 0 or 1, 1 is standard
    :param swap_day_count_conventions: array of dcc strings, first element is float leg, second is fixed
    :param swap_coupon_ivls: array of interval strings, first element is float leg, second is fixed
    :param swap_bad_day_convention: swap bad day convention (usually M)
    :param float_rate_fixed: unsupported supply 0
    :param fix_dates: unsupported supply 0
    :param fix_rates: unsupported supply 0
    :param futures_flags: unsupported supply 0
    :param futures_mm_date: unsupported supply 0
    :param ir_vol_model: unsupported supply 0
    :param str coupon_interpolation: interpolation type, 'N'
    :param str zero_interpolation: interpolation type string, F or S
    :param extrapolation_date: date or 0
    :param holiday_file: holiday file
    :return: alib zero curve object
    """

    # raise NotImplementedError('Still not quite working')

    if isinstance(stub_zero_curve, Alib_Class.ZC):
        clear_stub = False
        alib_stub_zero_curve = stub_zero_curve
    else:
        clear_stub = True
        alib_stub_zero_curve = make_single_date_stub_curve(stub_zero_curve)
        # Conversion of the first set of arrays, that use nTypes as their length
        ntypes = 0
    if hasattr(instrument_types, "__len__"):
        ntypes = len(instrument_types)
    alib_ntypes = c_long(ntypes)
    alib_inst_types = (c_void_p * ntypes)(*[])  # Array of size numTypes
    alib_start_dates = (c_void_p * ntypes)(*[])  # Array of size numStartDts */
    alib_end_dates = (c_void_p * ntypes)(*[])  # Array of same size as Types */
    alib_rates = (c_double * ntypes)(*[])  # Array of same size as Types */
    alib_prices = (c_double * ntypes)(*[])  # Array of size numPrices */
    alib_include_flags = (c_long * ntypes)(*[])  # Array of size numInclude */
    for i in range(0, ntypes):
        alib_inst_type = Alib_Class.ZCIT()
        alib_obj_coerce(instrument_types[i], 'ZCIT', byref(alib_inst_type))
        alib_inst_types[i] = alib_inst_type.handle
        # start date
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(start_dates[i]), "DOI", byref(doi))
        alib_start_dates[i] = doi.handle
        # end date
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(end_dates[i]), "DOI", byref(doi))
        alib_end_dates[i] = doi.handle
        #
        alib_rates[i] = c_double(rates[i])
        alib_prices[i] = c_double(prices[i])
        alib_include_flags[i] = c_long(include_flags[i])

    # Conversion of the second set of arrays, that use nadjustments as their length
    nadjustments = 0
    if hasattr(adjustment_rates, "__len__"):
        nadjustments = len(adjustment_rates)
    alib_num_adjustments = c_long(nadjustments)
    alib_adjustment_rates = (c_double * nadjustments)(*[])
    for i in range(0, nadjustments):
        alib_adjustment_rates[i] = c_double(adjustment_rates[i])

    # Conversion of the third set of arrays, that use nswaplegs as their length
    nswaplegs = 0
    if hasattr(swap_day_count_conventions, "__len__"):
        nswaplegs = len(swap_day_count_conventions)
    alib_num_swap_legs = c_long(nswaplegs)
    alib_swap_dccs = (c_void_p * nswaplegs)(*[])  # Array of size numSwapDCCs */
    alib_swap_ivls = (c_void_p * nswaplegs)(*[])
    for i in range(0, nswaplegs):
        alib_swap_dcc = Alib_Class.DCC()
        alib_obj_coerce(swap_day_count_conventions[i], 'DCC', byref(alib_swap_dcc))
        alib_swap_dccs[i] = alib_swap_dcc.handle
        alib_swap_ivl = Alib_Class.IVL()
        alib_obj_coerce(swap_coupon_ivls[i], 'IVL', byref(alib_swap_ivl))
        alib_swap_ivls[i] = alib_swap_ivl.handle

    # Conversion of the fourth set of arrays, that use nfixed_dates as their length
    nfixed_dates = 0
    if hasattr(fix_rates, "__len__"):
        nfixed_dates = len(fix_rates)
    alib_num_fixed_dates = c_long(nfixed_dates)
    alib_fix_dates = (c_void_p * nfixed_dates)(*[])
    alib_fix_rates = (c_double * nfixed_dates)(*[])
    for i in range(0, nfixed_dates):
        # date conversion
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(fix_dates[i]), "DOI", byref(doi))
        alib_fix_dates[i] = doi.handle
        alib_fix_rates[i] = c_double(fix_rates[i])

    # Conversion of the fifth set of arrays, that use nfutures_flags as their length
    nfutures_flags = 0
    if hasattr(futures_flags, "__len__"):
        nfutures_flags = len(futures_flags)
    alib_num_futures_flags = c_long(nfutures_flags)
    alib_futures_flags = (c_long * nfutures_flags)(*[])
   for i in range(0, nfutures_flags):
        alib_futures_flags[i] = c_long(futures_flags[i])

    #
    # Scalar values in order
    alib_mm_dcc = Alib_Class.DCC()
    alib_obj_coerce(money_market_dcc, 'DCC', byref(alib_mm_dcc))
    #
    alib_value_floating_leg = c_long(value_floating_leg)
    alib_swap_bdc = ascii_encode(swap_bad_day_convention)
    alib_float_rate_fixed = c_long(float_rate_fixed)
    #
    alib_futures_mm_date = Alib_Class.DOI()
    alib_obj_coerce(futures_mm_date, "DOI", byref(alib_futures_mm_date))
    #
    alib_ir_vol_model = c_long(ir_vol_model)
    alib_coupon_interpolation = ascii_encode(coupon_interpolation)
    #
    alib_zero_interpolation = ascii_encode(zero_interpolation)
    #
    alib_extrapolation_date = Alib_Class.DOI()
    if isinstance(extrapolation_date, (dt.date, dt.datetime)):
        alib_obj_coerce(python_date_to_alib_str_date(extrapolation_date), "DOI", byref(alib_extrapolation_date))
    else:
        alib_obj_coerce("NONE", "DOI", byref(alib_extrapolation_date))
    #
    alib_holiday_file = ascii_encode(holiday_file)
    fit_curve = Alib_Class.ZC()

    # On Windows, ctypes uses win32 structured exception handling to prevent crashes from general protection faults
    # when functions are called with invalid argument values:

    failure = ALib.ALIB_ZERO_CURVE3(
        alib_stub_zero_curve,  # CLASS_ZC      StubCurve,             /* (I) Scalar */
        alib_ntypes,  # int           numTypes,              /* (I) Size of Types array */
        alib_inst_types,  # string[]      Types,                 /* (I) Array of size numTypes */
        alib_ntypes,  # int           numStartDts,           /* (I) Size of StartDts array */
        alib_start_dates,  # string[]      StartDts,              /* (I) Array of size numStartDts */
        alib_end_dates,  # string[]      EndDts,                /* (I) Array of same size as Types */
        alib_rates,  # double[]      Rates,                 /* (I) Array of same size as Types */
        alib_ntypes,  # int           numPrices,             /* (I) Size of Prices array */
        alib_prices,  # double[]      Prices,                /* (I) Array of size numPrices */
        alib_ntypes,  # int           numInclude,            /* (I) Size of Include array */
        alib_include_flags,  # int[]         Include,               /* (I) Array of size numInclude */
        alib_num_adjustments,  # int           numAdj,                /* (I) Size of Adj array */
        alib_adjustment_rates,  # double[]      Adj,                   /* (I) Array of size numAdj */
        alib_mm_dcc,  # string        MMDCC,                 /* (I) Scalar */
        alib_value_floating_leg,  # int           ValueFlt,              /* (I) Scalar */
        alib_num_swap_legs,  # int           numSwapDCCs,           /* (I) Size of SwapDCCs array */
        alib_swap_dccs,  # string[]      SwapDCCs,              /* (I) Array of size numSwapDCCs */
        alib_num_swap_legs,  # int           numSwapIVLs,           /* (I) Size of SwapIVLs array */
        alib_swap_ivls,  # string[]      SwapIVLs,              /* (I) Array of size numSwapIVLs */
        alib_swap_bdc,  # string        SwapBDC,               /* (I) Scalar */
        alib_float_rate_fixed,  # int           FltFixed,              /* (I) Scalar */
        alib_num_fixed_dates,  # int           numFixDates,           /* (I) Size of FixDates array */
        alib_fix_dates,  # string[]      FixDates,              /* (I) Array of size numFixDates */
        alib_fix_rates,  # double[]      FixRates,              /* (I) Array of same size as FixDates */
        alib_num_futures_flags,  # int           numFutFlags,           /* (I) Size of FutFlags array */
        alib_futures_flags,  # int[]         FutFlags,              /* (I) Array of size numFutFlags */
        alib_futures_mm_date,  # string        FutMMDate,             /* (I) Scalar */
        alib_ir_vol_model,  # CLASS_VMIR    VolModelIR,            /* (I) Scalar */
        alib_coupon_interpolation,  # string        CpnInterp,             /* (I) Scalar */
        alib_zero_interpolation,  # string        ZeroInterp,            /* (I) Scalar */
        alib_extrapolation_date,  # string        ExtrapDate,            /* (I) Scalar */
        alib_holiday_file,  # string        HolidayFile,           /* (I) Scalar */
        byref(fit_curve)  # out CLASS_ZC  ZeroCurve              /* (O) Scalar */
    )
    free_objects(*alib_start_dates)
    free_objects(*alib_end_dates)
    free_objects(*alib_fix_dates)
    free_objects(*alib_inst_types)
    free_objects(*alib_swap_dccs)
    free_objects(*alib_swap_ivls)
    free_objects(alib_mm_dcc, alib_futures_mm_date, alib_extrapolation_date)
    if clear_stub:
        free_object(alib_stub_zero_curve)

    if failure:
        base_err_msg = 'Error in alib zc3 bootstrap'
        raise_val_err_w_log(base_err_msg)

    return fit_curve


def ac_zero_curve4_cb(stub_disc_curve, stub_est_curve, instrument_types, start_dates, end_dates, rates, basis, prices,
                      include_flags, adjustment_rates, money_market_dcc, swap_day_count_conventions, swap_coupon_ivls,
                      swap_bad_day_convention, float_rate_fixed, fix_dates, fix_rates, futures_flags, futures_mm_date,
                      ir_vol_model, coupon_interpolation, zero_interpolation, extrapolation_date, holiday_file,
                      holiday_file_basis):
    """
    :description: Wrapping of the raw alib_zero_curve4_cb  function.
    :param stub_disc_curve: date or alib zero curve object
    :param stub_est_curve: date or alib zero curve object
    :param instrument_types: array of alib instrument type strings
    :param start_dates: array of start dates
    :param end_dates: array of end dates
    :param rates: array of rates
    :param basis: array of basis
    :param prices: array of prices
    :param include_flags: array of include flags
    :param adjustment_rates: unsupported, supply 0
    :param money_market_dcc: money market dcc string
    :param swap_day_count_conventions: array of dcc strings, first element is float leg, second is fixed
    :param swap_coupon_ivls: array of interval strings, first element is float leg, second is fixed
    :param swap_bad_day_convention: swap bad day convention (usually M)
    :param float_rate_fixed: unsupported supply 0
    :param fix_dates: unsupported supply 0
    :param fix_rates: unsupported supply 0
    :param futures_flags: unsupported supply 0
    :param futures_mm_date: unsupported supply 0
    :param ir_vol_model: unsupported supply 0
    :param coupon_interpolation: unsupported supply 0
    :param zero_interpolation: interpolation type string, F or S
    :param extrapolation_date: date or 0
    :param holiday_file: holiday file
    :return: tuple of alib zero curve objects
    """

    if isinstance(stub_disc_curve, Alib_Class.ZC):
        clear_stub = False
        alib_stub_disc_curve = stub_disc_curve
    else:
        clear_stub = True
        alib_stub_disc_curve = make_single_date_stub_curve(stub_disc_curve)

    if isinstance(stub_est_curve, Alib_Class.ZC):
        alib_stub_est_curve = stub_est_curve
    else:
        alib_stub_est_curve = Alib_Class.ZC()
        alib_stub_est_curve = make_single_date_stub_curve(stub_est_curve)

    # Conversion of the first set of arrays, that use nTypes as their length
    ntypes = len(instrument_types)
    alib_ntypes = c_long(ntypes)
    alib_inst_types = (c_void_p * ntypes)(*[])  # Array of size numTypes
    alib_start_dates = (c_void_p * ntypes)(*[])  # Array of size numStartDts */
    alib_end_dates = (c_void_p * ntypes)(*[])  # Array of same size as Types */
    alib_rates = (c_double * ntypes)(*[])  # Array of same size as Types */
    alib_basis = (c_double * ntypes)(*[])  # Array of same size as Types */
    alib_prices = (c_double * ntypes)(*[])  # Array of size numPrices */
    alib_include_flags = (c_long * ntypes)(*[])  # Array of size numInclude */
    for i in range(0, ntypes):
        alib_inst_type = Alib_Class.ZCIT()
        alib_obj_coerce(instrument_types[i], 'ZCIT', byref(alib_inst_type))
        alib_inst_types[i] = alib_inst_type.handle
        # start date
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(start_dates[i]), "DOI", byref(doi))
        alib_start_dates[i] = doi.handle
        # end date
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(end_dates[i]), "DOI", byref(doi))
        alib_end_dates[i] = doi.handle
        #
        alib_rates[i] = c_double(rates[i])
        alib_basis[i] = c_double(basis[i])
        alib_prices[i] = c_double(prices[i])
        alib_include_flags[i] = c_long(include_flags[i])

    # Conversion of the second set of arrays, that use nadjustments as their length
    nadjustments = len(adjustment_rates)
    alib_num_adjustments = c_long(nadjustments)
    alib_adjustment_rates = (c_double * nadjustments)(*[])
    for i in range(0, nadjustments):
        alib_adjustment_rates[i] = c_double(adjustment_rates[i])

    # Conversion of the third set of arrays, that use nswaplegs as their length
    nswaplegs = len(swap_day_count_conventions)
    alib_num_swap_legs = c_long(nswaplegs)
    alib_swap_dccs = (c_void_p * nswaplegs)(*[])  # Array of size numSwapDCCs */
    alib_swap_ivls = (c_void_p * nswaplegs)(*[])
    for i in range(0, nswaplegs):
        alib_swap_dcc = Alib_Class.DCC()
        alib_obj_coerce(swap_day_count_conventions[i], 'DCC', byref(alib_swap_dcc))
        alib_swap_dccs[i] = alib_swap_dcc.handle

        alib_swap_ivl = Alib_Class.IVL()
        alib_obj_coerce(swap_coupon_ivls[i], 'IVL', byref(alib_swap_ivl))
        alib_swap_ivls[i] = alib_swap_ivl.handle

        # Conversion of the fourth set of arrays, that use nfixed_dates as their length
    nfixed_dates = len(fix_rates)
    alib_num_fixed_dates = c_long(nfixed_dates)
    alib_fix_dates = (c_void_p * nfixed_dates)(*[])
    alib_fix_rates = (c_double * nfixed_dates)(*[])
    for i in range(0, nfixed_dates):
        # date conversion
        doi = Alib_Class.DOI()
        alib_obj_coerce(python_date_to_alib_str_date(fix_dates[i]), "DOI", byref(doi))
        alib_fix_dates[i] = doi.handle
        alib_fix_rates[i] = c_double(fix_rates[i])

    # Conversion of the fifth set of arrays, that use nswaplegs as their length
    nfutures_flags = len(futures_flags)
    alib_num_futures_flags = c_long(nfutures_flags)
    alib_futures_flags = (c_long * nfixed_dates)(*[])
    for i in range(0, nfutures_flags):
        alib_futures_flags[i] = c_long(futures_flags[i])

    #
    # Scalar values in order
    alib_mm_dcc = Alib_Class.DCC()
    alib_obj_coerce(money_market_dcc, 'DCC', byref(alib_mm_dcc))
    #
    alib_swap_bdc = swap_bad_day_convention
    alib_float_rate_fixed = c_long(float_rate_fixed)
    #
    alib_futures_mm_date = Alib_Class.DOI()
    alib_obj_coerce(futures_mm_date, "DOI", byref(alib_futures_mm_date))
    #
    alib_ir_vol_model = c_long(ir_vol_model)
    alib_coupon_interpolation = ascii_encode(coupon_interpolation)
    #
    alib_zero_interpolation = ascii_encode(zero_interpolation)
    #
    alib_extrapolation_date = Alib_Class.DOI()
    if isinstance(extrapolation_date, (dt.date, dt.datetime)):
        alib_obj_coerce(python_date_to_alib_str_date(extrapolation_date), "DOI", byref(alib_extrapolation_date))
    else:
        alib_obj_coerce("NONE", "DOI", byref(alib_extrapolation_date))
    #
    alib_holiday_file = ascii_encode(holiday_file)
    alib_holiday_file_basis = ascii_encode(holiday_file_basis)
    disc_zero_curve = Alib_Class.ZC()
    est_zero_curve = Alib_Class.ZC()

    # On Windows, ctypes uses win32 structured exception handling to prevent crashes from general protection faults
    # when functions are called with invalid argument values:
    failure = ALib.ALIB_ZERO_CURVE4_CB(
        alib_stub_disc_curve,  # stubDiscZC
        alib_stub_est_curve,  # stubEstZC
        alib_ntypes,  # numTypes
        alib_inst_types,  # Types
        alib_ntypes,  # numStartDts
        alib_start_dates,  # startDts
        alib_end_dates,  # endDts
        alib_rates,  # rates
        alib_basis,  # basis rates
        alib_ntypes,  # numPrices
        alib_prices,  # prices
        alib_ntypes,  # numIncludes
        alib_include_flags,  # includes
        alib_num_adjustments,  # numAdj
        alib_adjustment_rates,  # adj
        alib_mm_dcc,  # mmDCC
        alib_num_swap_legs,  # numSwapsDCC
        alib_swap_dccs,  # swapDCC
        alib_num_swap_legs,  # numSwapsIVLs
        alib_swap_ivls,  # swapsIVLs
        alib_swap_bdc,  # swapBDC
        alib_float_rate_fixed,  # fltFixed
        alib_num_fixed_dates,  # numFixDates
        alib_fix_dates,  # fixDates
        alib_fix_rates,  # fixRates
        alib_num_futures_flags,  # numFutFlags
        alib_futures_flags,  # futFlags
        alib_futures_mm_date,  # futMMdt
        alib_ir_vol_model,  # volModelIR
        alib_coupon_interpolation,  # cpnInterp
        alib_zero_interpolation,  # zeroInterp
        alib_extrapolation_date,  # extrapDate
        alib_holiday_file,  # holidays
        alib_holiday_file_basis,  # basisHolidays
        byref(disc_zero_curve),
        byref(est_zero_curve)
   )

    free_objects(*alib_start_dates)
    free_objects(*alib_end_dates)
    free_objects(*alib_fix_dates)
    free_objects(*alib_inst_types)
    free_objects(*alib_swap_dccs)
    free_objects(*alib_swap_ivls)
    free_objects(alib_mm_dcc, alib_futures_mm_date, alib_extrapolation_date)
    if clear_stub:
        free_object(alib_stub_disc_curve)

    if failure:
        base_err_msg = 'Error in alib zc4_cb bootstrap'
        raise_val_err_w_log(base_err_msg)

    return disc_zero_curve, est_zero_curve


def ac_zero_curve_make(
        curve_dates,
        curve_zero_rates,
        curve_base_date,
        curve_dcc='Act/365F',
        interp_type='F',
        rate_type='Annual'):
    """
    :description: Zero curve constructor. This function makes a ZeroCurve (ZC) object from its component parts
    :param curve_dates: array of dates (does not include the base date)
    :param curve_zero_rates: array of zero rates
    :param curve_base_date: base date for curve
    :param curve_dcc: day count convention for the curve, default is Act/365
    :param interp_type: interpolation type for the curve [F]lat forward or [S]pline, default is F
    :param rate_type: rate type (default Annual)
    :return: alib curve object
    """
    ndates = len(curve_dates)

    alib_curve_dates = (c_long * ndates)(*[])
    alib_curve_rates = (c_double * ndates)(*[])

    for i in range(0, ndates):
        d = curve_dates[i]
        alib_curve_dates[i] = python_date_to_alib_Tdate(d)
        alib_curve_rates[i] = c_double(curve_zero_rates[i])

    alib_curve_base_date = python_date_to_alib_Tdate(curve_base_date)

    alib_dcc = Alib_Class.DCC()
    alib_obj_coerce(curve_dcc, 'DCC', byref(alib_dcc))

    alib_rate_type = Alib_Class.RT()
    alib_obj_coerce(rate_type, 'RT', byref(alib_rate_type))

    crv_obj = Alib_Class.ZC()
    failure = ALib.ALIB_ZERO_CURVE_MAKE(
        ndates,
        alib_curve_dates,
        alib_curve_rates,
        alib_dcc,
        ascii_encode(interp_type),
        alib_curve_base_date,
        alib_rate_type,
        byref(crv_obj)
    )
    free_objects(alib_dcc, alib_rate_type)

    if failure:
        free_object(crv_obj)
        base_err_msg = 'Error while making zero curve'
        raise_val_err_w_log(base_err_msg)

    return crv_obj


def ac_interp_pv(discount_curve, date):
    """
    :description: interpolate a discount factor from an alib curve
    :param discount_curve: alib curve
    :param date: date for disocunt factor
    :return: float
    """
    try:
        alib_date = python_date_to_alib_Tdate(date)
    except:
        alib_date = date
    df = c_double()

    failure = ALib.ALIB_INTERP_PV(discount_curve, alib_date, byref(df))

    if failure:
        base_err_msg = 'Could not compute discount factor'
        raise_val_err_w_log(base_err_msg)
    return df.value


def ac_rate_to_rate(
        input_rate, start_date, end_date,
        input_dcc_str, output_dcc_str,
        input_rate_type_string='Annual',
        output_rate_type_string='Continuous'
):
    """
    :description: converts a rate of one type (e.g. Act/365 Annual) to another (e.g. Act/365 continuous
    :param float input_rate: rate to convert
    :param dt.date start_date:
    :param dt.date end_date:
    :param input_dcc_str: input rate day count convention
    :param output_dcc_str: output rate day count convention
    :param input_rate_type_string: alib rate type, default 'Annual'
    :param output_rate_type_string: alib rate type, default 'Continuous'
    :return: float
    """
    input_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(input_dcc_str, "DCC", byref(input_dcc))

    output_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(output_dcc_str, "DCC", byref(output_dcc))

    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_end_date = python_date_to_alib_Tdate(end_date)

    input_rate_type = Alib_Class.RT()
    failure = alib_obj_coerce(input_rate_type_string, "RT", byref(input_rate_type))

    output_rate_type = Alib_Class.RT()
    failure = alib_obj_coerce(output_rate_type_string, "RT", byref(output_rate_type))

    output_rate = c_double()

    failure = ALib.ALIB_RATE_TO_RATE(
        c_double(input_rate),  # double                 Rate,                  /* (I) Scalar */
        alib_start_date,  # TDate                  StartDate,             /* (I) Scalar */
        alib_end_date,  # TDate                  MaturityDate,          /* (I) Scalar */
        input_dcc,  # string                 DCC,                   /* (I) Scalar */
        input_rate_type,  # CLASS_RT               RateType,              /* (I) Scalar */
        output_dcc,  # string                 DesiredDCC,            /* (I) Scalar */
        output_rate_type,  # CLASS_RT               DesiredRateType,       /* (I) Scalar */
        byref(output_rate)  # out double             Output1                /* (O) Scalar */
    )

    free_objects(input_dcc, output_dcc, input_rate_type, output_rate_type)
    if failure:
        base_err_msg = 'Failed to convert rate'
        raise_val_err_w_log(base_err_msg)
    return output_rate.value


def ac_mm_rate_o(projection_curve, start_date, end_date, dcc):
    """
    :description: Project a money market rate off a curve
    :param projection_curve: alib curve
    :param start_date:
    :param end_date:
    :param dcc: day count convenction (e.g. 'Act/360')
    :return: float
    """
    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    try:
        alib_end_date = python_date_to_alib_Tdate(end_date)
    except:
        alib_end_date = end_date

    alib_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(dcc, "DCC", byref(alib_dcc))

    rate = c_double()
    failure = ALib.ALIB_MM_RATE_O(
        projection_curve,  # CLASS_ZC      ZeroCurve,             /* (I) Scalar */
        alib_start_date,  # TDate         StartDate,             /* (I) Scalar */
        alib_end_date,  # TDate         MaturityDate,          /* (I) Scalar */
        alib_dcc,  # string        DesiredDCC,            /* (I) Scalar */
        byref(rate))  # out double    DesiredRate            /* (O) Scalar */

    free_object(alib_dcc)

    if failure:
        base_err_msg = 'Could not compute mm rate'
        raise_val_err_w_log(base_err_msg)

    return rate.value

def swap_conventions(conv_str, swap_conv_dict=None):
    if swap_conv_dict is None:
        swap_conv_dict = CONV_DICT

    if conv_str in swap_conv_dict:
        return swap_conv_dict[conv_str]
    else:
        raise ValueError('Conventions do not exist for specified string.  %s was given' % conv_str)

def ac_swap_rate_adj2(
        discount_curve, start_date, end_date, fixed_interval,
        fixed_dcc, value_float_leg, float_pv, projection_curve,
        float_interval, float_dcc, already_fixed, fixing_rate,
        stub_method, convexity_adjust, vol_model, accrual_bad_day_conv,
        pay_bad_day_conv, reset_bad_day_conv, holiday_file
):
    """
    :description: Calculate a par swap rate
    :param discount_curve: alib curve object
    :param start_date:
    :param end_date:
    :param fixed_interval: Alib interval string, e.g. 1Y
    :param fixed_dcc: Day count convention, e.g. '30/360'
    :param value_float_leg: 1 or 0
    :param float_pv: give the floating PV if value_float_leg=0, usually this is 1
    :param projection_curve: alib curve, only required if value_float_leg!=0
    :param float_interval: Alib interval string, e.g. 3M
    :param float_dcc: Day count convention, e.g. 'Act/360'
    :param already_fixed: Has the first floating period fixed 1 or 0
    :param fixing_rate: first period fixing, ingored if already_fixed=0
    :param stub_method: alib stub descriptor, usually 'f/s'
    :param convexity_adjust: apply convexity adjustment, usually 0
    :param vol_model: _vol model object for convexity adjustment, usually 0
    :param accrual_bad_day_conv: one of 'M', 'F' or 'N'
    :param pay_bad_day_conv: one of 'M', 'F' or 'N'
    :param reset_bad_day_conv: one of 'M', 'F' or 'N'
    :param holiday_file: holiday calendar
    :return: float
    """
    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    try:
        alib_end_date = python_date_to_alib_Tdate(end_date)
    except:
        alib_end_date = end_date

    alib_fixed_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(fixed_interval, "IVL", byref(alib_fixed_interval))

    alib_fixed_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(fixed_dcc, "DCC", byref(alib_fixed_dcc))

    alib_value_float_leg = value_float_leg if isinstance(value_float_leg, c_long) else c_long(value_float_leg)
    alib_float_pv = float_pv if isinstance(float_pv, c_double) else c_double(float_pv)

    alib_float_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(float_interval, "IVL", byref(alib_float_interval))

    alib_float_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(float_dcc, "DCC", byref(alib_float_dcc))

    alib_already_fixed = already_fixed if isinstance(already_fixed, c_long) else c_long(already_fixed)
    alib_fixing_rate = fixing_rate if isinstance(fixing_rate, c_double) else c_double(fixing_rate)

    alib_stub_method = Alib_Class.STB()
    failure = alib_obj_coerce(stub_method, "STB", byref(alib_stub_method))

    alib_convexity_adjust = convexity_adjust if isinstance(convexity_adjust, c_long) else c_long(convexity_adjust)

    alib_vol_model = Alib_Class.VMIR()
    failure = alib_obj_coerce(vol_model, "VMIR", byref(alib_vol_model))

    alib_accrual_bad_day_conv = ascii_encode(accrual_bad_day_conv)
    alib_pay_bad_day_conv = ascii_encode(pay_bad_day_conv)
    alib_reset_bad_day_conv = ascii_encode(reset_bad_day_conv)
    alib_holiday_file = ascii_encode(holiday_file)

    rate = c_double()

    failure = ALib.ALIB_SWAP_RATE2(
        discount_curve,
        alib_start_date,
        alib_end_date,
        alib_fixed_interval,
        alib_fixed_dcc,
        alib_value_float_leg,
        alib_float_pv,
        projection_curve,
        alib_float_interval,
        alib_float_dcc,
        alib_already_fixed,
        alib_fixing_rate,
        alib_stub_method,
        alib_convexity_adjust,
        alib_vol_model,
        alib_accrual_bad_day_conv,
        alib_pay_bad_day_conv,
        alib_reset_bad_day_conv,
        alib_holiday_file,
        byref(rate)
    )

    free_objects(alib_fixed_interval, alib_fixed_dcc, alib_float_interval,
                 alib_float_dcc, alib_stub_method, alib_vol_model)
    if failure:
        base_err_msg = 'Could not compute swap rate'
        raise_val_err_w_log(base_err_msg)
    return rate.value


def swap_rate_with_conv(
        discount_curve, start_date, end_date, value_float_leg, float_pv,
        projection_curve, already_fixed, fixing_rate, holiday_oracle, conv_str, swap_conv_dict=None
):
    """
    :description: compute a swap rate using standard market conventions
    :param discount_curve: alib curve object.
    :param start_date:
    :param end_date:
    :param value_float_leg: 1 or 0, usually 1
    :param float_pv: pv of the float leg, ignored unless value_float_leg=0
    :param projection_curve: alib curve object
    :param already_fixed: Has the first floating period fixed 1 or 0
    :param fixing_rate: first period fixing, ingored if already_fixed=0
    :param holiday_oracle: holiday oracle object
    :param conv_str: panormus convention string, e.g. USD.3ML
    :return: float
    """
    conventions = swap_conventions(conv_str, swap_conv_dict)

    needed_conventions = {}
    for s in ['fixed_interval', 'fixed_dcc', 'float_interval', 'float_dcc',
              'stub_method', 'accrual_bad_day_conv', 'convexity_adjust',
              'pay_bad_day_conv', 'reset_bad_day_conv', 'vol_model']:
        needed_conventions[s] = conventions[s]

    holiday_file = holiday_oracle[conventions['holiday_calendar_name']]

    return ac_swap_rate_adj2(
        discount_curve=discount_curve, start_date=start_date,
        end_date=end_date, value_float_leg=value_float_leg,
        float_pv=float_pv, projection_curve=projection_curve,
        already_fixed=already_fixed, fixing_rate=fixing_rate,
        holiday_file=holiday_file, **needed_conventions)


def ac_swap_fixed_pv(
        discount_curve, coupon_rate, start_date, fixed_interval,
        end_date, fixed_dcc, stub_method, accrual_bad_day_conv,
        pay_bad_day_conv, holiday_file, principal_initial_flag,
        principal_final_flag, value_date
):
    """
    :description: compute the fixed pv of a swap
    :param discount_curve: alib curve object
    :param coupon_rate: swap coupon
    :param start_date:
    :param fixed_interval: alib interval, e.g. 6M
    :param end_date:
    :param fixed_dcc: day count convention, e.g. '30/360'
    :param stub_method: alib stub descriptor, usually 'f/s'
    :param accrual_bad_day_conv: one of 'M', 'F' or 'N'
    :param pay_bad_day_conv: one of 'M', 'F' or 'N'
    :param holiday_file:
    :param principal_initial_flag: if 1 inserts a payment of -1 at the start date
    :param principal_final_flag: if 1 inserts a payment of 1 at the maturity date
    :param value_date:
    :return: float
    """
    alib_discount_curve = discount_curve

    alib_coupon_rate = c_double(float(coupon_rate))

    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    alib_fixed_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(fixed_interval, 'IVL', byref(alib_fixed_interval))

    alib_fixed_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(fixed_dcc, 'DCC', byref(alib_fixed_dcc))
    try:
        alib_end_date = python_date_to_alib_Tdate(end_date)
    except:
        alib_end_date = end_date

    alib_stub_method = Alib_Class.STB()
    failure = alib_obj_coerce(stub_method, 'STB', byref(alib_stub_method))

    alib_accrual_bad_day_conv = ascii_encode(accrual_bad_day_conv)

    alib_pay_bad_day_conv = ascii_encode(pay_bad_day_conv)

    alib_holiday_file = ascii_encode(holiday_file)

    alib_principal_initial_flag = c_long(int(principal_initial_flag))

    alib_principal_final_flag = c_long(int(principal_final_flag))

    try:
       alib_value_date = python_date_to_alib_Tdate(value_date)
    except:
        alib_value_date = value_date

    raw_res = c_double()
    failure = ALib.ALIB_SWAP_FIXED_PV(
        alib_discount_curve,
        alib_coupon_rate,
        alib_start_date,
        alib_fixed_interval,
        alib_end_date,
        alib_fixed_dcc,
        alib_stub_method,
        alib_accrual_bad_day_conv,
        alib_pay_bad_day_conv,
        alib_holiday_file,
        alib_principal_initial_flag,
        alib_principal_final_flag,
        alib_value_date,
        byref(raw_res)
    )
    free_objects(alib_fixed_interval, alib_fixed_dcc, alib_stub_method)
    if failure:
        base_err_msg = 'Could not compute swap_fixed_pv'
        raise_val_err_w_log(base_err_msg)
    swap_fixed_pv = raw_res.value
    return swap_fixed_pv


def swap_fixed_pv_with_conv(
        discount_curve, coupon_rate, start_date, end_date,
        holiday_oracle, value_date, conv_str, swap_conv_dict=None
):
    """
    :description: compute a swap fixed PV using standard market conventions
    :param discount_curve: alib curve object
    :param coupon_rate: swap coupon
    :param start_date:
    :param end_date:
    :param holiday_oracle: holiday oracle object
    :param value_date:
    :param conv_str: panormus convention string, e.g. EUR.6ML
    :return: float
    """
    conventions = swap_conventions(conv_str, swap_conv_dict)

    needed_conventions = {}
    for s in ['fixed_interval', 'fixed_dcc', 'stub_method', 'pay_bad_day_conv',
              'accrual_bad_day_conv', 'principal_initial_flag', 'principal_final_flag']:
        needed_conventions[s] = conventions[s]

    holiday_file = holiday_oracle[conventions['holiday_calendar_name']]

    return ac_swap_fixed_pv(discount_curve=discount_curve, coupon_rate=coupon_rate,
                            start_date=start_date, end_date=end_date,
                            holiday_file=holiday_file, value_date=value_date, **needed_conventions)


def swap_annuity_with_conv(
        discount_curve,
        start_date, end_date, holiday_oracle,
        value_date, conv_str,swap_conv_dict=None
):
    """
    :description: compute a swap annuity using standard market conventions
    :param discount_curve: alib curve object
    :param start_date:
    :param end_date:
    :param holiday_oracle: holiday oracle object
    :param value_date:
    :param conv_str: panormus convention string, e.g. EUR.6ML
    :return: float
    """
    return swap_fixed_pv_with_conv(
        discount_curve, 1.0, start_date, end_date,
        holiday_oracle, value_date, conv_str,swap_conv_dict)


def ac_swap_fixed_sens(
        discount_curve, coupon_rate, start_date, fixed_interval, maturity_date,
        fixed_dcc, stub_method, accrual_bad_day_conv,
        pay_bad_day_conv, holiday_file, principal_initial_flag,
        principal_final_flag, value_date, sens_type
):
    """
    :description: compute the PV or PVBP of a fixed swap leg
    :param discount_curve: alib_curve
    :param coupon_rate: swap rate
    :param start_date:
    :param fixed_interval: alib interval string, e.g. 6M
    :param maturity_date:
    :param fixed_dcc: alib day count convention, e.g. Act/360
    :param stub_method: stub descriptor, usually f/s
    :param accrual_bad_day_conv: one of 'M', 'F', or 'N'
    :param pay_bad_day_conv: one of 'M', 'F', or 'N'
    :param holiday_file: holiday file
    :param principal_initial_flag: if 1 inserts a payment of -1 at the start date
    :param principal_final_flag: if 1 inserts a payment of 1 at the maturity date
   :param value_date:
    :param sens_type: one of 'P' for PV, or 'V' for PVBP
    :return:
    """
    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    alib_fixed_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(fixed_interval, 'IVL', byref(alib_fixed_interval))

    try:
        alib_end_date = python_date_to_alib_Tdate(maturity_date)
    except:
        alib_end_date = maturity_date

    alib_fixed_dcc = Alib_Class.DCC()
   failure = alib_obj_coerce(fixed_dcc, 'DCC', byref(alib_fixed_dcc))

    alib_stub_method = Alib_Class.STB()
    failure = alib_obj_coerce(stub_method, 'STB', byref(alib_stub_method))

    alib_accrual_bad_day_conv = ascii_encode(accrual_bad_day_conv)
    alib_pay_bad_day_conv = ascii_encode(pay_bad_day_conv)
    alib_holiday_file = ascii_encode(holiday_file)
    alib_principal_initial_flag = c_long(int(principal_initial_flag))
    alib_principal_final_flag = c_long(int(principal_final_flag))

    try:
        alib_value_date = python_date_to_alib_Tdate(value_date)
    except:
        alib_value_date = value_date

    raw_res = c_double()

    failure = ALib.ALIB_SWAP_FIXED_SENS(
        discount_curve,
        c_double(coupon_rate),
        alib_start_date,
        alib_fixed_interval,
        alib_end_date,
        alib_fixed_dcc,
        alib_stub_method,
        alib_accrual_bad_day_conv,
        alib_pay_bad_day_conv,
        alib_holiday_file,
        alib_principal_initial_flag,
        alib_principal_final_flag,
        alib_value_date,
        ascii_encode(sens_type),
        byref(raw_res))

    free_objects(alib_fixed_interval, alib_fixed_dcc, alib_stub_method)
    if failure:
        base_err_msg = 'Could not compute swap fixed sens'
        raise_val_err_w_log(base_err_msg)
    return raw_res.value


def ac_swap_float_sens(
        discount_curve, notional, spread, projection_curve, start_date,
        float_interval, float_rate_interval, compound_method, end_date,
        float_dcc, stub_method, accrual_bad_day_conv,
        pay_bad_day_conv, reset_bad_day_conv, holiday_file, principal_initial_flag,
        principal_final_flag, first_float_fixed_flag, first_float_fixed_rate,
        value_date, convexity_adjust, vol_model, sens_type
):
    """
    :description: compute the PV or PVBP of a floating swap leg
    :param discount_curve: alib curve object
    :param notional: notional of the swap
    :param spread: spread over the projected rate
    :param projection_curve: alib curve object
    :param start_date:
    :param float_interval: alib interval string, e.g. 6M
    :param float_rate_interval: alib rate interval string, e.g. 6M
    :param compound_method: compound method of spread. 1=compounding, 2=flat compounding
    :param end_date:
    :param float_dcc: day count convention, e.g. 'Act/365F'
    :param stub_method: alib stub descriptor, usually 'f/s'
    :param accrual_bad_day_conv: one of 'M', 'F', or 'N'
    :param pay_bad_day_conv: one of 'M', 'F', or 'N'
    :param reset_bad_day_conv: one of 'M', 'F', or 'N'
    :param holiday_file:
    :param principal_initial_flag: if 1 inserts a payment of -1 at the start date
    :param principal_final_flag: if 1 inserts a payment of 1 at the maturity date
    :param first_float_fixed_flag: Flag to denote that the first floating rate has been fixed.
    :param first_float_fixed_rate: The fixing of the first floating rate. Only required if first float rate fixed = 1.
    :param value_date:
    :param convexity_adjust: unsupported, use 0
    :param vol_model: unsupported use 0
    :param sens_type: one of 'P' for PV, or 'V' for PVBP
    :return:
    """
    alib_notional = c_double(float(notional))
    alib_spread = c_double(float(spread))

    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    alib_float_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(float_interval, 'IVL', byref(alib_float_interval))

    alib_float_rate_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(float_rate_interval, 'IVL', byref(alib_float_rate_interval))

    alib_compound_method = c_long(int(compound_method))

    try:
        alib_end_date = python_date_to_alib_Tdate(end_date)
    except:
        alib_end_date = end_date

    alib_float_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(float_dcc, 'DCC', byref(alib_float_dcc))

    alib_stub_method = Alib_Class.STB()
    failure = alib_obj_coerce(stub_method, 'STB', byref(alib_stub_method))

    alib_accrual_bad_day_conv = ascii_encode(accrual_bad_day_conv)
    alib_pay_bad_day_conv = ascii_encode(pay_bad_day_conv)
    alib_reset_bad_day_conv = ascii_encode(reset_bad_day_conv)
    alib_holiday_file = ascii_encode(holiday_file)
    alib_principal_initial_flag = c_long(int(principal_initial_flag))
    alib_principal_final_flag = c_long(int(principal_final_flag))
    alib_first_float_fixed_flag = c_long(int(first_float_fixed_flag))
    alib_first_float_fixed_rate = c_double(float(first_float_fixed_rate))

    # l_fixed = first_float_fixed_rate
    # if not isinstance(first_float_fixed_rate, list):
    #     l_fixed = [first_float_fixed_rate]
    #
    # nfixed_rates = len(l_fixed)
    # alib_num_fixed_rates = c_long(nfixed_rates)
    # alib_fix_rates = (c_double * nfixed_rates)(*[])
    # for i in range(0, nfixed_rates):
    #     alib_fix_rates[i] = c_double(l_fixed[i])

    try:
        alib_value_date = python_date_to_alib_Tdate(value_date)
    except:
        alib_value_date = value_date

    alib_convexity_adjust = convexity_adjust if isinstance(convexity_adjust, c_long) else c_long(convexity_adjust)

    alib_vol_model = Alib_Class.VMIR()
    failure = alib_obj_coerce(vol_model, 'VMIR', byref(alib_vol_model))

    raw_res = c_double()
    failure = ALib.ALIB_SWAP_FLOAT_SENS(
        discount_curve,
        alib_notional,
        alib_spread,
        projection_curve,
        alib_start_date,
        alib_float_interval,
        alib_end_date,
        alib_float_dcc,
        alib_stub_method,
        alib_accrual_bad_day_conv,
        alib_pay_bad_day_conv,
        alib_reset_bad_day_conv,
        alib_holiday_file,
        alib_principal_initial_flag,
        alib_principal_final_flag,
        alib_first_float_fixed_flag,
        alib_first_float_fixed_rate,
        alib_value_date,
        alib_convexity_adjust,
        alib_vol_model,
        ascii_encode(sens_type),
        byref(raw_res)
    )

    # failure = ALib.ALIB_SWAP_FLOAT_SENS_GENERIC(
    #     discount_curve,
    #     alib_notional,
    #     alib_spread,
    #     projection_curve,
    #     alib_start_date,
    #     alib_float_interval,
    #     alib_float_rate_interval,
    #     alib_compound_method,
    #     alib_end_date,
    #     alib_float_dcc,
    #     alib_stub_method,
    #     alib_accrual_bad_day_conv,
    #     alib_pay_bad_day_conv,
    #     alib_reset_bad_day_conv,
    #     alib_holiday_file,
    #     alib_principal_initial_flag,
    #     alib_principal_final_flag,
    #     alib_first_float_fixed_flag,
    #     alib_num_fixed_rates,
    #     alib_fix_rates,
    #     alib_value_date,
    #     alib_convexity_adjust,
    #     alib_vol_model,
    #     ascii_encode(sens_type),
    #     byref(raw_res)
    # )

    free_objects(alib_float_interval, alib_float_rate_interval, alib_float_dcc, alib_stub_method, alib_vol_model)
    if failure:
        base_err_msg = 'Could not compute swap float sens'
        raise_val_err_w_log(base_err_msg)
    return raw_res.value


def ac_swap_float_pv(
        discount_curve, notional, spread, projection_curve, start_date,
        float_interval, float_rate_interval, compound_method, end_date, float_dcc, stub_method, accrual_bad_day_conv,
        pay_bad_day_conv, reset_bad_day_conv, holiday_file, principal_initial_flag,
        principal_final_flag, first_float_fixed_flag, first_float_fixed_rate,
        value_date, convexity_adjust, vol_model
):
    """
    :description: compute the floating pv of a swap
    :param discount_curve: alib curve object
    :param notional: notional of the swap
    :param spread: spread over the projected rate
    :param projection_curve: alib curve object
    :param start_date:
    :param float_interval: alib interval string, e.g. 6M
    :param float_rate_interval: alib rate interval string, e.g. 6M
    :param compound_method: compound method of spread. 1=compounding, 2=flat compounding
    :param end_date:
    :param float_dcc: day count convention, e.g. 'Act/365F'
    :param stub_method: alib stub descriptor, usually 'f/s'
    :param accrual_bad_day_conv: one of 'M', 'F', or 'N'
    :param pay_bad_day_conv: one of 'M', 'F', or 'N'
    :param reset_bad_day_conv: one of 'M', 'F', or 'N'
    :param holiday_file:
    :param principal_initial_flag: if 1 inserts a payment of -1 at the start date
    :param principal_final_flag: if 1 inserts a payment of 1 at the maturity date
    :param first_float_fixed_flag: Flag to denote that the first floating rate has been fixed.
    :param first_float_fixed_rate: The fixing of the first floating rate. Only required if first float rate fixed = 1.
    :param value_date:
    :param convexity_adjust: unsupported, use 0
    :param vol_model: unsupported use 0
    :return:
    """
    alib_notional = c_double(float(notional))

    alib_spread = c_double(float(spread))

    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    alib_float_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(float_interval, 'IVL', byref(alib_float_interval))

    alib_float_rate_interval = Alib_Class.IVL()
    failure = alib_obj_coerce(float_rate_interval, 'IVL', byref(alib_float_rate_interval))

    alib_compound_method = c_long(int(compound_method))

    try:
        alib_end_date = python_date_to_alib_Tdate(end_date)
    except:
        alib_end_date = end_date

    alib_float_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(float_dcc, 'DCC', byref(alib_float_dcc))

    alib_stub_method = Alib_Class.STB()
    failure = alib_obj_coerce(stub_method, 'STB', byref(alib_stub_method))

    alib_accrual_bad_day_conv = ascii_encode(accrual_bad_day_conv)
    alib_pay_bad_day_conv = ascii_encode(pay_bad_day_conv)
    alib_reset_bad_day_conv = ascii_encode(reset_bad_day_conv)
    alib_holiday_file = ascii_encode(holiday_file)
    alib_principal_initial_flag = c_long(int(principal_initial_flag))
    alib_principal_final_flag = c_long(int(principal_final_flag))
    alib_first_float_fixed_flag = c_long(int(first_float_fixed_flag))
    # alib_first_float_fixed_rate = c_double(float(first_float_fixed_rate))

    l_fixed = first_float_fixed_rate
    if not isinstance(first_float_fixed_rate, list):
        l_fixed = [first_float_fixed_rate]

    nfixed_rates = len(l_fixed)
    alib_num_fixed_rates = c_long(nfixed_rates)
    alib_fix_rates = (c_double * nfixed_rates)(*[])
    for i in range(0, nfixed_rates):
        alib_fix_rates[i] = c_double(l_fixed[i])

    try:
        alib_value_date = python_date_to_alib_Tdate(value_date)
    except:
        alib_value_date = value_date

    alib_convexity_adjust = convexity_adjust if isinstance(convexity_adjust, c_long) else c_long(convexity_adjust)

    alib_vol_model = Alib_Class.VMIR()
    failure = alib_obj_coerce(vol_model, 'VMIR', byref(alib_vol_model))

    raw_res = c_double()
    failure = ALib.ALIB_SWAP_FLOAT_PV_GENERIC(
        discount_curve,
        alib_notional,
        alib_spread,
        projection_curve,
        alib_start_date,
        alib_float_interval,
        alib_float_rate_interval,
        alib_compound_method,
        alib_end_date,
       alib_float_dcc,
        alib_stub_method,
        alib_accrual_bad_day_conv,
        alib_pay_bad_day_conv,
        alib_reset_bad_day_conv,
        alib_holiday_file,
        alib_principal_initial_flag,
        alib_principal_final_flag,
        alib_first_float_fixed_flag,
        alib_num_fixed_rates,
        alib_fix_rates,
        alib_value_date,
        alib_convexity_adjust,
        alib_vol_model,
        byref(raw_res)
    )

    free_objects(alib_float_interval, alib_float_rate_interval, alib_float_dcc, alib_stub_method, alib_vol_model)
    if failure:
        base_err_msg = 'Could not compute swap_float_pv'
        raise_val_err_w_log(base_err_msg)
    swap_float_pv = raw_res.value
    return swap_float_pv


def swap_float_pv_with_conv(
       discount_curve, notional, spread, projection_curve, start_date,
        end_date, holiday_oracle, compound_method, first_float_fixed_flag, first_float_fixed_rate,
        value_date, conv_str, swap_conv_dict=None
):
    """
    :description: pv swap float leg using standard conventions
    :param discount_curve: alib curve object
    :param float notional:
    :param float spread:
    :param projection_curve: alib curve object
    :param datetime.date start_date:
    :param datetime.date end_date:
    :param holiday_oracle: instance of qau.Holidays
    :param compound_method: spraad compounding method
    :param first_float_fixed_flag: Flag to denote that the first floating rate has been fixed.
    :param first_float_fixed_rate: The fixing of the first floating rate. Only required if first float rate fixed = 1.
    :param value_date:
    :param conv_str: panormus convention string, e.g. EUR.6ML
    :return: float pv
    """
    conventions = swap_conventions(conv_str, swap_conv_dict)

    needed_conv_list = ['float_interval', 'float_rate_interval', 'float_dcc', 'stub_method', 'accrual_bad_day_conv',
                        'pay_bad_day_conv', 'reset_bad_day_conv', 'principal_initial_flag',
                        'principal_final_flag', 'convexity_adjust', 'vol_model']
    needed_conventions = {s: conventions[s] for s in needed_conv_list}

    holiday_file = holiday_oracle[conventions['holiday_calendar_name']]

    return ac_swap_float_pv(
        discount_curve=discount_curve, notional=notional, spread=spread,
        projection_curve=projection_curve, start_date=start_date,
        end_date=end_date, holiday_file=holiday_file,
        compound_method=compound_method,
        first_float_fixed_flag=first_float_fixed_flag,
        first_float_fixed_rate=first_float_fixed_rate,
        value_date=value_date, **needed_conventions
    )

def swap_float_sens_with_conv(
        discount_curve, notional, spread, projection_curve, start_date,
        end_date, holiday_oracle, compound_method, first_float_fixed_flag, first_float_fixed_rate,
        value_date, conv_str, sens_type, swap_conv_dict=None
):
    """
    :description: sens swap float leg using standard conventions
    :param discount_curve: alib curve object
    :param float notional:
    :param float spread:
    :param projection_curve: alib curve object
    :param datetime.date start_date:
    :param datetime.date end_date:
    :param holiday_oracle: instance of qau.Holidays
    :param compound_method: spraad compounding method
    :param first_float_fixed_flag: Flag to denote that the first floating rate has been fixed.
    :param first_float_fixed_rate: The fixing of the first floating rate. Only required if first float rate fixed = 1.
    :param value_date:
    :param conv_str: panormus convention string, e.g. EUR.6ML
    :param sens_type: required sens type
    :return: float pv
    """
    conventions = swap_conventions(conv_str, swap_conv_dict)

    needed_conv_list = ['float_interval', 'float_rate_interval', 'float_dcc', 'stub_method', 'accrual_bad_day_conv',
                        'pay_bad_day_conv', 'reset_bad_day_conv', 'principal_initial_flag',
                        'principal_final_flag', 'convexity_adjust', 'vol_model']
    needed_conventions = {s: conventions[s] for s in needed_conv_list}

    holiday_file = holiday_oracle[conventions['holiday_calendar_name']]

    return ac_swap_float_sens(
        discount_curve=discount_curve, notional=notional, spread=spread,
        projection_curve=projection_curve, start_date=start_date,
        end_date=end_date, holiday_file=holiday_file,
        compound_method=compound_method,
        first_float_fixed_flag=first_float_fixed_flag,
        first_float_fixed_rate=first_float_fixed_rate,
        value_date=value_date, sens_type=sens_type, **needed_conventions
    )


def alib_date_to_python(alib_date):
    """
    :description: convert an alib date to a python date
    :param alib_date:
    :return: python datetime
    """
    alib_int = alib_date.value if isinstance(alib_date, c_long) else alib_date
    return ALIB_TIME_STARTS + dt.timedelta(days=alib_int)


def ac_day_cnt_frac(start_date, end_date, dcc_str):
    """
    :description: calculate a day count fraction
    :param start_date:
    :param end_date:
    :param dcc_str: day count convention, e.g. 30/360
    :return: float
    """
    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_end_date = python_date_to_alib_Tdate(end_date)

    alib_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(dcc_str, 'DCC', byref(alib_dcc))
    if failure:
        base_err_msg = 'Could not convert dcc_str to object'
        raise_val_err_w_log(base_err_msg)

    dcc_dbl = c_double()
    failure = ALib.ALIB_DAY_CNT_FRACT_O(
        alib_start_date,
        alib_end_date,
        alib_dcc,
        byref(dcc_dbl))

    free_object(alib_dcc)
    if failure:
        base_err_msg = 'Could not compute dcc'
        raise_val_err_w_log(base_err_msg)
    return dcc_dbl.value


def ac_fx_cross_date(start_date, usd_ivl_adj, ccy1_ivl_adj, ccy2_ivl_adj):
    """
    :description:
    :param start_date:
    :param usd_ivl_adj:
    :param ccy1_ivl_adj:
    :param ccy2_ivl_adj:
    :return:
    """
    alib_start_date = python_date_to_alib_Tdate(start_date)

    raw_res = c_long()
    failure = ALib.ALIB_FX_CROSS_DATE(
        alib_start_date,
        usd_ivl_adj,
        ccy1_ivl_adj,
        ccy2_ivl_adj,
        byref(raw_res))

    if failure:
        base_err_msg = 'Could not compute fx date'
        raise_val_err_w_log(base_err_msg)
    return alib_date_to_python(raw_res.value)


def ac_is_business_day(check_date, holiday_file):
    """
    :description: determines if date is a business date in context of holiday string
    :param check_date:
    :param str holiday_file: holiday file as returned from qau.Holidays()['USD']
    :return: bool
    """
    holiday_file = ascii_encode(holiday_file)
    alib_check_date = python_date_to_alib_Tdate(check_date)
    is_bd = c_bool()
    failure = ALib.ALIB_IS_BUSINESS_DAY(
        alib_check_date,
        holiday_file,
        byref(is_bd))

    if failure:
        base_err_msg = 'Could not check business day'
        raise_val_err_w_log(base_err_msg)
    return is_bd.value


def ac_date_fwd_adj2(start_date, date_offset_str, holiday_file):
    """
    :description: Adjust date forward or backward according to date_offset_str and holiday_file
    :param datetime.date start_date:
    :param str date_offset_str: alib interval string such as '1D,B' for 1 business day forward.
    :param str holiday_file: holiday file as returned from qau.Holidays()['USD']
    :return: adjusted date
    """
    holiday_file = ascii_encode(holiday_file)
    # Handle 0 business day adjustments by looking for a convention string on the end.
    if date_offset_str.upper().startswith('0D,B') or date_offset_str.upper().startswith('-0D,B'):
        for conv in ['M', 'F', 'P']:
            if date_offset_str[4:].upper().find(conv) >= 0:
                return ac_bus_day(start_date=start_date, conv_str=conv, holiday_file=holiday_file)

       wn.warn(
            'Zero day adjustment requested without a bad day convention. Returning start_date.',
            stacklevel=2
        )
        return start_date

    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_doi = Alib_Class.DOI()
    failure = alib_obj_coerce(
        date_offset_str, 'DOI', byref(alib_doi)
    )
    if failure:
        base_err_msg = 'Could not convert date_offset_str to object'
        raise_val_err_w_log(base_err_msg)
    raw_res = c_long()
    failure = ALib.ALIB_DATE_FWD_ADJ2(
        alib_start_date,
        alib_doi,
        holiday_file,
        byref(raw_res)
    )

    free_object(alib_doi)
    if failure:
        base_err_msg = 'Could not compute forward date'
        raise_val_err_w_log(base_err_msg)
    alib_fwd_date = raw_res.value
    return alib_date_to_python(alib_fwd_date)


def ac_bus_day(start_date, conv_str, holiday_file):
    '''
    :description: Adjusts start_date to a valid business date according to given convention and holiday calendar
    :param datetime.date start_date:
    :param conv_str: Business day convention. [text] - (P)revious, (F)ollowing, \
      (M)odified following, (N)one will return the input date with no adjustment.
    :param str holiday_file: Name of holiday list. [text] - file name, 'None', or 'No_Weekends'.
    :return: adjusted date.
    '''
    alib_start_date = python_date_to_alib_Tdate(start_date)
    conv_str = ascii_encode(conv_str)
    holiday_file = ascii_encode(holiday_file)

    raw_res = c_long()
    failure = ALib.ALIB_BUSINESS_DAY(
        alib_start_date,
        conv_str,
        holiday_file,
        byref(raw_res)
    )
    if failure:
        base_err_msg = 'Could not compute business day.'
        raise_val_err_w_log(base_err_msg)
    alib_bus_date = raw_res.value
    return alib_date_to_python(alib_bus_date)


def ac_date_fwd_any_o(start_date, num_periods, interval_str):
    """
    :description: adjust start_date by a num_periods * interval for valid interval_str
    :param datetime.date start_date:
    :param int num_periods:
    :param str interval_str: an interval that alib can parse. See alib documentation.
    :return: adjusted date
    """
    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_ivl = Alib_Class.IVL()
    failure = alib_obj_coerce(
        interval_str, 'IVL', byref(alib_ivl)
    )
    if failure:
        base_err_msg = 'Could not convert interval_str to object'
        raise_val_err_w_log(base_err_msg)
    raw_res = c_long()
    failure = ALib.ALIB_DATE_FWD_ANY_O(
        alib_start_date,
        c_long(int(num_periods)),
        alib_ivl,
        byref(raw_res)
    )
    free_object(alib_ivl)
    if failure:
        base_err_msg = 'Could not compute forward date'
        raise_val_err_w_log(base_err_msg)
    alib_fwd_date = raw_res.value
    return alib_date_to_python(alib_fwd_date)


def ac_bus_days_diff(start_date, end_date, holiday_file):
    """
    :description: count of business days between start date and end date
    :param datetime.date start_date:
    :param datetime.date end_date:
    :param str holiday_file: Name of holiday list. [text] - file name, 'None', or 'No_Weekends'.
    :return: int
    """
    holiday_file = ascii_encode(holiday_file)
    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_end_date = python_date_to_alib_Tdate(end_date)
    raw_res = c_long()
    failure = ALib.ALIB_BUS_DAYS_DIFF(
        alib_start_date,
        alib_end_date,
        holiday_file,
        byref(raw_res)
    )
    if failure:
        base_err_msg = 'Could not compute bus day diff'
        raise_val_err_w_log(base_err_msg)
    return int(raw_res.value)


def ac_bus_days_offset(start_date, offset, holiday_file):
    '''
    :description: simplified function to adjust a number of business days
    :param start_date: start date is either a python date (datetime.date) or a alib date (int)
    :param int offset: business day offset
    :param holiday_file: static holiday files
    :return: python date or alib date depending on start date input
    '''
    holiday_file = ascii_encode(holiday_file)

    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date

    res = c_long()
    failure = ALib.ALIB_BUS_DAYS_OFFSET(
        alib_start_date,
        c_long(offset),
        holiday_file,
        byref(res)
    )
    if failure:
        base_err_msg = 'Could not compute bus day offset'
        raise_val_err_w_log(base_err_msg)

    alib_candidate_date = res.value
    if isinstance(start_date, int):
        return alib_candidate_date
    elif isinstance(start_date, c_long):
        return c_long(alib_candidate_date)
    else:
        return ALIB_TIME_STARTS + dt.timedelta(days=alib_candidate_date)


def bus_ycf(start_date, end_date, conv_str, holidays, bd_per_year=251):
    """
    :description: year count fraction between start and end date
    :param datetime.date start_date:
    :param datetime.date end_date:
    :param conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :param int bd_per_year: business days per year for denominator of ycf
    :return: float
    """
    conv_str = str(conv_str)
    holiday_file = holidays[YCF_CAL_DICT[conv_str]]
    bdc = ac_bus_days_diff(
        start_date=start_date, end_date=end_date, holiday_file=holiday_file)
    return float(bdc) / float(bd_per_year)


def val_date_from_trade_date(trade_date, conv_str, holidays):
    """
    :description: determine swap value date from trade date
    :param datetime.date trade_date:
    :param conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :return: datetime.date
    """
    candidate_date = trade_date
    date_bumps = VAL_DATE_CONV_DICT[conv_str]

    for bump_str, bump_cal_name in date_bumps:
        candidate_date = ac_date_fwd_adj2(candidate_date, bump_str, holidays[bump_cal_name])

    return candidate_date


def start_date_from_trade_date(trade_date, fwd_str, conv_str, holidays):
    """
    :description: determine start date from trade date
    :param datetime.date trade_date:
    :param str fwd_str: forward interval interpretable by alib.
    :param str conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :return: datetime.date
    """
    ivl = Alib_Class.IVL()
    alib_obj_coerce(fwd_str, 'IVL', byref(ivl))

    if 'i' not in fwd_str:
        try:
            alib_trade_date = python_date_to_alib_Tdate(trade_date)
        except:
            alib_trade_date = trade_date

        res = c_long()
        ALib.ALIB_DATE_FWD_ANY_O(
            alib_trade_date,
            c_long(1),
            ivl,
            byref(res)
        )
        free_object(ivl)
        fwd_date = res.value
        if not isinstance(trade_date, int):
            fwd_date = ALIB_TIME_STARTS + dt.timedelta(days=fwd_date)
        return val_date_from_trade_date(trade_date=fwd_date, conv_str=conv_str, holidays=holidays)
    else:
        spot_val_date = val_date_from_trade_date(
            trade_date=trade_date, conv_str=conv_str, holidays=holidays)
        try:
            alib_spot_val_date = python_date_to_alib_Tdate(spot_val_date)
        except:
            alib_spot_val_date = spot_val_date

        res = c_long()
        ALib.ALIB_DATE_FWD_ANY_O(
            alib_spot_val_date,
            c_long(1),
            ivl,
            byref(res)
        )
        free_object(ivl)
        start_date = res.value
        return ALIB_TIME_STARTS + dt.timedelta(days=start_date)


def end_date_from_start_date(start_date, ten_str):
    """
    :description: determine end date from start date
    :param datetime.date start_date:
    :param str ten_str: swap tenor interpretable as alib interval.
    :return: datetime.date
    """
    try:
        alib_start_date = python_date_to_alib_Tdate(start_date)
    except:
        alib_start_date = start_date
    ivl = Alib_Class.IVL()
    alib_obj_coerce(ten_str, 'IVL', byref(ivl))
    res = c_long()
    ALib.ALIB_DATE_FWD_ANY_O(
        alib_start_date,
        c_long(1),
        ivl,
        byref(res)
    )
    free_object(ivl)
    end_date = res.value
    if not isinstance(start_date, int):
        end_date = ALIB_TIME_STARTS + dt.timedelta(days=end_date)
    return end_date


def end_date_from_trade_date(trade_date, fwd_str, ten_str, conv_str, holidays):
    """
    :description: determine end date from trade date
    :param datetime.date trade_date:
    :param str fwd_str: forward interval interpretable by alib.
    :param str ten_str_str: swap tenor interpretable as alib interval.
    :param str conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :return: datetime.date
    """
    start_date = start_date_from_trade_date(
        trade_date=trade_date,
        fwd_str=fwd_str,
        conv_str=conv_str,
        holidays=holidays
    )
    end_date = end_date_from_start_date(
        start_date=start_date,
        ten_str=ten_str
    )
    return end_date


def dates_from_trade_date(trade_date, fwd_str, ten_str, conv_str, holidays):
    """
    :description: determine (start_date, end_date) from trade date
    :param datetime.date trade_date:
    :param str fwd_str: forward interval interpretable by alib.
    :param str ten_str_str: swap tenor interpretable as alib interval.
    :param str conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :return: tuple of datetime.date
    """
    start_date = start_date_from_trade_date(
        trade_date=trade_date,
        fwd_str=fwd_str,
        conv_str=conv_str,
        holidays=holidays
    )
    end_date = end_date_from_start_date(
        start_date=start_date,
        ten_str=ten_str
    )
    return (start_date, end_date)


def expiry_date_from_trade_date(trade_date, expiry_str, conv_str, holidays):
    """
    :description: determine option expiry date from trade date
    :param datetime.date trade_date:
    :param str expiry_str: expiry interval interpretable by alib.
    :param str conv_str: panormus convention string, e.g. USD.3ML
    :param holidays: qau.Holidays instance
    :return: datetime.date
    """
    adjust_str, hol_str = EXP_DATE_CONV_DICT[conv_str]

    bump_str = ','.join([expiry_str, adjust_str])
    try:
        alib_trade_date = python_date_to_alib_Tdate(trade_date)
    except:
        alib_trade_date = trade_date
    doi = Alib_Class.DOI()
    alib_obj_coerce(bump_str, 'DOI', byref(doi))
    res = c_long()
    ALib.ALIB_DATE_FWD_ADJ2(
        alib_trade_date,
        doi,
        ascii_encode(holidays[hol_str]),
        byref(res)
    )
    free_object(doi)
    alib_candidate_date = res.value
    if isinstance(trade_date, int):
        return alib_candidate_date
    else:
        return ALIB_TIME_STARTS + dt.timedelta(days=alib_candidate_date)


def swap_tenor_from_dates(start_date, end_date):
    """
    :description: generate the tenor str as nY or nM based on dates.
    :param dt.date start_date:
    :param dt.date end_date:
    :rtype: str
    """
    year_frac = (end_date - start_date).days / 365.0
    if abs(year_frac % 1) > (1.0 / 13.0):
        return str(int(round(year_frac * 12.0, 0))) + 'M'
    else:
        return str(int(round(year_frac))) + 'Y'


def cash_settled_annuity(fwd_rate, fixed_periods_per_year, tenor_int):
    """
    :description: cash settle annuity for fwd rate
    :param float fwd_rate:
    :param int fixed_periods_per_year:
    :param int tenor_int: number of years
    :return: float
    """
    EPS = 1.0E-8
    n = fixed_periods_per_year * tenor_int
    if abs(fwd_rate) < EPS:
        return tenor_int
    else:
        return 1.0 / fwd_rate * (1. - 1. / (1.0 + fwd_rate / fixed_periods_per_year) ** n)


def cash_annuity_with_conv(
        discount_curve, estimating_curve,
        start_date, end_date, holiday_oracle, conv_str, swap_conv_dict=None):
    """
    :description: cash annuity from conventions
    :param discount_curve: alib curve object
    :param estimating_curve: alib curve object
    :param datetime.date start_date:
    :param datetime.date end_date:
    :param holiday_oracle: instance of qau.Holidays
    :param str conv_str: panormus convention string, e.g. EUR.6ML
    :return: float
    """
    conventions = swap_conventions(conv_str, swap_conv_dict)

    fwd_rate = swap_rate_with_conv(
        discount_curve, start_date, end_date,
        1, 0.0, estimating_curve,
        0, 0., holiday_oracle, conv_str, swap_conv_dict
    )
    fixed_ivl = conventions['fixed_interval']
    fixed_periods_per_year = 1.0 / ac_ivl_years(fixed_ivl)
    tenor_int = round((end_date - start_date).days / 365., 2)
    df = ac_interp_pv(discount_curve, start_date)

    return df * cash_settled_annuity(fwd_rate, fixed_periods_per_year, tenor_int)


def ac_fx_forward_rate(spot_date, spot_rate, forward_date, domestic_curve, foreign_curve, inverted_quote):
    """
    Wraps ALIB_FX_FORWARD_RATE_O
    :param spot_date: python date
    :param spot_rate: double
    :param forward_date: python date
    :param domestic_curve: alib zero curve for the domestic currency (typically USD.OIS)
    :param foreign_curve: alib zero curve for the foreign currency (e.g. EUR.XCCY.USD)
    :param int inverted_quote: 1 if the currency is quoted as CCYUSD, 0 otherwise
    :return: forward level
    """

    alib_spot_date = python_date_to_alib_Tdate(spot_date)
    alib_spot_rate = c_double(spot_rate)
    alib_forward_date = python_date_to_alib_Tdate(forward_date)
    alib_inverted_quote = c_int(inverted_quote)

    alib_forward = c_double()

    failure = ALib.ALIB_FX_FORWARD_RATE_O(
        alib_spot_date, alib_spot_rate, alib_forward_date, domestic_curve, foreign_curve, alib_inverted_quote,
        byref(alib_forward))

    if failure:
        base_err_msg = 'Failed to compute fx forward date'
        raise_val_err_w_log(base_err_msg)
    return alib_forward.value


# TODO: Bond methods haven't been tested for unicode strings (python 3).
def ac_bond_make(bond_type, cpn_freq, cpn_rate, mat_date, dated_date, first_cpn_date, redem_val):
    """
    Creates an alib bond object
    See the ALIB manual for a list of bond types
    """
    bond_obj = Alib_Class.BOND()
    alib_mat_date = python_date_to_alib_Tdate(mat_date)
    alib_dated_date = python_date_to_alib_Tdate(dated_date)
    alib_cpn_freq = c_long(int(cpn_freq))
    if first_cpn_date is None:
        alib_first_cpn_date = c_long(0)
    else:
        alib_first_cpn_date = python_date_to_alib_Tdate(first_cpn_date)
    failure = ALib.ALIB_BOND_MAKE(
        ascii_encode(bond_type),  # string                 BondType,              /* (I) Scalar */
        alib_cpn_freq,  # int                    CpnFreq,               /* (I) Scalar */
        c_double(cpn_rate),  # double                 CpnRate,               /* (I) Scalar */
        alib_mat_date,  # TDate                  MatDate,               /* (I) Scalar */
        alib_dated_date,  # TDate                  DatedDate,             /* (I) Scalar */
        alib_first_cpn_date,  # TDate                  FirstCpnDate,          /* (I) Scalar */
        c_double(redem_val),  # double                 RedemptionVal,         /* (I) Scalar */
        byref(bond_obj))  # out CLASS_BOND         Bond                   /* (O) Scalar */

    if failure:
        free_object(bond_obj)
        base_err_msg = 'Failed to create bond object'
        raise_val_err_w_log(base_err_msg)
    return bond_obj


def ac_bondtrade_make(settlement_date, is_exdiv=0, trade_date=None, value_date=None):
    """
    Creates an alib bondtrade object. This is usually just the settlement date.
    """
    bondtrade_obj = Alib_Class.BONDTRADE()
    alib_settle_date = python_date_to_alib_Tdate(settlement_date)
    if trade_date is None:
        alib_trade_date = alib_settle_date
    else:
        alib_trade_date = python_date_to_alib_Tdate(trade_date)

    if value_date is None:
        alib_value_date = alib_settle_date
    else:
        alib_value_date = python_date_to_alib_Tdate(value_date)

    failure = ALib.ALIB_BONDTRADE_MAKE(
        alib_settle_date,
        c_int(is_exdiv),
        alib_trade_date,
        alib_value_date,
        byref(bondtrade_obj)
    )
    if failure:
        free_object(bondtrade_obj)
        base_err_msg = 'Failed to create bondtrade object'
        raise_val_err_w_log(base_err_msg)
    return bondtrade_obj


def ac_bonds_ai(bond_obj, settle_date, output_flag, holiday_file):
    '''
    This function calculates accrued interest for a bond object. It is the object version of BONDS_AI (page 238). It can return
    one of two quantities: the amount of accrued interest, and the number of days of accrual.

    :param bond_obj:
    :param settle_date:
    :param output_flag: 1 = accrued interest, 2 = number of accrued days
    :param holiday_file:
    :return:
    '''
    holiday_file = ascii_encode(holiday_file)
    alib_bond_trade = ac_bondtrade_make(settle_date)
   acc_int = c_double()
    failure = ALib.ALIB_BONDS_AI_O(
        bond_obj,
        alib_bond_trade,
        c_int(output_flag),
        holiday_file,
        byref(acc_int))

    if failure:
        base_err_msg = 'Could not compute accrued'
        raise_val_err_w_log(base_err_msg)

    return acc_int.value


def ac_bonds_ytm(bond, settle_date, pay_bad_day_conv, holiday_file, bond_price, is_dirty_price, calc_type):
    """
    :descrioption: calculate bond's yield to maturity
    :param bond:
    :param settle_date:
    :param pay_bad_day_conv:
    :param holiday_file:
    :param bond_price:
    :param is_dirty_price:
    :param calc_type:
    :return:
    """
    pay_bad_day_conv = ascii_encode(pay_bad_day_conv)
    holiday_file = ascii_encode(holiday_file)
    alib_bondtrade = ac_bondtrade_make(settle_date)
    ytm = c_double()

    failure = ALib.ALIB_BONDS_YTM_O(
        bond,  # CLASS_BOND             Bond,              /* (I) Scalar *
        alib_bondtrade,  # CLASS_BONDTRADE        Trade,             /* (I) Scalar */
        pay_bad_day_conv,  # string                 PayBadDayConv,     /* (I) Scalar */
        holiday_file,  # string                 HolidayFile,       /* (I) Scalar */
        c_double(bond_price),  # double                 Price,             /* (I) Scalar */
        c_int(is_dirty_price),  # int                    IsDirtyPrice,      /* (I) Scalar */
        ascii_encode(calc_type),  # string                 CalcType,          /* (I) Scalar */
        byref(ytm))  # out double             YTM                /* (O) Scalar */

    if failure:
        base_err_msg = 'Failed to calcuate bond ytm'
        raise_val_err_w_log(base_err_msg)
    if failure:
        base_err_msg = 'Failed to calcuate bond ytm'
        raise_val_err_w_log(base_err_msg)
    return ytm.value


def ac_bonds_repo(
        bond, spot_settle_date, fwd_settle_date,
        repo_dcc, repo_method,
        coup_invest,
        pay_bad_day_conv,
        holiday_file,
        tax_method,
        calc_type,
        repo_rate=None,
        spot_price=None,
        fwd_price=None
):
    """
    Given two of spot price, forward price and repo rate compute the third
    Calc type is one of
    'F' - Calc forward price
    'S' - Calc spot price
    'R' - Calc implied repo
    """
    holiday_file = ascii_encode(holiday_file)
    alib_spottrade = ac_bondtrade_make(spot_settle_date)
    alib_fwdtrade = ac_bondtrade_make(fwd_settle_date)
    alib_dcc = Alib_Class.DCC()
    failure = alib_obj_coerce(repo_dcc, "DCC", byref(alib_dcc))

    alib_rr = c_double(0.0) if repo_rate is None else c_double(repo_rate)
    alib_sp = c_double(0.0) if spot_price is None else c_double(spot_price)
    alib_fp = c_double(0.0) if fwd_price is None else c_double(fwd_price)

    output = c_double()
    failure = ALib.ALIB_BONDS_REPO_O(
        bond,
        alib_spottrade,
        alib_fwdtrade,
        alib_dcc,
        repo_method,
        coup_invest,
        pay_bad_day_conv,
        holiday_file,
        tax_method,
        alib_rr,
        alib_sp,
        alib_fp,
        calc_type,
        byref(output)
    )
    free_object(alib_dcc)
    if failure:
        base_err_msg = 'Alib bonds repo failed'
        raise_val_err_w_log(base_err_msg)

    return output.value


def ac_bonds_sens(bond, settle_date, pay_bad_day_conv, holiday_file, ytm, calc_type, sens_type):
    """
    Calculates bond sensitivites
    sens_type is one of
    1 - clean price
    2 - convexity
    3 - Macaulay durationz
    4 - modified duration
    5 - present value
    6 - PVBP
    7 - theta
    """
    holiday_file = ascii_encode(holiday_file)
    alib_bondtrade = ac_bondtrade_make(settle_date)
    sens = c_double()
    failure = ALib.ALIB_BONDS_SENS_O(
        bond,
        alib_bondtrade,
        pay_bad_day_conv,
        holiday_file,
        c_double(ytm),
        ascii_encode(calc_type),
        c_int(sens_type),
        byref(sens)
    )

    if failure:
        base_err_msg = 'Alib bonds sens failed'
        raise_val_err_w_log(base_err_msg)
    return sens.value


def ac_bonds_zc_spread(
        bond, settle_date,
        holiday_file, pay_bad_day_conv, zero_curve,
        price, is_dirty_price, spread_type
):
    """
    Imply spread to zero curve needed to price a bond back to the market
    spread type is one of
    0 - no spread
    1 - average spread
    2 - forward spread
    """
    alib_bondtrade = ac_bondtrade_make(settle_date)

    spread = c_double()

    failure = ALib.ALIB_BONDS_ZC_SPREAD_O(
        bond,
        alib_bondtrade,
        ascii_encode(holiday_file),
        ascii_encode(pay_bad_day_conv),
        zero_curve,
        c_double(price),
        c_int(is_dirty_price),
        c_int(spread_type),
        byref(spread)
    )
    if failure:
        base_err_msg = 'Alib bonds ZC spread failed'
        raise_val_err_w_log(base_err_msg)
    return spread.value


def ac_bonds_sens_zc(
        bond, settle_date,
        holiday_file,
        pay_bad_day_conv,
        zero_curve,
        spread_type,
        spread,
        sens_type
):
    """
    Price a bond using a zero curve
    spread type is one of
    0 - no spread
    1 - average spread
    2 - forward spread
    sens_type is one of
    1 - clean price
    2 - convexity
    3 - Macaulay durations
    4 - modified duration
    5 - present value
    6 - PVBP
    7 - theta
    """
    alib_bondtrade = ac_bondtrade_make(settle_date)
    sens = c_double()

    failure = ALib.ALIB_BONDS_SENS_ZC_O(
        bond,
        alib_bondtrade,
        ascii_encode(holiday_file),
        ascii_encode(pay_bad_day_conv),
        zero_curve,
        c_int(spread_type),
        c_double(spread),
        c_int(sens_type),
        byref(sens)
    )
    if failure:
        base_err_msg = 'Alib bonds sens ZC failed'
        raise_val_err_w_log(base_err_msg)

    return sens.value


def ac_opt_vol_norm(fwd_undl, strike, yte, put_call, fwd_prem, vol_guess):
    """
    :description:
    :param fwd_undl:
    :param strike:
    :param yte:
    :param put_call:
    :param fwd_prem:
    :param vol_guess:
    :return:
    """

    def target_func(proposed_vol):
        price_guess = qo.norm_opt_sens(strike, yte, put_call, proposed_vol, fwd_undl, 'p')
        return price_guess - fwd_prem

    return newton(target_func, vol_guess)


def ac_opt_sens(fwd_undl, strike, yte, ytp, put_call, vol, disc_r, sens_type):
    """
    :description:
    :param fwd_undl:
    :param strike:
    :param yte:
    :param ytp:
    :param put_call:
    :param vol:
    :param disc_r:
    :param sens_type:
    :return:
    """
    fwd_undl = c_double(fwd_undl)
    strike = c_double(strike)
    yte = c_double(yte)
    ytp = c_double(ytp)
    vol = c_double(vol)
    disc_r = c_double(disc_r)
    put_call = ascii_encode(put_call)
    sens_type = ascii_encode(sens_type)
    sens_result = c_double()

    failure = ALib.ALIB_OPT_SENS(
        put_call,
        fwd_undl,
        strike,
        yte,
        ytp,
        vol,
        disc_r,
        sens_type,
        byref(sens_result))

    if failure:
        base_err_msg = 'Could not compute option norm _vol'
        raise_val_err_w_log(base_err_msg)

    return sens_result.value


def ac_opt_sens2(spot_undl, strike, yte, ytp, put_call, vol, disc_r, div_r, grw_r, sens_type):
    '''
    :description:This function calculates the price and sensitivities of a generic European option, \
    using Black's option model.
    :param float spot_undl: Spot underlying variable of the option
    :param float strike: strike
    :param float yte: Time to option expiration in years (>=0)
    :param float ytp: Time to option payment in years (>=0)
    :param str put_call: (C)all or (P)ut
    :param float vol: volatility
    :param float disc_r: Annually compounded (Act/365) risk free interest rate (>-1).
    :param float div_r: Annually compounded rate underlier is expected to lose (>-1).
    :param float grw_r:Annually compounded. Rate underlier is expected to gain (>-1).
    :param str sens_type: Determines whether option premium, or which sensitivity is calculated. \
    Use the following: (P)remium, (D)elta, (G)amma, (V)ega, (T)heta, (R)ho (discount), \
    (Y) rho (dividend), (E) rho (growth earnings), (T)heta = derivative of price w.r.t. years to expiry., \
    (C)arry = derivative of price w.r.t. years to payment., (F)orward theta = much like theta,
    forward of underlier is held constant instead of spot.
    :return: Option premium or sensitivity
    '''
    spot_undl = c_double(spot_undl)
    strike = c_double(strike)
    yte = c_double(yte)
    ytp = c_double(ytp)
    vol = c_double(vol)
    disc_r = c_double(disc_r)
    div_r = c_double(div_r)
    grw_r = c_double(grw_r)
    put_call = ascii_encode(put_call)
    sens_type = ascii_encode(sens_type)
    sens_result = c_double()

    failure = ALib.ALIB_OPT_SENS2(
        put_call,
        spot_undl,
        strike,
        yte,
        ytp,
        vol,
        disc_r,
        div_r,
        grw_r,
        sens_type,
        byref(sens_result))

    if failure:
        base_err_msg = 'Could not compute option sensitivities'
        raise_val_err_w_log(base_err_msg)

    return sens_result.value


def ac_opt_sens2_american(spot_undl, strike, yte, ytp, put_call, vol, disc_r, div_r, grw_r, sens_type):
    '''
    This function calculates the price and sensitivities of a vanilla American option, \
    using a trinomial tree model.

    :param float spot_undl: Spot underlying variable of the option
    :param float strike: strike
    :param float yte: Time to option expiration in years (>=0)
    :param float ytp: Time to option payment in years (>=0)
    :param str put_call: (C)all or (P)ut
    :param float vol: volatility
    :param float disc_r: Annually compounded (Act/365) risk free interest rate (>-1).
    :param float div_r: Annually compounded rate underlier is expected to lose (>-1).
    :param float grw_r: Annually compounded. Rate underlier is expected to gain (>-1).
    :param str sens_type: Determines whether option premium, or other sensitivity is calculated. \
    One of the following: (P)remium, (D)elta, (G)amma.

    :return: Option premium or sensitivity
    '''
    spot_undl = c_double(spot_undl)
    strike = c_double(strike)
    yte = c_double(yte)
    ytp = c_double(ytp)
    vol = c_double(vol)
    disc_r = c_double(disc_r)
    div_r = c_double(div_r)
    grw_r = c_double(grw_r)
    put_call = ascii_encode(put_call)
    sens_type = ascii_encode(sens_type)
    sens_result = c_double()

    failure = ALib.ALIB_OPT_SENS2_AMERICAN(
        put_call,
        spot_undl,
        strike,
        yte,
        ytp,
        vol,
        disc_r,
        div_r,
       grw_r,
        sens_type,
        byref(sens_result))

    if failure:
        base_err_msg = 'Could not compute option sensitivities'
        raise_val_err_w_log(base_err_msg)

    return sens_result.value


def bus_day_of_month(cal_date, hol_file=None):
    '''
    :description: Calculate the number of business days from the 1st to cal_date.day
    :param cal_date: any date object with .year, .month, and .day fields
    :param hol_file: str path to alib holiday file (use holiday oracle)
    :return: int
    '''
    hol_file = ascii_encode(hol_file)
    return ac_bus_days_diff(
        start_date=dt.date(cal_date.year, cal_date.month, 1) - dt.timedelta(days=1),
        end_date=cal_date,
        holiday_file=hol_file
    )


def bus_day_frac_of_month(cal_date, hol_file=None):
    '''
    :description: Calculate the fraction of business days that have passed in cal_date.month
    :param cal_date: any date object with .year, .month, and .day fields
    :param hol_file: str path to alib holiday file (use holiday oracle)
    :return: float
    '''
    return bus_day_of_month(cal_date=cal_date, hol_file=hol_file) \
           / float(total_bus_days_in_month(cal_date=cal_date, hol_file=hol_file))


def total_bus_days_in_month(cal_date, hol_file=None):
    '''
    :description: calculate the total number of business days in the month of cal_date
    :param cal_date: any date object with .year, .month, and .day fields
    :param hol_file: str path to alib holiday file (use holiday oracle)
    :return: int
    '''
    yr = cal_date.year
    mo = cal_date.month
    last_day = cal_mr(yr, mo)[1]
    return bus_day_of_month(dt.date(yr, mo, last_day), hol_file)


def swap_roll_date(value_date, maturity_date, pay_freq, holiday_file, bad_day_conv):
    """
    :description:
    :param value_date:
    :param maturity_date:
    :param pay_freq:
    :param holiday_file:
    :param bad_day_conv:
    :return:
    """
    mult_mapping_dict = {
        '1m': 12,
        '3m': 4,
        '6m': 2,
        '12m': 1,
        '1y': 1
    }
    mult_by = mult_mapping_dict[pay_freq.lower()]
    num_periods = round((maturity_date - value_date).days * mult_by / 365.0, 0)
    test_roll_date = ac_date_fwd_any_o(maturity_date, -num_periods, ascii_encode(pay_freq))
    if test_roll_date > value_date:
        test_roll_date = ac_date_fwd_any_o(maturity_date, -(num_periods + 1), ascii_encode(pay_freq))
    return ac_bus_day(test_roll_date, bad_day_conv, holiday_file)


def swap_fixing_date(roll_date, fixing_offset, holiday_file):
    """
    :description:
    :param roll_date:
    :param fixing_offset:
    :param holiday_file:
    :return:
    """
    fixing_term = ''.join((fixing_offset, 'D,B'))
    return ac_date_fwd_adj2(roll_date, fixing_term, holiday_file)


def swap_dates_from_conv(start_date, maturity_date, holiday_oracle, swap_conv, swap_conv_dict=None):
    """
    :description:
    :param start_date:
    :param maturity_date:
    :param holiday_oracle:
    :param swap_conv:
    :param dict swap_conv_dict:
    :return:
    """
    conventions = swap_conventions(swap_conv, swap_conv_dict)

    fixed_ivl = conventions['fixed_interval']
    fixed_period_type = fixed_ivl[-1]
    fixed_num_periods = int(fixed_ivl.replace(fixed_period_type, ''))
    fix_dcc = conventions['fixed_dcc']

   float_ivl = conventions['float_interval']
    float_period_type = float_ivl[-1]
    float_num_periods = int(float_ivl.replace(float_period_type, ''))
    float_reset_offset = abs(conventions['fixing_offset'])
    flt_dcc = conventions['float_dcc']
    float_rate_ivl = conventions['float_rate_interval']

    stub_method = conventions['stub_method']
    stub_loc = stub_method[0].upper()
    stub_type = 0 if stub_method[-1].lower() == 's' else 1
    acc_bdc = conventions['accrual_bad_day_conv']
    pay_bdc = conventions['pay_bad_day_conv']
    res_bdc = conventions['reset_bad_day_conv']

    holiday_file = holiday_oracle[conventions['holiday_calendar_name']]

    fix_coupon_dates_dict = ac_coupon_dates_swap(
        start_date, maturity_date,
        fixed_num_periods, fixed_period_type,
        1,
        0,
        0,
        0,
        stub_loc, stub_type,
        0,
        0,
        0,
        acc_bdc, pay_bdc, res_bdc,
        holiday_file
    )
    fix_coupon_dates_dict['dccs'] = [
        ac_day_cnt_frac(sd, ed, fix_dcc)
        for sd, ed in
        zip(fix_coupon_dates_dict['acc_start_dates'], fix_coupon_dates_dict['acc_end_dates'])
    ]

    flt_coupon_dates_dict = ac_coupon_dates_swap(
        start_date, maturity_date,
        float_num_periods, float_period_type,
        1,
        0,
        0,
        float_reset_offset,
        stub_loc, stub_type,
        0,
        0,
        0,
        acc_bdc, pay_bdc, res_bdc,
        holiday_file
    )

    flt_coupon_dates_dict['dccs'] = [
        ac_day_cnt_frac(sd, ed, flt_dcc)
        for sd, ed in zip(flt_coupon_dates_dict['acc_start_dates'], flt_coupon_dates_dict['acc_end_dates'])
    ]
    flt_ivl_adj = float_rate_ivl + ',' + acc_bdc
    flt_rate_end_dates = [
        ac_date_fwd_adj2(asd, flt_ivl_adj, holiday_file)
        for asd in flt_coupon_dates_dict['acc_start_dates']
    ]
    flt_coupon_dates_dict['rate_end_dates'] = flt_rate_end_dates

    return {
        'fix_leg_dates': fix_coupon_dates_dict,
        'flt_leg_dates': flt_coupon_dates_dict
    }


def ac_coupon_dates_swap(
        start_date, maturity_date, num_periods, period_type,
        adjust_last_accrual, arrears_setting_indic, pay_offset_days,
        reset_offset_days, stub_location, stub_type,
        first_roll_date, last_roll_date, full_first_coupon_date,
        acc_day_bdc, pay_day_bdc, res_day_bdc, holiday_file
):
    """
    :description:
    :param start_date: valid date
    :param maturity_date:  valid date > start_date
    :param int num_periods: integer
    :param period_type: alib period type (e.g. 'Y' for annual, 'I' for IMM
    :param int adjust_last_accrual: 0 or 1 (1 is standard)
    :param int arrears_setting_indic: 0 or 1 (0 is standard)
    :param int pay_offset_days: integer
    :param int reset_offset_days: integer
    :param str stub_location: 'B' or 'F' ('F'ront is standard)
    :param int stub_type: 0 or 1 (0 short stub, is standard)
    :param first_roll_date: 0 or valid date, see alib docs if further details needed
    :param last_roll_date: 0 or valid date, see alib docs if further details needed
    :param full_first_coupon_date: 0 or valid date, see alib docs if further details needed
    :param acc_day_bdc: alib bad day convention, 'M', 'F', 'P' or 'N'
    :param pay_day_bdc: alib bad day convention, 'M', 'F', 'P' or 'N'
    :param res_day_bdc: alib bad day convention, 'M', 'F', 'P' or 'N'
    :param holiday_file: holiday_file, from a holiday oracle
    :return:
    """
    alib_start_date = python_date_to_alib_Tdate(start_date)
    alib_maturity_date = python_date_to_alib_Tdate(maturity_date)

    alib_first_roll_date = c_long(0)
    if first_roll_date != 0:
        alib_first_roll_date = python_date_to_alib_Tdate(first_roll_date)

    alib_last_roll_date = c_long(0)
    if last_roll_date != 0:
        alib_last_roll_date = python_date_to_alib_Tdate(last_roll_date)

    alib_full_first_coupon_date = c_long(0)
    if full_first_coupon_date != 0:
        alib_full_first_coupon_date = python_date_to_alib_Tdate(full_first_coupon_date)

    maxSize = 1000
    accrual_start_dates = (c_int * maxSize)(*[])
    accrual_end_dates = (c_int * maxSize)(*[])
    reset_dates = (c_int * maxSize)(*[])
    payment_dates = (c_int * maxSize)(*[])
    stub_loc = c_int()

    failure = ALib.ALIB_COUPON_DATES_SWAP(
        alib_start_date,
        alib_maturity_date,
        c_long(num_periods),
        ascii_encode(period_type),
        c_int(adjust_last_accrual),
        c_int(arrears_setting_indic),
        c_long(pay_offset_days),
        c_long(reset_offset_days),
        ascii_encode(stub_location),
        c_int(stub_type),
        alib_first_roll_date,
        alib_last_roll_date,
        alib_full_first_coupon_date,
        ascii_encode(acc_day_bdc),
        ascii_encode(pay_day_bdc),
        ascii_encode(res_day_bdc),
        ascii_encode(holiday_file),
        byref(accrual_start_dates),
        byref(accrual_end_dates),
        byref(payment_dates),
        byref(reset_dates),
        byref(stub_loc)
    )
    if failure:
        base_err_msg = 'Could not compute swap schedule'
        raise_val_err_w_log(base_err_msg)

    return {
        'acc_start_dates': [alib_date_to_python(asd) for asd in accrual_start_dates if asd != 0],
        'acc_end_dates': [alib_date_to_python(aed) for aed in accrual_end_dates if aed != 0],
        'reset_dates': [alib_date_to_python(rd) for rd in reset_dates if rd != 0],
        'payment_dates': [alib_date_to_python(pd) for pd in payment_dates if pd != 0],
        'stub': stub_loc.value
    }


def ac_object_save(alib_obj, file_name, encoding_format='xml'):
    """
    :description: Writes an object to an ASCII file in a very specific format which resembles the format for \
    initializing a C data structure. The function calls the object’s class encode method to convert the object \
    into an ASCII representation
    :param alib_obj:
    :param string file_name:
    :param string encoding_format:
    :return: int error status. 0 if success.
    """
    file_name = ascii_encode(file_name)
    encoding_format = ascii_encode(encoding_format)
    failure = ALib.ALIB_OBJECT_SAVE(alib_obj, file_name, encoding_format)
    if failure:
        base_err_msg = 'Failed to serialize alib object'
        raise_val_err_w_log(base_err_msg)

    return failure


def ac_disc_to_rate(discount_factor, start_date, end_date, dcc='Act/365', rate_type='Annual'):
    """
    :description:
    :param discount_factor:
    :param start_date:
    :param end_date:
    :param dcc:
    :param rate_type:
    :return:
    """
    alib_dcc = Alib_Class.DCC()
    alib_obj_coerce(dcc, 'DCC', byref(alib_dcc))

    alib_rate_type = Alib_Class.RT()
    alib_obj_coerce(rate_type, 'RT', byref(alib_rate_type))

    rate = c_double()

    alib_start_date = ALib.MDY_TO_TDATE(start_date.month, start_date.day, start_date.year)
    alib_end_date = ALib.MDY_TO_TDATE(end_date.month, end_date.day, end_date.year)

    failure = ALib.ALIB_DISC_TO_RATE_O(
        c_double(discount_factor),
        alib_start_date,
        alib_end_date,
        alib_dcc,
        alib_rate_type,
        byref(rate))

    free_objects(alib_dcc, alib_rate_type)
    if failure:
        base_err_msg = 'Error while converity discount factor to rate'
        raise_val_err_w_log(base_err_msg)
    return rate.value


def ac_bonds_cfl_make(bond_obj, settle_date, pay_bdc, holiday_file, include_ai):
    """
    :description:
    :param bond_obj:
    :param settle_date:
    :param pay_bdc:
    :param holiday_file:
    :param include_ai:
    :return:
    """
    bond_trade = ac_bondtrade_make(settle_date)
    cfl = Alib_Class.CFL()

    failure = ALib.ALIB_BONDS_CFL_MAKE(
        bond_obj,
        bond_trade,
        ascii_encode(pay_bdc),
        ascii_encode(holiday_file),
        include_ai,
        byref(cfl)
    )
    free_object(cfl)
    if failure:
        base_err_msg = 'Error while computing bond cashflows'
        raise_val_err_w_log(base_err_msg)

    return cfl


def ac_frn_type_make(dcc='ACT/360', bda='M', creeping=0, adjust_dm=0, ex_div_rule="None", ex_div_days=0):
    """

    :param dcc: Day count convention.
    :param str bda: Bad day convention.
    :param int creeping: Creeping coupons.
    :param int adjust_dm: Adjust discount margin frequency.
    :param str ex_div_rule: Ex-dividend rule.
    :param int ex_div_days: Ex-dividend days.
    :return: a FRN_TYPE object
    """
    alib_mm_dcc = Alib_Class.DCC()
    alib_obj_coerce(dcc, "DCC", byref(alib_mm_dcc))
    frn_type = Alib_Class.FRN_TYPE()

    failure = ALib.ALIB_FRN_TYPE_MAKE(
        alib_mm_dcc,  # Day count convention. [DCC]
        ascii_encode(bda),  # Bad day convention. [text]
        c_int(creeping),  # Creeping coupons. [integer]
        c_int(adjust_dm),  # Adjust discount margin frequency. [integer]
        ascii_encode(ex_div_rule),  # Ex-dividend rule. [text]
        c_int(ex_div_days),  # Ex-dividend days. [integer]
        byref(frn_type)  # Base object name. [text]
    )
    free_objects(alib_mm_dcc)

    if failure:
        free_object(frn_type)
        base_err_msg = 'Error while construct an FRN_TYPE object'
        raise_val_err_w_log(base_err_msg)

    return frn_type


def ac_frn_make(frn_type, cpn_freq, quote_margin, quote_margin_dt, is_perpetual, redemption, current_cpn,
                current_cpn_dt, next_cpn_dt, hols, calls, call_dates, puts, put_dates):
    """

   :param frn_type: Floating rate note type
    :param cpn_freq: Coupon frequency.
    :param quote_margin: Quoted margins.
    :param quote_margin_dt: Quoted margin dates.
    :param is_perpetual: Is perpetual flag.
    :param redemption: Redemption value.
    :param current_cpn: Current coupon.
    :param current_cpn_dt: Current coupon date.
    :param next_cpn_dt: Next coupon date.
    :param hols: Name of holiday list.
    :param calls: Call values.
    :param call_dates: Call dates.
    :param puts: Put values.
    :param put_dates: Put dates.
    :return: A floating rate note object
    """

    # Convert - Quoted margins. [real number array]
    n_quote_margin = len(quote_margin)
    alib_quote_margin = (c_double * n_quote_margin)(*[])
    for i in range(0, n_quote_margin):
        alib_quote_margin[i] = c_double(quote_margin[i])

    # Convert - Quoted margin dates. [date array]
    n_quote_margin_dt = len(quote_margin_dt)
    alib_quote_margin_dt = (c_long * n_quote_margin_dt)(*[])
    for i in range(0, n_quote_margin_dt):
        alib_quote_margin_dt[i] = python_date_to_alib_Tdate(quote_margin_dt[i])

    # Convert - Current coupon. [real number]
    alib_current_cpn = c_double(current_cpn)

    # Convert - Current coupon date. [date]
    alib_current_cpn_dt = python_date_to_alib_Tdate(current_cpn_dt)

    # Convert - Next coupon date. [date]
    alib_next_cpn_dt = python_date_to_alib_Tdate(next_cpn_dt)

    # Convert - Call values. [real number array]
    n_calls = len(calls)
    alib_calls = (c_double * n_calls)(*[])
    for i in range(0, n_calls):
        alib_calls[i] = c_double(calls[i])

    # Convert - Quoted margin dates. [date array]
    n_call_dates = len(call_dates)
    alib_call_dates = (c_long * n_call_dates)(*[])
    for i in range(0, n_call_dates):
        alib_call_dates[i] = python_date_to_alib_Tdate(call_dates[i])

    # Convert - Put values. [real number array]
    n_puts = len(puts)
    alib_puts = (c_double * n_puts)(*[])
    for i in range(0, n_puts):
        alib_puts[i] = c_double(puts[i])

    # Convert - Put dates. [date array]
    n_put_dates = len(put_dates)
    alib_put_dates = (c_long * n_put_dates)(*[])
    for i in range(0, n_put_dates):
        alib_put_dates[i] = python_date_to_alib_Tdate(put_dates[i])

    frn = Alib_Class.FRN()
    failure = ALib.ALIB_FRN_MAKE(
        frn_type,  # Floating rate note type. [FRN_TYPE]
        c_long(cpn_freq),  # Coupon frequency. [integer]
        c_long(n_quote_margin_dt),
        alib_quote_margin,  # Quoted margins. [real number array]
        alib_quote_margin_dt,  # Quoted margin dates. [date array
        c_long(is_perpetual),  # Is perpetual flag. [integer]
        c_double(redemption),  # Redemption value. [real number]
        alib_current_cpn,  # Current coupon. [real number]
        alib_current_cpn_dt,  # Current coupon date. [date]
        alib_next_cpn_dt,  # Next coupon date. [date]
        ascii_encode(hols),  # Name of holiday list. [text]
        c_long(n_calls),
        alib_calls,  # Call values. [real number array]
        alib_call_dates,  # Call dates. [date array]
        c_long(n_puts),
        alib_puts,  # Put values. [real number array]
        alib_put_dates,  # Put dates. [date array]
        byref(frn)  # Base object name. [text]
    )

    if failure:
        free_object(frn)
        base_err_msg = 'Error while computing frn make'
        raise_val_err_w_log(base_err_msg)

    return frn


def ac_trade_make(settlement_date, is_ex_div, trade_date, value_date):
    """

    :param settlement_date: settlement date of the trade
    :param is_ex_div: flag on whether the bond is currently trading ex-dividend
    :param trade_date: trade date of the bond
    :param value_date: value date of the bond
    :return: a TRADE object
    """

    alib_settlement_date = python_date_to_alib_Tdate(settlement_date)
    alib_is_ex_div = c_long(is_ex_div)
    if isinstance(trade_date, dt.date):
        alib_trade_date = python_date_to_alib_Tdate(trade_date)
    else:
        alib_trade_date = (c_long * 0)(*[])

    if isinstance(value_date, dt.date):
        alib_value_date = python_date_to_alib_Tdate(value_date)
    else:
        alib_value_date = (c_long * 0)(*[])

    trade = Alib_Class.TRADE()

    failure = ALib.ALIB_TRADE_MAKE(
        alib_settlement_date,
        alib_is_ex_div,
        alib_trade_date,
        alib_value_date,
        byref(trade)
    )

    if failure:
        free_object(trade)
        base_err_msg = 'Error while construct an TRADE object'
        raise_val_err_w_log(base_err_msg)

    return trade


def ac_frn_dm_o(frn, settle_dt, current_index_rate, assumed_index_rate, price, is_dirty):
    '''

    :param frn: a FRN object
    :param settle_dt: settle date
    :param current_index_rate: simple rate to next coupon date. This is assumed to be \
    in the FRN’s day count convention.
    :param assumed_index_rate: assumed indicator rate for all coupons after the next \
    coupon date. This is assumed to be in the FRN’s day count convention.
    :param price: price of the FRN
    :param is_dirty: 0 = clean price, 1 = dirty price
    :return: discount margin
    '''
    alib_current_index_rate = c_double(current_index_rate)
    alib_assumed_index_rate = c_double(assumed_index_rate)
    alib_price = c_double(price)
    alib_is_dirty = c_long(is_dirty)

    alib_trade = ac_trade_make(settle_dt, 0, settle_dt, settle_dt)
    my_dm = c_double()
    failure = ALib.ALIB_FRN_DM_O(frn, alib_trade, alib_current_index_rate, alib_assumed_index_rate,
                                 alib_price, alib_is_dirty, byref(my_dm))
    free_object(alib_trade)
    if failure:
        base_err_msg = 'Error in frn dm'
        raise_val_err_w_log(base_err_msg)
    return my_dm.value


def trading_date_list(start_date, end_date, holiday_file):
    '''
    :description: generate a list of datetime objects based on the holiday convention
    :param dt.date start_date: start date for the trading date list
    :param dt.date end_date: end date for the trading date list
    :param holiday_file: a holiday object
    :return: a list of datetime objects
    '''
    wn.warn(
        "This function will be going away soon. Use same function name from panormus.quant.market_date instead.",
        PendingDeprecationWarning)
    use_start_date = ac_bus_day(start_date, 'F', holiday_file)
    use_end_date = ac_bus_day(end_date, 'P', holiday_file)

    curr_date = use_start_date
    dates_out = [curr_date]
    while curr_date < use_end_date:
        curr_date = ac_date_fwd_adj2(curr_date, '1D,B', holiday_file)
        dates_out.append(curr_date)

    return dates_out


def ac_object_get_num_items(curve):
    '''
    :description: get the number of items
    :param curve:
    :return:
    '''
    num_items = c_long()
    num_items2 = c_long()
    failure = ALib.ALIB_OBJECT_GET(curve, ascii_encode('numItems'), byref(num_items), byref(num_items2))

    if failure:
        base_err_msg = 'Error getting number of items from curve'
        raise_val_err_w_log(base_err_msg)

    return num_items.value


def ac_object_get_rates(curve):
    num_items = ac_object_get_num_items(curve)
    rates = (c_double * MAX_ARRAY_SIZE)(*[])
    alib_dates = (c_long * MAX_ARRAY_SIZE)(*[])

    intZero = (c_long * 1)(*[])
    intZero[0] = 0

    rate_type = Alib_Class.RT()
    failure = ALib.ALIB_RT_MAKE_ID(
        c_long(1),
        byref(rate_type)
    )
    if failure:
        base_err_msg = 'Error getting rates from curve (RT_MAKE_ID step)'
        raise_val_err_w_log(base_err_msg)

    dcc_act_365Fstr = Alib_Class.DCC()
    failure = alib_obj_coerce("Act/365F", "DCC", byref(dcc_act_365Fstr))
    if failure:
        base_err_msg = 'Error getting rates from curve (RT_MAKE_ID step)'
        raise_val_err_w_log(base_err_msg)

    failure = ALib.ALIB_ZERO_CURVE_RATES(
        curve,
        c_long(len(intZero)),
        intZero,
        0,
        None,
        None,
        rate_type,
        dcc_act_365Fstr,
        byref(alib_dates),
        byref(rates)
    )

    free_objects(rate_type)
    free_objects(dcc_act_365Fstr)

    if failure:
        base_err_msg = 'Error getting rates from curve'
        raise_val_err_w_log(base_err_msg)

    python_dates = [alib_Tdate_to_python_date(d) for d in alib_dates]

    dates_lst = python_dates[:num_items]
    rates_lst = list(rates[:num_items])
    return dates_lst, rates_lst