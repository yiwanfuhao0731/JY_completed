import datetime as dt

from panormus.markable.loader import loader
from panormus.markable import irs as irs_mble
from panormus.quant.alib import conventions as quant_conventions
from panormus.quant.alib import utils as quant_utils
from panormus.trade import irs as irs_trade
import pandas as pd

curve_names_dictionary = quant_conventions.CURVE_NAMES_DICT

curve_location = 'ny'  # PvK, please switch to lon

holiday_oracle = quant_utils.Holidays()
print (holiday_oracle)
curve_loader = loader.CurveLoaderAlib(curve_location)
fixings_loader = loader.FixingsLoaderOpenData()

history_start_date = dt.date(2019, 1, 1)
history_end_date = dt.date(2019, 6, 11)

## This generates a list of trading days between two dates
trading_days = quant_utils.trading_date_list(history_start_date, history_end_date, holiday_oracle['GBP+USD.FX'])

## main example
swap_convention = 'USD.3ML'  ## could try USD.OIS or EUR.6ML

dcrv_name = curve_names_dictionary[swap_convention]['dcrv']
ecrv_name = curve_names_dictionary[swap_convention]['ecrv']

## 5Y5Y swap
swap_notional = 10000000

# get the start date of a swap
# get the end date of a swap
start_date, end_date = quant_utils.dates_from_trade_date(trading_days[0], '5y', '5y', swap_convention, holiday_oracle)
print ('start date,end date', start_date,end_date)
## need curves for today to get the par rate from the first trading day
# dcrv: discount curve
dcrv_today = curve_loader.get_curve(dcrv_name, trading_days[0])
print (trading_days[0])
print (dcrv_today)
# ecrv: projection curve
ecrv_today = curve_loader.get_curve(ecrv_name, trading_days[0])
print (ecrv_today)

coupon = quant_utils.swap_rate_with_conv(
    dcrv_today, start_date, end_date, 1, 0, ecrv_today, 0, 0, holiday_oracle, swap_convention)
print (coupon)

swap_trade = irs_trade.IRSTrade(
    1, swap_convention, start_date, end_date, coupon, 'p', holiday_oracle, notional=swap_notional)
print (swap_trade)
mble_list = [
    irs_mble.IRS(swap_trade, d, holiday_oracle, curve_loader, fixings_loader, curve_names_dictionary) for d intrading_days]
print (mble_list)
pv_list = [m.market_value() for m in mble_list]
print (pv_list)