'''
Created on 2019年3月23日

@author: user
'''
import blpapi
from enum import Enum


# region enums
class PeriodicityAdjustment(Enum):
    ACTUAL = 1
    CALENDAR = 2
    FISCAL = 3


class PeriodicitySelection(Enum):
    DAILY = 1
    WEEKLY = 2
    MONTHLY = 3
    QUARTERLY = 4
    SEMI_ANNUALLY = 5
    YEARLY = 6


class OverrideOption(Enum):
    OVERRIDE_OPTION_CLOSE = 1
    OVERRIDE_OPTION_GPA = 2


class PricingOption(Enum):
    PRICING_OPTION_PRICE = 1
    PRICING_OPTION_YIELD = 2


class NonTradingDayFillOption(Enum):
    NON_TRADING_WEEKDAYS = 1
    ALL_CALENDAR_DAYS = 2
    ACTIVE_DAYS_ONLY = 3


class NonTradingDayFillMethod(Enum):
    PREVIOUS_VALUE = 1
    NIL_VALUE = 2


# endregion

# This makes successive requests faster
DATE = blpapi.Name("date")
ERROR_INFO = blpapi.Name("errorInfo")
EVENT_TIME = blpapi.Name("EVENT_TIME")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
FIELD_ID = blpapi.Name("fieldId")
SECURITY = blpapi.Name("security")
SECURITY_DATA = blpapi.Name("securityData")

# region constants
exceptions = 'exceptions'
field_id = blpapi.Name('fieldId')
value_fld = blpapi.Name('value')
reason = 'reason'
category = 'category'
sub_category = 'subcategory'
description = 'description'
error_code = 'errorCode'
source = 'source'
security_error = 'securityError'
message = 'message'
response_error = 'responseError'
security_data = 'securityData'
field_exceptions = 'fieldExceptions'
error_info = 'errorInfo'
field_eid_data = 'eidData'
datetime = 'DATETIME'
open_fld = 'OPEN'
high = 'HIGH'
low = 'LOW'
close = 'CLOSE'
volume = 'VOLUME'
number_of_ticks = 'NUMBER_OF_TICKS'
value = 'VALUE'
return_eids = 'returnEids'
max_data_points = 'maxDataPoints'
start_date = 'startDate'
end_date = 'endDate'
start_date_time = 'startDateTime'
end_date_time = 'endDateTime'
event_type = 'eventType'
gap_fill_initial_bar = 'gapFillInitialBar'

# historical request settings
calendar_code_override = blpapi.Name('calendarCodeOverride')
currency_code = blpapi.Name('currencyCode')
periodicity_adjustment = blpapi.Name('periodicityAdjustment')
periodicity_selection = blpapi.Name('periodicitySelection')
currency = blpapi.Name('currency')
override_option = blpapi.Name('overrideOption')
pricing_option = blpapi.Name('pricingOption')
non_trading_day_fill_option = blpapi.Name('nonTradingDayFillOption')
non_trading_day_fill_method = blpapi.Name('nonTradingDayFillMethod')
follow_dpdf = blpapi.Name('adjustmentFollowDPDF')
adjustment_split = blpapi.Name('adjustmentSplit')
adjustment_normal = blpapi.Name('adjustmentNormal')
adjustment_abnormal = blpapi.Name('adjustmentAbnormal')
override_currency = blpapi.Name('currency')
overrides = blpapi.Name('overrides')
# endregion

# region Authorization Message Types
auth_success = 'AuthorizationSuccess'
auth_failure = 'AuthorizationFailure'
auth_revoked = 'AuthorizationRevoked'
auth_entitlement_changed = 'EntitlementChanged'
# endregion


if __name__ == '__main__':
    print('constants are defined')

 