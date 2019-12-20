from copy import deepcopy

from panormus.quant.alib.conventions import CONV_DICT
import panormus.quant.alib.utils as qau
import panormus.trade.base as aab


class IRSTrade(aab.Trade):
    '''
    Standard fixed-float interest rate swap
    '''

    def __eq__(self, other):
        if isinstance(other, IRSTrade):
            return all([
                self.swap_conv == other.swap_conv,
                self.ccy == other.ccy,
                self.start_date == other.start_date,
                self.end_date == other.end_date,
                self.coupon == other.coupon,
                self.payrec == other.payrec,
                self.notional == other.notional,
                self.float_leg_spread == other.float_leg_spread
            ])
        else:
            return False

    def __init__(
            self, trade_id,
            swap_conv, start_date, end_date,
            coupon, payrec_fixed,
            holiday_oracle, swap_convention_dictionary=None,
            notional=1.0, float_leg_spread=0.0, oRide_fix_ivl=None, oRide_fix_dcc=None
    ):
        '''
        :param trade_id:
        :param swap_conv:
        :param start_date:
        :param end_date:
        :param coupon:
        :param payrec_fixed:
        :param holiday_oracle:
        :param dict|None swap_convention_dictionary: if none, defaults to standard conventions
        :param notional:
        :param float_leg_spread:
        '''
        super().__init__(trade_id)
        if swap_convention_dictionary is None:
            swap_conv_dict = deepcopy(CONV_DICT)
        else:
            swap_conv_dict = deepcopy(swap_convention_dictionary)

        self.swap_conv = swap_conv
        self.ccy = self.swap_conv[:3].lower()
        self.start_date = start_date
        self.end_date = end_date
        self.coupon = coupon
        self.payrec = payrec_fixed[0].lower()
        self.notional = notional
        self.float_leg_spread = float_leg_spread

        if oRide_fix_ivl is not None:
            swap_conv_dict[self.swap_conv]['fixed_interval'] = oRide_fix_ivl
        if oRide_fix_dcc is not None:
            swap_conv_dict[self.swap_conv]['fixed_dcc'] = oRide_fix_dcc

        self.swap_convention_dictionary = swap_conv_dict

        self.holiday_oracle = holiday_oracle
        self.fixed_dcc = self.swap_convention_dictionary[self.swap_conv]['fixed_dcc']
        self.float_dcc = self.swap_convention_dictionary[self.swap_conv]['float_dcc']
        self.fixed_ivl = self.swap_convention_dictionary[self.swap_conv]['fixed_interval']
       self.float_ivl = self.swap_convention_dictionary[self.swap_conv]['float_interval']
        self.float_rate_ivl = self.swap_convention_dictionary[self.swap_conv]['float_rate_interval']
        self.stub_method = self.swap_convention_dictionary[self.swap_conv]['stub_method']
        self.accrual_bdc = self.swap_convention_dictionary[self.swap_conv]['accrual_bad_day_conv']
        self.pay_bdc = self.swap_convention_dictionary[self.swap_conv]['pay_bad_day_conv']
        self.reset_bdc = self.swap_convention_dictionary[self.swap_conv]['reset_bad_day_conv']
        self.principal_init_flag = self.swap_convention_dictionary[self.swap_conv]['principal_initial_flag']
        self.principal_final_flag = self.swap_convention_dictionary[self.swap_conv]['principal_final_flag']
        self.holiday_calendar_name = self.swap_convention_dictionary[self.swap_conv]['holiday_calendar_name']

        self._schedule_done = False
        self._fixed_leg_acc_start_dates = None
        self._fixed_leg_acc_end_dates = None
        self._fixed_leg_pay_dates = None
        self._fixed_leg_dccs = None
        self._fixed_leg_stub_loc = None

        self._float_leg_acc_start_dates = None
        self._float_leg_acc_end_dates = None
        self._float_leg_reset_dates = None
        self._float_leg_pay_dates = None
        self._float_leg_dccs = None
        self._float_rate_end_dates = None
        self._float_rate_stub_loc = None

    def fixed_leg_acc_start_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._fixed_leg_acc_start_dates

    def fixed_leg_acc_end_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._fixed_leg_acc_end_dates

    def fixed_leg_pay_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._fixed_leg_pay_dates

    def fixed_leg_dccs(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._fixed_leg_dccs

    def float_leg_acc_start_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_leg_acc_start_dates

    def float_leg_acc_end_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_leg_acc_end_dates

    def float_leg_reset_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_leg_reset_dates

    def float_leg_pay_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_leg_pay_dates

    def float_leg_dccs(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_leg_dccs

    def float_rate_end_dates(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_rate_end_dates

    def float_rate_stub(self):
        if not self._schedule_done:
            self.generate_schedule()
        return self._float_rate_stub_loc

    def generate_schedule(self):
        self._schedule_done = True
        swap_date_dicts = qau.swap_dates_from_conv(
            self.start_date, self.end_date,
            self.holiday_oracle,
            self.swap_conv,
            self.swap_convention_dictionary)

        self._fixed_leg_acc_start_dates = swap_date_dicts['fix_leg_dates']['acc_start_dates']
        self._fixed_leg_acc_end_dates = swap_date_dicts['fix_leg_dates']['acc_end_dates']
        self._fixed_leg_pay_dates = swap_date_dicts['fix_leg_dates']['payment_dates']
        self._fixed_leg_dccs = swap_date_dicts['fix_leg_dates']['dccs']
        self._fixed_leg_stub_loc = swap_date_dicts['fix_leg_dates']['stub']

        self._float_leg_acc_start_dates = swap_date_dicts['flt_leg_dates']['acc_start_dates']
        self._float_leg_acc_end_dates = swap_date_dicts['flt_leg_dates']['acc_end_dates']
        self._float_leg_reset_dates = swap_date_dicts['flt_leg_dates']['reset_dates']
        self._float_leg_pay_dates = swap_date_dicts['flt_leg_dates']['payment_dates']
        self._float_leg_dccs = swap_date_dicts['flt_leg_dates']['dccs']
        self._float_rate_end_dates = swap_date_dicts['flt_leg_dates']['rate_end_dates']
        self._float_rate_stub_loc = swap_date_dicts['flt_leg_dates']['stub']

    @staticmethod
    def build_roll_details_from_trade_string(trade_string):
        trade_properties = trade_string.lower().split('.')
        return {
            'start_term': trade_properties[4],
            'end_term': trade_properties[5],
            'conv_str': '.'.join([trade_properties[2], trade_properties[3]]).upper(),
            'pay_rec': trade_properties[6]
        }