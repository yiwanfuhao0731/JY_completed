# @justinY 20191228
# this file is to lift up for the RV system strategy the volatility to a target
# input would be the signals, instrument durations, 5 year swap rate,

from strategy.Analytics_for_pc.abs_sig_genr import abstract_sig_genr_portfolio as abs_sig

class signal3(abs_sig):
    def __init__(self):
        super(signal3, self).__init__()

    # the target is to simply lift the RV strategy to 4% target vol
    def run_rb_constant_main(self,*args,**kwargs):
        pass

    def run_rb_constant_fund_scale(self,*args,**kwargs):
        pass