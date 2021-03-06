# automatic full bless test suite
import pandas as pd
import datetime
import pickle
import os
import pathlib
from unittest import (TestCase, skip)
import importlib
from backtesting_utils.clear_all_cache import clear_all_cache


class TestBacktestFns(TestCase):
    def test_get_params(self):
        # test if parameters are the same
        def __check_default_param(folder, signal_file):
            # import strategy obj
            mod_dir = '.'.join(['basket', folder, signal_file])
            sig_mod = importlib.import_module(mod_dir)

        sig_instance = sig_mod.signal3()
        sig_instance.add_strat_info()
        sig_instance.add_dir_info()
        params = sig_instance.import_parse_param(sig_instance.MASTER_INPUT_DIR, sig_instance.Short_Name)

        baseline_pickle = os.path.join(sig_instance.PROJ_DIR, 'offline_data', 'baseline_pickle',
                                       sig_instance.Short_Name + '_param.pickle')

        overwrite_baseline = False  # do this once, this will reset the baseline
        if overwrite_baseline:
            with open(baseline_pickle, "wb") as f:
                pickle.dump(params, f, pickle.HIGHEST_PROTOCOL)

        # compare with a pickle
        with open(baseline_pickle, "rb") as f:
            params_baseline = pickle.load(f)

        for a, b in zip(list(params.values()), list(params_baseline.values())):
            assert (a.equals(b))

    __check_default_param('AUS_Rates1', 'run_basket_baseline')
    __check_default_param('CAN_Rates1', 'run_basket_baseline')
    __check_default_param('GBR_Rates1', 'run_basket_baseline')
    __check_default_param('USA_Rates1', 'run_basket_baseline')
    __check_default_param('RATES_portfolio_G4', 'run_basket_b_baseline')
    print('Passed: Default parameters unchanged vs. baseline.')


def test_default_backtest_full_bless(self):
    """Run full backtest history and check that each datapoint is unchanged"""

    def __backtest_full_bless(folder, signal_file):
        cut_off = '2020-01-21'
        # import strategy obj
        mod_dir = '.'.join(['basket', folder, signal_file])
        sig_mod = importlib.import_module(mod_dir)
        sig_instance = sig_mod.signal3()
        backtest_data = sig_instance.get_data_dict(running_mode='production', get_or_set_market_data='set')

        baseline_pickle = os.path.join(sig_instance.PROJ_DIR, 'offline_data', 'baseline_pickle',
                                       sig_instance.Short_Name + '_strat.pickle')

        overwrite_baseline = False  # do this once, this will reset the baseline
        if overwrite_baseline:
            with open(baseline_pickle, "wb") as f:
                pickle.dump(backtest_data, f, pickle.HIGHEST_PROTOCOL)

        with open(baseline_pickle, "rb") as f:
            backtest_data_baseline = pickle.load(f)

        pd.testing.assert_frame_equal(backtest_data['strategy_data'][['cumprof']].dropna().loc[:cut_off, :],
                                      backtest_data_baseline['strategy_data'][['cumprof']].dropna().loc[:cut_off, :],
                                      check_less_precise=6)

    __backtest_full_bless('AUS_Rates1', 'run_basket_baseline')
    __backtest_full_bless('CAN_Rates1', 'run_basket_baseline')
    __backtest_full_bless('GBR_Rates1', 'run_basket_baseline')
    __backtest_full_bless('USA_Rates1', 'run_basket_baseline')
    __backtest_full_bless('RATES_portfolio_G4', 'run_basket_b_baseline')
    print('Passed: Backtester passed full bless compare unchanged vs. baseline.')


if __name__ == '__main__':
    clear_all_cache()
    t = TestBacktestFns()
    t.test_get_params()
    t.test_default_backtest_full_bless()

