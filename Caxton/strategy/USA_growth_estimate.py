# import sys when runing from the batch code
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from textwrap import wrap
# visualisation packages
import math
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import collections
from matplotlib.font_manager import FontProperties

import Analytics.series_utils as s_util
import Analytics.abstract_sig as abs_sig
import Analytics.wfcreate as wf
from Analytics.wfcreate import use_cached_in_class
from Analytics.wfcreate import fwd_growthTree
from signals.RATES_FWDGROW_USA.run_signal import signal3 as base_signal3


class signal3(base_signal3):
    def __init__(self):
        super(signal3, self).__init__()

    def add_strat_info(self):
        # this is the part to override
        self.signal_ID = 'sig_0028'
        self.Short_Name = 'RATES_FWDGROW_USA_6m'
        self.Description = '''
                        Variation of the USA forward growth model: double the length of changes

                       Cutting through the USA forward growth, i.e estimate the growth of different component individually with the impulse methodology.
                       GDP = C + I + G + X - M. In theory estimating each component and sum up to figure will give the best estimates on the forward growth.
                       The target is to estimate the forward 6m growth, but ideally 3m.
                       More specifically, to estimate the change of a variable A impact on the change of variable B. A should be normalised (maybe a second order diff is a good starting point).
                       B should be the change on the individual component of GDP.

                       C: bond prices, housing prices, equity prices, credit
                       I (residential): housing prices, housing start, mortgage rate
                       I (business): corporate credit spread
                       G: Fiscal impulse
                       X: trading partners growth, commodity, FX
                       M: mainly depends on the domestic demand. maybe we don't really care too much of this because the trend should in theory be the same with consumption.

                       The weight: should be able to decide the weight based on regression analysis.
                       However, maybe it is not a bad idea to decide the weight simply with correlation or visualisation??

                       Visualisation is also a key part of the analysis, especially the impulse responsive function is in particular a 
                       good starting point of visualisation already.
                       '''

    def add_dir_info(self):
        self.WKDIR = os.path.dirname(os.path.realpath(__file__))
        self.PROJ_DIR = os.path.join(self.WKDIR, "../..")
        self.MASTER_INPUT_DIR = os.path.join(self.PROJ_DIR, "input/master_input.xlsx")
        self.OUTPUT_DIR = os.path.join(self.PROJ_DIR, "output")
        self.SCRATCH_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name)
        self.INDICATOR_EXP_DIR = os.path.join(self.PROJ_DIR, "zzz_NO_commit_folder", self.Short_Name, 'indicator_group')
        self.TEMP_LOCAL_PICKLE = os.path.join(os.environ['TEMP'], 'TEMP_' + self.Short_Name + '_local_db.pickle')
        # Firstly export all the relevant result into the csv format. Secondly plot into the charts
        self.SHARE_DIR = r'Y:\MacroQuant\JYang\JY_Project'
        self.RPTDIR = os.path.join(self.SHARE_DIR, 'reporting', self.Short_Name) if os.access(self.SHARE_DIR,
                                                                                              os.W_OK) else os.path.join(
            self.PROJ_DIR, 'reporting', self.Short_Name)
        #######
        self.IMPORT_DATA_DIR = os.path.join(self.WKDIR, '   ')


def run():
    GP = signal3()
    GP.initialise_and_run()


if __name__ == "__main__":
    run()
    print('Done!')