# import sys when runing from the batch code
import sys
import os

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), "../.."))
import pandas as pd
import numpy as np
from datetime import datetime
# visualisation packages
import math
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import collections
from matplotlib.font_manager import FontProperties
from signals.RATES_LVL_GROWmPOT.run_signal import get_data as RATES_LVL_GROWmPOT_get

import Analytics.loess_filter as lf
import Analytics.series_utils as s_util
from Analytics.download_util import Downloader as Downloader
from Analytics.scoring_methods import scoring_method_collection as smc
import Analytics.abstract_sig as abs_sig
from panormus.utils.cache import cache_response, clear_cache

# part of the utilities
dateparse = lambda x: pd.datetime.strptime(x, '%d/%m/%Y')
dateparse2 = lambda x: pd.datetime.strptime(x, '%Y-%m-%d')


class signal3(abs_sig.abstract_sig_genr):
    def __init__(self):
        # in this case, both Econ and csv file data are used
        super(signal3, self).__init__()

    @cache_response('RATES_LVL_TW_GROW', region='disk_8h', skip_first_arg=True)
    def get_data_dict(self, *args, **kwargs):
        self.add_sig_info()
        self.add_dir_info(**kwargs)
        self.pre_run_result = self.pre_run(**kwargs)
        self.raw_data_new_fmt = self.load_data_wrapper(self.MASTER_INPUT_DIR, self.Short_Name, **kwargs)
        self.create_folder(self.INDICATOR_EXP_DIR)
        self.import_parse_param(self.MASTER_INPUT_DIR, self.Short_Name)
        self.cleaned_data = self.sanity_check()
        self.indicator = self.apply_sig_genr()
        self.convert_indicator_tolist_new(self.indicator, self.INDICATOR_EXP_DIR)
        result_dict = self.fullkey_result_dict(self.indicator)

        if kwargs.get('run_chart') == True:
            self.out_folder = self.create_tearsheet_folder(self.RPTDIR)
            self.create_report_page()

        return result_dict

    def pre_run(self, **kwargs):
        result_dict = {}
        result_dict.update(RATES_LVL_GROWmPOT_get(**kwargs))
        return result_dict

    def add_sig_info(self):
        self.signal_ID = 'sig_0013'
        self.Short_Name = 'RATES_LVL_TW_GROW'
        self.Description = '''
                      High frequency, trading partners growth, weighted by export weights. 
                      Plotting against exports_goods of non commodity as percentage of GDP, 
                      6m difference annualized
                      '''
        self.exec_path = __file__

    def import_parse_param(self, master_input_dir, Short_Name):
        '''
        :return: import from a csv file and parse the relevant parameters
        three parameter, orders should be the same in the csv file
        loess_start_year: the start year for the potential growth
        loess_end_year: the end year for the potential growth
        regression_param: the frac parameter in the loess regression
        '''
        param_df = pd.read_excel(master_input_dir, sheet_name=Short_Name, index_col=False, header=0,
                                 na_values=['NA', ''])
        self.all_ISO = param_df.iloc[:, 0].values.tolist()

        # in this case, should be hard-coded as 1800, or 360*5
        self.roll_mean = 5 * 12
        self.roll_std = 5 * 12
        return

    @cache_response('RATES_LVL_TW_GROW_sanity_check', 'disk_8h', skip_first_arg=True)
    def sanity_check(self, ignore_xto_country_list=[]):
        '''
        :return: convert the date to monthly
        other types of conversion is also possible
        '''
        SU = s_util.date_utils_collection()

        self.cleaned_data = collections.OrderedDict()
        assert len(self.all_ISO) > 0, 'sorry, list of countries have not been provided'
        # all_xport to MUST EXCLUDE EU countries to avoid double counting
        all_ETO = ['ARG', 'AUS', 'BRA', 'BUL', 'CAN', 'CHE', 'CHL', 'CHN', 'COL', 'CRO', 'CZE', 'DEN', 'EUR', 'GBR',
                   'HKG', 'HUN', 'IDN', 'IND', 'ISE', 'JPN', 'KOR', 'MAL', 'MEX', 'NOR', 'NZD', 'PER', 'PHP', 'POL',
                   'ROM', 'RUS', 'SAF', 'SAR', 'SGP', 'SWE', 'TAW', 'THA', 'TUR', 'URU', 'USA', 'VTN']

        df_gdp_matrix = None
        # construct the df_GDP matrix
        # print (self.all_ISO[:-1])
        for iso in self.all_ISO[:-1]:
            this_df = self.raw_data_new_fmt[iso + '_RGDP_EXT_yoy']
            if df_gdp_matrix is None:
                df_gdp_matrix = SU.conversion_down_to_m(this_df)
            else:
                df_gdp_matrix = pd.merge(df_gdp_matrix, SU.conversion_down_to_m(this_df), left_index=True,
                                         right_index=True, how='outer')

        gdp_names = [i.split('_')[0] for i in df_gdp_matrix.columns.tolist()]
        df_gdp_matrix.columns = gdp_names
        df_gdp_matrix.index = pd.to_datetime(df_gdp_matrix.index)
        df_gdp_matrix.to_csv(os.path.join(self.INDICATOR_EXP_DIR, 'raw_gdp_mat.csv'))
        last_dt = df_gdp_matrix['EUR'].last_valid_index()
        mask = (df_gdp_matrix.index <= last_dt)
        df_gdp_matrix = df_gdp_matrix.loc[mask, :]
        df_gdp_matrix.fillna(inplace=True,
                             method='ffill')  # fill forward the missing CAI, to get the current month print
        self.cleaned_data['df_gdp_matrix'] = df_gdp_matrix
        df_ex_to = self.raw_data_new_fmt['ALL_ISO_XTO'].copy()
        df_ex_to = SU.conversion_down_to_m(df_ex_to, col_to_repeat=[k for k in range(df_ex_to.shape[1])])
        all_columns = df_ex_to.columns.tolist()
        all_iso = [i.split('_')[0] for i in all_columns]
        all_iso = sorted(list(set(all_iso)))
        self.all_iso_genr_from_eto = all_iso

        empty_m = SU.empty_M_df()
        empty_m['Date'] = pd.to_datetime(empty_m['Date'], format="%Y-%m-%d")
        empty_m.set_index('Date', inplace=True)
        empty_m = SU.conversion_to_FOM(empty_m)

        for i in all_iso[:]:
            all_ETO_ex_itself = [_ for _ in all_ETO if _ != i]
            if i == 'EUR':
                all_ETO_ex_itself = [_ for _ in all_ETO if _ not in [i, 'POL', 'CZE', 'HUN']]
            if i == 'CHN':
                all_ETO_ex_itself = [_ for _ in all_ETO if _ not in [i, 'HKG', 'SGP']]
            this_columns = [('').join([i, '_XTO_', j]) for j in all_ETO_ex_itself if
                            j not in ignore_xto_country_list]  # ignore_xto_country_list: for example: ignore export to China
            this_columns = sorted(list(set(this_columns).intersection(set(all_columns))))
            this_ETO_df = df_ex_to.loc[:, this_columns]
            eto_name = [i.split('_')[-1] for i in this_ETO_df.columns.tolist()]
            this_ETO_df.columns = eto_name
            this_ETO_df.dropna(
                inplace=True)  # drop all the NAs and fill forward/backward so that will get the full list of trade weight

            empty_m_new = empty_m.copy()
            this_ETO_df = pd.merge(empty_m_new, this_ETO_df, left_index=True, right_index=True, how='outer')
            this_ETO_df.fillna(method='ffill', inplace=True)
            this_ETO_df.fillna(method='backfill', inplace=True)
            self.cleaned_data[i + '_eto_matrix'] = this_ETO_df

        return self.cleaned_data

    @cache_response('RATES_LVL_TW_GROW_apply_sig_genr', 'disk_8h', skip_first_arg=True)
    def apply_sig_genr(self, ignore_xto_country_list=[]):
        SU = s_util.date_utils_collection()
        # init chart info dict
        self.indicator = collections.OrderedDict()

        df_gdp = self.cleaned_data['df_gdp_matrix']

        for iso in self.all_iso_genr_from_eto:
            df_gdp_copy = df_gdp
            df_eto = self.cleaned_data[iso + '_eto_matrix'].copy()
            # make the export_weight and gdp matrix exactly the same
            common_column = list(set(df_gdp_copy.columns.tolist()).intersection(set(df_eto.columns.tolist())))
            common_dates = list(set(df_gdp_copy.index.tolist()).intersection(set(df_eto.index.tolist())))

            df_gdp_copy = df_gdp_copy.loc[common_dates, common_column]
            df_eto = df_eto.loc[common_dates, common_column]

            df_gdp_copy.sort_index(inplace=True)
            df_eto.sort_index(inplace=True)

            # convert everything into numpy ndarray so that the calculation is
            gdp_matrix_mat = df_gdp_copy.values.astype(np.float)
            ARG_matrix_mat = df_eto.values.astype(np.float)
            # TODO: FT: enable the GDP data to repeat forward, when the latest data is not availalble.
            # TODO: However , for the repeated value give it lower weightings
            gdp_notNA_mask = (~np.isnan(gdp_matrix_mat))

            output = np.full(ARG_matrix_mat.shape[0], np.nan)

            for i in range(gdp_matrix_mat.shape[0]):
                this_gdp = gdp_matrix_mat[i]
                this_eto = ARG_matrix_mat[i]
                this_notNA = gdp_notNA_mask[i]
                this_trade_weighted_gdp = np.sum(np.multiply(this_gdp[this_notNA], this_eto[this_notNA])) / np.sum(
                    this_eto[this_notNA])
                output[i] = this_trade_weighted_gdp
            # make the numpy ndarray back to pandas dataframe
            iso_trade_w_gdp = pd.DataFrame(output)
            iso_trade_w_gdp.index = df_eto.index.tolist()
            iso_trade_w_gdp.columns = [iso + '_trade_weighted_gdp']
            iso_trade_w_gdp.sort_index(inplace=True)
            self.indicator[iso + '_trade_weighted_gdp'] = iso_trade_w_gdp

        # get the export goods non commodity as percentage of GDP
        for iso in self.all_iso_genr_from_eto:
            key_good = iso + '_NON_CMD_Export_GDP'

        if not key_good in self.raw_data_new_fmt.keys():
            self.indicator[iso + '_NON_CMD_Export_GDP'] = pd.DataFrame()
        else:
            df_good = self.raw_data_new_fmt[key_good]
            df_good.columns = [key_good]
            df_good = SU.conversion_down_to_m(df_good)
            df_good = SU.rolling_ignore_nan(df_good, _window=3, _func=np.mean)
            df_good = df_good.diff(periods=6) * 2 * 100
            self.indicator[iso + '_NON_CMD_Export_GDP'] = df_good

        # get the export goods non commodity MIL USD
        for iso in self.all_iso_genr_from_eto:
            key_good = iso + '_NON_CMD_Export_USD'

            if not key_good in self.raw_data_new_fmt.keys():
                self.indicator[iso + '_NON_CMD_Export_USD'] = pd.DataFrame()
            else:
                df_good = self.raw_data_new_fmt[key_good]
                df_good.columns = [key_good]
                df_good = SU.conversion_down_to_m(df_good)
                df_good = SU.rolling_ignore_nan(df_good, _window=3, _func=np.mean)
                df_good = df_good.diff(periods=6) * 2
                self.indicator[iso + '_NON_CMD_Export_USD'] = df_good
        return self.indicator

    def create_report_page(self):
        '''
        :param all_dict: usually use the dictionary tha has the result in the form of (iso,df_series) format
        :return: the target is to create academic style
        '''
        # this is to create the report in pdf, from the dictionary generated
        # (which should include all times series, with the ISO country name as the key
        # and all time-series as the item)

        chart_start_dt = '2005-01-01'
        chart_end_dt = datetime.now().strftime('%Y-%m-%d')
        # TODO: check first if the PDF file is open, kill pdf first
        report = PdfPages(
            os.path.join(self.RPTDIR, self.Short_Name + datetime.now().strftime('%Y%m%d') + '.pdf'))

        # create the front page
        fig, ax = plt.subplots(1, 1, figsize=(18.27, 12.69))
        # plt.subplots_adjust(left=0, bottom=0, right=0, top=0, wspace=0, hspace=0)
        last_update = datetime.strftime(datetime.now(), format='%Y-%m-%d')
        txt = [['    Trade Weighted Growth :'], ['    - Trade Weighted Growth'],
               ['    - Non Commodities Export %GDP (6m ann)'], ['    - Non Commodities Export USD (6m ann)'], [''],
               ['Last update : ' + last_update], [''], [''], [''], [''], [''], [''], [''], [''], ['']]
        collabel = (['Global Growth : '])
        # ax.axis('tight')
        ax.axis('off')
        table = ax.table(cellText=txt, colLabels=collabel, loc='center')
        for key, cell in table.get_celld().items():
            cell.set_linewidth(0)
        cells = [key for key in table._cells]
        print(cells)
        for cell in cells:
            table._cells[cell]._loc = 'left'
            table._cells[cell].set_text_props(fontproperties=FontProperties(family='serif', size=20))
            table._cells[cell].set_height(.05)
        table._cells[(0, 0)].set_text_props(fontproperties=FontProperties(weight='bold', family='serif', size=30))

        # table.scale(1, 4)
        report.savefig(fig, bbox_inches='tight', dpi=100)

        chart_each_page = 12
        chart_rows = 4
        chart_cols = 3

        iso_list = ['ARG', 'AUS', 'BRA', 'BUL', 'CAN', 'CHE', 'CHL', 'CHN', 'COL', 'CRO', 'CZE', 'EUR', 'GBR', 'HKG',
                    'HUN', 'IDN', 'IND', 'ISE', 'JPN', 'KOR', 'MAL', 'MEX', 'NOR', 'NZD', 'PER', 'PHP', 'POL', 'ROM',
                    'RUS', 'SAF', 'SGP', 'SWE', 'TAW', 'THA', 'TUR', 'USA']
        iso_list = sorted([iso + '_' + str(i) for i in range(2) for iso in iso_list])

        pages_number = math.ceil(len(iso_list) / chart_each_page)
        chart_in_page = [chart_each_page] * (pages_number - 1) + [len(iso_list) - chart_each_page * (pages_number - 1)]
        print('chart_in_each_page=', chart_in_page)

        print("CREATING RETURNS PAGE")

        # print (sorted_list)
        # split iso codes into each page!
        for i, n in enumerate(chart_in_page):
            fig, axarr = plt.subplots(chart_rows, chart_cols, figsize=(18.27, 12.69), dpi=100)
            start_idx = i * chart_each_page
            end_idx = start_idx + n
            df_in_this_page = iso_list[start_idx:end_idx]
            # print (df_in_this_page)

            for i in range(chart_rows):
                for j in range(chart_cols):
                    if i * chart_cols + j < len(df_in_this_page):
                        ax = axarr[i, j]
                        id = df_in_this_page[i * chart_cols + j]
                        # print (i,j,iso_df)
                        print(i, j, id)
                        # extract the iso code and des for this country
                        current_iso = id.split('_')[0]
                        number = int(id.split('_')[1])

                        if number == 0:  # chart 0: trade partners weighted growth vs. export % gdp
                            print('charting...', current_iso, number)
                            df1 = pd.read_csv(
                                self.INDICATOR_EXP_DIR + '/' + current_iso + '_trade_weighted_gdp' + '.csv',
                                index_col=0, header=0)
                            df2 = pd.read_csv(
                                self.INDICATOR_EXP_DIR + '/' + current_iso + '_NON_CMD_Export_GDP' + '.csv',
                                index_col=0, header=0)
                            if len(df1.index) < 0.001:
                                self.set_ax_invisible(axarr[i, j])
                                continue
                            if len(df2.index) < 0.0001:
                                df2 = df1.copy()
                                df2[current_iso + '_NON_CMD_Export_GDP'] = np.nan
                            mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                            df1 = df1.loc[mask, :]
                            mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                            df2 = df2.loc[mask, :]

                            # print (df)
                            x1 = pd.to_datetime(df1.index).date
                            x2 = pd.to_datetime(df2.index).date
                            # print (type(x[0]))
                            y1 = df1.loc[:, current_iso + '_trade_weighted_gdp']  # GDP data, blue
                            y2 = df2.loc[:, current_iso + '_NON_CMD_Export_GDP']

                            line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label='Trade_Wighted_GDP')
                            # plot export
                            ax2 = ax.twinx()
                            if len(y2.dropna().values) < 0.001:
                                line2 = ax2.plot(x2, y2, color='grey', ls='dashed', lw=0.9,
                                                 label='_nolabel_')
                            else:
                                line2 = ax2.plot(x2, y2, color='grey', ls='dashed', lw=0.9,
                                                 label='Non_CMD_Export')

                            lns = line1 + line2
                            labs = [l.get_label() for l in lns]
                            # ax.legend(fontsize=8, loc=1, frameon=False)
                            # ax2.legend(fontsize=8, loc=9,frameon=False)
                            ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)
                            # set the range for FX data in y axis
                            fx_max = y1.max()
                            fx_min = y1.min()
                            y_max = fx_max + (fx_max - fx_min) * 0.1
                            y_min = fx_min - (fx_max - fx_min) * 0.1
                            ax.set_ylim(y_min, y_max)

                            ax.set_xlabel('')
                            ax.set_ylabel('')
                            ax2.set_xlabel('')
                            ax2.set_ylabel('')

                            last_date = df1.loc[:, [current_iso + '_trade_weighted_gdp']].dropna().index[-1]
                            try:
                                last_date = last_date.strftime('%Y-%m-%d')
                            except:
                                last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')
                            list_gdp = df1.loc[:, current_iso + '_trade_weighted_gdp']
                            last_value = list_gdp[~np.isnan(list_gdp)][-1]

                            title = current_iso + ' Trade Weighted GDP : 12m Non CMD Export chg : ' + last_date + ' : ' + "{0:.1f}".format(
                                last_value)

                            ax.set_title(title, y=1, fontsize=8, fontweight=600)
                            # add legend
                            # legend_name = df.columns.tolist()[-2]
                            # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                            # set ticks label size and width
                            ax.tick_params(labelsize=5, width=0.01)
                            ax2.tick_params(labelsize=5, width=0.01)
                            # change the y tick label to blue
                            ax.tick_params(axis='y', labelcolor='b')
                            ax2.tick_params(axis='y', labelcolor='k')
                            # add a zero line
                            ax2.axhline(linewidth=0.5, color='k')

                            # set border color and width
                            for spine in ax.spines.values():
                                spine.set_edgecolor('grey')
                                spine.set_linewidth(0.5)
                            for spine in ax2.spines.values():
                                spine.set_edgecolor('grey')
                                spine.set_linewidth(0.5)

                            # add year tickers as minor tick
                            years = mdates.YearLocator()
                            yearsFmt = mdates.DateFormatter('%Y')
                            ax.xaxis.set_major_formatter(yearsFmt)
                            ax.xaxis.set_minor_locator(years)
                            # set the width of minor tick
                            ax.tick_params(which='minor', width=0.008)
                            # set y-label to the right hand side
                            ax.yaxis.tick_right()
                            ax2.yaxis.tick_left()

                            # set date max
                            datemax = np.datetime64(x1[-1], 'Y')
                            datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                            x_tick_overrive = [datemin, datemax]
                            date_cursor = datemin
                            while date_cursor + np.timedelta64(5, 'Y') < datemax:
                                date_cursor = date_cursor + np.timedelta64(5, 'Y')
                                x_tick_overrive.append(date_cursor)

                            ax.xaxis.set_ticks(x_tick_overrive)
                            if x1[-1].month > 10:
                                ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                            else:
                                ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        elif number == 1:  # chart 0: trade partners weighted growth vs. export in USD
                            print('charting...', current_iso, number)
                            df1 = pd.read_csv(
                                self.INDICATOR_EXP_DIR + '/' + current_iso + '_trade_weighted_gdp' + '.csv',
                                index_col=0, header=0)
                            df2 = pd.read_csv(
                                self.INDICATOR_EXP_DIR + '/' + current_iso + '_NON_CMD_Export_USD' + '.csv',
                                index_col=0, header=0)
                            if len(df1.index) < 0.001:
                                self.set_ax_invisible(axarr[i, j])
                                continue
                            if len(df2.index) < 0.0001:
                                df2 = df1.copy()
                                df2[current_iso + '_NON_CMD_Export_USD'] = np.nan
                            mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                            df1 = df1.loc[mask, :]
                            mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                            df2 = df2.loc[mask, :]

                            # print (df)
                            x1 = pd.to_datetime(df1.index).date
                            x2 = pd.to_datetime(df2.index).date
                            # print (type(x[0]))
                            y1 = df1.loc[:, current_iso + '_trade_weighted_gdp']  # GDP data, blue
                            y2 = df2.loc[:, current_iso + '_NON_CMD_Export_USD']

                            line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label='Trade_Wighted_GDP')
                            # plot export
                            ax2 = ax.twinx()
                            if len(y2.dropna().values) < 0.001:
                                line2 = ax2.plot(x2, y2, color='red', ls='dashed', lw=0.9,
                                                 label='_nolabel_')
                            else:
                                line2 = ax2.plot(x2, y2, color='red', ls='dashed', lw=0.9,
                                                 label='Non_CMD_Export_USD')

                            lns = line1 + line2
                            labs = [l.get_label() for l in lns]
                            # ax.legend(fontsize=8, loc=1, frameon=False)
                            # ax2.legend(fontsize=8, loc=9,frameon=False)
                            ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)
                            # set the range for FX data in y axis
                            fx_max = y1.max()
                            fx_min = y1.min()
                            y_max = fx_max + (fx_max - fx_min) * 0.1
                            y_min = fx_min - (fx_max - fx_min) * 0.1
                            ax.set_ylim(y_min, y_max)

                            ax.set_xlabel('')
                            ax.set_ylabel('')
                            ax2.set_xlabel('')
                            ax2.set_ylabel('')

                            last_date = df1.loc[:, [current_iso + '_trade_weighted_gdp']].dropna().index[-1]
                            try:
                                last_date = last_date.strftime('%Y-%m-%d')
                            except:
                                last_date = pd.to_datetime(last_date).strftime('%Y-%m-%d')
                            list_gdp = df1.loc[:, current_iso + '_trade_weighted_gdp']
                            last_value = list_gdp[~np.isnan(list_gdp)][-1]

                            title = current_iso + ' Trade Weighted GDP : 12m Non CMD Export USD chg : ' + last_date + ' : ' + "{0:.1f}".format(
                                last_value)

                            ax.set_title(title, y=1, fontsize=8, fontweight=600)
                            # add legend
                            # legend_name = df.columns.tolist()[-2]
                            # ax.legend(['CAI','FCI_9(zto)'], fontsize = 7,loc=9,frameon=False)
                            # set ticks label size and width
                            ax.tick_params(labelsize=5, width=0.01)
                            ax2.tick_params(labelsize=5, width=0.01)
                            # change the y tick label to blue
                            ax.tick_params(axis='y', labelcolor='b')
                            ax2.tick_params(axis='y', labelcolor='r')
                            # add a zero line
                            ax2.axhline(linewidth=0.5, color='k')

                            # set border color and width
                            for spine in ax.spines.values():
                                spine.set_edgecolor('grey')
                                spine.set_linewidth(0.5)
                            for spine in ax2.spines.values():
                                spine.set_edgecolor('grey')
                                spine.set_linewidth(0.5)

                            # add year tickers as minor tick
                            years = mdates.YearLocator()
                            yearsFmt = mdates.DateFormatter('%Y')
                            ax.xaxis.set_major_formatter(yearsFmt)
                            ax.xaxis.set_minor_locator(years)
                            # set the width of minor tick
                            ax.tick_params(which='minor', width=0.008)
                            # set y-label to the right hand side
                            ax.yaxis.tick_right()
                            ax2.yaxis.tick_left()

                            # set date max
                            datemax = np.datetime64(x1[-1], 'Y')
                            datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                            x_tick_overrive = [datemin, datemax]
                            date_cursor = datemin
                            while date_cursor + np.timedelta64(5, 'Y') < datemax:
                                date_cursor = date_cursor + np.timedelta64(5, 'Y')
                                x_tick_overrive.append(date_cursor)

                            ax.xaxis.set_ticks(x_tick_overrive)
                            if x1[-1].month > 10:
                                ax.set_xlim(datemin, datemax + np.timedelta64(15, 'M'))
                            else:
                                ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        else:
                            print('The chart has not been plotted', current_iso, number)

                    else:
                        self.set_ax_invisible(axarr[i, j])

            plt.tight_layout()
            report.savefig(fig, bbox_inches='tight')  # the current page is saved
        report.close()
        plt.close('all')

    def set_ax_invisible(self, ax):
        ax.axis('off')


def run():
    GP = signal3()
    reporting_to = sys.argv[1] if len(sys.argv) > 1.01 else None
    data = GP.get_data_dict(run_chart=True, reporting_to=reporting_to)


if __name__ == "__main__":
    clear_cache('RATES_LVL_TW_GROW', region='disk_8h')
    run()
    print('Done!')

