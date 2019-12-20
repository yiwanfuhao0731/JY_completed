"""Pairwise correlation report"""

import pandas as pd

import numpy as np

import utils_pvk.lib_plot_fns as plot_fns

from utils_pvk.cl_email import Email

import utils_pvk.lib_html_fns as html_fns


class PwCorrReport:

    def __init__(self, perf_1, perf_2, name_1_short=None, name_2_short=None, **kwargs):

        """perf_1 and perf_2 should be series of arithmetic cumulative returns and appropriately named"""

        # align the two series

        sd = max(perf_1.index[0], perf_2.index[0])

        ed = min(perf_1.index[-1], perf_2.index[-1])

        p_1, p_2 = perf_1[sd:ed].align(perf_2[sd:ed], join='outer')

        self.perf_1 = p_1.ffill() - p_1[0]

        self.perf_2 = p_2.ffill() - p_2[0]

        if name_1_short is None:

            self.name_1_short = perf_1.name

        else:

            self.name_1_short = name_1_short

        if name_2_short is None:

            self.name_2_short = perf_2.name

        else:

            self.name_2_short = name_2_short

        p = dict()

        p['rets_ndays'] = kwargs.pop('rets_ndays', 3)

        p['use_exponential_corr'] = kwargs.pop('use_exponential_corr', False)

        p['spanwindow'] = kwargs.pop('spanwindow', 252)

        self.p = p

        self.report = None


def get_rolling_corr(self, rets_ndays=None, use_exponential_corr=None, spanwindow=None):
    # parse input

    if rets_ndays is None:
        rets_ndays = self.p['rets_ndays']

    if use_exponential_corr is None:
        use_exponential_corr = self.p['use_exponential_corr']

    if spanwindow is None:
        spanwindow = self.p['spanwindow']

    # compute rolling corr

    rets = self.get_rets(rets_ndays)

    if use_exponential_corr:

        rolling_corr = rets.iloc[:, 0].ewma(span=spanwindow).corr(rets.iloc[:, 1])

    else:

        rolling_corr = rets.iloc[:, 0].rolling(window=spanwindow).corr(rets.iloc[:, 1])

    return rolling_corr


def get_sample_corr(self, rets_ndays=None, start_date=None, end_date=None):
    # parse input

    if rets_ndays is None:
        rets_ndays = self.p['rets_ndays']

    if start_date is None:
        start_date = self.perf_1.index[0]

    if end_date is None:
        end_date = self.perf_1.index[-1]

    # compute rolling corr

    corr = pd.concat([self.perf_1, self.perf_2], axis=1).diff(rets_ndays)[start_date:end_date].corr().iloc[0, 1]

    return corr


def get_rets(self, rets_ndays=None):
    """DF of returns of both columns"""

    if rets_ndays is None:
        rets_ndays = self.p['rets_ndays']

    return pd.concat([self.perf_1, self.perf_2], axis=1).ffill().diff(rets_ndays)


def get_rolling_corr_plot(self, title=None, figsize=None):
    """get rolling return"""

    if title is None:
        rets_ndays = self.p['rets_ndays']

        title = f'Rolling Correlation {self.perf_1.name} vs. {self.perf_2.name} ({rets_ndays}b overlapping returns)'

    rolling_corr = self.get_rolling_corr()

    img_path = plot_fns.plt_linelabels(rolling_corr.to_frame(), title=title, plot_close_after_use=True,

                                       use_overlabels=False, use_grid=True, rhs_axis=True, figsize=figsize,

                                       edge=dict(l=0.06, r=0.06, b=0.13, t=0.08), hline=0)

    return img_path


def get_cumulative_return_plot(self, title=None, figsize=None):
    """get rolling return"""

    if title is None:
        title = f'Cumulative Returns {self.perf_1.name} vs. {self.perf_2.name}'

    plot_data = pd.concat([self.perf_1, self.perf_2], axis=1)

    img_path = plot_fns.plt_linelabels(plot_data, title=title, plot_close_after_use=True,

                                       use_overlabels=False, use_grid=True, rhs_axis=True,

                                       legend=list(plot_data.columns), figsize=figsize,

                                       edge=dict(l=0.06, r=0.06, b=0.13, t=0.08), hline=0)

    return img_path


def get_rolling_return_plot(self, title=None, lookback_ndays=252, figsize=None):
    """get rolling return"""

    if title is None:
        title = f'{lookback_ndays}b Rolling Returns {self.perf_1.name} vs. {self.perf_2.name}'

    plot_data = pd.concat([self.perf_1, self.perf_2], axis=1).diff(lookback_ndays)

    img_path = plot_fns.plt_linelabels(plot_data, title=title, plot_close_after_use=True,

                                       use_overlabels=False, use_grid=True, rhs_axis=True,

                                       legend=list(plot_data.columns), figsize=figsize,

                                       edge=dict(l=0.06, r=0.06, b=0.13, t=0.08), hline=0)

    return img_path


def get_3m_rolling_return_quartile_table(self, n_quantiles=10, name_primary='Strategy', name_secondary='CI'):
    rolling_3m_return = self.get_rets(63).dropna(axis=0)

    rolling_3m_corr = self.get_rolling_corr(3, spanwindow=63).dropna()

    rolling_3m_corr.name = 'corr'

    all = pd.concat([rolling_3m_return, rolling_3m_corr], axis=1).dropna(axis=0, how='any')

    quantile_rng = [i / n_quantiles for i in range(n_quantiles)] + [1]

    quantiles = all[self.perf_2.name].quantile(quantile_rng)

    quantiles.iloc[-1] += 1

    table_out = pd.DataFrame(index=np.array(range(n_quantiles)),

                             columns=range(5))

    for i in range(n_quantiles):
        # i = 0

        q_lo = quantiles.iloc[i]

        q_hi = quantiles.iloc[i + 1]

        idx = (q_lo <= all.iloc[:, 1].values) & (all.iloc[:, 1].values < q_hi)

        table_out.iloc[i, 0] = q_lo

        table_out.iloc[i, 1] = q_hi

        table_out.iloc[i, 2] = all.iloc[idx, 1].mean()

        table_out.iloc[i, 3] = all.iloc[idx, 0].mean()

        table_out.iloc[i, 4] = all.iloc[idx, 2].mean()

    colnames = [name_secondary + '<br> From', name_secondary + '<br> To',

                name_secondary + '<br> Avg', name_primary + '<br> Avg',

                'Corr<br> Avg']

    table_out.columns = colnames

    table_out.index = [f'{round(q / n_quantiles, 3)}-{round((q + 1) / n_quantiles, 3)} quantile' for q in
                       range(n_quantiles)]

    return table_out


def send_email_report(self, to, report_type='default', title=None, cc='', bcc='', extra_corr_periods=None,

                      n_quantiles=4):
    if title is None:
        title = f'Correlation report {self.perf_1.name} vs. {self.perf_2.name}'

    if report_type == 'default':

        p = dict(rolling_plot=True, cumret_plot=True, rolret_plot=True, quantiles=True)

    elif report_type == 'ex_quantiles':

        p = dict(rolling_plot=True, cumret_plot=True, rolret_plot=True, quantiles=False)

    elif report_type == 'only_quantiles':

        p = dict(rolling_plot=False, cumret_plot=False, rolret_plot=False, quantiles=True)

    else:

        raise ValueError(f'report_type {report_type} not supported.')

    email = Email(to, title, cc=cc, bcc=bcc)

    html = email.initial_html()

    html += f'Correlation report between {self.perf_1.name} and {self.perf_2.name}<br><br>'

    corr_full_sample = self.get_sample_corr(3)

    html += f'Full Sample correlation: <b>{corr_full_sample:.2f}</b><br>'

    if extra_corr_periods is not None:

        for nm, rng in extra_corr_periods.items():
            i_corr = self.get_sample_corr(start_date=rng[0], end_date=rng[1])

            html += f'{nm} correlation: <b>{i_corr:.2f}</b><br>'

    html += '<br>'

    html += 'Please see correlation  below:<br>'

    if p['rolling_plot']:
        html += '- correlation per quantile table<br>'

    if p['rolling_plot']:
        html += f'- rolling returns chart<br>'

    if p['cumret_plot']:
        html += '- cumulative returns chart<br>'

    if p['rolling_plot']:
        html += '- rolling 1y returns chart<br>'

    html += '<br><br>'

    if p['quantiles']:

        table_df = self.get_3m_rolling_return_quartile_table(n_quantiles, self.name_1_short, self.name_2_short)

        if n_quantiles == 4:

            html += '<b><u>Correlation per return Quartile:</u></b><br>'

            table_df.index = [f'Bottom {self.name_2_short} 3m Return Quartile', '2nd', '3rd',

                              f'Top {self.name_2_short} 3m Return Quartile']

            quantile_name = 'quartile'

        elif n_quantiles == 10:

            html += '<b><u>Correlation per return Decile:</u></b><br>'

            table_df.index = [f'Bottom {self.name_2_short} 3m Return Decile', '2nd', '3rd', '4th', '5th', '6th',

                              '7th', '8th', '9th', f'Top {self.name_2_short} 3m Return Decile']

            quantile_name = 'decile'

        else:

            html += '<b><u>Correlation per return Quantile:</u></b><br>'

            quantile_name = 'quantile'

        table_df = table_df.iloc[:, 2:]

        html += html_fns.create_html_table(table_df.applymap(lambda x: f'{x:.2f}'), cell_width=80)

        html += (f'<i>{self.name_2_short} 3m rolling performance is split into 4 {quantile_name}s. The table ' +

                 f'shows the averages of the 3m rolling returns and 3m rolling correlations conditional on CI ' +

                 f'3m rolling return being in the respective {quantile_name}.</i>')

        html += '<br><br>'

    if p['rolling_plot']:
        html += '<b><u>Rolling 1y returns chart:</u></b><br>'

        rolling_title = f'1y Rolling Correlation of {self.perf_1.name} vs {self.perf_2.name}'

        img_path = self.get_rolling_corr_plot(title=rolling_title, figsize=[10, 3.5])

        html = email.add_inline_image(html, img_path)

        html += '<br>'

    if p['cumret_plot']:
        html += '<b><u>Cumulative returns chart:</u></b><br>'

        cumret_title = f'Cumulative Returns of {self.perf_1.name} vs {self.perf_2.name}'

        img_path = self.get_cumulative_return_plot(title=cumret_title, figsize=[10, 5])

        html = email.add_inline_image(html, img_path)

        html += '<br>'

    if p['cumret_plot']:
        html += '<b><u>1y rolling returns chart:</u></b><br>'

        rollret_title = f'Rolling 1y Returns of {self.perf_1.name} vs {self.perf_2.name}'

        img_path = self.get_rolling_return_plot(title=rollret_title, figsize=[10, 3.5])

        html = email.add_inline_image(html, img_path)

        html += '<br>'

    email.add_html_body(html)

    email.send()