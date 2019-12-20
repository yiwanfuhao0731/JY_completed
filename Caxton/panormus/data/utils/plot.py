import math

from matplotlib.ticker import ScalarFormatter

import numpy as np

import matplotlib

import matplotlib.pyplot as plt

from matplotlib.colors import ListedColormap

import pandas as pd

import seaborn as sns


def basic_plot(

        data_frame,

        title=None,

        show_grid=False,

        show_legend=True,

        export_path=None,

        ylim=None,

        figsize=(12, 8),

        kind='line', overlay_bars=False, colors_override=None

):
    """

    :description: basic plot

    :param pandas.DataFrame data_frame:

    :param str title: DEPRECATED. Title display does not work properly and needs rework.

    :param bool show_grid: boolean to determine whether to show grid

    :param bool show_legend: boolean to determine whether to show legend

    :param str export_path: export path the user would like to use the plot to

    :param tuple[float, float] ylim: tuple (bottom, top) position

    :param tuple figsize: size of the figure

    :param str kind: plot kind, passed to dataframe plot method. line, bar, etc.

    :return: fig: matplotlib.figure.Figure object; ax: Axes object or array of Axes objects.

    """

    # TODO: This is a poorly written function. Arguments are too specific and all plot kwargs should be passed.

    df = data_frame.copy()

    if str(df.index.dtype).startswith('period'):
        df.index = df.index.to_timestamp()

    colors = ['blue', 'red', 'green', 'gray', 'black']

    cmap = ListedColormap(colors if colors_override is None else colors_override)

    fig, ax = plt.subplots(figsize=figsize)

    if overlay_bars:

        df.iloc[:, 0].plot(kind=kind, ax=ax, color='y', alpha=0.6)

        df_lines = df.iloc[:, 1:]

        for i in range(df_lines.shape[1]):
            ax.plot(ax.get_xticks(), df_lines.iloc[:, i], label=df_lines.columns[i])

    else:

        df.plot(kind=kind, ax=ax, cmap=cmap)

    [label.set_visible(False) for label in ax.get_xticklabels()[::2]];

    if show_grid:
        ax.set_axisbelow(True)

        ax.yaxis.grid(color='gray', linestyle='dashed')

        ax.xaxis.grid(color='gray', linestyle='dotted')

    if title:
        # fig.suptitle(title, size=30)

        pass

    if show_legend:
        ax.legend(loc="upper left", fontsize=12)

    if export_path:
        fig.savefig(export_path)

    if ylim:
        ax.set_ylim(ylim)

    return fig, ax


def basic_line_plot_double_y_axis(

        left_df,

        right_df,

        title=None,

        show_grid_left=False,

        show_grid_right=False,

        x_axis_label=None,

        left_axis_label=None,

        right_axis_label=None,

        invert_left_axis=False,

        invert_right_axis=False,

        ylim1=None,

        ylim2=None,

        xlim1=None,

        xlim2=None,

        figsize=(12, 8),

        linewidth=1.0,

        title_y_loc=1.08, override_left_colors=None, override_right_colors=None,

        left_line_right_bar=False

):
    """

    :description: line plot with primary y axis and secondary y axis

    :param pandas.DataFrame left_df: data frame corresponding to the left y axis

    :param pandas.DataFrame right_df: data frame corresponding to the right y axis

    :param str title: DEPRECATED. Title display does not work properly and needs rework.

    :param bool show_grid_left: boolean to determine whether to show left grid

    :param bool show_grid_right: boolean to determine whether to show right grid

    :param str x_axis_label: x-axis label

    :param str left_axis_label: left y-axis label

    :param str right_axis_label: right y-axis label

    :param bool invert_right_axis: whether to invert right axis

    :param bool invert_left_axis: whether to invert left axis

    :param tuple figsize: figure size

    :param float title_y_loc: the location for the y axis title

    :return: fig: matplotlib.figure.Figure object; ax1, ax2: Axes object or array of Axes objects.

    """

    # TODO: This is a poorly written function. Arguments are too specific and all plot kwargs should be passed.

    red_label = '#d64848'

    blue_label = '#4c48d6'

    colors_right = ['blue', 'red', 'green', 'gray']

    reds_cmap = ListedColormap(colors_right if override_right_colors is None else override_right_colors)

    colors_left = list(reversed(colors_right))

    blues_cmap = ListedColormap(colors_left if override_left_colors is None else override_left_colors)

    fig, ax1 = plt.subplots(figsize=figsize)

    ax2 = ax1.twinx()

    # Right chart as a bar chart and left chart as a line chart

    if left_line_right_bar:

        df = pd.concat([right_df, left_df], axis=1, sort=True)

        df.iloc[:, 0] = df.iloc[:, 0].fillna(0)  # Replace NA by 0 for bar chart only

        df.iloc[:, 0].plot(cmap=blues_cmap, ax=ax2, kind='bar')

        ax1.plot(ax1.get_xticks(), df.iloc[:, 1])

        [label.set_visible(False) for label in ax1.get_xticklabels()[::2]];

        # Rotate x axis label

        for ax in fig.axes:
            matplotlib.pyplot.sca(ax)

            plt.xticks(rotation=40)



    else:

        left_df.plot(cmap=blues_cmap, ax=ax1, linewidth=linewidth)

        right_df.plot(cmap=reds_cmap, ax=ax2, linewidth=linewidth)

    ax1.tick_params('x', labelsize=11)

    ax1.tick_params('y', labelsize=11)

    ax1.legend(loc="upper left", fontsize=12)

    ax2.legend(loc="upper right", fontsize=12)

    ax2.tick_params('y', labelsize=11)

    if show_grid_left:
        ax1.grid()

    if show_grid_right:
        ax2.grid()

    if x_axis_label:
        ax1.set_xlabel(x_axis_label, size=16)

    if left_axis_label:
        ax1.set_ylabel(left_axis_label, size=16)

    if right_axis_label:
        ax2.set_ylabel(right_axis_label, size=16)

    if invert_left_axis:
        ax1.invert_yaxis()

    if invert_right_axis:
        ax2.invert_yaxis()

    if ylim1:
        ax1.set_ylim(ylim1)

    if ylim2:
        ax2.set_ylim(ylim2)

    if xlim1:
        ax1.set_xlim(xlim1)

    if xlim2:
        ax2.set_xlim(xlim2)

    if title:
        # fig.suptitle(title, size=30, y=title_y_loc)

        pass

    return fig, ax1, ax2


def logify_yaxis(ax, max_tick_power=4):
    """

    :description: convert y axis to log scale. Estimates a log base b such that \

     b**max_tick_powers and b**-max_tick_powers is the new plot bounds.

    :param ax: axis object

    :param max_tick_power: Maximum power of log base.

    :return: None. Axis is modified in place.

    """

    ybounds = ax.get_ylim()

    if min(ybounds) <= 0:
        # Invalid bounds, attempt tighter bounds.

        ax.autoscale(axis='y', enable=True, tight=True)

        ybounds = ax.get_ylim()

    log_y_absmax = max(abs(np.log(ybounds)))

    max_y = np.exp(log_y_absmax)

    # Defaults to 2 digit accuracy on the scale ratio

    default_ratio_precision = 3

    precision_factor = 10 ** default_ratio_precision

    y_scale_ratio = int(max_y ** (1. / (max_tick_power)) * precision_factor + 1) / float(precision_factor)

    if y_scale_ratio > 5:

        y_scale_ratio = math.ceil(y_scale_ratio)

    elif y_scale_ratio > 1.25:

        y_scale_ratio = math.ceil(y_scale_ratio * 10) / 10.

    y_tick_powers = np.arange(-max_tick_power, 1 + max_tick_power)

    y_ticks = y_scale_ratio ** y_tick_powers

    y_tick_labels = ['1' if y == 1 else '%0.0f' % (y_scale_ratio * 100) + '%^' + str(p)

                     for y, p in zip(y_ticks, y_tick_powers)]

    ax.set_yscale('log')

    ax.set_yticks(y_ticks)

    ax.set_yticklabels(y_tick_labels)

    ax.set_ylim(y_ticks[0], y_ticks[-1])

    ax_right = ax.twinx()


ax_right.set_yscale('log')

ax_right.grid('off')

ax_right.set_yticks(ax.get_yticks())

ax_right.set_yticklabels(y_ticks)

ax_right.set_ylim(ax.get_ylim())

ax_right.get_yaxis().set_major_formatter(ScalarFormatter())


def plot_dist_detail(

        ser,

        ax=None,

        distplot_color='blue',

        mean_vert_color='red',

        pct_span_color='yellow',

        draw_zero_vert=True, draw_kde=True,

        pct_low=0.10, pct_high=0.90,

        pct_dist_trim=0.005,

        center_x_at_zero=True,

        legend_decimal_precision=4

):
    '''

    :param ser: series to plot

    :param ax: optional axis to plot onto

    :param str distplot_color: (xkcd color) for histogram and kde line

    :param str|None mean_vert_color: (xkcd color) of mean vertical line

    :param str|None pct_span_color: (xkcd color) applied to background from low percentile to high percentile

    :param bool draw_zero_vert: draw a vertical line at zero?

    :param bool draw_kde: draw the kde line on top of the histogram?

    :param float pct_low: between 0 and 1, percentile to start coloring background, if pct_span_color is not None

    :param float pct_high: between 0 and 1, percentile to end coloring background, if pct_span_color is not None

    :param float|None pct_dist_trim: ratio of data to not show in dist plot to avoid wide x axis due to tails

    :param bool center_x_at_zero: adjust x limits to center axis at zero

    :param legend_decimal_precision: number of decimals in the legend

    :return: tuple of figure and axis. If axis is given as an inputs, you can ignore these outputs.

    '''

    fig = None

    if ax is None:
        fig, ax = plt.subplots(1, 1)

    decimal_fmt = '%0.' + str(int(legend_decimal_precision)) + 'f'

    decimal_line = decimal_fmt + '\n'

    label_fmt = ''.join((

        'mean: ', decimal_line,

        '%0.0fpct: ', decimal_line,

        '%0.0fpct: ', decimal_line,

        'P(pos): %0.0f\n',

        'E(pos): ', decimal_line,

        'P(neg): %0.0f\n',

        'E(neg): ', decimal_line,

    ))

    quantile_low = ser.quantile(pct_low)

    quantile_high = ser.quantile(pct_high)

    if pct_dist_trim:

        trim_low = ser.quantile(pct_dist_trim)

        trim_high = ser.quantile(1 - pct_dist_trim)

        trim_idx = (ser > trim_low) & (ser < trim_high)

        plot_ser = ser[trim_idx.values]

    else:

        plot_ser = ser

    sns.distplot(

        plot_ser, ax=ax,

        color=sns.xkcd_rgb[distplot_color], kde=draw_kde

    );

    if draw_zero_vert:
        ax.axvline(0, c='0.2');

    if mean_vert_color:
        ax.axvline(ser.mean(), c=sns.xkcd_rgb[mean_vert_color]);

    if pct_span_color:
        ax.axvspan(

            xmin=quantile_low, xmax=quantile_high,

            alpha=0.4, color=pct_span_color, zorder=0.9

        );

    if center_x_at_zero:
        max_lim = max(abs(plot_ser.min()), abs(plot_ser.max()))

        ax.set_xlim([-max_lim, max_lim])

    ax.text(

        0.02, 0.98,

        s=(label_fmt % (

            ser.mean(),

            pct_low * 100, quantile_low,

            pct_high * 100, quantile_high,

            (ser > 0).mean() * 100,

            ser[ser > 0].mean(),

            (ser < 0).mean() * 100,

            ser[ser < 0].mean(),

        )).replace('percent', r'%'),

        transform=ax.transAxes, ha='left', va='top'

    );

    if fig:
        fig.tight_layout()

    return (fig, ax)


def plot_cdf_on_axis(

        ax, ser,

        highlight_point=None,

        highlight_text='point: %0.2f\npercentile: %0.2f',

        zero_line=True

):
    ax.plot(

        ser.sort_values(),

        ser.sort_values().rank(pct=True)

    );

    ax.set_ylabel('Percentile');

    if zero_line:
        ax.axvline(0, c='0.2', alpha=0.8);

    if highlight_point is not None:

        prctile = (ser < highlight_point).mean()

        ax.axvline(highlight_point, c='y');

        ax.axhline(prctile, c='y');

        if highlight_text:
            ax.text(

                0.02, 0.98,

                s=highlight_text % (highlight_point, prctile),

                transform=ax.transAxes, ha='left', va='top');


def double_y_plot(

        x, y1, y2, x_label=None, y1_label=None, y2_label=None,

        y1_color=None, y2_color=None, grid_alpha=0.4,

        y1_linestyle='-', y2_linestyle='--', ax=None,

        y1_kwargs=None, y2_kwargs=None

):
    """

    Plots x values against y1 (left y) and y2 (right y) with twinned x values.\

    All arguments are passed through to the .plot() method of ax.



    :param x: list, series, etc.

    :param y1: list, series, etc.

    :param y2: list, series, etc.

    :param str x_label: Label for x axis

    :param str y1_label: Label for y1 axis

    :param str y2_label: Label for y2 axis

    :param str y1_color: hex color. Defaults to color cycle position zero.

    :param str y2_color: hex color. Default to color cycle position 1.

    :param grid_alpha: opacity of grid lines (0 to 1)

    :param y1_linestyle: a pyplot linestyle string

    :param y2_linestyle: a pyplot linestyle string

    :param ax: Optional axis to draw plots onto. One is made from plt.subplot() by default.

    :param y1_kwargs: dictionary of args for y1 plot command

    :param y2_kwargs: dictionary of args for y2 plot command

    :return: left axis, right axis

    """

    color_cycle = plt.rcParams["axes.prop_cycle"].by_key()["color"]

    y1_color = y1_color or color_cycle[0]

    y2_color = y2_color or color_cycle[1]

    if ax is None:
        ax = plt.subplot()

    ax.plot(x, y1, color=y1_color, linestyle=y1_linestyle, **(y1_kwargs or {}))

    if grid_alpha:
        ax.grid(color=y1_color, alpha=grid_alpha)

    if x_label:
        ax.set_xlabel(x_label)

    if y1_label:
        ax.set_ylabel(y1_label, color=y1_color)

    ax2 = ax.twinx()

    ax2.plot(x, y2, color=y2_color, linestyle=y2_linestyle, **(y2_kwargs or {}))

    if grid_alpha:
        ax2.grid(color=y2_color, alpha=grid_alpha)

    if y2_label:
        ax2.set_ylabel(y2_label, color=y2_color)

    return ax, ax2