import matplotlib.lines as mlines

from matplotlib.dates import date2num

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import itertools
import matplotlib.dates as mdates

from utils_pvk.lib_labellines import labelLine, labelLines

import utils_pvk.lib_report_fns as report_fns
import utils_pvk.lib_color_fns as color_fns


def plt_intraday(plot_data, title):
    return plt_linelabels(plot_data, title, intraday=True)


def plt_linelabels(plot_data, title, intraday=False, return_filepath=True, **options):
    if len(plot_data.columns) == 0:
        return None

    # logic for adding lines (using the labelLines package)
    label_zorder = options.pop('label_zorder', 100)
    if 'zorder' in options.keys():
        raise ValueError("Please rename argument zorder to label_zorder")
    align = options.pop('align', False)
    xvals = options.pop('xvals', None)
    edge = options.pop('edge', None)
    label_offset = options.pop('label_offset', 0.03)
    colors = options.pop('colors', None)
    title_color = options.pop('title_color', None)
    rhs_axis = options.pop('rhs_axis', False)
    use_grid = options.pop('use_grid', False)
    outlier_cutoffs = options.pop('outlier_cutoffs', None)
    figsize = options.pop('figsize', None)
    plot_close_after_use = options.pop('plot_close_after_use', True)
    use_house_colors = options.pop('use_house_colors', True)
    drop_all_na = options.pop('drop_all_na', False)
    legend = options.pop('legend', None)
    use_overlabels = options.pop('use_overlabels', True)
    override_path = options.pop('override_path', None)
    cross_out_plot = options.pop('cross_out_plot', False)
    ylabel = options.pop('ylabel', None)
    xlabel = options.pop('xlabel', None)
    line_zorders = options.pop('line_zorders', None)
    add_topleft_text = options.pop('add_topleft_text', '')
    fig = options.pop('fig', None)
    ax = options.pop('ax', None)
    hline = options.pop('hline', None)

    def cutaway(x, min, max):
        if x < min or x > max:
            return np.nan
        else:
            return x

    if outlier_cutoffs is not None:
        cut_ub = 5
        cut_lb, cut_ub = (outlier_cutoffs[0], outlier_cutoffs[1])
        if cut_lb > plot_data.min().min() or cut_ub < plot_data.max().max():
            plot_data = plot_data.applymap(lambda x: cutaway(x, cut_lb, cut_ub))
            if add_topleft_text == "": add_topleft_text += " "
            add_topleft_text += "Values <{} and >{} have been cut from the data.".format(cut_lb, cut_ub)

    # get the first non-nan value for each series in the dataframe
    if xvals is not None:
        if hasattr(xvals, '__iter__') and not isinstance(xvals, str):
            xvals_used = xvals
        else:
            xvals_used = []
            n = len(list(plot_data))
            for i in range(n):
                # i = 1
                if xvals.lower() == 'middle':
                    target = plot_data.index[int(len(plot_data.index) * (i + 1) / (n + 1))]
                elif xvals.lower() == 'first_obs':
                    if plot_data.iloc[:, i].dropna().empty:
                        target = plot_data.index[0]
                    else:
                        target_raw = plot_data.iloc[:, i].dropna().index[0]
                        target_min = plot_data.index[int(label_offset * len(plot_data.index))]
                        target = max(target_raw, target_min)
                elif xvals.lower() == 'last_obs':
                    if plot_data.iloc[:, i].dropna().empty:
                        target = plot_data.index[-1]
                    else:
                        target = plot_data.iloc[:, i].dropna().index[-1]
                elif xvals.lower() == 'full_left':
                    target = plot_data.index[int(label_offset * len(plot_data.index))]
                elif xvals.lower() == 'full_right':
                    target = plot_data.index[-1]
                else:
                    raise ValueError("label location spec (\'xvals\') param \'{}\' not supported".format(xvals))

                try:
                    x = plot_data.iloc[:, i].loc[:target].index[-1]
                except:
                    try:
                        x = plot_data.iloc[:, i].loc[target:].index[0]
                    except:
                        x = np.nan
                if not pd.isnull(x):
                    xvals_used.append(date2num(plot_data.iloc[:, i].loc[:target].index[-1]))
                else:
                    xvals_used.append(np.nan)  # empty columns will be pruned in the next step
    else:
        xvals_used = None

    if drop_all_na:
        plot_data_clean = plot_data.dropna(axis=1, how='all')
    else:
        plot_data_clean = plot_data

    if xvals_used is not None:
        xvals_clean = list(itertools.compress(xvals_used, [x in plot_data_clean.columns for x in plot_data.columns]))
    else:
        xvals_clean = None

    # STEP 1 - create plot
    if fig is None:
        if figsize is None:
            fig, ax = plt.subplots()
        else:
            fig, ax = plt.subplots(figsize=figsize)
    else:
        if ax is None:
            ax = fig.axes[0]

    if use_house_colors and colors is None:
        housecolors = list(color_fns.housecolor().values())
        colors = housecolors[:plot_data_clean.shape[1]]

    if hline is not None:
        ax.axhline(hline, color='#4d4d4d', linewidth=1)

    if plot_data_clean.max().max() > 100000:
        ax.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))  # scilimits=(0, 0)

    if line_zorders is None:
        line_zorders = np.array(range(len(plot_data_clean.columns), 0, -1)) * 5
    else:
        line_zorders *= 5

    if colors is None:
        colors = np.zeros(len(plot_data_clean.columns))

    for i in range(len(colors)):
        plot_data_clean.iloc[:, i].plot(ax=ax, title=title, legend=False, color=colors[i], zorder=line_zorders[i])

    # TODO: this needs to be debugged properly
    lines = plt.gca().get_lines()
    tries = 0
    while len(lines) == 0 and tries < 5:
        if figsize is None:
            figr, axr = plt.subplots()
        else:
            figr, axr = plt.subplots(figsize=figsize)

        if use_house_colors and colors is None:
            housecolors = list(color_fns.housecolor().values())
            colors = housecolors[:plot_data_clean.shape[1]]

        if plot_data_clean.max().max() > 100000:
            axr.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))  # scilimits=(0, 0)

        # if rhs_axis:
        #     axr2 = ax.twinx()
        #     # ax.tick_params(labeltop=False, labelright=True)
        #     if plot_data_clean.max().max() > 100000:
        #         axr2.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))  # scilimits=(0, 0)

        if colors is None:
            plot_data_clean.plot(ax=axr, title=title, legend=False, )
        else:
            for i in range(len(colors)):
                plot_data_clean.iloc[:, i].plot(ax=axr, title=title, legend=False, color=colors[i])
        lines = figr.gca().get_lines()
        plt.close(figr)
        tries += 1

    if rhs_axis:
        ax2 = ax.twinx()

        if plot_data_clean.max().max() > 100000:
            ax2.ticklabel_format(style='sci', axis='y', scilimits=(0, 0))  # scilimits=(0, 0)
        ax2.set_ylim(ax.get_ylim())

    if title_color is not None:
        ax.set_title(title, color=title_color)

    ax.set_xlabel("")
    ax.axes.set_xlim(left=np.min(plot_data_clean.index))

    # STEP 2 - create line labels
    if use_overlabels:
        labelLines(lines, zorder=label_zorder, align=align, xvals=xvals_clean, **options)

    if ylabel is not None:
        ax.set_ylabel(ylabel)

    if xlabel is not None:
        ax.set_xlabel(xlabel)

    if intraday:
        hours = mdates.HourLocator(interval=1)
        h_fmt = mdates.DateFormatter('%H:%M:%S')
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(h_fmt)

    if edge is not None:
        if isinstance(edge, str) and edge == 'tight':
            fig.tight_layout()
        elif isinstance(edge, dict):
            fig.subplots_adjust(left=edge['l'], bottom=edge['b'], right=1 - edge['r'], top=1 - edge['t'],
                                wspace=0, hspace=0)
        else:
            fig.subplots_adjust(left=edge, bottom=edge, right=1 - edge, top=1 - edge, wspace=0, hspace=0)

    if use_grid:
        ax.grid(color="#c9c9c9")

    if legend:
        ax.legend(legend)

    if add_topleft_text is not None:
        x_pos = plt.xlim()[0] + (plt.xlim()[1] - plt.xlim()[0]) * 0.025
        y_pos = plt.ylim()[1] - (plt.ylim()[1] - plt.ylim()[0]) * 0.035
        ax.text(x_pos, y_pos, add_topleft_text)

    if cross_out_plot:
        x_lo, x_hi = plt.xlim()
        y_lo, y_hi = plt.ylim()
        l = mlines.Line2D([x_lo, x_hi], [y_lo, y_hi], color='r', linewidth=1)
        ax.add_line(l)
        l = mlines.Line2D([x_hi, x_lo], [y_lo, y_hi], color='r', linewidth=1)
        ax.add_line(l)

        x_pos = plt.xlim()[0] + (plt.xlim()[1] - plt.xlim()[0]) * 0.03
        if add_topleft_text is not None:
            y_pos = plt.ylim()[1] - (plt.ylim()[1] - plt.ylim()[0]) * 0.07
        else:
            y_pos = plt.ylim()[1] - (plt.ylim()[1] - plt.ylim()[0]) * 0.035
        ax.text(x_pos, y_pos, "unreliable data", color='r')

    if return_filepath:
        filename = report_fns.get_temp_filename(override_path)
        fig.savefig(filename, dpi=80, transparent=True)  # , facecolor=fig.get_facecolor()
        if plot_close_after_use:
            plt.close(fig)
        return filename
    else:
        return fig


def plt_dual(plot_left, plot_right, title=None, y_left_label=None, y_right_label=None, label_axes=False,
             return_filepath=True, **options):
    blue = (23 / 255, 55 / 255, 94 / 255)
    mred = (192 / 255, 0 / 255, 0 / 255)

    # logic for adding lables (using the labelLines package)
    label_zorder = options.pop('label_zorder', 100)
    if 'zorder' in options.keys():
        raise ValueError("Please rename argument zorder to label_zorder")
    align = options.pop('align', False)
    edge = options.pop('edge', None)
    plot_close_after_use = options.pop('plot_close_after_use', True)
    ylabel_left = options.pop('ylabel_left', None)
    ylabel_right = options.pop('ylabel_right', None)
    xlabel = options.pop('xlabel', None)

    # STEP 1 - create plot
    fig, ax1 = plt.subplots()
    ax1.plot(plot_left, color=blue, label=y_left_label)
    ax1.axes.set_xlim(left=np.min(plot_left.index))

    # Make the y-axis label, ticks and tick labels match the line color.
    if y_left_label is not None and label_axes:
        ax1.set_ylabel(y_left_label, color=blue)
    ax1.tick_params('y', colors=blue)

    if y_left_label is not None:
        # xvals = (plot_left.index[0], plot_left.index[int(len(plot_left)/4)],)
        xval_target = plot_left.index[int(len(plot_left) * 1 / 6)]
        xval_min = plot_left.dropna().index[0]
        xval_max = plot_left.dropna().index[-1]
        xvals = [min(max(xval_target, xval_min), xval_max)]
        labelLines(plt.gca().get_lines(), zorder=zorder, align=align, xvals=xvals, **options)

    ax2 = ax1.twinx()

    ax2.plot(plot_right, color=mred, label=y_right_label)
    # plot_right.plot(ax=ax2, color=mred, label=y_right_label)
    if y_right_label is not None and label_axes:
        ax2.set_ylabel(y_right_label, color=mred)
    ax2.tick_params('y', colors=mred)
    ax2.axes.set_xlim(left=np.min(plot_right.index))

    if title is not None:
        ax1.set_title(title)

    if y_right_label is not None:
        # xvals = (plot_left.index[int(len(plot_left)*3/4)], plot_left.index[-1],)
        xval_target = plot_left.index[int(len(plot_left) * 5 / 6)]
        xval_min = plot_left.dropna().index[0]
        xval_max = plot_left.dropna().index[-1]
        xvals = [min(max(xval_target, xval_min), xval_max)]
        labelLines(plt.gca().get_lines(), zorder=zorder, align=align, xvals=xvals, **options)

    if ylabel_left is not None:
        ax1.set_ylabel(ylabel_left)

    if ylabel_right is not None:
        ax2.set_ylabel(ylabel_right)

    if xlabel is not None:
        ax1.set_xlabel(xlabel)

    if edge is not None:
        fig.subplots_adjust(left=edge, bottom=edge, right=1 - edge, top=1 - edge, wspace=0, hspace=0)

    # STEP 2 - get filename and save
    if return_filepath:
        filename = report_fns.get_temp_filename()

        # fig.patch.set_facecolor('xkcd:mint green')
        fig.savefig(filename, dpi=80, transparent=True)  # , facecolor=fig.get_facecolor()
        if plot_close_after_use:
            plt.close(fig)
        return filename
    else:
        return fig, ax1, ax2


if __name__ == '__main__':
    print("testing functions")
