import os
import random
import string

from dateutil.relativedelta import relativedelta

from matplotlib.dates import date2num
import pandas as pd
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from pylatex import Document, Section, Figure, NoEscape, LongTable, MultiColumn, Package

# utils_pvk imports
from utils_pvk.lib_labellines import labelLine, labelLines

STORAGE_LOCATION = "H:/python_local/storage/temp/"


def randomString(stringLength=6):
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase + "0123456789"
    return ''.join(random.choice(letters) for _ in range(stringLength))


def get_storage_location():
    return STORAGE_LOCATION


def get_temp_filename(override_path=None, name="plot", extension="png"):
    """return filename for a plot"""
    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    rndstr = randomString(10)
    if not override_path:
        filename = STORAGE_LOCATION + "{}_{}_{}.{}".format(name, time, rndstr, extension)
    else:
        filename = override_path + "{}_{}_{}.{}".format(name, time, rndstr, extension)

    return filename


def plt_rangebox(diamond, left, right, min_rng=-1, max_rng=1, centerline=True):
    """plot range box"""
    fig = plt.figure(figsize=(2.8, 0.5))

    ax = fig.add_subplot(111)

    ax.plot([left, right], [0, 0], "k+")
    if centerline:
        ax.plot([0, 0], [2, -2], color=(0.8, 0.8, 0.8), linewidth=1)
    ax.plot([min_rng, max_rng], [0, 0], "k-", linewidth=1)
    ax.plot([diamond], [0], color=(0, 32/252, 96/252), marker="D", linewidth=1)
    ax.axis('off')

    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    i = 0
    file_available = False
    while not file_available and i < 100:
        i += 1
        filename = STORAGE_LOCATION + "{}_plot{:03}.png".format(time, i)
        if not os.path.isfile(filename):
            file_available = True

    fig.savefig(filename, dpi=300, bbox_inches='tight')

    # trim={<left> <lower> <right> <upper>}
    rangebox_command = ((r"""\hspace*{-2mm}""" +
                         r"""\raisebox{-0.8mm}{""" +
                         r"""\includegraphics[trim={12.45mm 10mm 5mm 5mm}, clip, scale=0.55]{[filename]}}""" +
                         r"""\hspace*{-2mm}""")
                        .replace('[filename]', filename))

    return NoEscape(rangebox_command)


def plt_corrhist(excel_data, return_latex=False):

    assetname_a = excel_data.columns[0]
    assetname_b = excel_data.columns[1]
    corrs = excel_data['corr_comb']
    corrs_smt = excel_data['corrs_smt']
    lower_trigger = excel_data['lower_trigger']
    upper_trigger = excel_data['upper_trigger']
    release = excel_data['release']
    event_trigger = excel_data['plot_trigger']
    event_release = excel_data['plot_release']
    price_a = excel_data[assetname_a]
    price_b = excel_data[assetname_b]

    grey = (127/255, 127/255, 127/255)
    dred = (149/255,  55/255,  53/255)
    mred = (192/255,   0/255,   0/255)
    lred = (255/255,   0/255,   0/255)
    green = ( 0/255, 180/255,   0/255)
    blue = ( 23/255,  55/255,  94/255)


    """plot range box"""
    fig = plt.figure(figsize=(14, 5))

    #################################################################
    # SUBPLOT 1 - CORRELATIONS
    #################################################################
    ax = fig.add_subplot(121)

    # linewidth = 2, markersize = 12, marker='o', linestyle='dashed', color='green',

    ax.plot(corrs,         label="Corr",          color='black', linewidth=2)
    ax.plot(corrs_smt,     label="Smooth",        color=grey   , linewidth=1, linestyle='dashed')
    ax.plot(lower_trigger, label="Lower Trigger", color=dred   , linewidth=1)
    ax.plot(upper_trigger, label="Upper Trigger", color=dred   , linewidth=1)
    ax.plot(release,       label="Release",       color=mred   , linewidth=2)
    ax.plot(event_trigger, label="Triggered",     color=lred   , marker='x', markersize=10, linestyle='none')
    ax.plot(event_release, label="Released",      color=green  , marker='x', markersize=10, linestyle='none')

    ax.legend(loc=3)
    ax.axes.set_xlim(left=np.min(corrs.index))
    ax.grid(True)
    ax.set_title("Past 1y Correlations: " + assetname_a + " vs. " + assetname_b)

    #################################################################
    # SUBPLOT 2 - PRICES
    #################################################################

    ay1 = fig.add_subplot(122)

    ay1.plot(price_a, label=assetname_a, color=blue, linewidth=1)
    ay1.xaxis.grid(True)

    ay2 = ay1.twinx()
    invert_ay2 = np.mean(corrs) < -0.10

    if invert_ay2:
        y2label = assetname_b + " (inverted)"
    else:
        y2label = assetname_b

    ay2.plot(price_b, label=y2label, color=mred, linewidth=1)

    if invert_ay2:
        ay2.invert_yaxis()
        # ay2.set_ylim(ay2.get_ylim()[::-1])

    ay1.legend(loc=2)
    ay2.legend(loc=1)
    ay1.axes.set_xlim(left=np.min(price_a.index))
    ay1.set_title("Past 1y Prices: " + assetname_a + " vs. " + assetname_b)

    # ADD TRIGGER VERTICAL LINES
    triggers = event_trigger[~np.isnan(event_trigger)].index
    for trigger in triggers:
        ay1.axvline(x=trigger, color=lred, linewidth=1)

    releases = event_release[~np.isnan(event_release)].index
    for release in releases:
        ay1.axvline(x=release, color=green, linewidth=1)

    # fig.show()
    # STEP 2 - save to latex code
    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    i = 0
    file_available = False
    while not file_available and i < 100:
        i += 1
        filename = STORAGE_LOCATION + "{}_plot{:03}.png".format(time, i)
        if not os.path.isfile(filename):
            file_available = True

    fig.subplots_adjust(hspace=0.1, wspace=0.1)


    if return_latex:
        fig.savefig(filename, dpi=300, bbox_inches='tight')
        # trim={<left> <lower> <right> <upper>}
        rangebox_command = ((r"""\hspace*{-2mm}""" +
                             r"""\raisebox{-0.8mm}{""" +
                             r"""\includegraphics[trim={12.45mm 10mm 5mm 5mm}, clip, scale=0.55]{[filename]}}""" +
                             r"""\hspace*{-2mm}""")
                            .replace('[filename]', filename))

        return NoEscape(rangebox_command)
    else:
        fig.savefig(filename, dpi=80, bbox_inches='tight')
        return filename


def plt_intraday(plot_data, title):
    return plt_linelabels(plot_data, title, intraday=True)


def plt_linelabels(plot_data, title, intraday=False, **options):

    # logic for adding lines (using the labelLines package)
    # options = dict()
    zorder = options.pop('zorder', 2.5)
    align = options.pop('align', False)
    xvals = options.pop('xvals', None)
    edge = options.pop('edge', None)
    ticks = options.pop('ticks', None)
    return_ticks = options.pop('return_ticks', False)

    # DUMMY - get plot labels
    plot_data_dummy = plot_data.copy()
    try:
        plot_data_dummy.index = date2num(plot_data_dummy.index)
    except:
        plot_data_dummy.index = date2num([x.to_timestamp(how="E") for x in plot_data_dummy.index])

    fig, ax = plt.subplots()
    plot_data_dummy.plot(ax=ax, title=title, legend=False, )
    ticksb = plt.xticks()

    # STEP 1 - create plot
    if ticks is not None:
        plt.xticks(ticks[0], ticks[1])
    else:
        fig, ax = plt.subplots()

        plot_data.plot(ax=ax, title=title, legend=False, )
        ax.set_xlabel("")

        ticks = plt.xticks()
        ticks = (ticksb[0], ticks[1])

    labelLines(plt.gca().get_lines(), zorder=zorder, align=align, xvals=xvals, **options)

    if intraday:
        hours = mdates.HourLocator(interval=1)  #
        h_fmt = mdates.DateFormatter('%H:%M:%S')
        ax.xaxis.set_major_locator(hours)
        ax.xaxis.set_major_formatter(h_fmt)

    if edge is not None:
        fig.subplots_adjust(left=edge, bottom=edge, right=1-edge, top=1-edge, wspace=0, hspace=0)


    # if major_formatter is not None:
    #     frmt = TimeSeries_DateFormatter("Y", dynamic_mode=True, minor_locator=False, plot_obj=ax)
    #     ax.xaxis.set_major_formatter(frmt)
    #     fig.gcf().autofmt_xdate()

    # STEP 2 - get filename and save

    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    i = 0
    file_available = False
    while not file_available and i < 100:
        i += 1
        filename = STORAGE_LOCATION + "{}_plot{:03}.png".format(time, i)
        if not os.path.isfile(filename):
            file_available = True

    # fig.patch.set_facecolor('xkcd:mint green')
    fig.savefig(filename, dpi=80)  # , facecolor=fig.get_facecolor()
    if return_ticks:
        return filename, ticks
    else:
        return filename



def plt_dual(plot_left, plot_right, title=None, y_left_label=None, y_right_label=None, x_label=None, label_axes=False,
             **options):

    blue = ( 23/255,  55/255,  94/255)
    mred = (192/255,   0/255,   0/255)

    # logic for adding lables (using the labelLines package)
    # options = dict()
    zorder = options.pop('zorder', 100)
    align = options.pop('align', False)
    edge = options.pop('edge', None)
    ticks = options.pop('ticks', None)
    return_ticks = options.pop('return_ticks', False)

    # STEP 1 - create plot
    fig, ax1 = plt.subplots()
    ax1.plot(plot_left, color=blue, label=y_left_label)
    # plot_left.to_frame().plot(ax=ax1, color=blue, label=y_left_label)

    if x_label is None:
        ticks = plt.xticks()

    # if ticks is not None:
    #     plt.xticks(ticks[0], ticks[1])
    # else:
    #     ticks = plt.xticks()

    # Make the y-axis label, ticks and tick labels match the line color.
    if y_left_label is not None and label_axes:
        ax1.set_ylabel(y_left_label, color=blue)
    ax1.tick_params('y', colors=blue)

    if y_left_label is not None:
        # xvals = (plot_left.index[0], plot_left.index[int(len(plot_left)/4)],)
        xvals = [plot_left.index[int(len(plot_left)*1/6)]]
        labelLines(plt.gca().get_lines(), zorder=zorder, align=align, xvals=xvals, **options)

    ax2 = ax1.twinx()

    ax2.plot(plot_right, color=mred, label=y_right_label)
    # plot_right.plot(ax=ax2, color=mred, label=y_right_label)
    if y_right_label is not None and label_axes:
        ax2.set_ylabel(y_right_label, color=mred)
    ax2.tick_params('y', colors=mred)

    # set or load ticks
    if ticks is not None:
        ax1.set_xticks(ticks[0])
        plt.xticks(ticks[0], ticks[1])

    if title is not None:
        ax1.set_title(title)

    if y_right_label is not None:
        # xvals = (plot_left.index[int(len(plot_left)*3/4)], plot_left.index[-1],)
        xvals = [plot_left.index[int(len(plot_left)*5/6)]]
        labelLines(plt.gca().get_lines(), zorder=zorder, align=align, xvals=xvals, **options)

    if edge is not None:
        fig.subplots_adjust(left=edge, bottom=edge, right=1-edge, top=1-edge, wspace=0, hspace=0)

    # frmt = TimeSeries_DateFormatter(freq="m")
    # if major_formatter is not None:
    #     ax1.xaxis.set_major_formatter(mdates.DateFormatter(major_formatter))

    # STEP 2 - get filename and save
    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    i = 0
    file_available = False
    while not file_available and i < 100:
        i += 1
        filename = STORAGE_LOCATION + "{}_plot{:03}.png".format(time, i)
        if not os.path.isfile(filename):
            file_available = True

    # fig.patch.set_facecolor('xkcd:mint green')
    fig.savefig(filename, dpi=80)  # , facecolor=fig.get_facecolor()
    if return_ticks:
        return filename, ticks
    else:
        return filename


def plt_simple(data, title=None, return_latex=False, figsize=(14, 4.9)):

    grey = (127/255, 127/255, 127/255)
    dred = (149/255,  55/255,  53/255)
    mred = (192/255,   0/255,   0/255)
    lred = (255/255,   0/255,   0/255)
    green = ( 0/255, 180/255,   0/255)
    blue = ( 23/255,  55/255,  94/255)


    """plot range box"""
    fig = plt.figure(figsize=figsize)
    ax1 = fig.add_subplot(111)

    #################################################################
    # SUBPLOT 1 - CORRELATIONS
    #################################################################

    ax1.plot(data)

    # ax1.legend(loc=3)
    ax1.axes.set_xlim(left=np.min(data.index))
    ax1.grid(True)
    if title is not None:
        fig.set_title(title)

    # fig.show()
    # STEP 2 - save to latex code
    time = dt.datetime.today().strftime("%Y-%m-%d_%H-%M-%S")
    i = 0
    file_available = False
    while not file_available and i < 100:
        i += 1
        filename = STORAGE_LOCATION + "{}_plot{:03}.png".format(time, i)
        if not os.path.isfile(filename):
            file_available = True

    fig.subplots_adjust(hspace=0.1, wspace=0.1)

    if return_latex:
        fig.savefig(filename, dpi=300, bbox_inches='tight')
        # trim={<left> <lower> <right> <upper>}
        rangebox_command = ((r"""\hspace*{-2mm}""" +
                             r"""\raisebox{-0.8mm}{""" +  # trim={<left> <lower> <right> <upper>}
                             r"""\includegraphics[trim={12.45mm 20mm 5mm 15mm}, clip, scale=0.55]{[filename]}}""" +
                             r"""\hspace*{-2mm}""")
                            .replace('[filename]', filename))

        return NoEscape(rangebox_command)
    else:
        fig.savefig(filename, dpi=80, bbox_inches='tight')
        return filename


def test_plt_corrhist():
    testfile = r"\\clndata01\home$\pvklooster\excel\02_corr_monitor\example_data.xlsx"
    test_data_full = pd.read_excel(testfile).set_index('date').sort_index()
    test_data = test_data_full.loc[test_data_full.index[-1] - relativedelta(years=1):]

    plt_corrhist(test_data, return_latex=False)


if __name__ == '__main__':


    print("testing functions")
    test_plt_corrhist()
