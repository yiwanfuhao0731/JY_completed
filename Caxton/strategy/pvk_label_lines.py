from math import atan2, degrees
import numpy as np
import pandas as pd

from matplotlib.dates import date2num
from datetime import datetime


# Label line with line2D label data
def labelLine(line, x, label=None, align=True, **kwargs):
    '''Label a single matplotlib line at position x

    Parameters
    ----------
    line : matplotlib.lines.Line
       The line holding the label
    x : number
       The location in data unit of the label
    label : string, optional
       The label to set. This is inferred from the line by default
    kwargs : dict, optional
       Optional arguments passed to ax.text
    '''
    ax = line.axes
    xdata = line.get_xdata()
    ydata = line.get_ydata()

    if isinstance(ydata, np.ma.core.MaskedArray):
        ydata = pd.Series(ydata).fillna(method='ffill').fillna(method='bfill').values

    # Convert datetime objects to floats
    # if isinstance(x, datetime):
    try:
        x = date2num(x)
    except AttributeError:
        None

    try:
        x = date2num(x.to_timestamp(how="E"))
    except AttributeError:
        None

    # if isinstance(xdata[0], datetime):
    try:
        xdata = date2num(xdata)
    except AttributeError:
        None
    period = False
    try:
        xdata = date2num([x.to_timestamp(how="E") for x in xdata])
        period = True
    except AttributeError:
        None

    if (x < xdata[0]) or (x > xdata[-1]):
        raise Exception('x label location is outside data range!')

    # Find corresponding y co-ordinate and angle of the
    ip = 1
    for i in range(len(xdata)):
        if x < xdata[i]:
            ip = i
            break

    y = ydata[ip-1] + (ydata[ip]-ydata[ip-1]) * \
        (x-xdata[ip-1])/(xdata[ip]-xdata[ip-1])

    if not label:
        label = line.get_label()

    if align:
        # Compute the slope
        dx = xdata[ip] - xdata[ip-1]
        dy = ydata[ip] - ydata[ip-1]
        ang = degrees(atan2(dy, dx))

        # Transform to screen co-ordinates
        pt = np.array([x, y]).reshape((1, 2))
        trans_angle = ax.transData.transform_angles(np.array((ang, )), pt)[0]

    else:
        trans_angle = 0

    # Set a bunch of keyword arguments
    if 'color' not in kwargs:
        kwargs['color'] = line.get_color()

    if ('horizontalalignment' not in kwargs) and ('ha' not in kwargs):
        kwargs['ha'] = 'center'

    if ('verticalalignment' not in kwargs) and ('va' not in kwargs):
        kwargs['va'] = 'center'

    if 'backgroundcolor' not in kwargs:
        kwargs['backgroundcolor'] = ax.get_facecolor()

    if 'clip_on' not in kwargs:
        kwargs['clip_on'] = True

    if 'zorder' not in kwargs:
        kwargs['zorder'] = 2.5

    if 'bbox' not in kwargs:
        kwargs['bbox'] = dict(alpha=0.65, pad=-0.1, facecolor='white', edgecolor='white', fill=True)
    else:
        kwargs['bbox']['alpha']     = kwargs['bbox'].get('alpha', 0.65)
        kwargs['bbox']['pad']       = kwargs['bbox'].get('pad', -0.1)
        kwargs['bbox']['facecolor'] = kwargs['bbox'].get('facecolor', 'white')
        kwargs['bbox']['edgecolor'] = kwargs['bbox'].get('edgecolor', 'white')
        kwargs['bbox']['fill']      = kwargs['bbox'].get('fill', True)

    if period:
        xdata = np.array(xdata)
        if np.min(xdata) >= x:
            idx = xdata == xdata[xdata <= x][-1]
        else:
            idx = xdata == xdata[xdata >= x][0]
        x = line.get_xdata()[idx]

    ax.text(x, y, label, rotation=trans_angle, **kwargs)


def labelLines(lines, align=True, xvals=None, **kwargs):
    '''Label all lines with their respective legends.

    Parameters
    ----------
    lines : list of matplotlib lines
       The lines to label
    align : boolean, optional
       If True, the label will be aligned with the slope of the line
       at the location of the label. If False, they will be horizontal.
    xvals : (xfirst, xlast) or array of float, optional
       The location of the labels. If a tuple, the labels will be
       evenly spaced between xfirst and xlast (in the axis units).
    kwargs : dict, optional
       Optional arguments passed to ax.text
    '''
    ax = lines[0].axes
    labLines = []
    labels = []

    # Take only the lines which have labels other than the default ones
    for line in lines:
        label = line.get_label()
        if "_line" not in label:
            labLines.append(line)
            labels.append(label)

    if xvals is None:
        x = line.get_xdata()

        # Convert datetime objects to floats
        converted = False
        try:
            x = date2num(x)
            xvals = (x[0], x[-1])
            converted = True
        except AttributeError:
            None

        try:
            x1 = date2num(x[0].to_timestamp(how="E"))
            x2 = date2num(x[-1].to_timestamp(how="E"))
            xvals = (x1, x2)
            converted = True
        except AttributeError:
            None

        if not converted:
            xvals = ax.get_xlim() # set axis limits as annotation limits, xvals now a tuple

    else:
        try:
            xvals = date2num(xvals)
        except AttributeError:
            None

    if type(xvals) == tuple:
        xmin, xmax = xvals
        xscale = ax.get_xscale()
        if xscale == "log":
            xvals = np.logspace(np.log10(xmin), np.log10(xmax), len(labLines)+2)[1:-1]
        else:
            xvals = np.linspace(xmin, xmax, len(labLines)+2)[1:-1]

    for line, x, label in zip(labLines, xvals, labels):
        labelLine(line, x, label, align, **kwargs)