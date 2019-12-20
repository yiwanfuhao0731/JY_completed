import math
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime,timedelta
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from textwrap import wrap
import matplotlib.dates as mdates
import shutil
import collections
import os
WKDIR = os.path.dirname(os.path.realpath(__file__))


WKDIR = os.path.dirname(os.path.realpath(__file__))
PROJ_DIR = os.path.join(WKDIR,"..")

def parse_chart_param_for_bt(master_input,short_name,ser_container,pack_name = 'Component',customed_titles = {}):
    """
    :param master_input: the master input in xlsx
    :param short_name: the short name of the strategy
    :param ser_container: the object that contains all the series, should all be new_wf
    :return: return a list of object that has parameters all read through the xls
    """
    #init
    df_param = pd.read_excel(master_input,sheet_name=short_name,header=0,index_col=False)
    df_param = df_param.loc[df_param['Chart_pack'] == pack_name,:]
    #deal with NA
    df_param.fillna('invalid_name',inplace=True)
    _chart = collections.OrderedDict()
    _chart_n = collections.OrderedDict()  # number of lines
    _chart_title = collections.OrderedDict()
    _chart_color = collections.OrderedDict()
    _chart_style = collections.OrderedDict()
    _chart_twox = collections.OrderedDict()
    _chart_ratecycle = collections.OrderedDict()
    _chart_rate_rise_fall_file = collections.OrderedDict()

    #initialise the pack
    name_list = df_param['factor_name'].values.tolist()
    for name in name_list:
        _chart[name] = []
        _chart_n[name] = []
        _chart_title[name] = []
        _chart_color[name] = []
        _chart_style[name] = []
        _chart_twox[name] = []
        _chart_ratecycle[name] = []
        _chart_rate_rise_fall_file[name] = []
    #position holder
    name = 'zzz_blank'
    _chart[name] = []
    _chart_n[name] = []
    _chart_title[name] = []
    _chart_color[name] = []
    _chart_style[name] = []
    _chart_twox[name] = []
    _chart_ratecycle[name] = []
    _chart_rate_rise_fall_file[name] = []
    _chart['zzz_blank'].append(0)
    _chart_n['zzz_blank'].append(0)
    _chart_title['zzz_blank'].append(0)
    _chart_color['zzz_blank'].append(0)
    _chart_style['zzz_blank'].append(0)
    _chart_twox['zzz_blank'].append(0)
    _chart_ratecycle['zzz_blank'].append(0)
    _chart_rate_rise_fall_file['zzz_blank'].append(0)

    # parse the parameters line by line
    for i in range(df_param.shape[0]):
        this_df_param = df_param.iloc[[i],:]
        this_name = this_df_param['factor_name'].tolist()[0]
        if this_name.split('_')[0] != 'zzz':
            #parse series tuple
            ser_list = this_df_param.loc[:,['series1','series2','series3','series4']].values.tolist()[0]
            try:
                ser_list = [i for i in ser_list if i!='invalid_name']
                ser_tuple = tuple([ser_container.df[s] for s in ser_list])
                _chart[this_name].append(ser_tuple)

                #parse title
                title = this_df_param.loc[:,'title'].tolist()[0]
                title = '' if title == 'invalid_name' else title
                # check if title is customed
                if 'custom' in title.split('_',1)[0]:
                    title = customed_titles[title.split('_',1)[1]]
                else:
                    title = title
                _chart_title[this_name].append(title)

                # parse number
                _chart_n[this_name].append(len(ser_list))

                # parse color
                color_list = this_df_param.loc[:,['color1','color2','color3','color4']].values.tolist()[0]
                color_list = [i for i in color_list if i!='invalid_name']
                _chart_color[this_name].append(tuple(color_list))

                # parse style
                style_list = this_df_param.loc[:,['style1','style2','style3','style4']].values.tolist()[0]
                style_list = [i for i in style_list if i!='invalid_name']
                _chart_style[this_name].append(tuple(style_list))

                # parse twox
                twox = this_df_param.loc[:,'twox'].tolist()[0]
                _chart_twox[this_name].append(twox)

                #parse ratecycle
                ratecycle = this_df_param.loc[:,'ratecycle'].tolist()[0]
                _chart_ratecycle[this_name].append(ratecycle)

                #parse rate_raise_fall_file
                rise_fall = this_df_param.loc[:,'rate_rise_fall_file'].tolist()[0]
                _chart_rate_rise_fall_file[this_name] = _chart_rate_rise_fall_file[this_name] + [os.path.join(PROJ_DIR,rise_fall)]
            except:
                pass

    name_list_set = []
    for i in name_list:
        if i not in name_list_set or (i.split('_')[0] == 'zzz'):
            name_list_set.append(i)
    return name_list_set,_chart, _chart_title,_chart_n, _chart_color, _chart_style,_chart_twox,_chart_ratecycle,_chart_rate_rise_fall_file

def temp_charts(_component_name_list,_comp_chart,_comp_chart_title,_comp_chart_n,_comp_chart_color,_comp_chart_style,_comp_chart_twox,_comp_chart_ratecycle,_comp_rate_rise_fall_file=None,pdfpath = '',bt_backup_dir = None,start_dt='2000-01-01',end_dt=datetime.now().strftime('%Y-%m-%d'),page_name='',RPTDIR='',rise_dates_path=''):
    # firstly extract the charts parameter into a plain list, and plot one by one
    iso_page = []
    dfs = []
    titles = []
    number_of_lines = []
    colors = []
    styles = []
    two_x = []
    rate_cycle = []
    rate_rise_fall = []

    # create iso - id pairs, extract the title, n, color and style

    for factr in _component_name_list:
        #print (factr)
        n_of_charts = len(_comp_chart[factr])
        iso_page = iso_page + [factr + '_' + str(i) for i in range(n_of_charts)]
        #print ([factr + '_' + str(i) for i in range(n_of_charts)])
        dfs = dfs + _comp_chart[factr]
        titles = titles + _comp_chart_title[factr]
        number_of_lines = number_of_lines + _comp_chart_n[factr]
        #print (_comp_chart_n[factr])
        colors = colors + _comp_chart_color[factr]
        styles = styles + _comp_chart_style[factr]
        two_x = two_x + _comp_chart_twox[factr]
        rate_cycle = rate_cycle+_comp_chart_ratecycle[factr]
        if _comp_rate_rise_fall_file is None:
            rate_rise_fall = rate_rise_fall + [rise_dates_path]*n_of_charts
        else:
            if _comp_rate_rise_fall_file[factr] not in ['invalid_name']:
                rate_rise_fall = rate_rise_fall + _comp_rate_rise_fall_file[factr]
            else:
                rate_rise_fall = rate_rise_fall + [rise_dates_path]

    chart_each_page = 12
    chart_rows = 4
    chart_cols = 3

    pages_number = math.ceil(len(iso_page) / chart_each_page)
    chart_in_page = [chart_each_page] * (pages_number - 1) + [
        len(iso_page) - chart_each_page * (pages_number - 1)]

    report = PdfPages(pdfpath)

    print('chart_in_each_page=', chart_in_page)
    print("CREATING RETURNS PAGE")

    # split iso codes into each page!
    for i, n in enumerate(chart_in_page):
        chart_start_dt = start_dt
        chart_end_dt = end_dt

        chart_rows, chart_cols = chart_rows, chart_cols
        fig, axarr = plt.subplots(chart_rows, chart_cols, figsize=(18.27, 12.69), dpi=100)
        # add the main title
        if page_name !='':
            fig.suptitle(page_name, fontsize=16)
        print('the current page is: ', i)
        start_idx = sum(chart_in_page[:i])
        end_idx = start_idx + n
        df_in_this_page = iso_page[start_idx:end_idx]

        dfs_in_this_page, title_itp, n_itp, color_itp, stype_itp ,twox_itp,rate_cycle_itp,rate_rise_fall_path_itp = dfs[start_idx:end_idx], titles[
                                                                                           start_idx:end_idx], number_of_lines[
                                                                                                               start_idx:end_idx], colors[
                                                                                                                                   start_idx:end_idx], styles[
                                                                                                                                                       start_idx:end_idx],two_x[start_idx:end_idx],rate_cycle[start_idx:end_idx],rate_rise_fall[start_idx:end_idx]

        # print (df_in_this_page)
        for i in range(chart_rows):
            for j in range(chart_cols):
                if i * chart_cols + j < len(df_in_this_page):
                    ax = axarr[i, j]
                    id = df_in_this_page[i * chart_cols + j]
                    if id.split('_')[0]=='zzz':
                        print ('this is zzz')
                        set_ax_invisible(ax)
                        continue

                    current_dfs, current_title, current_n, current_color, current_style,current_twox,current_rate_cycle,current_rate_rise_path = dfs_in_this_page[
                                                                                              i * chart_cols + j], \
                                                                                          title_itp[i * chart_cols + j], \
                                                                                          n_itp[i * chart_cols + j], \
                                                                                          color_itp[i * chart_cols + j], \
                                                                                          stype_itp[i * chart_cols + j], twox_itp[i * chart_cols + j],rate_cycle_itp[i * chart_cols + j],rate_rise_fall_path_itp[i * chart_cols + j]
                    print(current_n)
                    print(i, j, id)
                    # extract the iso code and des for this country
                    current_iso = '_'.join(id.split('_')[:-1])
                    number = int(id.split('_')[-1])

                    if (current_n > 1.1) & (current_n < 2.2) :
                        double_line = True
                        tri_line = False
                        df1 = current_dfs[0]
                        df2 = current_dfs[1]
                    elif (current_n > 2.9):
                        tri_line = True
                        double_line  =False
                        df1 = current_dfs[0]
                        df2 = current_dfs[1]
                        df3 = current_dfs[2]
                    else:
                        try:
                            df1 = current_dfs[0]
                        except:
                            df1 = current_dfs
                        double_line = False
                        tri_line = False

                    #print (df1)

                    mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                    df1 = df1.loc[mask, :]
                    df1 = df1.dropna()

                    if double_line|tri_line:
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]
                        df2 = df2.dropna()
                    if tri_line:
                        mask = (df3.index >= chart_start_dt) & (df3.index <= chart_end_dt)
                        df3 = df3.loc[mask, :]
                        df3 = df3.dropna()

                    # print (df)
                    x1 = pd.to_datetime(df1.index).date
                    if double_line|tri_line:
                        x2 = pd.to_datetime(df2.index).date
                    if tri_line:
                        x3 = pd.to_datetime(df3.index).date

                    y1 = df1.ix[:, 0]
                    if double_line|tri_line:
                        y2 = df2.ix[:, 0]
                    if tri_line:
                        y3 = df3.ix[:, 0]

                    # enable to draw shaded line
                    if current_style[0] == 'shaded_with_line':
                        line1 = ax.plot(x1, y1, color=current_color[0], ls='solid', lw=0.4,
                                        label=df1.columns.tolist()[0])
                        line_shade1 = ax.fill_between(x1,0,y1,facecolors='aqua',alpha=0.3,label='_nolabel_')
                    else:
                        line1 = ax.plot(x1, y1, color=current_color[0], ls=current_style[0], lw=0.9,
                                        label=df1.columns.tolist()[0])
                    if double_line:
                        # allow the possibility of shaded_with_line
                        if current_twox == 'enable':
                            two_ax = True
                            ax2 = ax.twinx()
                            if current_style[1] == 'shaded_with_line':
                                line2 = ax2.plot(x2, y2, color=current_color[1], ls='solid', lw=0.4,
                                                 label=df2.columns.tolist()[0])
                                line_shade2 = ax2.fill_between(x2, 0, y2, facecolors='aqua', alpha=0.3,
                                                              label='_nolabel_')
                            else:
                                line2 = ax2.plot(x2, y2, color=current_color[1], ls=current_style[1], lw=0.9,
                                                 label=df2.columns.tolist()[0])
                        elif (current_twox == 'automatic') and (np.mean(np.abs(y1-y2))>10): #or np.max(y1/y2)>10 or np.max(y1/y2) < 0.1 or np.min(y1/y2) > 10 or np.min(y1/y2) < 0.1:
                            two_ax = True
                            ax2 = ax.twinx()
                            if current_style[1] == 'shaded_with_line':
                                line2 = ax2.plot(x2, y2, color=current_color[1], ls='solid', lw=0.4,
                                                 label=df2.columns.tolist()[0])
                                line_shade2 = ax2.fill_between(x2, 0, y2, facecolors='aqua', alpha=0.3,
                                                               label='_nolabel_')
                            else:
                                line2 = ax2.plot(x2, y2, color=current_color[1], ls=current_style[1], lw=0.9,
                                                 label=df2.columns.tolist()[0])
                        else:
                            two_ax = False
                            if current_style[1] == 'shaded_with_line':
                                line2 = ax.plot(x2, y2, color=current_color[1], ls='solid', lw=0.4,
                                                 label=df2.columns.tolist()[0])
                                line_shade2 = ax.fill_between(x2, 0, y2, facecolors='aqua', alpha=0.3,
                                                               label='_nolabel_')
                            else:
                                line2 = ax.plot(x2, y2, color=current_color[1], ls=current_style[1], lw=0.9,
                                                 label=df2.columns.tolist()[0])

                    if tri_line:
                        if current_twox == 'disable-enable':
                            ax2 = ax.twinx()
                            line2 = ax.plot(x2, y2, color=current_color[1], ls=current_style[1], lw=0.9,
                                             label=df2.columns.tolist()[0])
                            line3 = ax2.plot(x3, y3, color=current_color[2], ls=current_style[2], lw=0.9,
                                            label=df3.columns.tolist()[0])

                    if not (double_line or tri_line):
                        lns = line1
                    elif double_line:
                        lns=line1+line2
                    else:
                        lns = line1+line2+line3

                    labs = [l.get_label() for l in lns]
                    ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                    # set the range for the line1
                    if not (double_line or tri_line):
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max-l1_min
                        ymax = l1_max+l1_diff*.2
                        ymin = l1_min-l1_diff*.2
                        ax.set_ylim(ymin,ymax)

                    if double_line:
                        if two_ax:
                            l1_max = df1.loc[chart_start_dt:].max().values
                            l1_min = df1.loc[chart_start_dt:].min().values
                            l1_diff = l1_max - l1_min
                            ymax = l1_max + l1_diff * .2
                            ymin = l1_min - l1_diff * .2
                            ax.set_ylim(ymin, ymax)

                            l2_max = df2.loc[chart_start_dt:].max().values
                            l2_min = df2.loc[chart_start_dt:].min().values
                            l2_diff = l2_max - l2_min
                            ymax = l2_max + l2_diff * .2
                            ymin = l2_min - l2_diff * .2
                            ax2.set_ylim(ymin, ymax)
                        if not two_ax:
                            l1_max = df1.loc[chart_start_dt:].max().values
                            l1_min = df1.loc[chart_start_dt:].min().values
                            l1_diff = l1_max - l1_min

                            l2_max = df2.loc[chart_start_dt:].max().values
                            l2_min = df2.loc[chart_start_dt:].min().values
                            l2_diff = l2_max - l2_min

                            ymax = (l1_max + l1_diff * .2) if l1_max>l2_max else (l2_max + l2_diff * .2)
                            ymin = (l1_min - l1_diff * .2) if l1_min<l2_min else (l2_min - l2_diff * .2)
                            ax.set_ylim(ymin, ymax)

                    if tri_line:
                        if current_twox == 'disable-enable':
                            l1_max = df1.loc[chart_start_dt:].max().values
                            l1_min = df1.loc[chart_start_dt:].min().values
                            l1_diff = l1_max - l1_min
                            ymax = l1_max + l1_diff * .2
                            ymin = l1_min - l1_diff * .2
                            ax.set_ylim(ymin, ymax)

                            l3_max = df3.loc[chart_start_dt:].max().values
                            l3_min = df3.loc[chart_start_dt:].min().values
                            l3_diff = l3_max - l3_min
                            ymax = l3_max + l3_diff * .2
                            ymin = l3_min - l3_diff * .2
                            ax2.set_ylim(ymin, ymax)

                    ax.set_xlabel('')
                    ax.set_ylabel('')
                    if double_line:
                        if two_ax:
                            ax2.set_xlabel('')
                            ax2.set_ylabel('')
                    if tri_line:
                        if current_twox == 'disable-enable':
                            ax2.set_xlabel('')
                            ax2.set_ylabel('')

                    # wrap up the title since it can be too long
                    title = "\n".join(wrap(current_iso + ' ' + current_title, 60))
                    ax.set_title(title, y=1, fontsize=11, fontweight=600)

                    ax.tick_params(labelsize=12, width=0.1)
                    if double_line:
                        if two_ax:
                            ax2.tick_params(labelsize=12, width=0.1)
                        # change the y tick label to blue
                    if tri_line:
                        if current_twox == 'disable-enable':
                            ax2.tick_params(labelsize=12, width=0.1)
                    ax.tick_params(axis='y', labelcolor=current_color[0])
                    if double_line:
                        if two_ax:
                            ax2.tick_params(axis='y', labelcolor=current_color[1])
                    if tri_line:
                        if current_twox == 'disable-enable':
                            ax2.tick_params(axis='y', labelcolor=current_color[2])
                    # add a zero line
                    if double_line:
                        if two_ax:
                            if if_contains_zero(ax2.get_ylim()):
                                ax2.axhline(linewidth=0.5, color='k')
                            else:
                                ax.axhline(linewidth=0.5, color='k')
                            if if_contains_zero(ax.get_ylim()):
                                ax.axhline(linewidth=0.5, color='k')
                        else:
                            ax.axhline(linewidth=0.5, color='k')
                    else:
                        ax.axhline(linewidth=0.5, color='k')

                    if double_line:
                        if two_ax:
                            ax.set_zorder(10)
                            ax.patch.set_visible(False)

                    if tri_line:
                        if current_twox == 'disable-enable':
                            ax.set_zorder(10)
                            ax.patch.set_visible(False)

                    # set border color and width
                    for spine in ax.spines.values():
                        spine.set_edgecolor('grey')
                        spine.set_linewidth(0.5)

                    # add year tickers as minor tick
                    years = mdates.YearLocator()
                    yearsFmt = mdates.DateFormatter('%Y')
                    ax.xaxis.set_major_formatter(yearsFmt)
                    ax.xaxis.set_minor_locator(years)
                    # set the width of minor tick
                    ax.tick_params(which='minor', width=0.1)
                    # ax2.tick_params(axis='y', labelcolor='deepskyblue')
                    # set y-label to the right hand side
                    ax.yaxis.tick_right()
                    if double_line:
                        if two_ax:
                            ax2.yaxis.tick_left()
                    if tri_line:
                        if current_twox == 'disable-enable':
                           ax2.yaxis.tick_left()

                    # set date max
                    x_longer = x1
                    if double_line:
                        if two_ax:
                            x_longer = x1 if x1[-1] > x2[-1] else x2
                    if tri_line:
                        if current_twox == 'disable-enable':
                            x_longer = x1 if x1[-1] > x3[-1] else x3
                    datemax = np.datetime64(x_longer[-1], 'Y')
                    datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                    x_tick_overrive = [datemin, datemax]
                    date_cursor = datemin
                    while date_cursor + np.timedelta64(5, 'Y') < datemax:
                        date_cursor = date_cursor + np.timedelta64(5, 'Y')
                        x_tick_overrive.append(date_cursor)

                    ax.xaxis.set_ticks(x_tick_overrive)
                    ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))

                    # add rates rising falling vertical lines
                    if current_rate_cycle=='enable':
                        add_rates_rise_fall(ax,rise_dates_path=current_rate_rise_path)
                    elif current_rate_cycle=='type2':
                        add_rates_rise_fall(ax,'type2',rise_dates_path=current_rate_rise_path)
                else:
                    set_ax_invisible(axarr[i, j])

        plt.tight_layout()
        if page_name !='':
            fig.tight_layout()
            fig.subplots_adjust(top=0.88)
        report.savefig(fig, bbox_inches='tight')  # the current page is saved
    report.close()

    # make a copy to bt_backup_dir
    if bt_backup_dir:
        shutil.copy(pdfpath,bt_backup_dir)

def set_ax_invisible(ax):
    ax.axis('off')

def add_rates_rise_fall(ax,type = 'type1',rise_dates_path = os.path.join(PROJ_DIR,r"basket\USA_Rates1\usaratesfalling_rising_period.xlsx")):

    try:
        df = pd.read_excel(rise_dates_path, index_col=False, header=0, parse_dates=[0, 1])
    except:
        print (rise_dates_path)
    fall_dates = df['falling'].dropna().values
    rising_dates = df['rising'].dropna().values
    falling2_dates = df['falling2'].dropna().values
    if type == 'type1':
        for x1,x2 in zip(fall_dates,rising_dates):
            ax.axvspan(x1, x2, alpha=0.1, color='red')
        for x1,x2 in zip(rising_dates,falling2_dates):
            ax.axvspan(x1, x2, alpha=0.1, color='green')
    elif type == 'type2':
        for x1 in fall_dates:
            ax.axvline(x=x1, color='grey', linestyle='--',dashes=(5,5),linewidth=0.4)
        for x2 in rising_dates:
            ax.axvline(x=x2, color='grey', linestyle='--',dashes=(5,5),linewidth=0.4)

def add_vertical_line_on_date(ax,sample_end = datetime.today()+timedelta(days=2)):
    end_point = sample_end
    ax.axvline(x=end_point, color='green', linestyle='--',dashes=(5,10),linewidth=0.2)

def if_contains_zero(rng): return True if (rng[0]<0) and (rng[1]>0) else False

def firm_presentation_equity_curve(cumprof,dd,title,dir,sample_start,sample_end):
    fig,axarr = plt.subplots(1,1,figsize=(8, 6),dpi=100)
    ax = axarr
    cumprof,dd = cumprof.loc[sample_start:sample_end,:],dd.loc[sample_start:sample_end,:]
    x1,x2 = cumprof.dropna().index,dd.dropna().index
    y1,y2 = cumprof.dropna().iloc[:,0],dd.dropna().iloc[:,0]
    line1 = ax.plot(x1,y1,color='b',linewidth=2)
    charttitle = title
    ax.set_title("\n".join(wrap(charttitle)), fontsize=12)
    vals = ax.get_yticks()
    #ax.set_yticklabels(['{:3.1f}%'.format(x * 100) for x in vals])
    ax.grid(True, linestyle='dotted', color='k')
    ax.margins(x=0)

    ax1 = ax.twinx()
    line2 = ax1.plot(x2,y2,color='r', linewidth=2, alpha=0.6)
    lims = plt.ylim()
    ax1.set_ylim([lims[0], 0])
   vals1 = ax1.get_yticks()
    print (vals1)
    ax1.set_yticklabels(['{:3.1f}%'.format(x * 100) for x in vals1])
    ax1.margins(x=0)
    plt.savefig(dir)

def rates_tree_component_plot(plot_dict,chart_start_dt = '2010-01-01',chart_end_dt='2035-01-01',rate_rise_fall = '',pdfpath='',page_name = '',bt_backup_dir = None):
    '''
    :param plot_dict: dictionary which defines df pairs, titles,
    :param chart_start_dt:
    :return:
    '''
    chart_each_page = 12
    chart_rows = 4
    chart_cols = 3

    df_pair,title_list,chart_style = plot_dict['df_res'],plot_dict['title_res'],plot_dict['chart_type']
    rate_rise_fall = [rate_rise_fall] * len(df_pair)
    pages_number = math.ceil(len(plot_dict['title_res'])/chart_each_page)
    chart_in_page = [chart_each_page] * (pages_number - 1) + [
        len(plot_dict['title_res']) - chart_each_page * (pages_number - 1)]

    report = PdfPages(pdfpath)

    print('chart_in_each_page=', chart_in_page)
    print("CREATING RETURNS PAGE")

    # split into ech page!
    for i,n in enumerate(chart_in_page):
        chart_rows, chart_cols = chart_rows, chart_cols
        fig, axarr = plt.subplots(chart_rows, chart_cols, figsize=(18.27, 12.69), dpi=100)
        # add the main title
        if page_name != '':
            fig.suptitle(page_name, fontsize=16)
        start_idx = sum(chart_in_page[:i])
        end_idx = start_idx + n
        dfs_in_this_page,title_itp,chart_style_itp,rate_rise_fall_path_itp = df_pair[start_idx:end_idx],title_list[start_idx:end_idx],chart_style[start_idx:end_idx],rate_rise_fall[start_idx:end_idx]

        for i in range(chart_rows):
            for j in range(chart_cols):
                if i * chart_cols + j < len(title_itp):
                    ax = axarr[i, j]
                    current_dfs,current_title,current_style,current_rate_rise_path = dfs_in_this_page[i * chart_cols + j],title_itp[i * chart_cols + j],chart_style_itp[i * chart_cols + j],rate_rise_fall_path_itp[i * chart_cols + j]

                    if current_style in ['raw_vs_trend']:
                        df1,df2 = current_dfs[0].dropna(),current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1 , x2 = pd.to_datetime(df1.index).date,pd.to_datetime(df2.index).date
                        y1 , y2 = df1.iloc[:,0],df2.iloc[:,0]

                        line1 = ax.plot(x1,y1,color='blue',ls = 'solid',lw=0.9,label=df1.columns[0])
                        line2 = ax.plot(x2, y2, color='black', ls='dashed', lw=0.9,
                                        label=df2.columns[0])
                        lns = line1+line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min

                        l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min

                        ymax = (l1_max + l1_diff * .2) if l1_max > l2_max else (l2_max + l2_diff * .2)
                        ymin = (l1_min - l1_diff * .2) if l1_min < l2_min else (l2_min - l2_diff * .2)
                        ax.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='blue')

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()

                        # set date max
                        x_longer = x1

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                    if current_style in ['raw_only','z_score']:
                        df1= current_dfs.dropna()
                        #print (current_dfs)
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]

                        x1 = pd.to_datetime(df1.index).date
                        y1 = df1.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label=df1.columns[0])
                        lns = line1
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=1, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min

                        ymax = (l1_max + l1_diff * .2)
                        ymin = (l1_min - l1_diff * .2)
                        ax.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='blue')

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()

                        # set date max
                        x_longer = x1

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['smooth_z_score']:
                        df1, df2 = current_dfs[0].dropna(), current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1, x2 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date
                        y1, y2 = df1.iloc[:, 0], df2.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label=df1.columns[0])
                        line2 = ax.plot(x2, y2, color='red', ls='dashed', lw=0.9,
                                        label=df2.columns[0])
                        lns = line1 + line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min

                       l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min

                        ymax = (l1_max + l1_diff * .2) if l1_max > l2_max else (l2_max + l2_diff * .2)
                        ymin = (l1_min - l1_diff * .2) if l1_min < l2_min else (l2_min - l2_diff * .2)
                        ax.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='blue')

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()

                        # set date max
                        x_longer = x1

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['gauge_vs_yield']:
                        df1,df2 = current_dfs[0].dropna(),current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1, x2 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date
                        y1, y2 = df1.iloc[:, 0], df2.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='red', ls='solid', lw=0.9, label=df1.columns[0])
                        ax2 = ax.twinx()
                        line2 = ax2.plot(x2, y2, color='blue', ls='solid', lw=0.9, label=df2.columns[0])
                        lns = line1 + line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the axis limit
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min
                        ymax = l1_max + l1_diff * .2
                        ymin = l1_min - l1_diff * .2
                        ax.set_ylim(ymin, ymax)

                        l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min
                        ymax = l2_max + l2_diff * .2
                        ymin = l2_min - l2_diff * .2
                        ax2.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')
                        ax2.set_xlabel('')
                        ax2.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax2.tick_params(labelsize=12, width=0.1)

                        ax.tick_params(axis='y', labelcolor='red')
                        ax2.tick_params(axis='y', labelcolor='blue')

                        ax2.axhline(linewidth=0.5, color='k')
                        if if_contains_zero(ax.get_ylim()):
                            ax.axhline(linewidth=0.5, color='k')

                        ax.set_zorder(10)
                        ax.patch.set_visible(False)

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # ax2.tick_params(axis='y', labelcolor='deepskyblue')
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()
                        ax2.yaxis.tick_left()

                        # set date max
                        x_longer = x1 if x1[-1] > x2[-1] else x2

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['gauge_vs_parent']:
                        df1, df2 = current_dfs[0].dropna(), current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1, x2 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date
                        y1, y2 = df1.iloc[:, 0], df2.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='red', ls='solid', lw=0.9, label=df1.columns[0])
                        line2 = ax.plot(x2, y2, color='blue', ls='solid', lw=0.9,
                                        label=df2.columns[0])
                        lns = line1 + line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min

                        l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min

                        ymax = (l1_max + l1_diff * .2) if l1_max > l2_max else (l2_max + l2_diff * .2)
                        ymin = (l1_min - l1_diff * .2) if l1_min < l2_min else (l2_min - l2_diff * .2)
                        ax.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='blue')

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()

                        # set date max
                        x_longer = x1

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['raw_vs_trend_vs_yield']:
                        df1, df2, df3 = current_dfs[0].dropna(), current_dfs[1].dropna(),current_dfs[2].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]
                        mask = (df3.index >= chart_start_dt) & (df3.index <= chart_end_dt)
                        df3 = df3.loc[mask, :]

                        x1, x2, x3 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date,pd.to_datetime(df3.index).date
                        y1, y2, y3 = df1.iloc[:, 0], df2.iloc[:, 0],df3.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label=df1.columns[0])
                        line2 = ax.plot(x2, y2, color='black', ls='dashed', lw=0.9,
                                        label=df2.columns[0])
                        ax2 = ax.twinx()
                        line3 = ax2.plot(x3, y3, color='lightgrey', ls='solid', lw=0.9,
                                         label=df3.columns[0])
                        lns = line1 + line2+line3
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min
                        ymax = l1_max + l1_diff * .2
                        ymin = l1_min - l1_diff * .2
                        ax.set_ylim(ymin, ymax)

                        l3_max = df3.loc[chart_start_dt:].max().values
                        l3_min = df3.loc[chart_start_dt:].min().values
                        l3_diff = l3_max - l3_min
                        ymax = l3_max + l3_diff * .2
                        ymin = l3_min - l3_diff * .2
                        ax2.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')
                        ax2.set_xlabel('')
                        ax2.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='blue')
                        ax2.tick_params(labelsize=12, width=0.1)
                        ax2.tick_params(axis='y', labelcolor='grey')
                        ax2.tick_params(labelsize=12, width=0.1)

                        ax.set_zorder(10)
                        ax.patch.set_visible(False)

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()

                        # set date max
                        x_longer = x1 if x1[-1] > x3[-1] else x3
                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['raw_only_vs_yield']:
                        df1, df2 = current_dfs[0].dropna(), current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1, x2 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date
                        y1, y2 = df1.iloc[:, 0], df2.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='blue', ls='solid', lw=0.9, label=df1.columns[0])
                        ax2 = ax.twinx()
                        line2 = ax2.plot(x2, y2, color='lightgrey', ls='solid', lw=0.9, label=df2.columns[0])
                        lns = line1 + line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the axis limit
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min
                        ymax = l1_max + l1_diff * .2
                        ymin = l1_min - l1_diff * .2
                        ax.set_ylim(ymin, ymax)

                        l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min
                        ymax = l2_max + l2_diff * .2
                        ymin = l2_min - l2_diff * .2
                        ax2.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')
                        ax2.set_xlabel('')
                        ax2.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax2.tick_params(labelsize=12, width=0.1)

                        ax.tick_params(axis='y', labelcolor='red')
                        ax2.tick_params(axis='y', labelcolor='blue')

                        ax2.axhline(linewidth=0.5, color='k')
                        if if_contains_zero(ax.get_ylim()):
                            ax.axhline(linewidth=0.5, color='k')

                        ax.set_zorder(10)
                        ax.patch.set_visible(False)

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # ax2.tick_params(axis='y', labelcolor='deepskyblue')
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()
                        ax2.yaxis.tick_left()

                        # set date max
                        x_longer = x1 if x1[-1] > x2[-1] else x2

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                        date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax, rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)
                    if current_style in ['signal_flip_vs_yield']:
                        df1, df2 = current_dfs[0].dropna(), current_dfs[1].dropna()
                        mask = (df1.index >= chart_start_dt) & (df1.index <= chart_end_dt)
                        df1 = df1.loc[mask, :]
                        mask = (df2.index >= chart_start_dt) & (df2.index <= chart_end_dt)
                        df2 = df2.loc[mask, :]

                        x1, x2 = pd.to_datetime(df1.index).date, pd.to_datetime(df2.index).date
                        y1, y2 = df1.iloc[:, 0], df2.iloc[:, 0]

                        line1 = ax.plot(x1, y1, color='deepskyblue', ls='solid', lw=0.4,
                                        label=df1.columns[0])
                        line_shade1 = ax.fill_between(x1, 0, y1, facecolors='aqua', alpha=0.3, label='_nolabel_')
                        ax2 = ax.twinx()
                        line2 = ax2.plot(x2, y2, color='black', ls='solid', lw=0.4,
                                         label=df2.columns[0])

                        lns = line1 + line2
                        labs = [l.get_label() for l in lns]
                        ax.legend(lns, labs, loc=9, frameon=False, ncol=2, fontsize=8)

                        # set the max and min
                        l1_max = df1.loc[chart_start_dt:].max().values
                        l1_min = df1.loc[chart_start_dt:].min().values
                        l1_diff = l1_max - l1_min
                        ymax = l1_max + l1_diff * .2
                        ymin = l1_min - l1_diff * .2
                        ax.set_ylim(ymin, ymax)

                        l2_max = df2.loc[chart_start_dt:].max().values
                        l2_min = df2.loc[chart_start_dt:].min().values
                        l2_diff = l2_max - l2_min
                        ymax = l2_max + l2_diff * .2
                        ymin = l2_min - l2_diff * .2
                        ax2.set_ylim(ymin, ymax)

                        ax.set_xlabel('')
                        ax.set_ylabel('')
                        ax2.set_xlabel('')
                        ax2.set_ylabel('')

                        # wrap up the title since it can be too long
                        title = "\n".join(wrap(current_title, 60))
                        ax.set_title(title, y=1, fontsize=11, fontweight=600)

                        ax.tick_params(labelsize=12, width=0.1)
                        ax.tick_params(axis='y', labelcolor='deepskyblue')
                        ax2.tick_params(labelsize=12, width=0.1)
                        ax2.tick_params(axis='y', labelcolor='black')

                        # add zero line
                        ax.axhline(linewidth=0.5, color='k')

                        if if_contains_zero(ax2.get_ylim()):
                            ax2.axhline(linewidth=0.5, color='k')
                        else:
                            ax.axhline(linewidth=0.5, color='k')
                        if if_contains_zero(ax.get_ylim()):
                            ax.axhline(linewidth=0.5, color='k')

                        # set border color and width
                        for spine in ax.spines.values():
                            spine.set_edgecolor('grey')
                            spine.set_linewidth(0.5)

                        ax.set_zorder(10)
                        ax.patch.set_visible(False)

                        # add year tickers as minor tick
                        years = mdates.YearLocator()
                        yearsFmt = mdates.DateFormatter('%Y')
                        ax.xaxis.set_major_formatter(yearsFmt)
                        ax.xaxis.set_minor_locator(years)
                        # set the width of minor tick
                        ax.tick_params(which='minor', width=0.1)
                        # set y-label to the right hand side
                        ax.yaxis.tick_right()
                        ax2.yaxis.tick_left()

                        # set date max
                        x_longer = x1 if x1[-1] > x2[-1] else x2

                        datemax = np.datetime64(x_longer[-1], 'Y')
                        datemin = np.datetime64(x1[0], 'Y') - np.timedelta64(1, 'Y')
                        x_tick_overrive = [datemin, datemax]
                       date_cursor = datemin
                        while date_cursor + np.timedelta64(5, 'Y') < datemax:
                            date_cursor = date_cursor + np.timedelta64(5, 'Y')
                            x_tick_overrive.append(date_cursor)

                        ax.xaxis.set_ticks(x_tick_overrive)
                        ax.set_xlim(datemin, datemax + np.timedelta64(1, 'Y'))
                        add_rates_rise_fall(ax,'type2',rise_dates_path=current_rate_rise_path)
                        add_vertical_line_on_date(ax)

                else:
                    set_ax_invisible(axarr[i, j])
        plt.tight_layout()
        if page_name != '':
            fig.tight_layout()
            fig.subplots_adjust(top=0.88)
        report.savefig(fig, bbox_inches='tight')  # the current page is saved
    report.close()

    # make a copy to bt_backup_dir
    if bt_backup_dir:
        try:
            shutil.copy(pdfpath,bt_backup_dir)
        except:
            pass