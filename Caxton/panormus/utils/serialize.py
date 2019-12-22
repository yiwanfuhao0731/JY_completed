import locale

import matplotlib.colors as mplc

import numpy as np

import seaborn as sns

import xml.etree.ElementTree as ET

import pandas as pd

locale.setlocale(locale.LC_ALL, "")

RED = np.array([255, 0, 0]) / 255.

BLACK = np.array([0, 0, 0]) / 255.

WHITE = np.array([255, 255, 255]) / 255.

YELLOW = np.array([255, 255, 0]) / 255.

BLUE = np.array([0, 0, 255]) / 255.

MEDIUM_RED = np.array([255, 102, 102]) / 255.

MEDIUM_YELLOW = np.array([255, 255, 102]) / 255.

PORTRAIT_RED = np.array([255, 191, 82]) / 255.

PORTRAIT_GREEN = np.array([141, 235, 156]) / 255.

RED_HEX = mplc.rgb2hex(RED)

BLACK_HEX = mplc.rgb2hex(BLACK)

WHITE_HEX = mplc.rgb2hex(WHITE)

YELLOW_HEX = mplc.rgb2hex(YELLOW)

BLUE_HEX = mplc.rgb2hex(BLUE)

MEDIUM_RED_HEX = mplc.rgb2hex(MEDIUM_RED)

MEDIUM_YELLOW_HEX = mplc.rgb2hex(MEDIUM_YELLOW)

PORTRAIT_RED_HEX = mplc.rgb2hex(PORTRAIT_RED)

PORTRAIT_GREEN_HEX = mplc.rgb2hex(PORTRAIT_GREEN)

FONT_NORMAL = 'normal'

FONT_BOLD = 'bold'

PORTRAIT_COLOR_MAP = {

    'red-green': sns.blend_palette(

        [PORTRAIT_RED, WHITE, PORTRAIT_GREEN], as_cmap=True),

    'red-white': sns.blend_palette(

        [PORTRAIT_RED, WHITE], as_cmap=True),

    'red-blue': sns.blend_palette(

        [RED, WHITE, BLUE], as_cmap=True),

}


def is_numeric(scalar):
    '''

    Generically test for numpy numeric type or built-in int or float

    '''

    return isinstance(scalar, (int, float, np.number))


def remove_border(val, flag, remove_top):
    '''

    :description: Remove the border of a html table to realize the same effect similar to "merge cells" in excel

    :param val: a table entry. Type can be either string or float

    :param flag: a flag with same data type as val. If val is equal to flag, then remove table border

    :param bool remove_top: remove top border if remove_top is true

    :return: None

    '''

    if remove_top:

        border_str = 'border-top-color:'

    else:

        border_str = 'border-bottom-color:'

    if val == flag:

        return border_str + 'white'

    else:

        return border_str + 'black'


def font_weight_high_low(

        val, high_val, low_val,

        high_weight=FONT_BOLD,

        low_weight=FONT_BOLD,

        default_weight=FONT_NORMAL

):
    '''

    :description: Assign different font weights for a table entry depending on a user specified threshold.

    :param float val: a table entry.

    :param float high_val: high weight format will be assigned if val is greater than or equal to high_val.

    :param float low_val: low weight format will be assigned if val is smaller than or equal to low_val.

    :param constant high_weight: constant defined in format.py, either bold or normal.

    :param constant low_weight: constant defined in format.py, either bold or normal.

    :param constant default_weight: constant defined in format.py, either bold or normal.

    :return: None

    '''

    font_weight_str = 'font-weight: '

    if val >= high_val:

        return font_weight_str + high_weight

    elif val <= low_val:

        return font_weight_str + low_weight

    else:

        return font_weight_str + default_weight


def font_color_high_low(

        val, high_val, low_val,

        high_color=BLACK_HEX,

        low_color=RED_HEX,

        default_color=WHITE_HEX

):
    '''

    :description: Assign different colors for a table entry depending on a user specified threshold.

    :param float val: a table entry.

    :param float high_val: high color will be assigned if val is greater than or equal to high_val.

    :param float low_val: low color will be assigned if val is smaller than or equal to low_val.

    :param constant high_color: constant defined in format.py.

    :param constant low_color: constant defined in format.py.

    :param constant default_color: constant defined in format.py.

    :return: None

    '''

    font_color_str = 'color: '

    if val >= high_val:

        return font_color_str + high_color

    elif val <= low_val:

        return font_color_str + low_color

    else:

        return font_color_str + default_color


def background_color_high_low(

        val, high_val, low_val,

        high_color=PORTRAIT_GREEN_HEX,

        low_color=PORTRAIT_RED_HEX,

        default_color=WHITE_HEX

):
    '''

    :description: Assign different background colors for a table entry depending on a user specified threshold.

    :param float val: a table entry.

    :param float high_val: high background color will be assigned if val is greater than or equal to high_val.

    :param float low_val: low background color will be assigned if val is smaller than or equal to low_val.

    :param constant high_color: constant defined in format.py.

    :param constant low_color: constant defined in format.py.

    :param constant default_color: constant defined in format.py.

    :return: None

    '''

    background_color_str = 'background-color: '

    if val >= high_val:

        return background_color_str + high_color

    elif val <= low_val:

        return background_color_str + low_color

    else:

        return background_color_str + default_color


def background_color_from_map(value_in_unit_window,

                              color_map=PORTRAIT_COLOR_MAP['red-green']):
    return 'background-color: ' + mplc.rgb2hex(color_map(value_in_unit_window))


def format_and_link(

        df_data, df_format=None, df_link=None, extra_properties=None, table_attributes=None,

        data_format='%0.2f', float_commas=True, column_format_dict=None

):
    '''

    :description: Combines 3 dataframes into a single df styler.

    :param pd.DataFrame df_data: data to display in table.

    :param None|pd.DataFrame df_format: styler matching specs of df_data

    :param None|pd.DataFrame df_link: links matching specs of df_data

    :param dict|None extra_properties: dict of properties passed to styler.set_properties method.

    :param dict|None table_attributes: dict of properties passed to styler.set_table_attributes.

    :param str|pd.DataFrame data_format: default formatting string for numeric data, e.g. '%0.2f' or %s. \

    Or a dataframe indexed like df_data with format strings per cell, superseded by column_format_dict.

    :param bool float_commas: boolean for using commas (thousands) in numeric data format.

    :param None|dict column_format_dict: formatting override for numeric data, e.g. '%0.2f' applied to \

    columns by dict key.

    :return: dataframe with df data formatted and linked.

    '''

    if not column_format_dict:
        column_format_dict = {}

    df_styled = df_data.copy()

    # Apply links and string casting

    if df_link is not None:

        for row_idx, row in df_styled.iterrows():

            for col_idx, val in row.iteritems():

                if is_numeric(df_styled.loc[row_idx, col_idx]):
                    default_fmt_str = column_format_dict.get(col_idx, data_format.loc[row_idx, col_idx] if isinstance(

                        data_format, pd.DataFrame) else data_format)

                    df_styled.loc[row_idx, col_idx] = locale.format(

                        default_fmt_str, df_data.loc[row_idx, col_idx], grouping=float_commas)

                if df_link.loc[row_idx, col_idx] is not None:
                    df_styled.loc[row_idx, col_idx] = (r'<a href="%s">' + df_styled.loc[row_idx, col_idx] +

                                                       r'</a>') % df_link.loc[row_idx, col_idx]

    else:

        for row_idx, row in df_styled.iterrows():

            for col_idx, val in row.iteritems():

                if is_numeric(df_styled.loc[row_idx, col_idx]):
                    default_fmt_str = column_format_dict.get(col_idx, data_format.loc[row_idx, col_idx] if isinstance(

                        data_format, pd.DataFrame) else data_format)

                    df_styled.loc[row_idx, col_idx] = locale.format(

                        default_fmt_str, df_data.loc[row_idx, col_idx], grouping=float_commas)

    # Apply cell formats to stringified data

    if df_format is not None:

        # In case links are supplied, push styling down to the <a> tag

        for row_idx, row in df_format.iterrows():

            for col_idx, val in row.iteritems():

                if df_format.loc[row_idx, col_idx]:
                    df_styled.loc[row_idx, col_idx] = df_styled.loc[row_idx, col_idx].replace(

                        '<a ', '<a style=%s ' % df_format.loc[row_idx, col_idx])

        # Apply format to td tags

        df_styled = df_styled.style.apply(lambda _: df_format, axis=None)

    else:

        df_styled = df_styled.style

    # Apply extra table-level properties

    if extra_properties is not None:
        df_styled = df_styled.set_properties(**extra_properties)

    # Apply attributes on table tag

    if table_attributes is not None:
        attr_str = ' '.join(['%s=%s' % (k, v) for k, v in table_attributes.items()])

        df_styled = df_styled.set_table_attributes(attr_str)

    return df_styled


def df_to_html(df, width=750, align_text='center', align_index='left', data_format='%0.2f', border=1):
    """

    :return: a Pandas Dataframe converted to html - to be included into PDF

    """

    table_attr = 'border={} width={} style="text-align:{}; border-collapse:collapse;"'.format(border, width, align_text)

    table_styles = [

        {'selector': 'td', 'props': [('white-space', 'nowrap')]},

        {'selector': 'th',

         'props': [('white-space', 'nowrap'), ('background-color', 'LightBlue'), ("text-align", align_index)]},

        {'selector': '.col_heading',

         'props': [('white-space', 'nowrap'), ('background-color', 'LightBlue'), ("text-align", "center")]}

    ]

    format_df = df.applymap(

        lambda x: font_color_high_low(x, high_val=0.0001, low_val=-0.0001))

    style_obj = format_and_link(df, df_format=format_df, df_link=None,

                                extra_properties=None, table_attributes=None,

                                data_format=data_format, float_commas=True, column_format_dict={})

    return style_obj.set_table_attributes(table_attr).set_table_styles(table_styles).render()


def list_to_html_table(string_list, axis=1):
    '''

    :description: Wraps a list of strings in html to generate an html table as row or column.

    :param list string_list: list of strings to put in html table.

    :param int axis: 1 for 1 column, 0 for 1 row.

    :return: html <table> block.

    '''

    if 0 == axis:

        table_body = '</td><td>'.join(string_list)

    elif 1 == axis:

        table_body = '</td></tr><td><tr>'.join(string_list)

    else:

        raise ValueError('Invalid axis. Set axis=1 to stack into a column, or axis=0 to stack into a row.')

    return '<table><tr><td>' + table_body + '</td></tr></table>'


def dict_to_html_table(string_dict, axis=1):
    '''

    :description: Wraps a dict of strings in html to generate an html table.

    :param dict string_dict: dictionry with strings for keys and values.

    :param int axis: 0 for a 2-row table. 1 for a 2-column table.

    :return: html <table> block.

    '''

    if 0 == axis:

        table_body = '</td><td>'.join([k for k in string_dict]) + '</td></tr><td><tr>' + '</td><td>'.join(

            [string_dict[k] for k in string_dict])

    elif 1 == axis:

        table_body = '</td></tr><td><tr>'.join([k + '</td><td>' + string_dict[k] for k in string_dict])

    else:

        raise ValueError('Invalid axis. Set axis=1 for a 2-column table or axis=0 for a 2-row table.')

    return '<table><tr><td>' + table_body + '</td></tr></table>'


def wrap_html_body(html):
    '''

    :description: Wraps html with a <html><body>.

    :param str html: html without body tag or html

    :return: wrapped html.

    '''

    return '<html><body>%s</body></html>' % html


def write_dict_to_ot_xml_format(asof_date, quote_dict, xml_path, quote_type, market='Official', append_mode=True):
    '''

    :description: This function is used for Orchestrade. Write a dict (key: quote name => value: bid/ask) to  \

    Orchestrade QuoteXML format

    :param datetime.date asof_date: as of date for the quote

    :param quote_dict: the quote name - quote value dictionary

    :param xml_path: xml path to be serialized in the file system

    :param quote_type: types of quote, 'Price', 'Yield', etc.

    :param market: intended pricing setup in the Orchestrade envinronment

    :param append_mode: whether to append to the XML or overwrite it

    :return:

    '''

    quote_date_str = asof_date.strftime('%Y%m%d')

    if append_mode:

        tree = ET.parse(xml_path)

        root = tree.getroot()



    else:

        root = ET.Element('ArrayOfQuoteXML',

                          **{'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'},

                          **{'xmlns:xsd': 'http://www.w3.org/2001/XMLSchema'})

        tree = ET.ElementTree(root)

    for k, v in quote_dict.items():
        quote = ET.SubElement(root, 'QuoteXML')

        close_name = ET.SubElement(quote, 'CloseName')

        close_name.text = market

        name = ET.SubElement(quote, 'Name')

        name.text = k

        quote_date = ET.SubElement(quote, 'QuoteDate')

        quote_date.text = quote_date_str

        bid = ET.SubElement(quote, 'Bid')

        bid.text = str(v)

        Ask = ET.SubElement(quote, 'Ask')

        Ask.text = str(v)

        quotation = ET.SubElement(quote, 'Quotation')

        quotation.text = quote_type

    tree.write(xml_path, encoding='UTF-8', xml_declaration=True)


def apply_heatmap_to_dataframe(df_data, col_idx, lower_bound, upper_bound, color_map=PORTRAIT_COLOR_MAP['red-blue']):
    '''

    :description: apply heatmap to chosen columns of a dataframe

    :param df_data: dataframe with only numerical data columns

    :param col_idx: the list of column indices to apply heatmap

    :param lower_bound: lower bound of the heatmap

    :param upper_bound: upper bound of the heatmap

    :param color_map: seaborn color map definition

    :return: a dataframe specifying heatmap formatting with same dimension with df_data

    '''

    df_format = pd.DataFrame().reindex_like(df_data)

    for i in col_idx:
        df_format[df_format.columns[i]] = df_data[df_data.columns[i]].apply(

            lambda x: background_color_from_map(min(max((x - lower_bound) / (upper_bound - lower_bound), 0), 0.9999),

                                                color_map))

    return df_format.replace(np.nan, '')


def apply_highlight_to_dataframe(df_data, idx_col_tuple_list, highlight_color=YELLOW_HEX):
    '''

    :description: apply highlight to chosen cells of a dataframe

    :param df_data: dataframe with only numerical data columns

    :param idx_col_tuple_list: the list of tuples containing the iloc information for the entries to be highlighted

    :param highlight_color: highlighting color

    :return: a dataframe specifying highlight formatting with same dimension with df_data

    '''

    df_format = pd.DataFrame().reindex_like(df_data)

    for i, j in idx_col_tuple_list:
        df_format.iloc[i, j] = background_color_high_low(1, 1, 1, highlight_color, highlight_color, highlight_color)

    return df_format.replace(np.nan, '')