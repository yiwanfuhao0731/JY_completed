import panormus.data.open_data as opendata


def load_uncached(source_tickers, start_date, end_date):
    data = opendata.df_for_observable_strings(source_tickers, start_date, end_date)
    data.columns = ['|'.join(x) for x in data.columns]
    data.index = [i_dt.replace(tzinfo=None) for i_dt in data.index]
    return data


if __name__ == "__main__":
    import lib_ticker_fns as tck

    ccy = 'usd'
    tenor = '10y'
    metric = 'par'
    expiries_str = ['', '1m', '3m']
    start_date = None
    end_date = None
    source_tickers = [tck.od_swap_rate(ccy, tenor, i_exp, metric) for i_exp in expiries_str]

pvk_color_fns.py

from colour import Color


def name2hex(name):
    return Color(name).hex


def housecolor(color=None):
    colors = dict(
        dblu=(23 / 255, 54 / 255, 93 / 255),
        mred=(192 / 255, 0 / 255, 0 / 255),
        grey=(127 / 255, 127 / 255, 127 / 255),
        lblu=(141 / 255, 179 / 255, 226 / 255),
        pink=(229 / 255, 185 / 255, 183 / 255),
        purp=(95 / 255, 73 / 255, 122 / 255),
        blck=(0 / 255, 0 / 255, 0 / 255),
        mrsh=(148 / 255, 138 / 255, 84 / 255),
        mblu=(66 / 255, 130 / 255, 208 / 255),
        lred=(188 / 255, 105 / 255, 105 / 255),
        lgry=(167 / 255, 167 / 255, 167 / 255),
        bdux=(152 / 255, 60 / 255, 54 / 255),
        lprp=(158 / 255, 137 / 255, 184 / 255),

        dgry=(105 / 255, 105 / 255, 105 / 255),
        llgr=(208 / 255, 208 / 255, 208 / 255),
    )

    if color is None:
        return colors
    else:
        return colors[color]