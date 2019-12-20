def swap_rate(ccy, tenor, fwd_start=''):
    """get default format swap ticker"""
    return f'{ccy}swp_{fwd_start}{tenor}'

print (swap_rate('eur','1m'))

def swap_duration(ccy, tenor, fwd_start=''):
    """get default format swap duration ticker"""
    return f'{ccy}swd_{fwd_start}{tenor}'


def swap_vol(ccy, tenor, expiry):
    """get default format swaption vol ticker"""
    return f'{ccy}swo_{expiry}{tenor}'


def curve_vol(ccy, expiry, tenor_a, tenor_b):
    """get default format cms curve vol ticker"""
    return f'{ccy}swo_{expiry}{tenor_a[:-1]}s{tenor_b[:-1]}s'


def fx_rate(quote_ccy, base_ccy='usd', fwd_start=''):
    """get default format fx rate ticker"""
    if fwd_start is None or fwd_start == '':
        return f'{base_ccy}{quote_ccy}'
    else:
        return f'{base_ccy}{quote_ccy}_{fwd_start}'


###################################################################
# Opendata tickers
###################################################################

def od_swap_rate(ccy, tenor, fwd_start='', metric='par'):
    if fwd_start == '':
        fwd_start = '0d'

    if metric.lower() == 'par':
        metric = 'parcoupon'
    elif metric.lower() in ('ann', 'dur'):
        metric = 'annuity'
    else:
        raise ValueError((f'Metric: \'{metric}\' not supported for OpenData data right now ' +
                          f'(only \'par\' and \'ann\'/\'dur\' atm)'))

    if ccy.lower() == 'usd':
        return f'rates_parswap_usd_usdlibor3m_semi_{fwd_start}_{tenor}|{metric}|nyclose|rates_grp_clean'
    elif ccy.lower() == 'eur':
        return f'rates_parswap_eur_euribor6m_annual_{fwd_start}_{tenor}|{metric}|lonclose|rates_grp_clean'
    else:
        raise ValueError(f'Ccy {ccy} not supported right now.')


###################################################################
# JPMDQ tickers
###################################################################

def jpm_swap_rate(ccy, tenor, fwd_start='', metric='par'):
    if metric.lower() == 'par':
        metric = 'RT_MID'
    elif metric.lower() in ('dur', 'ann'):
        metric = 'AM_MOD_DUR'
    else:
        raise ValueError((f'Metric: \'{metric}\' not supported for JPM data right now ' +
                          f'(only \'par\' and \'ann\'/\'dur\' atm)'))

    if len(fwd_start) > 0:
        fwd_start = fwd_start.zfill(3)

    return f'DB(CCV,CRVSWAPMMKT,{ccy},{tenor},{fwd_start},{metric})'


def jpm_curve_vol(ccy, expiry, tenor_a, tenor_b, metric='vol'):
    if metric.lower() == 'vol':
        metric = 'BPVOL'
    else:
        raise ValueError((f'Metric: \'{metric}\' not supported for JPM data right now ' +
                          f'(only \'vol\')'))

    if ccy.lower() != 'usd':
        raise ValueError((f'Ccy: \'{metric}\' not supported for JPM data right now ' +
                          f'(only \'usd\')'))

    return f'DB(FDER,YCSO,{tenor_a}x{tenor_b},{expiry},S,ATMF,0,{metric})'


def jpm_fx_rate(quote_ccy, base_ccy='usd', fwd_start=''):
    """get default format fx rate ticker"""

    quote_ccy = quote_ccy.lower()
    base_ccy = base_ccy.lower()

    if len(fwd_start) > 0:
        fwd_start = fwd_start.zfill(3)

    if quote_ccy == base_ccy:
        raise ValueError(f'Quote ccy ({quote_ccy}) is the same as base ccy ({base_ccy}')

    if base_ccy == 'usd':
        if quote_ccy in ('eur', 'gbp', 'aud', 'nzd'):
            ticker = f"1 / DB(CFX,{quote_ccy.upper()},{fwd_start})"
        else:
            ticker = f"DB(CFX,{quote_ccy.upper()},{fwd_start})"
    else:
        quote_ticker = f"DB(CFX,{quote_ccy.upper()},{fwd_start})"
        base_ticker = f"DB(CFX,{base_ccy.upper()},{fwd_start})"

        if base_ccy in ('eur', 'gbp', 'aud', 'nzd'):
            if quote_ccy in ('eur', 'gbp', 'aud', 'nzd'):
                ticker = quote_ticker + " / " + base_ticker
            else:
                ticker = "1 / " + base_ticker + " / " + quote_ticker
        else:
            if quote_ccy in ('eur', 'gbp', 'aud', 'nzd'):
                ticker = base_ticker + " * " + quote_ticker
            else:
                ticker = base_ticker + " / " + quote_ticker

    return ticker


###################################################################
# CITI tickers
###################################################################

def citi_swap_rate(ccy, tenor, fwd_start='', metric='par'):
    if metric.lower() != 'par':
        raise ValueError('Citi only has par rates data for swap rates')

    if fwd_start == '':
        return f'RATES.SWAP.{ccy}.PAR.{tenor}'
    else:
        return f'RATES.SWAP.{ccy}.FWD.{fwd_start}.{tenor}'

