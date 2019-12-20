'''
Created on 2019年3月23日

@author: user
'''
from collections import OrderedDict

OD_CURVE_RATE_CORE_DICT = {
    'USD.OIS': {
        'item_prefix': 'rates_parswap_usd_ois_annual',
        'fra_prefix': 'rates_fra_usd_ois',
        'cut': 'nyclose',
    },
    'USD.1ML': {
        'item_prefix': 'rates_parswap_usd_usdlibor1m_annual',
        'fra_prefix': 'rates_fra_usd_usdlibor1m',
        'cut': 'nyclose',
    },
    'USD.3ML': {
        'item_prefix': 'rates_parswap_usd_usdlibor3m_semi',
        'fra_prefix': 'rates_fra_usd_usdlibor3m',
        'cut': 'nyclose',
    },
    'USD.6ML': {
        'item_prefix': 'rates_parswap_usd_usdlibor6m_semi',
        'fra_prefix': 'rates_fra_usd_usdlibor6m',
        'cut': 'nyclose',
    },
    'CHF.OIS': {
        'item_prefix': 'rates_parswap_chf_tois_annual',
        'fra_prefix': 'rates_fra_chf_tois',
        'cut': 'lonclose',
    },
    'CHF.1ML': {
        'item_prefix': 'rates_parswap_chf_chflibor1m_annual',
        'fra_prefix': 'rates_fra_chf_chflibor31',
        'cut': 'lonclose',
    },
    'CHF.3ML': {
        'item_prefix': 'rates_parswap_chf_chflibor3m_annual',
        'fra_prefix': 'rates_fra_chf_chflibor3m',
        'cut': 'lonclose',
    },
    'CHF.6ML': {
        'item_prefix': 'rates_parswap_chf_chflibor6m_annual',
        'fra_prefix': 'rates_fra_chf_chflibor6m',
        'cut': 'lonclose',
    },
    'EUR.OIS': {
        'item_prefix': 'rates_parswap_eur_eonia_annual',
        'fra_prefix': 'rates_fra_eur_eonia',
        'cut': 'lonclose',
    },
    'EUR.1ML': {
        'item_prefix': 'rates_parswap_eur_euribor1m_annual',
        'fra_prefix': 'rates_fra_eur_euribor1m',
        'cut': 'lonclose',
    },
    'EUR.3ML': {
        'item_prefix': 'rates_parswap_eur_euribor3m_annual',
        'fra_prefix': 'rates_fra_eur_euribor3m',
        'cut': 'lonclose',
    },
    'EUR.6ML': {
        'item_prefix': 'rates_parswap_eur_euribor6m_annual',
        'fra_prefix': 'rates_fra_eur_euribor6m',
        'cut': 'lonclose',
    },
    'GBP.OIS': {
        'item_prefix': 'rates_parswap_gbp_sonia_annual',
        'fra_prefix': 'rates_fra_gbp_sonia',
        'cut': 'lonclose',
    },
    'GBP.1ML': {
        'item_prefix': 'rates_parswap_gbp_gbplibor1m_annual',
        'fra_prefix': 'rates_fra_gbp_gbplibor1m',
        'cut': 'lonclose',
    },
    'GBP.3ML': {
        'item_prefix': 'rates_parswap_gbp_gbplibor3m_quarterly',
        'fra_prefix': 'rates_fra_gbp_gbplibor3m',
        'cut': 'lonclose',
    },
    'GBP.6ML': {
        'item_prefix': 'rates_parswap_gbp_gbplibor6m_semi',
        'fra_prefix': 'rates_fra_gbp_gbplibor6m',
        'cut': 'lonclose',
    },
    'CAD.OIS': {
        'item_prefix': 'rates_parswap_cad_corra_annual',
        'fra_prefix': 'rates_fra_cad_corra',
        'cut': 'nyclose',
    },
    'CAD.3ML': {
        'item_prefix': 'rates_parswap_cad_cdor3m_semi',
        'fra_prefix': 'rates_fra_cad_cdor3m',
        'cut': 'nyclose',
    },
    'JPY.OIS': {
        'item_prefix': 'rates_parswap_jpy_tonar_annual',
        'fra_prefix': 'rates_fra_jpy_tonar',
        'cut': 'tkyclose',
    },
    'JPY.1ML': {
        'item_prefix': 'rates_parswap_jpy_jpylibor1m_annual',
        'fra_prefix': 'rates_fra_jpy_jpylibor1m',
        'cut': 'tkyclose',
    },
    'JPY.3ML': {
        'item_prefix': 'rates_parswap_jpy_jpylibor3m_quarterly',
        'fra_prefix': 'rates_fra_jpy_jpylibor3m',
        'cut': 'tkyclose',
    },
    'JPY.6ML': {
        'item_prefix': 'rates_parswap_jpy_jpylibor6m_semi',
        'fra_prefix': 'rates_fra_jpy_jpylibor6m',
        'cut': 'tkyclose',
    },
    'SEK.OIS': {
        'item_prefix': 'rates_parswap_sek_stina_annual',
        'fra_prefix': 'rates_fra_sek_stina',
        'cut': 'lonclose',
    },
    'SEK.3ML': {
        'item_prefix': 'rates_parswap_sek_stibor3m_annual',
        'fra_prefix': 'rates_fra_sek_stibor3m',
        'cut': 'lonclose',
    },
    'AUD.OIS': {
        'item_prefix': 'rates_parswap_aud_aonia_annual',
        'fra_prefix': 'rates_fra_aud_aonia',
        'cut': 'tkyclose',
    },
    'AUD.3ML': {
        'item_prefix': 'rates_parswap_aud_bbsw3m_quarterly',
        'fra_prefix': 'rates_fra_aud_bbsw3m',
        'cut': 'tkyclose',
    },
    'AUD.6ML': {
        'item_prefix': 'rates_parswap_aud_bbsw6m_semi',
        'fra_prefix': 'rates_fra_aud_bbsw6m',
        'cut': 'tkyclose',
    },
    'NOK.OIS': {
        'item_prefix': 'rates_parswap_nok_nowa_annual',
        'fra_prefix': 'rates_fra_nok_nowa',
        'cut': 'lonclose',
    },
    'NOK.3ML': {
        'item_prefix': 'rates_parswap_nok_nibor3m_annual',
        'fra_prefix': 'rates_fra_nok_nibor3m',
        'cut': 'lonclose',
    },
    'NOK.6ML': {
        'item_prefix': 'rates_parswap_nok_nibor6m_annual',
        'fra_prefix': 'rates_fra_nok_nibor6m',
        'cut': 'lonclose',
    },
    'DKK.OIS': {
        'item_prefix': 'rates_parswap_dkk_cita_annual',
        'fra_prefix': 'rates_fra_dkk_cita',
        'cut': 'lonclose',
    },
    'DKK.3ML': {
        'item_prefix': 'rates_parswap_dkk_cibor3m_annual',
        'fra_prefix': 'rates_fra_dkk_cibor3m',
        'cut': 'lonclose',
    },
    'DKK.6ML': {
        'item_prefix': 'rates_parswap_dkk_cibor6m_annual',
        'fra_prefix': 'rates_fra_dkk_cibor6m',
        'cut': 'lonclose',
    },
    'NZD.OIS': {
        'item_prefix': 'rates_parswap_nzd_nonia_annual',
        'fra_prefix': 'rates_fra_nzd_nonia',
        'cut': 'tkyclose',
    },
    'NZD.3ML': {
        'item_prefix': 'rates_parswap_nzd_bbr3m_semi',
        'fra_prefix': 'rates_fra_nzd_bbr3m',
        'cut': 'tkyclose',
    },
    'HUF.OIS': {
        'item_prefix': 'rates_parswap_huf_hufonia_annual',
        'fra_prefix': 'rates_fra_huf_hufonia',
        'cut': 'lonclose',
    },
    'HUF.3ML': {
        'item_prefix': 'rates_parswap_huf_bubor3m_annual',
        'fra_prefix': 'rates_fra_huf_bubor3m',
        'cut': 'lonclose',
    },
    'HUF.6ML': {
        'item_prefix': 'rates_parswap_huf_bubor6m_annual',
        'fra_prefix': 'rates_fra_huf_bubor6m',
        'cut': 'lonclose',
    },
    'PLN.OIS': {
        'item_prefix': 'rates_parswap_pln_polonia_annual',
        'fra_prefix': 'rates_fra_pln_polonia',
        'cut': 'lonclose',
    },
    'PLN.3ML': {
        'item_prefix': 'rates_parswap_pln_wibor3m_annual',
        'fra_prefix': 'rates_fra_pln_wibor3m',
        'cut': 'lonclose',
    },
    'PLN.6ML': {
        'item_prefix': 'rates_parswap_pln_wibor6m_annual',
        'fra_prefix': 'rates_fra_pln_wibor6m',
        'cut': 'lonclose',
    },
    'CZK.OIS': {
        'item_prefix': 'rates_parswap_czk_czeonia_annual',
        'fra_prefix': 'rates_fra_czk_czeonia',
        'cut': 'lonclose',
    },
    'CZK.3ML': {
        'item_prefix': 'rates_parswap_czk_pribor3m_annual',
        'fra_prefix': 'rates_fra_czk_pribor3m',
        'cut': 'lonclose',
    },
    'CZK.6ML': {
        'item_prefix': 'rates_parswap_czk_pribor6m_annual',
        'fra_prefix': 'rates_fra_czk_wpribor6m',
        'cut': 'lonclose',
    },
}

OD_SWAPTION_CORE_DICT = {
    'USD.3ML': {
        'item_prefix': 'rates_swaption_usd_usdlibor3m_semi',
        'cut': 'nyclose',
    },
    'EUR.6ML': {
        'item_prefix': 'rates_swaption_eur_euribor6m_annual',
        'cut': 'lonclose',
    },
    'GBP.6ML': {
        'item_prefix': 'rates_swaption_gbp_gbplibor6m_semi',
        'cut': 'lonclose',
    },
}

ICAP_SWAPTION_GRID_DICT = {
    'USD.3ML': {
        'expiries': ['1m', '3m', '6m', '1y', '2y', '3y', '4y',
                     '5y', '7y', '10y', '15y', '20y'],
        'tenors': ['3m', '1y', '2y', '3y', '4y', '5y', '7y', '10y',
                   '15y', '20y', '30y'],
    },
    'EUR.6ML': {
        'expiries': ['1m', '3m', '6m', '1y', '2y', '3y', '4y',
                     '5y', '7y', '10y', '15y', '20y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y',
                   '9y', '10y', '15y', '20y', '25y', '30y'],
    },
    'GBP.6ML': {
        'expiries': ['1m', '3m', '6m', '1y', '2y', '3y', '4y',
                     '5y', '7y', '10y', '15y', '20y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y',
                   '9y', '10y', '15y', '20y', '25y', '30y'],
    },
}

BARCAP_SWAPTION_FILE_DICT = {
    'USD.3ML': {
        'expiries': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'tenors': ['3m', '1y', '2y', '3y', '4y', '5y', '7y', '10y', '12y',
                   '15y', '20y', '30y'],
        'flat_extrap_expiries': ['15y', '20y'],
    },
    'EUR.6ML': {
        'expiries': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y', '15y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y',
                   '9y', '10y', '12y', '15y', '20y', '25y', '30y'],
        'flat_extrap_expiries': ['20y'],
    },
    'GBP.6ML': {
        'expiries': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y', '15y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y',
                   '9y', '10y', '12y', '15y', '20y', '25y', '30y'],
        'flat_extrap_expiries': ['20y'],
    },
}

OD_DERIVED_MIDCURVE_GRID_DICT = {
    'USD.3ML': {
        'expiries': ['0d', '1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'forwards': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '7y', '10y', '12y',
                   '15y', '20y', '30y'],
    },
    'EUR.6ML': {
        'expiries': ['0d', '1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'forwards': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '7y', '10y', '12y',
                   '15y', '20y', '30y'],
    },
    'GBP.6ML': {
        'expiries': ['0d', '1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'forwards': ['1m', '3m', '6m', '9m', '1y', '18m', '2y', '3y', '4y',
                     '5y', '7y', '10y'],
        'tenors': ['3m', '6m', '1y', '2y', '3y', '4y', '5y', '7y', '10y', '12y',
                   '15y', '20y', '30y'],
    },
}

CCY_OBS_DICT = {
    'USD': {
        'item': 'rates_parswap_usd_usdlibor3m_semi',
        'cut': 'nyclose',
        'source': 'rates_grp_clean',
    },
    'EUR': {
        'item': 'rates_parswap_eur_euribor6m_annual',
        'cut': 'lonclose',
        'source': 'rates_grp_clean',
    },
    'GBP': {
        'item': 'rates_parswap_gbp_gbplibor6m_semi',
        'cut': 'lonclose',
        'source': 'rates_grp_clean',
   }
}

OD_UPLOAD_COLS = ['Item', 'Attribute', 'Cut', 'Source', 'Timestamp', 'Value']

OD_RATE_FIXING_CONIFG = {
    'USD.1ML': 'rates_fixing_usd_usdlibor1m|rate|nyclose|qa_grp_snap',
    'USD.2ML': 'rates_fixing_usd_usdlibor2m|rate|nyclose|qa_grp_snap',
   'USD.3ML': 'rates_fixing_usd_usdlibor3m|rate|nyclose|qa_grp_snap',
    'USD.6ML': 'rates_fixing_usd_usdlibor6m|rate|nyclose|qa_grp_snap',
    'USD.12ML': 'rates_fixing_usd_usdlibor12m|rate|nyclose|qa_grp_snap',
    'CAD.1ML': 'rates_fixing_cad_cdor1m|rate|nyclose|qa_grp_snap',
    'CAD.2ML': 'rates_fixing_cad_cdor2m|rate|nyclose|qa_grp_snap',
    'CAD.3ML': 'rates_fixing_cad_cdor3m|rate|nyclose|qa_grp_snap',
    'CAD.6ML': 'rates_fixing_cad_cdor6m|rate|nyclose|qa_grp_snap',
    'CAD.12ML': 'rates_fixing_cad_cdor12m|rate|nyclose|qa_grp_snap',
    'EUR.1ML': 'rates_fixing_eur_euribor1m|rate|lonclose|qa_grp_snap',
    'EUR.2ML': 'rates_fixing_eur_euribor2m|rate|lonclose|qa_grp_snap',
    'EUR.3ML': 'rates_fixing_eur_euribor3m|rate|lonclose|qa_grp_snap',
    'EUR.6ML': 'rates_fixing_eur_euribor6m|rate|lonclose|qa_grp_snap',
    'EUR.12ML': 'rates_fixing_eur_euribor12m|rate|lonclose|qa_grp_snap',
    'GBP.1ML': 'rates_fixing_gbp_gbplibor1m|rate|lonclose|qa_grp_snap',
    'GBP.2ML': 'rates_fixing_gbp_gbplibor2m|rate|lonclose|qa_grp_snap',
    'GBP.3ML': 'rates_fixing_gbp_gbplibor3m|rate|lonclose|qa_grp_snap',
    'GBP.6ML': 'rates_fixing_gbp_gbplibor6m|rate|lonclose|qa_grp_snap',
    'GBP.12ML': 'rates_fixing_gbp_gbplibor12m|rate|lonclose|qa_grp_snap',
    'JPY.1ML': 'rates_fixing_jpy_jpylibor1m|rate|tkyclose|qa_grp_snap',
    'JPY.2ML': 'rates_fixing_jpy_jpylibor2m|rate|tkyclose|qa_grp_snap',
    'JPY.3ML': 'rates_fixing_jpy_jpylibor3m|rate|tkyclose|qa_grp_snap',
    'JPY.6ML': 'rates_fixing_jpy_jpylibor6m|rate|tkyclose|qa_grp_snap',
    'JPY.12ML': 'rates_fixing_jpy_jpylibor12m|rate|tkyclose|qa_grp_snap',
    'AUD.1ML': 'rates_fixing_aud_bbsw1m|rate|tkyclose|qa_grp_snap',
    'AUD.2ML': 'rates_fixing_aud_bbsw2m|rate|tkyclose|qa_grp_snap',
    'AUD.3ML': 'rates_fixing_aud_bbsw3m|rate|tkyclose|qa_grp_snap',
    'AUD.6ML': 'rates_fixing_aud_bbsw6m|rate|tkyclose|qa_grp_snap',
    'NZD.1ML': 'rates_fixing_nzd_bbr1m|rate|tkyclose|qa_grp_snap',
    'NZD.2ML': 'rates_fixing_nzd_bbr2m|rate|tkyclose|qa_grp_snap',
    'NZD.3ML': 'rates_fixing_nzd_bbr3m|rate|tkyclose|qa_grp_snap',
    'NZD.6ML': 'rates_fixing_nzd_bbr6m|rate|tkyclose|qa_grp_snap',
    'AUD.OIS': 'rates_fixing_aud_aonia|rate|tkyclose|qa_grp_snap',
    'CHF.1ML': 'rates_fixing_chf_chflibor1m|rate|lonclose|qa_grp_snap',
    'CHF.3ML': 'rates_fixing_chf_chflibor3m|rate|lonclose|qa_grp_snap',
    'CHF.6ML': 'rates_fixing_chf_chflibor6m|rate|lonclose|qa_grp_snap',
    'CHF.OIS': 'rates_fixing_chf_saron|rate|lonclose|qa_grp_snap',
    'DKK.3ML': 'rates_fixing_dkk_cibor3m|rate|lonclose|qa_grp_snap',
    'DKK.6ML': 'rates_fixing_dkk_cibor6m|rate|lonclose|qa_grp_snap',
    'DKK.OIS': 'rates_fixing_dkk_cita|rate|lonclose|qa_grp_snap',
    'EUR.OIS': 'rates_fixing_eur_eonia|rate|lonclose|qa_grp_snap',
    'GBP.OIS': 'rates_fixing_gbp_sonia|rate|lonclose|qa_grp_snap',
    'HUF.3ML': 'rates_fixing_huf_bubor3m|rate|lonclose|qa_grp_snap',
    'HUF.6ML': 'rates_fixing_huf_bubor6m|rate|lonclose|qa_grp_snap',
    'NOK.OIS': 'rates_fixing_nok_nowa|rate|lonclose|qa_grp_snap',
    'NOK.3ML': 'rates_fixing_nok_nibor3m|rate|lonclose|qa_grp_snap',
    'NOK.6ML': 'rates_fixing_nok_nibor6m|rate|lonclose|qa_grp_snap',
    'NZD.OIS': 'rates_fixing_nzd_nonia|rate|tkyclose|qa_grp_snap',
    'PLN.OIS': 'rates_fixing_pln_polonia|rate|lonclose|qa_grp_snap',
    'PLN.3ML': 'rates_fixing_pln_wibor3m|rate|lonclose|qa_grp_snap',
    'PLN.6ML': 'rates_fixing_pln_wibor6m|rate|lonclose|qa_grp_snap',
    'SEK.OIS': 'rates_fixing_sek_stina|rate|lonclose|qa_grp_snap',
    'SEK.3ML': 'rates_fixing_sek_stibor3m|rate|lonclose|qa_grp_snap',
    'SEK.6ML': 'rates_fixing_sek_stibor6m|rate|lonclose|qa_grp_snap',
    'SEK.RIBA': 'rates_fixing_sek_riba|rate|lonclose|qa_grp_snap',
    'USD.1WGC': 'rates_fixing_usd_gcrepotsy1w|rate|nyclose|qa_grp_snap',
    'USD.2WGC': 'rates_fixing_usd_gcrepotsy2w|rate|nyclose|qa_grp_snap',
    'USD.3WGC': 'rates_fixing_usd_gcrepotsy3w|rate|nyclose|qa_grp_snap',
    'USD.1MGC': 'rates_fixing_usd_gcrepotsy1m|rate|nyclose|qa_grp_snap',
    'JPY.OIS': 'rates_fixing_jpy_tonar|rate|tkyclose|qa_grp_snap',
    'CAD.BOC': 'rates_fixing_cad_boc|rate|nyclose|qa_grp_snap',
    'CAD.OIS': 'rates_fixing_cad_corra|rate|nyclose|qa_grp_snap',
    'HUF.OIS': 'rates_fixing_huf_hufonia|rate|lonclose|qa_grp_snap',
    'USD.OIS': 'rates_fixing_usd_fedfunds|rate|nyclose|qa_grp_snap',
    'USD.ONGC': 'rates_fixing_usd_gcrepotsyon|rate|nyclose|qa_grp_snap',
    'CZK.OIS': 'rates_fixing_czk_czeonia|rate|lonclose|qa_grp_snap',
    'CZK.1ML': 'rates_fixing_czk_pribor1m|rate|lonclose|qa_grp_snap',
    'CZK.3ML': 'rates_fixing_czk_pribor3m|rate|lonclose|qa_grp_snap',
    'CZK.6ML': 'rates_fixing_czk_pribor6m|rate|lonclose|qa_grp_snap',
}

OD_BOND_CONFIG = {
    'T': {
        'item_base': 'rates_sovbond_usd_',
        'cut': 'nyclose',
        'source': 'rates_grp_clean'
    }
}

OD_BOND_DEFAULT_ATTRIBUTES = [
    'clean_price', 'dirty_price', 'acc_int', 'ytm', 'asw_libor_matchmat', 'asw_ois_matchmat',
    'pvbp', 'z_spd_libor', 'z_spd_ois'
]

FUTURE_CONTRACT_TICKERS = OrderedDict([
    ('TU', 'rates_bondfuture_usd_tu_%s|price|nyclose|rates_grp_clean'),
    ('FV', 'rates_bondfuture_usd_fv_%s|price|nyclose|rates_grp_clean'),
    ('TY', 'rates_bondfuture_usd_ty_%s|price|nyclose|rates_grp_clean'),
    ('TY-ULTRA', 'rates_bondfuture_usd_ty-ultra_%s|price|nyclose|rates_grp_clean'),
    ('UXY', 'rates_bondfuture_usd_ty-ultra_%s|price|nyclose|rates_grp_clean'),
    ('US', 'rates_bondfuture_usd_us_%s|price|nyclose|rates_grp_clean'),
    ('US-ULTRA', 'rates_bondfuture_usd_us-ultra_%s|price|nyclose|rates_grp_clean'),
    ('WN', 'rates_bondfuture_usd_us-ultra_%s|price|nyclose|rates_grp_clean'),
    ('L ', 'rates_bondfuture_gbp_l_%s|price|lonclose|rates_grp_clean'),
    ('G ', 'rates_bondfuture_gbp_g_%s|price|lonclose|rates_grp_clean'),
    ('CN', 'rates_bondfuture_cad_cn_%s|price|nyclose|rates_grp_clean'),
    ('BA', 'rates_bondfuture_cad_ba_%s|price|nyclose|rates_grp_clean'),
    ('RX', 'rates_bondfuture_eur_rx_%s|price|frkclose|rates_grp_clean'),
])


def build_od_bond_ticker(type, isin, attribute):
    item = OD_BOND_CONFIG[type]['item_base'] + isin.upper()
    cut = OD_BOND_CONFIG[type]['cut']
    source = OD_BOND_CONFIG[type]['source']
    return '|'.join((item, attribute.lower(), cut, source))

 