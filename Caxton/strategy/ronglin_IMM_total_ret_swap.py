import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

' this is swap names for ins_name in get_imm_swap()
name_list = ['USD_3M', 'EUR_6M', 'GBP_6M', 'JPY_6M',
             'CHF_6M', 'AUD_6M', 'NZD_3M', 'CAD_3M',
             'NOK_6M', 'SEK_3M']


def get_imm_swap(ins_name, tenor, imm='IMM1'):
    '''
    get IMM swap price
    Input:
    ins_name: name from name_list
    tenor: '2Y','5Y','10Y'
    imm: the first two IMMs, IMM1 and IMM2

    Output:
    price: the spot price used as entry price
    price_adj: roll adjusted price used for calculating pnl, assumes DV01 constant
    '''

    hdf_path = "Y:\\DataShare\\IMM_total_return.h5"
    price_df = pd.read_hdf(hdf_path, ins_name + '_' + tenor + '_' + imm)
    price_df[price_df == -10000] = np.NaN
    price_df.fillna(method='ffill', inplace=True)
    price_df.fillna(method='bfill', inplace=True)
    price = price_df['CloseP']
    price = price.to_frame('closep')
    cumbasis = np.cumsum(price_df['Basis'].values[::-1])[::-1]
    cumbasis = np.append(cumbasis[1:], 0)
    price_adj = price_df['CloseP'] + cumbasis
    price_adj = price_adj.to_frame('closep')

    return price, price_adj


if __name__ == '__main__':
    price, price_adj = get_imm_swap('USD_3M', '5Y', imm='IMM1')
    plt.plot(price, label='spot')
    plt.plot(price_adj, label='roll adjusted')
    plt.legend()
    plt.show()