import pandas as pd
import numpy as np
import datetime as dt


def rolling_window(p, window):
    '''
    rolling window for rolling calculation
    '''

    shape = p.shape[:-1] + (p.shape[-1] - window + 1, window)
    strides = p.strides + (p.strides[-1],)
    return np.lib.stride_tricks.as_strided(p, shape=shape, strides=strides)


def mva(p, n, type='s'):
    '''
    moving average for series p in window n
    input and output are numpy ndarray
    '''

    p = np.asarray(p)
    length = len(p)
    # change to 1 dimension array
    try:
        if p.shape[1] == 1:
            p = p[:, 0]
    except:
        pass

    if type == 's':
        # simple moving average
        weights = np.ones(n)
        weights /= weights.sum()
        a = np.convolve(p, weights, mode='full')[:length]
    a[:n - 1] = np.nan

    elif type == 'e':
    # exponential moving average
    alpha = 2. / (n + 1)
    # window = np.max([100,n*5])
    # fast but values at the beginning are not accurate
    # extend array to left, fill with the first value
    p_ = np.insert(p, 0, p[0] * np.ones(n * 5))
    weights = [(1 - alpha) ** x for x in range(0, n * 5)]
    weights = np.asarray(weights)
    weights /= weights.sum()
    a_ = np.convolve(p_, weights, mode='full')[:length + n * 5]
    a = a_[n * 5:]
    a[:n - 1] = np.nan

    # slow but values at the beginning are accurate
    # only use this to calculate first 200 values
    # idx = np.where(~np.isnan(p))[0][0]
    idx = 0
    a[n - 1 + idx] = np.mean(p[idx:n + idx])
    for i in range(n + idx, np.min([length, n + idx + n * 5])):
        a[i] = alpha * p[i] + (1 - alpha) * a[i - 1]

elif type == 'sm':
# smoothed moving average
a = np.empty((length,))
a.fill(np.NaN)
# first non nan in data
idx = np.where(~np.isnan(p))[0][0]
a[n - 1 + idx] = np.mean(p[idx:n + idx])
for i in range(n + idx, length):
    a[i] = (a[i - 1] * (n - 1) + p[i]) / n

return a


def macd(p, s=12, l=26, sig_line=9, ma_type='e'):
    '''
     macd, input and output is np array
     '''

    mv1 = mva(p, s, ma_type)
    mv2 = mva(p, l, ma_type)
    mvd = mv1 - mv2
    mvd_sig = mva(mvd, sig_line, ma_type)
    hist = mvd - mvd_sig

    return mvd, mvd_sig, hist


def bollinger(p, window, thr):
    '''
    bollinger band
    '''

    mv = mva(p, window)
    sd = np.empty(len(p))
    sd.fill(np.NaN)
    sd[window - 1:] = np.std(rolling_window(p, window), axis=1)
    upper_band = mv + thr * sd
    lower_band = mv - thr * sd
    band_width = (upper_band - lower_band) / mv
    pctb = (p - lower_band) / (upper_band - lower_band)

    return upper_band, lower_band, band_width, pctb, mv


def donchian(highp, lowp=None, closep=None, window=20):
    '''
    donchian channel
    '''

    if lowp is None and closep is None:
        lowp = highp
        closep = highp

    length = len(closep)
    p_max = np.empty(length)
    p_max.fill(np.NaN)
    p_min = np.empty(length)
    p_min.fill(np.NaN)
    don = np.empty(length)
    don.fill(np.NaN)

    p_max[window - 1:] = np.max(rolling_window(highp, window), axis=1)
    p_min[window - 1:] = np.min(rolling_window(lowp, window), axis=1)

    don = np.empty(len(closep))
    don.fill(np.NaN)

    for i in range(window, len(closep)):
        if closep[i] > p_max[i - 1]:
            don[i] = 1
        elif closep[i] < p_min[i - 1]:
            don[i] = -1
        else:
            don[i] = don[i - 1]

    return p_max, p_min, don


def atr(highp, lowp=None, closep=None, window=14):
    '''
    exp moving average of true range
    '''

    if isinstance(highp, np.float64):
        out = tr(highp, lowp, closep)
    else:
        length = len(highp)
        if window > length:
            print('Period of ATR cannot be larger than length of prices.')
            return

        if window is None or window == 0:
            true_range = tr(highp, lowp, closep)
            out = np.mean(true_range)
        elif window == 1:
            out = tr(highp, lowp, closep)
        else:
            true_range = tr(highp, lowp, closep)
            out = mva(true_range, window, 'e')
    return out


def cta_position():
    sdate = dt.datetime.strptime('1990-01-01', '%Y-%m-%d')
    edate = dt.datetime.now() - dt.timedelta(days=1)
    weekdays = pd.bdate_range(sdate, edate, freq='B')
    output = pd.DataFrame(index=weekdays)

    # get price
    df = pd.read_csv("Y:\\DataShare\\temp\\TY1.csv")
    df.set_index('Date', inplace=True)
    df.index = pd.to_datetime(df.index, format='%d/%m/%Y')

    insname = 'TY1'
    cumbasis = np.cumsum(df['Basis'][::-1])[::-1]
    cumbasis = np.append(cumbasis[1:], 0)
    highp = df['High'].values + cumbasis
    lowp = df['Low'].values + cumbasis
    closep = df['Close'].values + cumbasis
    dates = df.index

    # 10 indicators
    ind1 = np.zeros(len(closep), dtype=int)
    ind2 = np.zeros(len(closep), dtype=int)
    ind3 = np.zeros(len(closep), dtype=int)
    ind4 = np.zeros(len(closep), dtype=int)
    ind5 = np.zeros(len(closep), dtype=int)
    ind6 = np.zeros(len(closep), dtype=int)
    ind7 = np.zeros(len(closep), dtype=int)
    ind8 = np.zeros(len(closep), dtype=int)
    ind9 = np.zeros(len(closep), dtype=int)
    ind10 = np.zeros(len(closep), dtype=int)

    # long term moving averages
    mv50 = mva(closep, 50)
    mv200 = mva(closep, 200)
    mvd = mv50 - mv200
    mvd[np.isnan(mvd)] = 0
    mv200[np.isnan(mv200)] = 0
    ind1[mv200 > 0] = 1
    ind1[mv200 < 0] = -1
    ind2[mvd > 0] = 1
    ind2[mvd < 0] = -1

    # donchian channel
    ind3 = donchian(highp, lowp, closep, 120)[2]
    ind4 = donchian(highp, lowp, closep, 200)[2]
    ind3[np.isnan(ind3)] = 0
    ind4[np.isnan(ind4)] = 0

    # rate of change
    roc1 = df['Close'] - df['Close'].shift(200)
    roc2 = df['Close'] - df['Close'].shift(250)
    ind5[roc1 >= 0] = 1
    ind5[roc1 < 0] = -1
    ind6[roc2 >= 0] = 1
    ind6[roc2 < 0] = -1

    # macd
    macd1 = macd(closep, 60, 130, 45)[2]
    macd1[np.isnan(macd1)] = 0
    ind7[macd1 >= 0] = 1
    ind7[macd1 < 0] = -1
    macd2 = macd(closep, 12, 26, 9)[2]
    macd2[np.isnan(macd2)] = 0
    ind8[macd2 >= 0] = 1
    ind8[macd2 < 0] = -1

    # bollinger
    upp_band1, low_band1 = bollinger(closep, 150, 0.5)[:2]
    upp_band2, low_band2 = bollinger(closep, 150, 1.5)[:2]
    upp_band1[np.isnan(upp_band1)] = 0
    low_band1[np.isnan(low_band1)] = 0
    upp_band2[np.isnan(upp_band2)] = 0
    low_band2[np.isnan(low_band2)] = 0
    ind9[closep > upp_band1] = 1
    ind9[closep < low_band1] = -1
    ind10[closep > upp_band2] = 1
    ind10[closep < low_band2] = -1

    ind = ind1 + ind2 + ind3 + ind4 + ind5 + ind6 + ind7 + ind8 + ind9 + ind10
    ind = pd.Series(ind, index=dates)
    ind = ind.reindex(weekdays, method='ffill')
    output[insname] = ind
    # output = output.rolling(5).mean()
    output.to_csv("Y:\\DataShare\\temp\\TY1_cta.csv")


if __name__ == '__main__':
    cta_position()
