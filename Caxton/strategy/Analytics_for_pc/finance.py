import numpy as np


def calc_duration(ytm, maturity, number_of_payment=2, forward_starting=0, type='Mod'):
    # number of payment is number of payment per year
    # this is the Mac duration
    ytm = ytm / 100
    if np.isnan(ytm):
        return np.nan
    if ytm <= 0.000000000001:
        return maturity
    n = maturity * number_of_payment
    cash = [ytm * 100 / number_of_payment for _ in range(n)]
    cash[-1] += 100
    discounted = [cash[i] / (1 + ytm / number_of_payment) ** ((i + forward_starting * number_of_payment + 1)) for i in
                  range(len(cash))]
    price = sum(discounted)
    # print (price)
    if type == 'Mac':
        return sum([i * (t + 1) for t, i in enumerate(discounted)]) / price / number_of_payment
    elif type == 'Mod':
        return sum([i * (t + 1) for t, i in enumerate(discounted)]) / price / number_of_payment / (
                    1 + ytm / number_of_payment)

