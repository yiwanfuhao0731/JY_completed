class Signal(abs_sig):

    def __init__(self, country, **params_ovrd):
        # definitions of the signal
        self.signal_obj_hierarchy = {}

        self.config_file = CONFIG_FILE

        self.data_tab_mapping = {'usd', 'basket_USD_Rates1',
                                 'cad'}
        self.params = get_params(self.config_file, **params_ovrd)

    def load_market_date(self):
        hierarchy = self.get_hir
        tickers = self.tickers
        data = load_md(tickers, sd, ed)
        return data

    def get_market_data(self, recompute):
        if self.market_date is None or recompute:
            self.market_date = self.load_market_data()

    def set_market_data(self, market_data):
        self.market_date = market_data

    def get_signal(self, recompute):
        hierarcy = self.get
        hier

        data = self.get_data()
        compute_signal
        return signal

    def get_twi(self):
        if stored.age < 12:
            return stored
        else:
            return compute_twi