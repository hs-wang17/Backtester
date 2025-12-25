import pandas as pd


class stk:
    """
    A class representing a stock.
    """

    def __init__(self, code, price, up_price, low_price, trade_fee=0.001):
        """
        Initialize a stock object.

        Parameters
        ----------
        code : str
            Stock code.
        price : float
            Current price of the stock.
        up_price : float
            Upper limit price of the stock.
        low_price : float
            Lower limit price of the stock.
        sellable_vol : float
            The volume of stock that can be sold.
            (The initial value is 0. After buying, the value equals volume last day.)
        trade_fee : float, optional
            Transaction fee and shock cost of the stock, default is 0.001.
            (Only used when selling the stock.)
        volume : float
            The volume of stock held.
            (The initial value is 0.)
        amt : float
            The total amount of stock held.
            (The initial value is 0.)
        minimum_vol : float
            The minimum volume of stock that can be bought.
            (The initial value is 200 for 68 stocks and 100 for other stocks.
            The stocks starting with 68 are STAR Market stocks, which have a minimum trading unit of 200 shares.
            The other stocks have a minimum trading unit of 100 shares.)
        unit_vol : float
            The trading unit of the stock.
            (The initial value is 1 for 68 stocks and 100 for other stocks.
            The stocks starting with 68 are STAR Market stocks, which can be traded in units of 1 share.
            The other stocks can be traded in units of 100 shares.)
        """
        self.code = code
        self.price = price
        self.up_price = up_price
        self.low_price = low_price
        self.sellable_vol = 0
        self.trade_fee = trade_fee
        self.volume = 0
        self.amt = 0
        prefix = ("%06d" % int(code))[:2]
        self.minimum_vol = 200 if prefix == "68" else 100
        self.unit_vol = 1 if prefix == "68" else 100

    def update_price(self, price):
        """
        Update the price of the stock.

        Parameters
        ----------
        price : float
            New price of the stock.

        Notes
        -----
        This function updates the price of the stock and also updates the total amount of the stock.
        """
        self.price = price
        self.amt = self.volume * price

    def update_info(self, price, up, low):
        """
        Update the price, upper limit price and lower limit price of the stock.

        Parameters
        ----------
        price : float
            New price of the stock.
        up : float
            New upper limit price of the stock.
        low : float
            New lower limit price of the stock.
        """
        self.update_price(price)
        self.up_price = up
        self.low_price = low

    def buy(self, vol):
        """
        Buy a certain amount of stock.

        Parameters
        ----------
        vol : float
            The amount of stock to buy.

        Returns
        -------
        amt : float
            The total amount of money spent on buying the stock.

        Notes
        -----
        This function buys a certain amount of stock and updates the total amount of the stock.
        """
        amt = vol * self.price
        self.volume += vol
        self.amt = self.volume * self.price
        return amt

    def sell(self, vol):
        """
        Sell a certain amount of stock.

        Parameters
        ----------
        vol : float
            The amount of stock to sell.

        Returns
        -------
        amt : float
            The total amount of money gained from selling the stock.

        Notes
        -----
        This function sells a certain amount of stock and updates the total amount of the stock.
        """
        amt = vol * self.price * (1 - self.trade_fee)
        self.volume -= vol
        self.sellable_vol -= vol
        self.amt = self.volume * self.price
        return amt


class account:
    """
    A class representing an account.
    """

    def __init__(self, money):
        """
        Initialize an account object.

        Parameters
        ----------
        money : float
            The total amount of money in the account.
        cash : float
            The total amount of currency available in the account.
        total_account : float
            The total amount of assets in the account including cash and stocks.
            (The initial value is 0.)
        hold_dict : dict
            A dictionary containing the stocks held in the account.
        trade_dict : dict
            A dictionary containing the trading records of the account.

        Notes
        -----
        This function initializes an account object with a given amount of money.
        """
        self.cash = money
        self.total_account = money
        self.hold_dict = {}
        self.trade_dict = {}
        self.date = None

    def cal_total(self):
        """
        Calculate the total amount of money in the account.

        Returns
        -------
        total_account : float
            The total amount of money in the account.

        Notes
        -----
        This function calculates the total amount of money in the account by summing up the value of all the stocks held and the cash available.
        """
        self.total_account = self.cash + sum(stk.amt for stk in self.hold_dict.values())
        return self.total_account

    def refresh_open(self, td_upper, td_lower, td_preclose, td_adj):
        """
        Refresh the account information at the start of a trading day.

        Parameters
        ----------
        td_upper : pandas.Series
            The upper limit prices for each stock.
        td_lower : pandas.Series
            The lower limit prices for each stock.
        td_preclose : pandas.Series
            The previous close prices for each stock.
        td_adj : pandas.Series
            The daily adjustment factors for each stock.

        Returns
        -------
        float
            The total amount of money in the account.

        Notes
        -----
        This function refreshes the account information at the start of a trading day by updating the upper limit prices, lower limit prices, and previous close prices for each stock.
        """
        self.td_upper = td_upper
        self.td_lower = td_lower
        self.td_price_now = td_preclose
        for code, st in self.hold_dict.items():
            # update price, upper price, lower price
            st.update_info(td_preclose[code], td_upper[code], td_lower[code])
            if code in td_adj:
                # adjust volume and amount for the stocks split or dividend
                st.volume *= td_adj[code]
                st.amt = st.volume * st.price
            # update sellable volume
            st.sellable_vol = st.volume
        return self.cal_total()

    def log_trade(self, code, price, vol):
        """
        Log a trade.

        Parameters
        ----------
        code : str
            The stock code.
        price : float
            The price of the stock.
        vol : float
            The volume of the stock.

        Notes
        -----
        This function logs a trade by storing the trade information in the trade_dict.
        """
        self.trade_dict.setdefault(code, []).append([vol, price, self.date])

    def buy_stk(self, code, vol):
        """
        Buy a certain amount of stock.

        Parameters
        ----------
        code : str
            The stock code.
        vol : float
            The volume of the stock to buy.

        Returns
        -------
        amt : float
            The total amount of money spent on buying the stock.

        Notes
        -----
        This function buys a certain amount of stock and updates the total amount of the stock.
        """
        amt = self.hold_dict[code].buy(vol)
        self.log_trade(code, self.hold_dict[code].price, vol)
        self.cash -= amt  # TODO: add try and except
        return amt

    def sell_stk(self, code, vol):
        """
        Sell a certain amount of stock.

        Parameters
        ----------
        code : str
            The stock code.
        vol : float
            The volume of the stock to sell.

        Returns
        -------
        amt : float
            The total amount of money gained from selling the stock.

        Notes
        -----
        This function sells a certain amount of stock and updates the total amount of the stock.
        """
        amt = self.hold_dict[code].sell(vol)
        self.log_trade(code, self.hold_dict[code].price, -vol)
        if self.hold_dict[code].volume == 0:
            del self.hold_dict[code]
        self.cash += amt
        return amt

    def fresh_price(self, price_s):
        """
        Refresh the price of all stocks in the portfolio.

        Parameters
        ----------
        price_s : dict
            A dictionary of stock codes and their prices.

        Notes
        -----
        This function updates the prices of all stocks in the portfolio.
        """
        self.td_price_now = price_s
        for code, st in self.hold_dict.items():
            if code in price_s:
                st.update_price(price_s[code])

    def daily_trade(self, cash_avail, to_buy_s, to_sell_s):
        """
        Daily trade function. This function sells all the stocks that needs to be sold and buys all the stocks that needs to be bought.
        The function first sells all the stocks that needs to be sold, then buys all the stocks that needs to be bought.
        The function will stop buying if the total amount of money gained from selling is not enough to buy all the stocks that needs to be bought.
        Parameters
        ----------
        cash : float
            The total amount of money available for trading.
        to_buy_s : dict
            A dictionary of stock codes and their corresponding the volume of the stock to buy.
        to_sell_s : dict
            A dictionary of stock codes and their the volume of the stock to sell.
        Returns
        -------
        total_buy : float
            The total amount of money gained from buying.
        total_sell : float
            The total amount of money gained from selling.
        Notes
        -----
        This function does not update the prices of the stocks in the portfolio.
        """
        total_buy = total_sell = 0

        # sell
        for code in to_sell_s.index:
            if code not in self.hold_dict:
                continue
            st = self.hold_dict[code]
            if st.low_price < st.price < st.up_price:  # TODO: remove the up_price for selling
                left_sell = st.sellable_vol
                if left_sell <= st.minimum_vol:
                    # when sellable volume is less than minimum volume, sell all
                    total_sell += self.sell_stk(code, left_sell)
                else:
                    # when sellable volume is greater than minimum volume, sell in units
                    sell_vol = round(to_sell_s[code] / st.unit_vol) * st.unit_vol
                    sell_vol = min(sell_vol, left_sell)
                    if sell_vol >= st.minimum_vol:
                        total_sell += self.sell_stk(code, sell_vol)
        # buy
        for code in to_buy_s.index:
            if total_buy >= cash_avail + total_sell - 1000:
                break
            if code not in self.hold_dict:
                self.hold_dict[code] = stk(code, self.td_price_now[code], self.td_upper[code], self.td_lower[code])
            st = self.hold_dict[code]
            if st.low_price < st.price < st.up_price:  # TODO: remove the low_price for buying
                # calculate maximum volume to buy according to available funds and trading units
                max_vol = int((cash_avail + total_sell - total_buy) / (st.price * st.unit_vol)) * st.unit_vol
                # calculate the maximum volume to buy according to available funds and trading units
                vol = min(round(to_buy_s[code] / st.unit_vol) * st.unit_vol, max_vol)
                if vol >= st.minimum_vol:
                    total_buy += self.buy_stk(code, vol)
                if st.volume == 0:
                    del self.hold_dict[code]
        return total_buy, total_sell

    def close_today(self):
        """
        This function closes all the positions today and returns the holding information and the trading information.
        The function first constructs the holding DataFrame from the holding dictionary, then constructs the trading DataFrame from the trading dictionary.
        Finally, it clears the trading dictionary and returns the holding DataFrame and the trading DataFrame.
        Returns
        -------
        hold_df : pd.DataFrame
            A DataFrame containing the holding information.
        trade_df : pd.DataFrame
            A DataFrame containing the trading information.
        Notes
        -----
        This function does not update the prices of the stocks in the portfolio.
        """
        if not self.hold_dict:
            hold_df = pd.DataFrame(columns=["volume", "amt"])
        else:
            hold_df = pd.DataFrame({c: [s.volume, s.amt] for c, s in self.hold_dict.items()}).T.sort_index()
            hold_df.columns = ["volume", "amt"]

        trade_df = pd.concat([pd.DataFrame(v) for v in self.trade_dict.values()], keys=self.trade_dict.keys()) if self.trade_dict else pd.DataFrame()
        if not trade_df.empty:
            trade_df = trade_df.reset_index().iloc[:, 1:]
            trade_df.columns = ["code", "volume", "price", "date"]
        self.trade_dict = {}
        return hold_df, trade_df
