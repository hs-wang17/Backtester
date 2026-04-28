import pandas as pd


class stock:
    def __init__(self, code, price, up_price, low_price, trade_fee=0.001):
        self.code, self.price, self.up_price, self.low_price = code, price, up_price, low_price
        self.sellable_volume, self.trade_fee, self.volume, self.amount = 0.0, trade_fee, 0.0, 0.0
        self.minimum_volume = 200 if ("%06d" % int(code))[:2] == "68" else 100
        self.unit_volume = 1 if ("%06d" % int(code))[:2] == "68" else 100

    def update_price(self, price):
        self.price = price
        self.amount = self.volume * price

    def update_info(self, price, up_price, low_price):
        self.update_price(price)
        self.up_price, self.low_price = up_price, low_price

    def buy(self, volume):
        amount = volume * self.price
        self.volume += volume
        self.amount = self.volume * self.price
        return amount

    def sell(self, volume):
        amount = volume * self.price * (1 - self.trade_fee)
        self.volume -= volume
        self.sellable_volume -= volume
        self.amount = self.volume * self.price
        return amount


class account:
    def __init__(self, money):
        self.cash, self.total_account, self.hold_dict, self.trade_dict, self.date = money, money, {}, {}, None

    def cal_total(self):
        self.total_account = self.cash + sum(stk.amount for stk in self.hold_dict.values())
        return self.total_account

    def refresh_open(self, td_upper, td_lower, td_preclose, td_adj):
        self.td_upper, self.td_lower, self.td_price_now = td_upper, td_lower, td_preclose
        for code, stk in self.hold_dict.items():
            stk.update_info(td_preclose[code], td_upper[code], td_lower[code])
            if code in td_adj:
                stk.volume *= td_adj[code]
                stk.amount = stk.volume * stk.price
            stk.sellable_volume = stk.volume
        return self.cal_total()

    def cal_sellable_amount(self):
        rows = []
        for code, stk in self.hold_dict.items():
            sellable_amount = stk.sellable_volume * stk.price
            rows.append({"code": code, "sellable_amount": sellable_amount})
        return pd.DataFrame(rows, columns=["code", "sellable_amount"])

    def log_trade(self, code, price, volume):
        self.trade_dict.setdefault(code, []).append([volume, price, self.date])
        
    def record_trade(self, td_price, to_buy_s, to_sell_s, date, act_s, cash_s, buy_s, sell_s, hold_df_dict, trade_df_dict, flag, close_fresh=None):
        self.fresh_price(td_price.to_dict())
        buy_amount, sell_amount = self.daily_trade(self.cash, to_buy_s, to_sell_s)
        sellable_amount = self.cal_sellable_amount()
        if close_fresh is not None:
            self.fresh_price(close_fresh.to_dict())
        act_s[date] = self.cal_total()
        cash_s[date] = self.cash
        buy_s[date + flag] = buy_amount
        sell_s[date + flag] = sell_amount
        hold_df, trade_df = self.close_today()
        hold_df_dict[date + flag] = hold_df
        trade_df_dict[date + flag] = trade_df
        return hold_df, sellable_amount

    def buy_stk(self, code, volume):
        amount = self.hold_dict[code].buy(volume)
        self.log_trade(code, self.hold_dict[code].price, volume)
        self.cash -= amount
        return amount

    def sell_stk(self, code, volume):
        amount = self.hold_dict[code].sell(volume)
        self.log_trade(code, self.hold_dict[code].price, -volume)
        if self.hold_dict[code].volume == 0:
            del self.hold_dict[code]
        self.cash += amount
        return amount

    def fresh_price(self, price_s):
        self.td_price_now = price_s
        for code, stk in self.hold_dict.items():
            if code in price_s:
                stk.update_price(price_s[code])

    def daily_trade(self, cash_avail, to_buy_s, to_sell_s):
        total_buy, total_sell = 0.0, 0.0

        # sell
        for code in to_sell_s.index:
            if code not in self.hold_dict:
                continue
            stk = self.hold_dict[code]
            if stk.low_price < stk.price < stk.up_price:
                if stk.sellable_volume <= stk.minimum_volume:
                    sell_volume = stk.sellable_volume
                else:
                    sell_volume = min(round(to_sell_s[code] / stk.unit_volume) * stk.unit_volume, stk.sellable_volume)
                if stk.sellable_volume <= stk.minimum_volume or sell_volume >= stk.minimum_volume:
                    total_sell += self.sell_stk(code, sell_volume)

        # buy
        for code in to_buy_s.index:
            if total_buy >= cash_avail + total_sell - 1000.0:
                break
            if code not in self.hold_dict:
                try:
                    self.hold_dict[code] = stock(code, self.td_price_now[code], self.td_upper[code], self.td_lower[code])
                except:
                    continue
            stk = self.hold_dict[code]
            if stk.low_price < stk.price < stk.up_price:
                volume = min(
                    round(to_buy_s[code] / stk.unit_volume) * stk.unit_volume,
                    int((cash_avail + total_sell - total_buy) / (stk.price * stk.unit_volume)) * stk.unit_volume,
                )
                if volume >= stk.minimum_volume:
                    total_buy += self.buy_stk(code, volume)
                if stk.volume <= 0:
                    del self.hold_dict[code]
        return total_buy, total_sell

    def close_today(self):
        if not self.hold_dict:
            hold_df = pd.DataFrame(columns=["volume", "amount"])
        else:
            hold_df = pd.DataFrame({c: [stk.volume, stk.amount] for c, stk in self.hold_dict.items()}).T.sort_index()
            hold_df.columns = ["volume", "amount"]

        trade_df = (
            pd.concat([pd.DataFrame(v) for v in self.trade_dict.values()], keys=self.trade_dict.keys()).reset_index(level=1, drop=True)
            if self.trade_dict
            else pd.DataFrame()
        )
        if not trade_df.empty:
            trade_df = trade_df.reset_index()
            trade_df.columns = ["code", "volume", "price", "date"]
        self.trade_dict = {}
        return hold_df, trade_df
