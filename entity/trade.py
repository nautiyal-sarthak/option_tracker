class Trade:
    def __init__(self,optionId,tradeDate, accountId, symbol, putCall,buySell, openCloseIndicator, strike, expiry, quantity, tradePrice, commission,assetCategory):
        self.optionId = optionId # a key to link the buy and sell trades for a given option
        self.tradeDate = tradeDate # the date the trade was executed
        self.accountId = accountId # account id
        self.symbol = symbol # the stock or option symbol
        self.putCall = putCall # put or call
        self.buySell = buySell # buy or sell
        self.openCloseIndicator = openCloseIndicator #trade is open or close
        self.strike = strike # the strike price
        self.expiry = expiry # the expiry date
        self.quantity = quantity # quantity
        self.tradePrice = tradePrice # the trade price of 1 share 
        self.commission = commission # total commission
        self.assetCategory = assetCategory # Stock or Option


    def __repr__(self):
        return f"Trade({self.optionId} ,{self.tradeDate}, {self.accountId}, {self.symbol}, {self.putCall}, {self.buySell}, {self.openCloseIndicator}, {self.strike}, {self.expiry}, {self.quantity}, {self.tradePrice}, {self.commission} , {self.assetCategory})"
    
