import pandas as pd
import logging
import numpy as np

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def transform_data(df):
    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
    df['tradePrice'] = pd.to_numeric(df['tradePrice']).abs()
    df['commission'] = pd.to_numeric(df['commission']).abs()
    df['quantity'] = pd.to_numeric(df['quantity']).abs()
    df['strike'] = pd.to_numeric(df['strike'])
    df['expiry'] = pd.to_datetime(df['expiry'])

    # make the openCloseIndicator more readable
    df['openCloseIndicator'] = df['openCloseIndicator'].map({
        'O': 'Open',
        'C': 'Close'
    })

    # make the putCall more readable
    df['putCall'] = df['putCall'].map({
        'P': 'Put',
        'C': 'Call'
    })

    # make the assetCategory more readable
    df['assetCategory'] = df['assetCategory'].map({
        'STK': 'Stock',
        'OPT': 'Option'
    })

    # assert that buySell col only has BUY or SELL
    assert df['buySell'].isin(['BUY', 'SELL']).all()


    # if the assetCatagory is option then multiply the trade price by 100
    df['total_premium'] = df.apply(lambda x: x['tradePrice']*100*x['quantity'] if x['assetCategory'] == 'Option' else x['tradePrice']* x['quantity'], axis=1)
    #df['total_premium'] = df.apply(lambda x: x['total_premium']* -1 if x['buySell'] == 'BUY' else x['total_premium'], axis=1)
    df['total_premium'] = df['total_premium'] - df['commission']

    return df

def find_matching_key(target_key, lookup_dict, trade_open_date):
    if len(target_key) < 5:
        return None  # Invalid key format

    for dic_key in lookup_dict:
        if len(dic_key) < 5:
            continue  # Skip invalid keys

        if target_key[0] == dic_key[0] and target_key[1] == dic_key[1] and target_key[2] == dic_key[2] and target_key[3] >= dic_key[3] and dic_key[3] >= trade_open_date:
            return dic_key  # Return the first matching key

    return None

def process_wheel_trades(df):
    df = df.copy()
    df = df.fillna("")
    df_grp = df.groupby(['optionId','symbol','putCall','buySell','openCloseIndicator','strike','accountId','tradePrice','tradeDate','assetCategory']).agg(
        quantity=pd.NamedAgg(column='quantity', aggfunc='sum'),
        commission=pd.NamedAgg(column='commission', aggfunc='sum'),
        total_premium=pd.NamedAgg(column='total_premium', aggfunc='sum')
    ).reset_index()

    df["asset_priority"] = df["putCall"].apply(lambda x: 1 if x in ["Call", "Put"] else 2)
    df.sort_values(by=["asset_priority", "symbol", "tradeDate"], ascending=[True, True, True], inplace=True)
    df.drop(columns=["asset_priority"], inplace=True)  # Remove helper column
    # Initialize processed trades list
    processed_trades = {}

    # Track assigned stocks, stock sales, buybacks, rolls, and exercises
    assigned_stocks = {}
    stock_sales = {}
    buybacks = {}
    

    # Iterate over trades
    for _, row in df.iterrows():
        symbol = row["symbol"]
        
        if row["assetCategory"] == "Option" and row["buySell"] == "SELL":
            # Option Sale Entry
            key = (symbol, row["putCall"], row["strike"], row["expiry"], row['accountId'])
            processed_trades[key] ={
                'accountId': row['accountId'],
                "symbol": symbol,
                "callorPut": row["putCall"],
                "buySell": row["buySell"],
                "trade_open_date": row["tradeDate"],
                "expiry_date": row["expiry"],
                "strike_price": row["strike"],
                "number_of_contracts_sold": row["quantity"] * -1,
                "premium_per_contract": row["tradePrice"],
                "net_buyback_price": None,
                "number_of_buyback": None,
                "buyback_date": None,
                "net_premium": row["total_premium"],
                "assign_price_per_share": None,
                "assign_quantity": None,
                "number_of_assign_contract":None,
                "assign_date":None,
                "net_assign_cost": None,
                "sold_price_per_share": None,
                "sold_quantity": None,
                "number_of_sold_contract": None,
                "net_sold_cost":None,
                "sold_date": None
            }

        elif row["assetCategory"] == "Option" and row["buySell"] == "BUY" and row["openCloseIndicator"] == "Close":
            # Option Buyback (Closing trade)
            key = (symbol, row["putCall"], row["strike"], row["expiry"], row['accountId'])
            buybacks[key] = {
                "total_premium": row["total_premium"] * -1,
                "number_of_buyback": row["quantity"],
                "buyback_date": row["tradeDate"]
            }
    
        elif row["assetCategory"] == "Stock" and row["buySell"] == "BUY":
            # Stock Assignment (from Put Option)
            key = (symbol, 'Put', row["tradePrice"],row["tradeDate"], row['accountId'])
            assigned_stocks[key] = {
                "assign_price": row["tradePrice"],  
                "quantity": row["quantity"],
                "tradeDate": row["tradeDate"],
                "number_of_assign_contract":row["quantity"],
                "net_assign_cost":row["total_premium"] * -1
            }

        elif row["assetCategory"] == "Stock" and row["buySell"] == "SELL":
            # Stock Sale
            key = (symbol, 'Call', row["tradePrice"],row["tradeDate"], row['accountId'])
            stock_sales[key] = {
                "sold_price": row["tradePrice"],  
                "quantity": row["quantity"] * -1,
                "tradeDate": row["tradeDate"],
                "number_of_sold_contract": row["quantity"] * -1,
                "net_sold_cost": row["total_premium"]
            }

    # Update processed trades with buybacks, rolls, assignments, early exercises, and P/L
    for trade_key in processed_trades:
        trade = processed_trades[trade_key]

        assigned_stocks_key = find_matching_key(trade_key,assigned_stocks,trade['trade_open_date'])
        stock_sales_key = find_matching_key(trade_key,stock_sales,trade['trade_open_date'])
        
        # Add buyback price if it exists
        if trade_key in buybacks:
            trade["net_buyback_price"] = buybacks[trade_key]['total_premium']
            trade["net_premium"] = trade["net_premium"] + buybacks[trade_key]['total_premium']
            trade["number_of_buyback"] = buybacks[trade_key]['number_of_buyback']
            trade["buyback_date"] = buybacks[trade_key]['buyback_date']
        
        if assigned_stocks_key:
            trade["assign_price_per_share"] = assigned_stocks[assigned_stocks_key]['assign_price']
            trade["assign_quantity"] = assigned_stocks[assigned_stocks_key]['quantity']
            trade["assign_date"] = assigned_stocks[assigned_stocks_key]['tradeDate']
            trade["number_of_assign_contract"] = assigned_stocks[assigned_stocks_key]['number_of_assign_contract']/100
            trade["net_assign_cost"] = assigned_stocks[assigned_stocks_key]['net_assign_cost']
            assigned_stocks.pop(assigned_stocks_key)
        
        if stock_sales_key:
            trade["sold_price_per_share"] = stock_sales[stock_sales_key]['sold_price']
            trade["sold_quantity"] = stock_sales[stock_sales_key]['quantity']
            trade["sold_date"] = stock_sales[stock_sales_key]['tradeDate']
            trade["number_of_sold_contract"] = stock_sales[stock_sales_key]['number_of_sold_contract']/100
            trade["net_sold_cost"] = stock_sales[stock_sales_key]['net_sold_cost']
            stock_sales.pop(stock_sales_key)

    for key, value in assigned_stocks.items():
        processed_trades[key] ={
                'accountId': key[4],
                "symbol": key[0],
                "callorPut": None,
                "buySell": "BUY",
                "trade_open_date": value["tradeDate"],
                "expiry_date": None,
                "strike_price": None,
                "number_of_contracts_sold": None,
                "premium_per_contract": None,
                "net_buyback_price": None,
                "number_of_buyback": None,
                "buyback_date": None,
                "net_premium": None,
                "assign_price_per_share": value["assign_price"],
                "assign_quantity": value["number_of_assign_contract"],
                "number_of_assign_contract":None,
                "assign_date": value["tradeDate"],
                "net_assign_cost": value["net_assign_cost"],
                "sold_price_per_share": None,
                "sold_quantity": None,
                "number_of_sold_contract": None,
                "net_sold_cost":None,
                "sold_date": None
            }

    for key, value in stock_sales.items():
        processed_trades[key] ={
                'accountId': key[4],
                "symbol": key[0],
                "callorPut": None,
                "buySell": "SELL",
                "trade_open_date": value['tradeDate'],
                "expiry_date": None,
                "strike_price": None,
                "number_of_contracts_sold": None,
                "premium_per_contract": None,
                "net_buyback_price": None,
                "number_of_buyback": None,
                "buyback_date": None,
                "net_premium": None,
                "assign_price_per_share": None,
                "assign_quantity": None,
                "number_of_assign_contract":None,
                "assign_date": None,
                "net_assign_cost": None,
                "sold_price_per_share": value['sold_price'],
                "sold_quantity": value['quantity'],
                "number_of_sold_contract": None,
                "net_sold_cost":value['net_sold_cost'],
                "sold_date": value['tradeDate']
            }

    df = pd.DataFrame(processed_trades).transpose()
    df = df.fillna(0)
    df = df.reset_index()
    # is open trade


    today = pd.Timestamp.today().normalize()
    df['expiry_date'] = pd.to_datetime(df['expiry_date'], errors='coerce')  # Convert expiry_date to datetime safely

    df['is_closed'] = (
                    (pd.to_numeric(df['number_of_contracts_sold']).abs() == pd.to_numeric(df['number_of_buyback'] + df['number_of_assign_contract'] + df['number_of_sold_contract']).abs()) 
                    | 
                    (df['expiry_date'].notna() & (df['expiry_date'] < today))  # Only compare if expiry_date is valid
                    )   
     
    df['is_win'] = (df['net_premium'] > 0) & (df['number_of_assign_contract'] == 0) & (df['number_of_sold_contract'] == 0) & (df['is_closed'] == True)

    return df

    

    