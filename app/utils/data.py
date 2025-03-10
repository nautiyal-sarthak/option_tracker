from ..brokers.broker_ibkr import IBKRBroker
from ..brokers.broker_quest import QuestradeBroker
from collections import defaultdict
import pandas as pd
import numpy as np
import json
from .serialization import convert_to_serializable
import pandas as pd
import logging
import numpy as np
from datetime import datetime, timedelta, date
from flask import session,current_app



def transform_data(df):
    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
    df['tradePrice'] = pd.to_numeric(df['tradePrice']).abs()
    df['commission'] = pd.to_numeric(df['commission']).abs()
    df['quantity'] = pd.to_numeric(df['quantity']).abs()
    df['strike'] = pd.to_numeric(df['strike'])
    df['expiry'] = pd.to_datetime(df['expiry'])

    #if expiry is NaT then set it to 9999-12-31
    df['expiry'] = df['expiry'].fillna(pd.Timestamp('2099-12-31'))

    # make the openCloseIndicator more readable
    df['openCloseIndicator'] = df['openCloseIndicator'].map({
        'O': 'Open',
        'C': 'Close'
    })

    # make the putCall more readable
    df['putCall'] = df['putCall'].map({
        'P': 'Put',
        'C': 'Call',
        'Call': 'Call',
        'Put': 'Put'
    })

    # make the assetCategory more readable
    df['assetCategory'] = df['assetCategory'].map({
        'STK': 'Stock',
        'OPT': 'Option'
    })

        # make the assetCategory more readable
    df['buySell'] = df['buySell'].map({
        'B': 'BUY',
        'S': 'SELL',
        'BUY': 'BUY',
        'SELL': 'SELL'
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
    df = df.groupby(['optionId','symbol','putCall','buySell','openCloseIndicator','strike','accountId','tradePrice','tradeDate','assetCategory','expiry']).agg(
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

def process_trade_data(email,token=None,broker_name=None,filter_type='all'):
    try:
        # check if session['master_trade_data'] is populated
        # if so, return the data from the session
        # if not, fetch the data from the broker
        current_app.logger.info('fetching user data with filter' + filter_type)
        if (session.get('master_trade_data') is None) or session.get('adhoc_email'):
            current_app.logger.info('fetching data from broker')
            is_test = False
            if broker_name == 'IBKR':
                broker = IBKRBroker(token,is_test)
            elif broker_name == 'Quest':
                broker = QuestradeBroker(token,is_test)
            else:
                raise Exception(f"Broker '{broker_name}' is not supported.")
            trade_data = broker.get_data(email)


            df = pd.DataFrame([vars(trade) for trade in trade_data])
            current_app.logger.info('transforming data')
            raw_df = transform_data(df)
            current_app.logger.info('processing wheel trades')
            processed_data = process_wheel_trades(raw_df)
            session['min_trade_data'] =  min(processed_data['trade_open_date'])
            session['master_trade_data'] = processed_data
        else:
            current_app.logger.info('using data from session')
            processed_data = session['master_trade_data']

        # get open cost
        open_positions = processed_data[(processed_data["assign_quantity"] > 0) | (processed_data['sold_quantity'] < 0) | ((processed_data['is_closed'] == False) & (processed_data['callorPut'] == 'Put'))]
        open_positions = open_positions[['accountId','symbol','strike_price','number_of_contracts_sold','number_of_buyback','assign_quantity','net_assign_cost','sold_quantity','net_sold_cost']]
        open_positions["net_open_contracts"] = open_positions['number_of_contracts_sold'] - open_positions['number_of_buyback']
        open_positions["net_open_contracts_cost"] =  open_positions["net_open_contracts"] * open_positions["strike_price"] * 100
        open_positions = open_positions[['accountId','symbol','net_open_contracts_cost','net_assign_cost','net_sold_cost']]
        investment_per_stock = open_positions.groupby(['symbol','accountId']).agg({
            'net_open_contracts_cost': 'sum',
            'net_assign_cost': 'sum',
            'net_sold_cost': 'sum'
        }).reset_index()

        # Calculate net money invested
        # Net money invested = total money spent (outflows) - total money received (inflows)
        investment_per_stock['net_money_invested'] = (
            investment_per_stock['net_open_contracts_cost'] +  # Negative means money spent
            investment_per_stock['net_assign_cost'] +         # Negative means money spent, positive means money received
            investment_per_stock['net_sold_cost']             # Positive means money received
        )

        # Display the result
        print(investment_per_stock[['symbol', 'net_money_invested']])

        total_investment = investment_per_stock['net_money_invested'].sum()

        # Apply time filter
        filtered_data = filter_by_time_period(processed_data, filter_type)
        

        # Account level aggregation
        account_summary = filtered_data.groupby('accountId').agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[~filtered_data['is_closed']].sum()),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(filtered_data['is_closed']) & (filtered_data['callorPut'].isin(["Call", "Put"]))].count()),
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[filtered_data['is_closed']==False].count()),
            total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[filtered_data['is_closed']].sum()),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum')
        ).reset_index()

        account_summary['total_loss'] = account_summary['total_trades'] - account_summary['total_wins']
        account_summary['stock_sale_pl'] = account_summary['total_stock_sale_cost'] + account_summary['total_stock_assign_cost']
        account_summary['total_profit'] = account_summary['total_premium_collected'] + account_summary['stock_sale_pl']

        # Total info
        total_premium_collected = account_summary['total_premium_collected'].sum().round(2)
        total_premium_collected_open = account_summary['total_premium_collected_open'].sum().round(2)
        total_premium_formated = str(total_premium_collected) + "(" + str(total_premium_collected_open) + ")"
        p_l_stock = account_summary['stock_sale_pl'].sum().round(2)
        total_wins = account_summary['total_wins'].sum()
        total_trades = account_summary['total_trades'].sum()
        total_open_trades = account_summary['total_open_trades'].sum()
        total_profit = total_premium_collected.round(2) + p_l_stock.round(2)
        total_loss = total_trades - total_wins
        win_percentage = ((total_wins / total_trades) * 100).round(2) if total_trades > 0 else 0

        account_summary = account_summary.round(2)
        # Stock level aggregation
        stock_summary = filtered_data.groupby(['accountId', 'symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[~filtered_data['is_closed']].sum()),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(filtered_data['is_closed']) & (filtered_data['callorPut'].isin(["Call", "Put"]))].count()),
            total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[filtered_data['is_closed']].sum()),
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[filtered_data['is_closed']==False].count()),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum')
        ).reset_index()

        stock_summary['stock_sale_pl'] = stock_summary['total_stock_sale_cost'] + stock_summary['total_stock_assign_cost']
        stock_summary['total_loss'] = stock_summary['total_trades'] - stock_summary['total_wins']
        stock_summary['total_profit'] = stock_summary['total_premium_collected'] + stock_summary['stock_sale_pl']
        stock_summary['w_L'] = np.where(
            stock_summary['total_trades'] == 0,
            0,  # Set to 0 when total_trades is 0
            (stock_summary['total_wins'] / stock_summary['total_trades']) * 100
        )
        stock_summary['total_stock_quantity'] = stock_summary['total_assign_quantity'] + stock_summary['total_sold_quantity']

        stock_summary = stock_summary.merge(investment_per_stock, on=['symbol','accountId'], how='left')
        stock_summary['net_money_invested'] = stock_summary['net_money_invested'].fillna(0)
        stock_summary['net_money_invested_percent'] =  stock_summary['net_money_invested']/total_investment * 100
        stock_summary = stock_summary.round(2)

        # Date-based aggregation
        date_summary = filtered_data.groupby(['accountId', 'symbol', 'trade_open_date']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum')
        ).reset_index()
        date_summary['total_profit'] = date_summary['total_premium_collected'] + date_summary['total_stock_sale_cost']
        date_summary['year'] = date_summary['trade_open_date'].dt.year
        date_summary['month'] = date_summary['year'].astype(str) + "-" + date_summary['trade_open_date'].dt.month.astype(str)
        date_summary['year_quarter'] = date_summary['trade_open_date'].dt.year.astype(str) + "-Q" + date_summary['trade_open_date'].dt.quarter.astype(str)
        
        # Calculate the week number of the month
        date_summary['week_of_month'] = date_summary['trade_open_date'].dt.day.sub(1).floordiv(7).add(1)

        # Create the year-month-week column
        date_summary['year_month_week'] = date_summary['trade_open_date'].dt.strftime('%Y-%m') + "-W" + date_summary['week_of_month'].astype(str)

        profit_by_month = date_summary.groupby(['year_month_week']).agg(
            total_profit=pd.NamedAgg(column='total_profit', aggfunc='sum')
        ).reset_index().sort_values('year_month_week')

        # Prepare account-stock dictionary
        account_dict = defaultdict(list)
        for item in stock_summary.round(2).to_dict(orient='records'):
            account_dict[item['accountId']].append(item)

        data = {
            'oldest_date': session['min_trade_data'],
            'total_premium_collected': total_premium_collected,
            'total_premium_collected_open': total_premium_collected_open,
            'total_premium_formated': total_premium_formated,
            'total_open_trades': total_open_trades,
            'total_profit': total_profit,
            'p_l_stock': p_l_stock,
            'total_wins': total_wins,
            'total_loss': total_loss,
            'win_percentage': win_percentage,
            'account_summary': account_summary.to_dict(orient='records'),
            'account_stk_merge': dict(account_dict),
            'profit_data': profit_by_month.to_dict(orient='records')
        }

        # Convert all NumPy types to Python native types
        return convert_to_serializable(data)

    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    
def filter_by_time_period(df, filter_type):
    today = datetime.now()
    end_date = today
    
    if filter_type == '15days':
        start_date = today - timedelta(days=15)
    elif filter_type == '1month':
        start_date = today - timedelta(days=30)
    elif filter_type == '3months':
        start_date = today - timedelta(days=90)
    elif filter_type == '6months':
        start_date = today - timedelta(days=180)
    elif filter_type == '1year':
        start_date = today - timedelta(days=365)
    elif filter_type == 'lastyear':    
        start_date = datetime(today.year - 1, 1, 1)  # January 1st of the previous year
        end_date = datetime(today.year - 1, 12, 31)
    elif filter_type == 'all':
        return df
    else:
        start_date = df['trade_open_date'].min()
    

    return df[(df['trade_open_date'] >= start_date) & (df['trade_open_date'] <= end_date)]