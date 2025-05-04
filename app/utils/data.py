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
from calendar import monthrange



def transform_data(df):
    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
    df['tradePrice'] = pd.to_numeric(df['tradePrice']).abs()
    df['commission'] = pd.to_numeric(df['commission'])
    df['quantity'] = pd.to_numeric(df['quantity'])
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
    df['total_premium'] = (df['total_premium'] - df['commission']) * -1
    df['total_premium'] = df['total_premium'].apply(lambda x: round(x, 2))

    # for trade date get only the date part
    df['tradeDate'] = df['tradeDate'].dt.date
    df['expiry'] = df['expiry'].dt.date

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
    try:
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
        sellbacks = {}
        

        # Iterate over trades
        for _, row in df.iterrows():
            symbol = row["symbol"]
            
            if row["assetCategory"] == "Option" and row["openCloseIndicator"] == "Open":
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
                    "number_of_contracts_sold": row["quantity"],
                    "premium_collected": row["total_premium"],

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
                    "sold_date": None,
                    "net_sold_cost":None,
                    

                    "close_date":None,
                    "ROI": None,
                    "status": "OPEN"
                }

            elif row["assetCategory"] == "Option" and row["openCloseIndicator"] == "Close":
                # Option Buyback (Closing trade)
                key = (symbol, row["putCall"], row["strike"], row["expiry"], row['accountId'])
                buybacks[key] = {
                    "total_premium": row["total_premium"],
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
                    "net_assign_cost":row["total_premium"]
                }

            elif row["assetCategory"] == "Stock" and row["buySell"] == "SELL":
                # Stock Sale
                key = (symbol, 'Call', row["tradePrice"],row["tradeDate"], row['accountId'])
                stock_sales[key] = {
                    "sold_price": row["tradePrice"],  
                    "quantity": row["quantity"] ,
                    "tradeDate": row["tradeDate"],
                    "number_of_sold_contract": row["quantity"] ,
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
                trade["net_premium"] = trade["premium_collected"] + buybacks[trade_key]['total_premium']
                trade["number_of_buyback"] = buybacks[trade_key]['number_of_buyback']
                trade["buyback_date"] = buybacks[trade_key]['buyback_date']
                trade["close_date"] = buybacks[trade_key]['buyback_date']
                trade["status"] = "BOUGHT BACK"
                trade["ROI"] = (trade["net_premium"] / (trade["strike_price"] * abs(trade["number_of_contracts_sold"]) * 100)) * 100
            
            if assigned_stocks_key:
                trade["assign_price_per_share"] = assigned_stocks[assigned_stocks_key]['assign_price']
                trade["assign_quantity"] = assigned_stocks[assigned_stocks_key]['quantity']
                trade["assign_date"] = assigned_stocks[assigned_stocks_key]['tradeDate']
                trade["number_of_assign_contract"] = assigned_stocks[assigned_stocks_key]['number_of_assign_contract']/100
                trade["net_assign_cost"] = assigned_stocks[assigned_stocks_key]['net_assign_cost']
                trade["close_date"] = assigned_stocks[assigned_stocks_key]['tradeDate']
                trade["status"] = "ASSIGNED"
                trade["ROI"] = 0
                
                assigned_stocks.pop(assigned_stocks_key)
            
            if stock_sales_key:
                trade["sold_price_per_share"] = stock_sales[stock_sales_key]['sold_price']
                trade["sold_quantity"] = stock_sales[stock_sales_key]['quantity']
                trade["sold_date"] = stock_sales[stock_sales_key]['tradeDate']
                trade["number_of_sold_contract"] = stock_sales[stock_sales_key]['number_of_sold_contract']/100
                trade["net_sold_cost"] = stock_sales[stock_sales_key]['net_sold_cost']
                trade["close_date"] = stock_sales[stock_sales_key]['tradeDate']
                trade["status"] = "TAKEN AWAY"
                trade["ROI"] = 0
                
                stock_sales.pop(stock_sales_key)

            today = pd.Timestamp.today().normalize()
            today = today.date()    

            if trade["expiry_date"] < today and trade["status"] == "OPEN":
                trade["status"] = "EXPIRED"
                trade["close_date"] = trade["expiry_date"]
                trade["ROI"] = (trade["net_premium"] / (trade["strike_price"] * abs(trade["number_of_contracts_sold"]) * 100)) * 100


        for key, value in assigned_stocks.items():
            processed_trades[key] ={
                    'accountId': key[4],
                    "symbol": key[0],
                    "callorPut": None,
                    "buySell": "BUY",
                    "trade_open_date": value["tradeDate"],
                    "expiry_date": value['tradeDate'],
                    "strike_price": None,
                    "number_of_contracts_sold": None,
                    "premium_collected": None,
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
                    "sold_date": None,
                    "close_date":value["tradeDate"],
                    'status': "BOUGHT STOCK"
                }

        for key, value in stock_sales.items():
            processed_trades[key] ={
                    'accountId': key[4],
                    "symbol": key[0],
                    "callorPut": None,
                    "buySell": "SELL",
                    "trade_open_date": value['tradeDate'],
                    "expiry_date": value['tradeDate'],
                    "strike_price": None,
                    "number_of_contracts_sold": None,
                    "premium_collected": None,
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
                    "sold_date": value['tradeDate'],
                    "close_date":value['tradeDate'],
                    'status': "SOLD STOCK"
                }

        df = pd.DataFrame(processed_trades).transpose()
        df = df.fillna(0)
        df = df.reset_index()
        # is open trade

        df['net_sold_cost'] = pd.to_numeric(df['net_sold_cost'], errors='coerce').fillna(0.0).astype(float)
        df['net_assign_cost'] = pd.to_numeric(df['net_assign_cost'], errors='coerce').fillna(0.0).astype(float)
        df['net_premium'] = pd.to_numeric(df['net_premium'], errors='coerce').fillna(0.0).astype(float)
        df['net_premium'] = df['net_premium'].apply(lambda x: round(x, 2))
        df['ROI'] = df['ROI'].apply(lambda x: round(x, 2))


        return df[['accountId','symbol', 'callorPut', 'buySell', 'trade_open_date', 'expiry_date','strike_price', 
                   'number_of_contracts_sold', 'premium_collected','net_buyback_price', 'number_of_buyback', 'buyback_date', 'net_premium',
                   'assign_price_per_share', 'assign_quantity','number_of_assign_contract', 'assign_date', 'net_assign_cost','sold_price_per_share', 
                   'sold_quantity', 'number_of_sold_contract','net_sold_cost', 'sold_date', 'close_date','ROI','status']]
    except Exception as e:
        logging.error(f"Error processing wheel trades: {e}")
        raise

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
            session['master_trade_data'] = processed_data
            session['raw_df'] = raw_df
        else:
            current_app.logger.info('using data from session')
            processed_data = session['master_trade_data']
            raw_df = session['raw_df']

    
        # Apply time filter
        filtered_data = filter_by_time_period(processed_data, filter_type)
        raw_df = filter_by_time_period(raw_df, filter_type,"tradeDate")

        stock_summary = getStockSummary(filtered_data)
        account_summary = getAccountSummary(stock_summary)
        total_summary = getTotalSummary(account_summary)
        profit_by_month = getProfitPerTimePeriod(filtered_data)


        # Prepare account-stock dictionary
        account_dict = defaultdict(list)
        for item in stock_summary.round(2).to_dict(orient='records'):
            account_dict[item['accountId']].append(item)

        data = {
            'total_premium_collected': total_summary['total_premium_collected'].values[0],
            'total_premium_collected_open': total_summary['total_premium_collected_open'].values[0],
            "total_premium_formated": str(total_summary['total_premium_collected'].values[0]) + f"({total_summary['total_premium_collected_open'].values[0]})",
            'total_open_trades': total_summary['total_open_trades'].values[0],
            'total_profit': total_summary['net_profit'].values[0],
            'p_l_stock': total_summary['realized_pnl'].values[0],
            'total_wins': total_summary['total_wins'].values[0],
            'total_loss': total_summary['total_lost_trades'].values[0],
            'avg_ROI': total_summary['avg_ROI'].values[0],
            'win_percentage': round((total_summary['total_wins'].values[0]/(total_summary['total_wins'].values[0] + total_summary['total_lost_trades'].values[0])) * 100,2),
            'account_summary': account_summary.to_dict(orient='records'),
            'account_stk_merge': dict(account_dict),
            'profit_data': profit_by_month.to_dict(orient='records'),
            'all_trades': format_processed_data(filtered_data).to_dict(orient='records')
        }

        # # Convert all NumPy types to Python native types
        out = convert_to_serializable(data)
        return out

    except Exception as e:
        raise Exception(f"An error occurred: {e}")

def format_processed_data(df):
    df['key'] = df['symbol'] + '_' + df['callorPut'].astype(str) + '_' + df['strike_price'].astype(str) + '_' + df['expiry_date'].astype(str) + '_' + df['buySell'].astype(str)
    df = df[['key','number_of_contracts_sold','ROI','status','trade_open_date','month_week','net_premium']]

    # # if status is open set the trade_open_date to ''
    df.loc[df['status'] == 'OPEN', 'month_week'] = ''

    df.sort_values(by=['month_week'], ascending=False, inplace=True)

    #rename columns
    df.rename(columns={
        'number_of_contracts_sold': '# Contracts',
        'net_premium': 'Net Profit',
        'status': 'Status',
        'trade_open_date': 'Trade Open Date',
        'month_week':'Close week'
    }, inplace=True)

    return df


def getTotalSummary(df):
    try:
        # Group by accountId and sum all other numeric columns
        agg_df = df.copy()
        agg_df = agg_df.sum(numeric_only=True).to_frame().T
        agg_df['avg_ROI'] = df['avg_ROI'].mean()  # Add the mean of the ROI column

        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing getTotalSummary : {e}")
        raise

def getProfitPerTimePeriod(df):
    try:
        # Function to get week of month
        def week_of_month(dt):
            if pd.isnull(dt):
                return np.nan
            first_day = dt.replace(day=1)
            dom = dt.day
            adjusted_dom = dom + first_day.weekday()  # adjust for the weekday of the 1st
            return int(np.ceil(adjusted_dom / 7.0))

        # Date-based aggregation
        df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
        df['week_of_month'] = df['close_date'].apply(week_of_month)
        df['month_week'] = df['close_date'].dt.strftime('%Y-%m-') + df['week_of_month'].astype(str) + 'W'
        weekly_data = df[df['status'] != 'OPEN'].copy()
        
        weekly_data_grp = weekly_data.groupby(['month_week']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
        ).reset_index()

        weekly_data_grp['cost_basis_per_share'] = np.where(weekly_data_grp["total_assign_quantity"] > 0,(weekly_data_grp["total_stock_assign_cost"]+weekly_data_grp.get("total_premium_collected", 0.0)) / weekly_data_grp["total_assign_quantity"],0) 
        weekly_data_grp['total_cost_of_sold_shares'] = weekly_data_grp['cost_basis_per_share'] * weekly_data_grp["total_sold_quantity"]
        weekly_data_grp['realized_pnl'] = weekly_data_grp["total_stock_sale_cost"] - weekly_data_grp['total_cost_of_sold_shares']

        weekly_data_grp["net_profit"] = weekly_data_grp['realized_pnl'] + weekly_data_grp['total_premium_collected']

        return weekly_data_grp[['month_week','net_profit']]
    except Exception as e:
        logging.error(f"Error processing profit by month: {e}")
        raise

def getAccountSummary(df):
    try:
        agg_df = df.groupby('accountId', as_index=False).agg({
                    'avg_ROI': 'mean',  # Calculate the mean for the ROI column
                    **{col: 'sum' for col in df.select_dtypes(include='number').columns if col != 'ROI'}  # Sum for other numeric columns
                })

        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing account summary: {e}")
        raise

def getStockSummary(df):
    try:
        # # Stock level aggregation
        stock_summary = df.groupby(['accountId', 'symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[df['status'] != 'OPEN'].sum()),
            total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[(df['status'] == 'OPEN') & (df['buySell'] == 'SELL')].sum()),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(df['callorPut'].isin(["Call", "Put"]))].count()),
            total_wins = pd.NamedAgg(
                            column='symbol',
                            aggfunc=lambda x: x[(df['net_premium'] > 0) & (df['status'].isin(['EXPIRED', 'BOUGHT BACK']))].count()
                        ),
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[df['status'] == 'OPEN'].count()),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
            avg_ROI=pd.NamedAgg(column='ROI', aggfunc=lambda x: x[df['status'] != 'OPEN'].mean())
        ).reset_index()

        stock_summary['total_lost_trades'] = stock_summary['total_trades'] - stock_summary['total_wins']

        stock_summary['cost_basis_per_share'] = np.where(stock_summary["total_assign_quantity"] > 0,(stock_summary["total_stock_assign_cost"]+stock_summary.get("total_premium_collected", 0.0)) / stock_summary["total_assign_quantity"],0) 
        stock_summary['total_cost_of_sold_shares'] = stock_summary['cost_basis_per_share'] * stock_summary["total_sold_quantity"]
        stock_summary['realized_pnl'] = stock_summary["total_stock_sale_cost"] - stock_summary['total_cost_of_sold_shares']

        stock_summary['win_percent'] = np.where(
            stock_summary['total_trades'] > 0,
            (stock_summary['total_wins'] / stock_summary['total_trades']) * 100,
            0
        )

        stock_summary["net_profit"] = stock_summary['realized_pnl'] + stock_summary['total_premium_collected']

        return stock_summary.round(2)
    except Exception as e:
        logging.error(f"Error processing stock summary: {e}")
        raise


def filter_by_time_period(df, filter_type, col_name='trade_open_date'):
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
        start_date = df[col_name].min()
    
    # Ensure start_date is a date object
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    elif isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    elif isinstance(end_date, datetime):
        end_date = end_date.date()

    return df[(df[col_name] >= start_date) & (df[col_name] <= end_date)]