from flask import Flask, render_template
from brokers.broker_ibkr import IBKRBroker
import pandas as pd
from utility import *
import logging
from collections import defaultdict
from stock  import *
import redis
import json
from datetime import datetime, timedelta
import os


# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

cache = {}  # Dictionary to store data with expiry timestamps
CACHE_EXPIRY = 60 * 24  # Cache duration in minutes

def set_to_cache(key, value):
    expiry_time = datetime.now() + timedelta(minutes=CACHE_EXPIRY)
    cache[key] = (value, expiry_time)

def get_from_cache(key):
    if key in cache:
        value, expiry_time = cache[key]
        if datetime.now() < expiry_time:
            return value  # Return if not expired
        else:
            del cache[key]  # Remove expired entry
    return None  # Return None if not found or expired

app = Flask(__name__)



if 'global_trade_info' not in globals():
    global_trade_info = None

@app.route('/account/<account_id>/symbol/<symbol>')
def stock_details_inner(account_id, symbol):
    if global_trade_info is None:
        return "Stock data is not available yet. Please try again later."

    # Filter stock details for the given account and symbol
    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) & 
        (global_trade_info['symbol'] == symbol)
    ]

    stock_data = stock_data.reset_index()

    # get summery data
    processed_data_global_stk_grp = stock_data.groupby(['accountId','symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(stock_data['is_closed']) & (stock_data['callorPut'].isin(["Call", "Put"]))].count()),
            total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[stock_data['is_closed']].sum()),  # Conditional sum
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_stock_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_stock_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
            total_investment=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
        ).reset_index()

    processed_data_global_stk_grp['stock_sale_pl'] = processed_data_global_stk_grp['total_stock_sale_cost']
    processed_data_global_stk_grp['total_loss'] = processed_data_global_stk_grp['total_trades'] - processed_data_global_stk_grp['total_wins'] 
    processed_data_global_stk_grp['total_profit'] = processed_data_global_stk_grp['total_premium_collected'] + processed_data_global_stk_grp['stock_sale_pl']
    processed_data_global_stk_grp['w_L'] = (processed_data_global_stk_grp['total_wins'] / processed_data_global_stk_grp['total_trades']) * 100
    processed_data_global_stk_grp['net_quantity'] = processed_data_global_stk_grp['total_stock_assign_quantity'] - processed_data_global_stk_grp['total_stock_sold_quantity']
    processed_data_global_stk_grp['acb'] = np.where(
                                                    processed_data_global_stk_grp['net_quantity'] == 0, 
                                                        0,  # Default value when dividing by zero
                                                        ((processed_data_global_stk_grp['total_stock_assign_cost'] * -1) - 
                                                        (processed_data_global_stk_grp['total_premium_collected'] / 100)) /
                                                        processed_data_global_stk_grp['net_quantity']
                                                    )
    processed_data_global_stk_grp['acb'] = processed_data_global_stk_grp['acb'].round(2)


    stock_data = stock_data[['callorPut','buySell','trade_open_date','expiry_date','strike_price','number_of_contracts_sold','premium_per_contract','net_buyback_price','number_of_buyback','buyback_date','net_premium','assign_quantity','assign_price_per_share','assign_date','sold_quantity','sold_price_per_share','sold_date','is_closed','is_win']]

    stock_data_open = stock_data[(stock_data["is_closed"] == False) & (stock_data['callorPut'].isin(["Call", "Put"]))]
    stock_data_close = stock_data[(stock_data["is_closed"] == True) & (stock_data['callorPut'].isin(["Call", "Put"]))]

    stock_data_open = stock_data_open.drop(columns=['is_closed','is_win'])
    stock_data_close = stock_data_close.drop(columns=['is_closed'])
    stock_data_close = stock_data_close.round(2)


    stocks_purchased_sold = stock_data[~(stock_data['callorPut'].isin(["Call", "Put"]))]
    stocks_purchased_sold = stocks_purchased_sold[['buySell','assign_quantity','assign_date','assign_price_per_share','sold_quantity','sold_price_per_share','sold_date']]


    
    stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    return render_template('stock_details.html',
                           account_id=account_id,
                           symbol=symbol,
                           stk_smry=stk_smry,
                           open_cols=stock_data_open.columns , open_data=stock_data_open.values.tolist(),
                           closed_cols=stock_data_close.columns, closed_data=stock_data_close.values.tolist(),
                           stocks_purchased_sold_cols = stocks_purchased_sold.columns,
                           stocks_purchased_sold_data=stocks_purchased_sold.values.tolist())


@app.route('/')
def index():
    global global_trade_info
    global raw_df

    try:
        cached_data = get_from_cache("trades")
        if cached_data:
            print("Loaded from cache")
            trade_data=cached_data
        else:

            broker_name = 'IBKR'
            is_test = False
        
            if broker_name == 'IBKR':
                broker = IBKRBroker(is_test)
            else:
                return f"Broker '{broker_name}' is not supported."    
        
            trade_data = broker.get_data()
            set_to_cache("trades",trade_data)
        
        df = pd.DataFrame([vars(trade) for trade in trade_data])
        raw_df = transform_data(df)

        
        processed_data = process_wheel_trades(raw_df)
        global_trade_info = processed_data

        ##################account info############################
        processed_data_global_account_grp = processed_data.groupby('accountId').agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(processed_data['is_closed']) & (processed_data['callorPut'].isin(["Call", "Put"]))].count() ),
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[processed_data['is_closed']==False].count()),
            total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[processed_data['is_closed']].sum()),  # Conditional sum
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum')
        ).reset_index()

        processed_data_global_account_grp['total_loss'] = processed_data_global_account_grp['total_trades'] - processed_data_global_account_grp['total_wins']
        processed_data_global_account_grp['stock_sale_pl'] = processed_data_global_account_grp['total_stock_sale_cost']
        processed_data_global_account_grp['total_open_trades'] = processed_data_global_account_grp['total_open_trades']
        processed_data_global_account_grp['total_profit'] = processed_data_global_account_grp['total_premium_collected'] + processed_data_global_account_grp['stock_sale_pl']
    

        ############# TOTAL INFO #############
        total_premium_coollected = processed_data_global_account_grp['total_premium_collected'].sum().round(2)
        p_l_stock = processed_data_global_account_grp['stock_sale_pl'].sum().round(2)
        total_wins = processed_data_global_account_grp['total_wins'].sum()
        total_trades = processed_data_global_account_grp['total_trades'].sum()
        total_open_trades = processed_data_global_account_grp['total_open_trades'].sum()

        total_profit = total_premium_coollected + p_l_stock
        total_loss = total_trades - total_wins
        win_percentage = ((total_wins / (total_trades)) * 100).round(2)

        # get the account-stock level info
        processed_data_global_stk_grp = processed_data.groupby(['accountId','symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(processed_data['is_closed']) & (processed_data['callorPut'].isin(["Call", "Put"]))].count()),
            total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[processed_data['is_closed']].sum()),  # Conditional sum
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[processed_data['is_closed']==False].count()),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum')
        ).reset_index()

        processed_data_global_stk_grp['stock_sale_pl'] = processed_data_global_stk_grp['total_stock_sale_cost']
        processed_data_global_stk_grp['total_loss'] = processed_data_global_stk_grp['total_trades'] - processed_data_global_stk_grp['total_wins'] 
        processed_data_global_stk_grp['total_profit'] = processed_data_global_stk_grp['total_premium_collected'] + processed_data_global_stk_grp['stock_sale_pl']
        processed_data_global_stk_grp['w_L'] = (processed_data_global_stk_grp['total_wins'] / processed_data_global_stk_grp['total_trades']) * 100
        processed_data_global_stk_grp['total_stock_quantity'] = processed_data_global_stk_grp['total_assign_quantity'] - processed_data_global_stk_grp['total_sold_quantity']


        # get trade data by date
        processed_data_by_date = processed_data.groupby(['accountId','symbol','trade_open_date']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum')
        ).reset_index()
        processed_data_by_date['total_profit'] = processed_data_by_date['total_premium_collected'] + processed_data_by_date['total_stock_sale_cost']
        processed_data_by_date['year'] = processed_data_by_date['trade_open_date'].dt.year
        processed_data_by_date['month'] = processed_data_by_date['year'].astype(str) + "-" + processed_data_by_date['trade_open_date'].dt.month.astype(str)
        processed_data_by_date['year_quarter'] = processed_data_by_date['trade_open_date'].dt.year.astype(str) + "-Q" + processed_data_by_date['trade_open_date'].dt.quarter.astype(str)

        processed_data_by_date = processed_data_by_date[['accountId','symbol','month','year_quarter','year','total_profit']]
         
        processed_data_by_year = processed_data_by_date.groupby(['month']).agg(
            total_profit=pd.NamedAgg(column='total_profit', aggfunc='sum')
        ).reset_index()
        processed_data_by_year = processed_data_by_year.sort_values('month')

        profit_data = processed_data_by_year.to_dict(orient='records')
    


        processed_data_global_stk_grp = processed_data_global_stk_grp.round(2)
        account_stk_merge_dic = processed_data_global_stk_grp.to_dict(orient='records')
        account_dict = defaultdict(list)
        for item in account_stk_merge_dic:
            account_dict[item['accountId']].append(item)

        # Convert defaultdict to a normal dictionary
        account_dict = dict(account_dict)

        return render_template('index.html', 
                           total_premium_collected=total_premium_coollected ,
                           total_open_trades=total_open_trades,
                            total_profit=total_profit,
                            p_l_stock=p_l_stock,
                            total_wins=total_wins, 
                            total_loss=total_loss, 
                            win_percentage=win_percentage,
                            account_summary=processed_data_global_account_grp.to_dict(orient='records'),
                            account_stk_merge=account_dict,
                            profit_data=profit_data
                           )

    except Exception as e:
        return f"An error occurred: {e}"


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render provides PORT as an environment variable
    app.run(host="0.0.0.0", port=port)
    