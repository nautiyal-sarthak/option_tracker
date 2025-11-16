from flask import Blueprint, render_template, current_app,request
from flask_login import login_required
from ..utils.data import *
import pandas as pd
import numpy as np
from ..utils.data import format_processed_data, getStockSummary
from datetime import date

bp = Blueprint('stock', __name__)

@bp.route('/account/<account_id>/symbol/<symbol>')
@login_required
def stock_details_inner(account_id, symbol):
    current_app.logger.info(f'Fetching stock details for {symbol}')
    
    if session.get('master_trade_data') is None:
        return "Stock data is not available yet. Please try again later."

    # Read from query params
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    grouping = request.args.get('grouping', 'month')

    # Fall back to session values if query params are missing
    if not start_date:
        start_date = session.get('start_date')
    if not end_date:
        end_date = session.get('end_date')

    oldest_trade_date = session['oldest_trade_date']


    global_trade_info = session['master_trade_data']
    stk_cost_per_share = session['stk_cost_per_share']

    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) &
        (global_trade_info['symbol'] == symbol)
    ].reset_index()

    stock_data = filter_by_time_period(stock_data, start_date, end_date)

    # Get summary data
    processed_data_global_stk_grp = getStockSummary(stock_data, stk_cost_per_share)
    stock_data_formated = format_processed_data(stock_data)

    stock_data_open = stock_data_formated[(stock_data_formated["Status"] == 'OPEN')]
    stock_data_close = stock_data_formated[(stock_data_formated["Status"] != 'OPEN')]
    stock_data_close = stock_data_close.drop(columns=['Colateral_used'])

    stock_data_open.drop(columns=['ROI', 'Status', 'Close week'], inplace=True)
    stock_data_open.rename(columns={'Net Profit': 'Unrealised Profit'}, inplace=True)

    stocks_purchased_sold = stock_data[
        stock_data["status"].isin(['ASSIGNED', 'SOLD STOCK', 'BOUGHT STOCK', 'TAKEN AWAY'])
    ]
    stocks_purchased_sold = format_stock_data(stocks_purchased_sold)

    # if no trades are found, return an empty summary
    sample_summary = {
        'accountId': '',
        'symbol': '',
        'total_premium_collected': 0,
        'total_premium_collected_open': 0.0,
        'total_trades': 0,
        'total_wins': 0,
        'total_open_trades': 0,
        'total_stock_sale_cost': 0.0,
        'total_stock_assign_cost': 0.0,
        'total_assign_quantity': 0,
        'total_sold_quantity': 0,
        'avg_ROI': 0.0,
        'min_date': date(2025, 7, 8),
        'cost_basis_per_share': 0.0,
        'total_lost_trades': 0,
        'net_assign_qty': 0,
        'total_cost_of_sold_shares': 0.0,
        'realized_pnl': 0.0,
        'win_percent': 0,
        'net_profit': 0
    }


    if not processed_data_global_stk_grp.empty:
        stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    else:
        stk_smry = sample_summary
        
    print("stk_smry:", stk_smry)

    profit_by_month = getProfitPerTimePeriod(stock_data, processed_data_global_stk_grp, grouping)

    return render_template('stock_details.html',
                           filter_start= start_date,
                           filter_end= end_date,
                           account_id=account_id,
                           symbol=symbol,
                           stk_smry=stk_smry,
                           open_cols=stock_data_open.columns,
                           open_data=stock_data_open.values.tolist(),
                           closed_cols=stock_data_close.columns,
                           closed_data=stock_data_close.values.tolist(),
                           stocks_purchased_sold_cols=stocks_purchased_sold.columns,
                           stocks_purchased_sold_data=stocks_purchased_sold.values.tolist(),
                           profit_data=profit_by_month.to_dict(orient='records'),
                           oldest_trade_date=oldest_trade_date ,
                           grouping=grouping
                           
                           )


def format_stock_data(df):
    df = df[['trade_open_date','assign_price_per_share','sold_price_per_share','assign_quantity','sold_quantity','status','Colateral_used']]
    df['Price'] = np.where(df['assign_price_per_share'] == 0, df['sold_price_per_share'] , df['assign_price_per_share'])
    df['Quantity'] = np.where(df['assign_quantity'] == 0, df['sold_quantity'] , df['assign_quantity'])
    
    df.rename(columns={
        'trade_open_date': 'Date'
    }, inplace=True)

    return df[['Date','status','Price','Quantity','Colateral_used']]

def getTotalSummary(df):
    try:
        # Group by accountId and sum all other numeric columns
        agg_df = df.sum(numeric_only=True).to_frame().T
        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing getTotalSummary : {e}")
        raise

def getAccountSummary(df):
    try:
        # Group by accountId and sum all other numeric columns
        agg_df = df.groupby('accountId', as_index=False).sum(numeric_only=True)
        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing account summary: {e}")
        raise


def getProfitPerTimePeriod(df,stk_smry,grouping):
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
        df['year_month'] = df['close_date'].dt.strftime('%Y-%m')
        weekly_data = df[df['status'] != 'OPEN'].copy() 

        if grouping == 'month':
            key = ['year_month','symbol','accountId']
        elif grouping == 'week':
            key = ['month_week','symbol','accountId']
        elif grouping == 'day':
            key = ['close_date','symbol','accountId']

        weekly_data_grp = weekly_data.groupby(key).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum')
        ).reset_index()

        stk_smry['stk_avg_cost'] = (abs(stk_smry["total_stock_assign_cost"])/stk_smry["total_assign_quantity"])
        stk_cost_basis = stk_smry[['accountId','symbol','stk_avg_cost']].copy()
        weekly_data_grp_cost_merge = weekly_data_grp.merge(stk_cost_basis, on=['accountId','symbol'], how='left')
        weekly_data_grp_cost_merge['stk_avg_cost'] = weekly_data_grp_cost_merge['stk_avg_cost'].fillna(0.0)


        weekly_data_grp_cost_merge['total_cost_of_sold_shares'] = (weekly_data_grp_cost_merge['stk_avg_cost']) * weekly_data_grp_cost_merge["total_sold_quantity"]
        weekly_data_grp_cost_merge['realized_pnl'] = abs(weekly_data_grp_cost_merge['total_stock_sale_cost']) - abs(weekly_data_grp_cost_merge['total_cost_of_sold_shares'])

        

        weekly_data_grp_cost_merge["net_profit"] = weekly_data_grp_cost_merge['realized_pnl'] + weekly_data_grp_cost_merge['total_premium_collected']

        # rename the key column to month_week or year_month
        weekly_data_grp_cost_merge.rename(columns={key[0]: 'period'}, inplace=True)

        out_df = weekly_data_grp_cost_merge.groupby(['period']).agg(
            net_profit=pd.NamedAgg(column='net_profit', aggfunc='sum')
        ).reset_index()

        return out_df[['period','net_profit']]
    except Exception as e:
        logging.error(f"Error processing profit by month: {e}")
        raise

@bp.route('/get_stock_data')
@login_required
def get_stock_data():
    account_id = request.args.get('account_id')
    symbol = request.args.get('symbol')
    grouping = request.args.get('grouping', 'month')
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')

    global_trade_info = session.get('master_trade_data')
    if global_trade_info is None:
        return {"error": "Stock data not available"}, 400

    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) & 
        (global_trade_info['symbol'] == symbol)
    ].reset_index(drop=True)
    
    stock_data = filter_by_time_period(stock_data, start_date, end_date)

    processed_data_global_stk_grp = getStockSummary(stock_data, session['stk_cost_per_share'])
    profit_by_group = getProfitPerTimePeriod(stock_data, processed_data_global_stk_grp, grouping)

    return profit_by_group.to_dict(orient="records")

