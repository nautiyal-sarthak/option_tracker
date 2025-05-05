from flask import Blueprint, render_template, current_app
from flask_login import login_required
from ..utils.data import *
import pandas as pd
import numpy as np
from ..utils.data import format_processed_data

bp = Blueprint('stock', __name__)

@bp.route('/account/<account_id>/symbol/<symbol>')
@login_required
def stock_details_inner(account_id, symbol):
    current_app.logger.info('fetching the details for the stock ' + symbol)
    if session.get('master_trade_data') is None:
        return "Stock data is not available yet. Please try again later."
    
    filter_type = session['filter_type']
    
    global_trade_info = session['master_trade_data']

    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) & 
        (global_trade_info['symbol'] == symbol)
    ].reset_index()
    stock_data = filter_by_time_period(stock_data, filter_type)

    # Get summary data
    processed_data_global_stk_grp = getStockSummary(stock_data)
    stock_data_formated = format_processed_data(stock_data)
    
    stock_data_open = stock_data_formated[(stock_data_formated["Status"] == 'OPEN')]
    stock_data_close = stock_data_formated[(stock_data_formated["Status"] != 'OPEN')]

    stock_data_open.drop(columns=['ROI', 'Status','Close week'], inplace=True)
    stock_data_open.rename(columns={
        'Net Profit': 'Unrealised Profit'
    }, inplace=True)

    stocks_purchased_sold = stock_data[(stock_data["status"].isin(['ASSIGNED']))]
    stocks_purchased_sold = stocks_purchased_sold[['assign_date','assign_quantity','assign_price_per_share','status']]

    stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    return render_template('stock_details.html',
                          account_id=account_id,
                          global_filter_type=session['filter_type'],
                          symbol=symbol,
                          stk_smry=stk_smry,
                          open_cols=stock_data_open.columns, open_data=stock_data_open.values.tolist(),
                          closed_cols=stock_data_close.columns, closed_data=stock_data_close.values.tolist(),
                          stocks_purchased_sold_cols=stocks_purchased_sold.columns,
                          stocks_purchased_sold_data=stocks_purchased_sold.values.tolist()
                          )



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
        # Group by accountId and sum all other numeric columns
        agg_df = df.groupby('accountId', as_index=False).sum(numeric_only=True)
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

