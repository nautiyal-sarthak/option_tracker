from flask import Blueprint, render_template, current_app
from flask_login import login_required
from ..utils.data import *
import pandas as pd
import numpy as np
from ..utils.data import format_processed_data, getStockSummary

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

    stocks_purchased_sold = stock_data[(stock_data["status"].isin(['ASSIGNED','SOLD STOCK','BOUGHT STOCK','TAKEN AWAY']))]
    stocks_purchased_sold = format_stock_data(stocks_purchased_sold)

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


def format_stock_data(df):
    df = df[['trade_open_date','assign_price_per_share','sold_price_per_share','assign_quantity','sold_quantity','status']]
    df['Price'] = np.where(df['assign_price_per_share'] == 0, df['sold_price_per_share'] , df['assign_price_per_share'])
    df['Quantity'] = np.where(df['assign_quantity'] == 0, df['sold_quantity'] , df['assign_quantity'])
    
    df.rename(columns={
        'trade_open_date': 'Date'
    }, inplace=True)

    return df[['Date','status','Price','Quantity']]

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

