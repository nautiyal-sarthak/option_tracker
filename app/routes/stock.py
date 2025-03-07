from flask import Blueprint, render_template, current_app
from flask_login import login_required
from ..utils.data import *
import pandas as pd
import numpy as np

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
    processed_data_global_stk_grp = stock_data.groupby(['accountId', 'symbol']).agg(
        total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
        total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[~stock_data['is_closed']].sum()),
        total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(stock_data['is_closed']) & (stock_data['callorPut'].isin(["Call", "Put"]))].count()),
        total_wins=pd.NamedAgg(column='is_win', aggfunc=lambda x: x[stock_data['is_closed']].sum()),
        total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
        total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
        total_stock_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
        total_stock_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
        total_investment=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
    ).reset_index()

    processed_data_global_stk_grp['stock_sale_pl'] = processed_data_global_stk_grp['total_stock_sale_cost'] + processed_data_global_stk_grp['total_stock_assign_cost']
    processed_data_global_stk_grp['total_loss'] = processed_data_global_stk_grp['total_trades'] - processed_data_global_stk_grp['total_wins']
    processed_data_global_stk_grp['total_profit'] = processed_data_global_stk_grp['total_premium_collected'] + processed_data_global_stk_grp['stock_sale_pl']
    processed_data_global_stk_grp['w_L'] = (processed_data_global_stk_grp['total_wins'] / processed_data_global_stk_grp['total_trades']) * 100
    processed_data_global_stk_grp['net_quantity'] = processed_data_global_stk_grp['total_stock_assign_quantity'] + processed_data_global_stk_grp['total_stock_sold_quantity']
    processed_data_global_stk_grp['acb'] = np.where(
        processed_data_global_stk_grp['net_quantity'] == 0,
        0,
        ((processed_data_global_stk_grp['total_stock_assign_cost'] * -1) - (processed_data_global_stk_grp['total_premium_collected'] / 100)) /
        processed_data_global_stk_grp['net_quantity']
    )
    processed_data_global_stk_grp['acb'] = processed_data_global_stk_grp['acb'].round(2)


    stock_data = stock_data[['callorPut', 'buySell', 'trade_open_date', 'expiry_date', 'strike_price', 'number_of_contracts_sold', 
                             'premium_per_contract', 'net_buyback_price', 'number_of_buyback', 'buyback_date', 'net_premium', 
                             'assign_quantity', 'assign_price_per_share', 'assign_date', 'sold_quantity', 'sold_price_per_share', 
                             'sold_date', 'is_closed', 'is_win']]

    stock_data_open = stock_data[(stock_data["is_closed"] == False) & (stock_data['callorPut'].isin(["Call", "Put"]))]
    stock_data_close = stock_data[(stock_data["is_closed"] == True) & (stock_data['callorPut'].isin(["Call", "Put"]))]

    stock_data_open = stock_data_open.drop(columns=['is_closed', 'is_win'])
    stock_data_close = stock_data_close.drop(columns=['is_closed'])
    stock_data_close = stock_data_close.round(2)

    stocks_purchased_sold = stock_data[~(stock_data['callorPut'].isin(["Call", "Put"]))]
    stocks_purchased_sold = stocks_purchased_sold[['buySell', 'assign_quantity', 'assign_date', 'assign_price_per_share', 
                                                   'sold_quantity', 'sold_price_per_share', 'sold_date']]
    stocks_purchased_sold = stocks_purchased_sold.round(2)
    stocks_purchased_sold['total_assign_cost'] = (stocks_purchased_sold['assign_quantity'] * stocks_purchased_sold['assign_price_per_share']) * -1
    stocks_purchased_sold['total_sold_cost'] = (stocks_purchased_sold['sold_quantity'] * stocks_purchased_sold['sold_price_per_share']) * -1
    stocks_purchased_sold["Qty"] = stocks_purchased_sold['assign_quantity'] + stocks_purchased_sold['sold_quantity']
    
    stocks_purchased_sold["trade_date"] = np.where(
        stocks_purchased_sold['assign_date']==0,
        stocks_purchased_sold['sold_date'],
        stocks_purchased_sold['assign_date']
        )
    stocks_purchased_sold["total_cost"] = stocks_purchased_sold['total_assign_cost'] + stocks_purchased_sold['total_sold_cost']

    stocks_purchased_sold = stocks_purchased_sold[['trade_date', 'buySell', 'Qty', 'total_cost']]
    

    processed_data_global_stk_grp = processed_data_global_stk_grp.round(2)
    stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    return render_template('stock_details.html',
                          account_id=account_id,
                          global_filter_type=session['filter_type'],
                          symbol=symbol,
                          stk_smry=stk_smry,
                          open_cols=stock_data_open.columns, open_data=stock_data_open.values.tolist(),
                          closed_cols=stock_data_close.columns, closed_data=stock_data_close.values.tolist(),
                          stocks_purchased_sold_cols=stocks_purchased_sold.columns,
                          stocks_purchased_sold_data=stocks_purchased_sold.values.tolist())


