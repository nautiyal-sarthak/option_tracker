from flask import Blueprint, render_template
from flask_login import login_required
from ..utils.data import *
import pandas as pd
import numpy as np

bp = Blueprint('stock', __name__)

@bp.route('/account/<account_id>/symbol/<symbol>')
@login_required
def stock_details_inner(account_id, symbol):
    if global_trade_info is None:
        return "Stock data is not available yet. Please try again later."
    
    filter_type = global_filter_type or 'all'
    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) & 
        (global_trade_info['symbol'] == symbol)
    ].reset_index()
    stock_data = filter_by_time_period(stock_data, filter_type)

    # Process stock data (similar to your existing logic)
    processed_data = stock_data.groupby(['accountId', 'symbol']).agg(
        total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
        # ... (rest of your aggregation logic)
    ).reset_index()

    # Add your additional processing here (stock_sale_pl, total_profit, etc.)
    processed_data['stock_sale_pl'] = processed_data['total_stock_sale_cost']
    # ... (rest of your processing)

    stk_smry = processed_data.round(2).to_dict(orient='records')[0]
    # Prepare open/closed data and stocks purchased/sold
    # ... (rest of your logic)

    return render_template('stock_details.html', 
                          account_id=account_id,
                          global_filter_type=filter_type,
                          symbol=symbol,
                          stk_smry=stk_smry,
                          # ... (rest of your template args)
                          )