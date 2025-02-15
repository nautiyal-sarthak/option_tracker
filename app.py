from flask import Flask, render_template
from brokers.broker_ibkr import IBKRBroker
import pandas as pd
from utility import *
import logging
from collections import defaultdict
from stock  import *


# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')




app = Flask(__name__)

if 'global_trade_info' not in globals():
    global_trade_info = None

def style_closed_trades(df):
    def highlight_row(row):
        if row["is_win"]:
            return ["background-color: #d4edda"] * len(row)  # Light green for win
        else:
            return ["background-color: #f8d7da"] * len(row)  # Light red for loss

    return df.style.apply(highlight_row, axis=1).set_table_attributes('class="table table-striped table-bordered"').to_html(escape=False)


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
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[stock_data['is_closed']].count()),
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


    stock_data = stock_data[['callorPut','trade_open_date','expiry_date','strike_price','number_of_contracts_sold','premium_per_contract','net_buyback_price','buyback_date','net_premium','assign_quantity','assign_date','sold_quantity','sold_date','is_closed','is_win']]

    stock_data_open = stock_data[stock_data["is_closed"] == False]
    stock_data_close = stock_data[stock_data["is_closed"] == True]

    stock_data_open = stock_data_open.drop(columns=['is_closed','is_win'])
    stock_data_close = stock_data_close.drop(columns=['is_closed'])
    stock_data_close = stock_data_close.round(2)


    
    stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    return render_template('stock_details.html',
                           account_id=account_id,
                           symbol=symbol,
                           stk_smry=stk_smry,
                           open_cols=stock_data_open.columns , open_data=stock_data_open.values.tolist(),
                           closed_cols=stock_data_close.columns, closed_data=stock_data_close.values.tolist())


@app.route('/')
def index():
    global global_trade_info
    global raw_df

    broker_name = 'IBKR'
    is_test = False
    
    if broker_name == 'IBKR':
        broker = IBKRBroker(is_test)
    else:
        return f"Broker '{broker_name}' is not supported."
    
    try:
        trade_data = broker.get_data()
        df = pd.DataFrame([vars(trade) for trade in trade_data])
        raw_df = transform_data(df)

        
        processed_data = process_wheel_trades(raw_df)
        global_trade_info = processed_data

        ##################account info############################
        processed_data_global_account_grp = processed_data.groupby('accountId').agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[processed_data['is_closed']].count()),
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
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[processed_data['is_closed']].count()),
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
                            account_stk_merge=account_dict
                           )

    except Exception as e:
        return f"An error occurred: {e}"


if __name__ == "__main__":
    app.run(debug=True)