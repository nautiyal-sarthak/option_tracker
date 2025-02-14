from flask import Flask, render_template


def get_account_and_stock_data(account_id, stock_symbol):
    # This should return account details and stock data for a given accountId and stock symbol.
    return {
        'account': {
            'accountId': account_id,
            'total_premium': 1000,
            'total_wins': 10,
            'total_losses': 5,
            'tot_profit': 300
        },
        'stock': {
            'symbol': stock_symbol,
            'total_premium': -200,
            'total_wins': 2,
            'total_losses': 1,
            'profit': 50,
            'tot_quantity_stk': 100,
            'w_L': 66.7
        }
    }


def stock_details(account_id, stock_symbol):
    # Get the data based on account_id and stock_symbol
    data = get_account_and_stock_data(account_id, stock_symbol)
    
    # Render the details page using a template, passing the data
    return render_template('stock_details.html', account=data['account'], stock=data['stock'])

