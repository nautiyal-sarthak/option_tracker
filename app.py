from flask import Flask, render_template, request, jsonify , redirect, url_for, session
from brokers.broker_ibkr import IBKRBroker
from brokers.broker_quest import QuestradeBroker
import pandas as pd
from utility import *
import logging
from collections import defaultdict
from datetime import datetime, timedelta, date
import os
import numpy as np
from authlib.integrations.flask_client import OAuth
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask_session import Session
from database import *
from dotenv import load_dotenv


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

app.secret_key = os.getenv("SECRET_KEY", "change_this_to_a_secure_value")
app.config['SESSION_TYPE'] = 'filesystem'  # Store session data in the file system
app.config['SESSION_PERMANENT'] = False
app.config['SESSION_USE_SIGNER'] = True  # Sign session cookies for security
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=1)  # Set session lifetime to 1 day
app.config['SESSION_FILE_THRESHOLD'] = 500  # Maximum number of session files to keep
app.config['SESSION_FILE_MODE'] = 0o600  # File mode for session files

Session(app)

# Google OAuth Configuration
load_dotenv()
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET")

oauth = OAuth(app)
oauth.register(
    name='google',
    client_id=GOOGLE_CLIENT_ID,
    client_secret=GOOGLE_CLIENT_SECRET,
    server_metadata_url='https://accounts.google.com/.well-known/openid-configuration',
    api_base_url='https://www.googleapis.com/oauth2/v1/',
    client_kwargs={'scope': 'openid email profile'}
)

# Flask-Login Setup
login_manager = LoginManager()
login_manager.login_view = "login"
login_manager.init_app(app)

class User(UserMixin):
    def __init__(self, id, name, email):
        self.id = id
        self.name = name
        self.email = email

users = {}

@login_manager.user_loader
def load_user(user_id):
    return users.get(user_id)

@app.route('/')
def home():
    check_and_create_table()
    return 'Welcome! <a href="/login">Login with Google</a>'

@app.route('/login')
def login():
    session.clear()
    return oauth.google.authorize_redirect(url_for('callback', _external=True))

@app.route('/login/callback')
def callback():
    expected_state = session.get('oauth_state')  # This is where Flask stores state
    token = oauth.google.authorize_access_token()
    user_info = oauth.google.get('userinfo').json()

    logging.debug(f"Expected State: {expected_state}")
    logging.debug(f"Received Token State: {token.get('state', 'N/A')}")

    user_id = user_info['id']
    user = User(id=user_id, name=user_info['name'], email=user_info['email'])
    users[user_id] = user

    login_user(user)
    return redirect(url_for('dashboard'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    return redirect(url_for('home'))


if 'global_trade_info' not in globals():
    global_trade_info = None

if 'global_filter_type' not in globals():
    global_filter_type = None


def filter_by_time_period(df, filter_type):
    today = datetime.now()
    
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
        return df[(df['trade_open_date'] >= start_date) & (df['trade_open_date'] < end_date)]
    elif filter_type == 'all':
        return df
    else:
        start_date = df['trade_open_date'].min()
    
    return df[df['trade_open_date'] >= start_date]

def convert_to_serializable(obj):
    """Convert NumPy types and NaN to Python native types for JSON serialization."""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        if np.isnan(obj):  # Handle NaN
            return None  # Convert NaN to JSON null
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(item) for item in obj]
    return obj

def process_trade_data(email,token=None,broker_name=None,filter_type='all'):
    global global_trade_info
    global global_filter_type

    global_filter_type = filter_type

    try:
        cached_data = get_from_cache(email)
        if cached_data:
            print("Loaded from cache")
            trade_data = cached_data
        else:
            is_test = False
            if broker_name == 'IBKR':
                broker = IBKRBroker(token,is_test)
            elif broker_name == 'Quest':
                broker = QuestradeBroker(token,is_test)
            else:
                raise Exception(f"Broker '{broker_name}' is not supported.")
            trade_data = broker.get_data(email)
            set_to_cache(email, trade_data)

        df = pd.DataFrame([vars(trade) for trade in trade_data])
        raw_df = transform_data(df)
        processed_data = process_wheel_trades(raw_df)
        global_trade_info = processed_data

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
        account_summary['stock_sale_pl'] = account_summary['total_stock_sale_cost']
        account_summary['total_profit'] = account_summary['total_premium_collected'] + account_summary['stock_sale_pl']

        # Total info
        total_premium_collected = account_summary['total_premium_collected'].sum().round(2)
        total_premium_collected_open = account_summary['total_premium_collected_open'].sum().round(2)
        total_premium_formated = str(total_premium_collected) + "(" + str(total_premium_collected_open) + ")"
        p_l_stock = account_summary['stock_sale_pl'].sum().round(2)
        total_wins = account_summary['total_wins'].sum()
        total_trades = account_summary['total_trades'].sum()
        total_open_trades = account_summary['total_open_trades'].sum()
        total_profit = total_premium_collected + p_l_stock
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

        stock_summary['stock_sale_pl'] = stock_summary['total_stock_sale_cost']
        stock_summary['total_loss'] = stock_summary['total_trades'] - stock_summary['total_wins']
        stock_summary['total_profit'] = stock_summary['total_premium_collected'] + stock_summary['stock_sale_pl']
        stock_summary['w_L'] = np.where(
            stock_summary['total_trades'] == 0,
            0,  # Set to 0 when total_trades is 0
            (stock_summary['total_wins'] / stock_summary['total_trades']) * 100
        )
        stock_summary['total_stock_quantity'] = stock_summary['total_assign_quantity'] + stock_summary['total_sold_quantity']

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

@app.route('/dashboard')
@login_required
def dashboard():
    try:
        token,broker = getUserToken(current_user.email)
        data = process_trade_data(current_user.email,token,broker,'all')
        return render_template('index.html', **data)
    except Exception as e:
        return str(e)

@app.route('/get_data', methods=['GET'])
@login_required
def get_data():
    try:
        filter_type = request.args.get('filter', 'all')
        data = process_trade_data(current_user.email,filter_type=filter_type)
        return data  # This will now work with converted types
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/account/<account_id>/symbol/<symbol>')
@login_required
def stock_details_inner(account_id, symbol):
    global global_trade_info
    global global_filter_type

    if global_trade_info is None:
        return "Stock data is not available yet. Please try again later."
    
    if global_filter_type is None:
        global_filter_type = "all"

    # Filter stock details for the given account and symbol
    stock_data = global_trade_info[
        (global_trade_info['accountId'] == account_id) & 
        (global_trade_info['symbol'] == symbol)
    ]

    stock_data = stock_data.reset_index()
    stock_data = filter_by_time_period(stock_data,global_filter_type)

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

    processed_data_global_stk_grp['stock_sale_pl'] = processed_data_global_stk_grp['total_stock_sale_cost']
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

    processed_data_global_stk_grp = processed_data_global_stk_grp.round(2)
    stk_smry = processed_data_global_stk_grp.to_dict(orient='records')[0]
    return render_template('stock_details.html',
                          account_id=account_id,
                          global_filter_type=global_filter_type,
                          symbol=symbol,
                          stk_smry=stk_smry,
                          open_cols=stock_data_open.columns, open_data=stock_data_open.values.tolist(),
                          closed_cols=stock_data_close.columns, closed_data=stock_data_close.values.tolist(),
                          stocks_purchased_sold_cols=stocks_purchased_sold.columns,
                          stocks_purchased_sold_data=stocks_purchased_sold.values.tolist())

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))  # Render provides PORT as an environment variable
    app.run(host="0.0.0.0", port=port,debug=True)