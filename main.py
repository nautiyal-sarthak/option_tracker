from flask import Flask, render_template
from brokers.broker_ibkr import IBKRBroker
from brokers.broker_quest import QuestradeBroker
import pandas as pd
import logging
from utility import *
from tabulate import tabulate

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

broker_name = 'Quest'
is_test = True

pd.set_option("display.max_columns", None)  # Show all columns
pd.set_option("display.width", 1000)       # Prevent line wrapping
pd.set_option("display.float_format", "{:.2f}".format)  # Format float numbers

def print_df(df):
    print(tabulate(df, headers="keys", tablefmt="psql"))

if broker_name == 'IBKR':
    broker = IBKRBroker(is_test)
else:
    broker = QuestradeBroker(is_test)

trade_data = broker.get_data()
df = pd.DataFrame([vars(trade) for trade in trade_data])
raw_df = transform_data(df)
print_df(raw_df)
pro_df = process_wheel_trades(raw_df) 
print_df(pro_df)

print("================================")


