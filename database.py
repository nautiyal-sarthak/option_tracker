import sqlite3
from entity.trade import Trade

DB_NAME = "trades.db"


def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def check_and_create_table():
    """Check if the 'trades' table exists; if not, create it."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
    
                   
        CREATE TABLE if not exists trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    optionId TEXT NOT NULL,
    tradeDate TEXT NOT NULL, 
    accountId TEXT NOT NULL, 
    symbol TEXT NOT NULL, 
    putCall TEXT, 
    buySell TEXT NOT NULL, 
    openCloseIndicator TEXT, 
    strike REAL, 
    expiry TEXT, 
    quantity INTEGER NOT NULL, 
    tradePrice REAL NOT NULL, 
    commission REAL, 
    assetCategory TEXT NOT NULL
);
        
    """)
    conn.commit()
    conn.close()

def get_max_trade_date():
    check_and_create_table()
    """Fetch the maximum trade_date from the 'trades' table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT MAX(tradeDate) FROM trades")
    max_date = cursor.fetchone()[0]  # Get the first column of the first row
    
    conn.close()
    return max_date

def get_all_trades():
    """Fetch all trades from the 'trades' table."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT optionId,tradeDate,accountId,symbol,putCall,buySell,openCloseIndicator,strike,expiry,quantity,tradePrice,commission,assetCategory FROM trades")
    rows = cursor.fetchall()  # Get all rows
    
    conn.close()

    trades = [Trade(*row) for row in rows]
    return trades

def insert_trades(trades):
    """Insert a list of Trade objects into the database."""
    conn = sqlite3.connect("trades.db")
    cursor = conn.cursor()
    
    insert_query = """
        INSERT INTO trades (
            optionId, tradeDate, accountId, symbol, putCall, buySell, openCloseIndicator, 
            strike, expiry, quantity, tradePrice, commission, assetCategory
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    
    # Convert Trade objects to tuples
    trade_tuples = [
        (trade.optionId, trade.tradeDate, trade.accountId, trade.symbol, trade.putCall, 
         trade.buySell, trade.openCloseIndicator, trade.strike, trade.expiry, 
         trade.quantity, trade.tradePrice, trade.commission, trade.assetCategory)
        for trade in trades
    ]

    # Insert all trades in one batch for efficiency
    cursor.executemany(insert_query, trade_tuples)

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print(f"Inserted {len(trades)} trades successfully!")    
