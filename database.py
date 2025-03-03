import sqlite3
from entity.trade import Trade
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = "trades.db"


def get_db_connection():
    try:
        logging.info("Connecting to database... " + DB_NAME)
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        raise

def check_and_create_table():
    """Check if the 'trades' and 'user_audit' tables exist; if not, create them."""
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Create 'trades' table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id VARCHAR(255),                 
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

        # Create 'user_audit' table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_audit (
            user_id VARCHAR(255) PRIMARY KEY,
            auth_token TEXT NOT NULL,
            broker_name TEXT NOT NULL
        );
        """)

        # Insert user into 'user_audit' only if it doesn't exist
        cursor.execute("""
        INSERT INTO user_audit (user_id, auth_token, broker_name)
        SELECT 'nautiyal.sarthak@gmail.com', '121034539652171842836741', 'IBKR'
        WHERE NOT EXISTS (
            SELECT 1 FROM user_audit WHERE user_id = 'nautiyal.sarthak@gmail.com'
        );
        """)

        # Insert user into 'user_audit' only if it doesn't exist
        cursor.execute("""
        INSERT INTO user_audit (user_id, auth_token, broker_name)
        SELECT 'nauty.om@gmail.com', 'jhjIZqExuB8_YAboAWHh-1Y8E-wY3IOr0', 'Quest'
        WHERE NOT EXISTS (
            SELECT 1 FROM user_audit WHERE user_id = 'nauty.om@gmail.com'
        );
        """)

        # Insert user into 'user_audit' only if it doesn't exist
        cursor.execute("""
        INSERT INTO user_audit (user_id, auth_token, broker_name)
        SELECT 'sahil.bhola2@gmail.com', 'dsgGZzzheE9bbGdlmMQ1wGWkGQ2hAZSn0', 'Quest'
        WHERE NOT EXISTS (
            SELECT 1 FROM user_audit WHERE user_id = 'sahil.bhola2@gmail.com'
        );
        """)

        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")
        raise
    finally:
        conn.close()

def get_max_trade_date(email):
    try:
        """Fetch the maximum trade_date from the 'trades' table."""
        conn = get_db_connection()
        cursor = conn.cursor()
        logging.info("Fetching max trade date for user: " + email)
        cursor.execute("SELECT MAX(tradeDate) FROM trades WHERE user_id = ?", (email,))
        max_date = cursor.fetchone()[0]  # Get the first column of the first row
        
        conn.close()
        return max_date
    except Exception as e:
        logging.error(f"Error fetching max trade date: {e}")
        raise

def get_all_trades(email):
    try:
        """Fetch all trades from the 'trades' table."""
        conn = get_db_connection()
        cursor = conn.cursor()
        logging.info("Fetching all trades for user: " + email)
        cursor.execute("SELECT optionId,tradeDate,accountId,symbol,putCall,buySell,openCloseIndicator,strike,expiry,quantity,tradePrice,commission,assetCategory FROM trades WHERE user_id = ?", (email,))
        rows = cursor.fetchall()  # Get all rows
        
        conn.close()

        trades = [Trade(*row) for row in rows]
        return trades
    except Exception as e:
        logging.error(f"Error fetching all trades: {e}")
        raise

def insert_trades(trades,email):
    try:
        logging.info(f"Inserting {len(trades)} trades into the database...")
        """Insert a list of Trade objects into the database."""
        conn = sqlite3.connect("trades.db")
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO trades (
                user_id,optionId, tradeDate, accountId, symbol, putCall, buySell, openCloseIndicator, 
                strike, expiry, quantity, tradePrice, commission, assetCategory
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        # Convert Trade objects to tuples
        trade_tuples = [
            (email,trade.optionId, trade.tradeDate, trade.accountId, trade.symbol, trade.putCall, 
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
    except Exception as e:
        print(f"Error inserting trades: {e}")
        raise   


def update_refresh_token(old_token, new_token):
    try:
        """Update the refresh token for a user."""
        logging.info(f"Updating refresh token from {old_token} to {new_token} for user..." + session['user']) 
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("UPDATE user_audit SET auth_token = ? WHERE auth_token = ?", (new_token, old_token))
            conn.commit()
        except Exception as e:
            print(f"Error updating refresh token: {e}")
        finally:
            conn.close()
    except Exception as e:
        print(f"Error updating refresh token: {e}")
        raise