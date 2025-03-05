import sqlite3
from app.models.trade import Trade
from flask import current_app

DB_NAME = "trades.db"


def get_db_connection():
    try:
        current_app.logger.info('fetching DB connection')
        conn = sqlite3.connect(DB_NAME)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception as e:
        current_app.logger.error(f"Error connecting to database: {e}")
        raise e

def check_and_create_table():
    """Check if the 'trades' and 'user_audit' tables exist; if not, create them."""
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
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
        raise e
    finally:
        conn.close()

def get_max_trade_date(email):
    try:
        current_app.logger.info('fetching max trade date')
        """Fetch the maximum trade_date from the 'trades' table."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(tradeDate) FROM trades WHERE user_id = ?", (email,))
        max_date = cursor.fetchone()[0]  # Get the first column of the first row
        
        conn.close()
        return max_date
    except Exception as e:
        print(f"Error fetching max trade date: {e}")
        raise e

def get_all_trades(email):
    try:
        current_app.logger.info('fetching all trades')
        """Fetch all trades from the 'trades' table."""
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT optionId,tradeDate,accountId,symbol,putCall,buySell,openCloseIndicator,strike,expiry,quantity,tradePrice,commission,assetCategory FROM trades WHERE user_id = ?", (email,))
        rows = cursor.fetchall()  # Get all rows
        
        conn.close()

        trades = [Trade(*row) for row in rows]
        return trades
    except Exception as e:
        print(f"Error fetching all trades: {e}")
        raise e

def insert_trades(trades,email):
    try:
        current_app.logger.info('inserting trades')
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

        current_app.logger.info('Inserted {len(trades)} trades successfully!')   
    except Exception as e:
        print(f"Error inserting trades: {e}")
        raise e    

def getUserToken(id):
    try:
        current_app.logger.info('fetching user token')
        conn = sqlite3.connect("trades.db")
        cursor = conn.cursor()
        cursor.execute("SELECT auth_token,broker_name FROM user_audit where user_id = ?", (id,))
        out = cursor.fetchall()
        if len(out) == 0:
            return None, None
        else:
            token = out[0][0]
            broker = out[0][1]

        conn.close()
        return token , broker
    except Exception as e:
        print(f"Error fetching user token: {e}")
        raise e

def update_refresh_token(user, new_token):
    try:
        """Update the refresh token for a user."""
        current_app.logger.info('updating refresh token')
        conn = get_db_connection()
        cursor = conn.cursor()

        try:
            cursor.execute("UPDATE user_audit SET auth_token = ? WHERE user_id = ?", (new_token, user))
            conn.commit()
        except Exception as e:
            print(f"Error updating refresh token: {e}")
        finally:
            conn.close()
    except Exception as e:
        print(f"Error updating refresh token: {e}")
        raise e