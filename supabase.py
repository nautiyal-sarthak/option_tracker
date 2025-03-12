import psycopg2
from psycopg2.extras import DictCursor
from app.models.trade import Trade
from flask import current_app
import os

# Get database connection details from environment variables
DB_URL = os.getenv("SUPABASE_DB_URL")

def get_db_connection():
    """Establishes and returns a connection to the Supabase PostgreSQL database."""
    try:
        current_app.logger.info('Fetching DB connection')
        conn = psycopg2.connect(DB_URL, cursor_factory=DictCursor)
        return conn
    except Exception as e:
        current_app.logger.error(f"Error connecting to database: {e}")
        raise e

def check_and_create_table():
    """Ensure 'trades' and 'user_audit' tables exist in the Supabase database."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Create 'trades' table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL ,
            user_id VARCHAR(255),
            optionId TEXT NOT NULL PRIMARY KEY,
            tradeDate DATE NOT NULL,
            accountId TEXT NOT NULL,
            symbol TEXT NOT NULL,
            putCall TEXT,
            buySell TEXT NOT NULL,
            openCloseIndicator TEXT,
            strike REAL,
            expiry DATE,
            quantity INTEGER NOT NULL,
            tradePrice REAL NOT NULL,
            commission REAL,
            assetCategory TEXT NOT NULL
        );
        """
        )
        
        # Create 'user_audit' table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user_audit (
            user_id VARCHAR(255) PRIMARY KEY,
            auth_token TEXT NOT NULL,
            broker_name TEXT NOT NULL
        );
        """
        )
        
        # Insert users if they do not exist
        users = [
            # ('nautiyal.sarthak@gmail.com', '121034539652171842836741', 'IBKR'),
            # ('nauty.om@gmail.com', 'jhjIZqExuB8_YAboAWHh-1Y8E-wY3IOr0', 'Quest'),
            ('sahil.bhola2@gmail.com', 'dsgGZzzheE9bbGdlmMQ1wGWkGQ2hAZSn0', 'Quest')
        ]
        for user in users:
            cursor.execute("""
            INSERT INTO user_audit (user_id, auth_token, broker_name)
            VALUES (%s, %s, %s)
            ON CONFLICT (user_id) DO NOTHING;
            """, user)
        
        conn.commit()
    except Exception as e:
        current_app.logger.error(f"Database error: {e}")
        raise e
    finally:
        conn.close()

def get_max_trade_date(email):
    """Fetch the latest trade date for a user."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT MAX(tradeDate) FROM trades WHERE user_id = %s;", (email,))
        max_date = cursor.fetchone()[0]
        return max_date
    except Exception as e:
        current_app.logger.error(f"Error fetching max trade date: {e}")
        raise e
    finally:
        conn.close()

def get_all_trades(email):
    """Retrieve all trades for a user."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT optionId, tradeDate, accountId, symbol, putCall, buySell, openCloseIndicator, strike, expiry, quantity, tradePrice, commission, assetCategory FROM trades WHERE user_id = %s;", (email,))
        rows = cursor.fetchall()
        trades = [Trade(*row) for row in rows]
        return trades
    except Exception as e:
        current_app.logger.error(f"Error fetching all trades: {e}")
        raise e
    finally:
        conn.close()

def insert_trades(trades, email):
    try:
        current_app.logger.info('Inserting trades into Supabase')

        insert_query = """
            INSERT INTO trades (
                user_id, optionId, tradeDate, accountId, symbol, putCall, buySell, openCloseIndicator, 
                strike, expiry, quantity, tradePrice, commission, assetCategory
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (optionId) 
            DO UPDATE SET optionId = trades.optionId
            RETURNING *;
        """

        conflict_insert_query = """
            INSERT INTO trade_conflicts (
                user_id, optionId, tradeDate, accountId, symbol, putCall, buySell, openCloseIndicator, 
                strike, expiry, quantity, tradePrice, commission, assetCategory
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        supabase_conn = get_db_connection()
        cursor = supabase_conn.cursor()

        for trade in trades:
            trade_tuple = (
                email,
                trade.optionId,
                trade.tradeDate,
                trade.accountId,
                trade.symbol,
                trade.putCall if trade.putCall else None,
                trade.buySell,
                trade.openCloseIndicator if trade.openCloseIndicator else None,
                float(trade.strike) if trade.strike else None,
                trade.expiry if trade.expiry != '' else None,
                int(trade.quantity),
                float(trade.tradePrice) if trade.tradePrice else None,
                float(trade.commission) if trade.commission else None,
                trade.assetCategory,
            )

            # Try inserting into `trades`
            cursor.execute(insert_query, trade_tuple)
            inserted_trade = cursor.fetchone()  # Get inserted row (if any)

            if not inserted_trade:
                # Conflict occurred, insert into `trade_conflicts`
                cursor.execute(conflict_insert_query, trade_tuple)

        supabase_conn.commit()
        cursor.close()
        supabase_conn.close()

        current_app.logger.info(f'Inserted {len(trades)} trades successfully!')
    except Exception as e:
        current_app.logger.error(f"Error inserting trades: {e}")
        raise e



def getUserToken(user_id):
    """Retrieve the authentication token and broker name for a user."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT auth_token, broker_name FROM user_audit WHERE user_id = %s;", (user_id,))
        result = cursor.fetchone()
        return result if result else (None, None)
    except Exception as e:
        current_app.logger.error(f"Error fetching user token: {e}")
        raise e
    finally:
        conn.close()

def update_refresh_token(user_id, new_token):
    """Update a user's authentication token."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("UPDATE user_audit SET auth_token = %s WHERE user_id = %s;", (new_token, user_id))
        conn.commit()
    except Exception as e:
        current_app.logger.error(f"Error updating refresh token: {e}")
        raise e
    finally:
        conn.close()
