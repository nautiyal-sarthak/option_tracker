import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


def calculate_rsi(data, periods=14):
    """Calculate RSI for a given price series."""
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_bollinger_bands(data, periods=20, std_dev=2):
    """Calculate Bollinger Bands for a given price series."""
    sma = data.rolling(window=periods).mean()
    std = data.rolling(window=periods).std()
    upper_band = sma + (std * std_dev)
    lower_band = sma - (std * std_dev)
    return lower_band, sma, upper_band

def get_stock_info(symbols):
    results = []

    for symbol in symbols:
        try:
            stock = yf.Ticker(symbol)
            stock_info = stock.info

            end_date = datetime.now()
            start_date = end_date - timedelta(days=300)
            df = stock.history(start=start_date, end=end_date)

            if df.empty:
                print(f"No data available for {symbol}")
                continue

            # Calculate indicators
            df['RSI'] = calculate_rsi(df['Close'], periods=14)
            df['BB_lower'], df['BB_middle'], df['BB_upper'] = calculate_bollinger_bands(df['Close'], periods=20, std_dev=2)
            df['MA200'] = df['Close'].rolling(window=200).mean()

            latest = df.iloc[-1]
            out = {
                'symbol': symbol,
                'price': round(latest['Close'], 2),
                'rsi': round(latest['RSI'], 2) if pd.notna(latest['RSI']) else None,
                'bb_lower': round(latest['BB_lower'], 2) if pd.notna(latest['BB_lower']) else None,
                'bb_middle': round(latest['BB_middle'], 2) if pd.notna(latest['BB_middle']) else None,
                'bb_upper': round(latest['BB_upper'], 2) if pd.notna(latest['BB_upper']) else None,
                'ma200': round(latest['MA200'], 2) if pd.notna(latest['MA200']) else None
            }
            results.append(out)

        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return pd.DataFrame(results)

def getKpiRanges(df):
    # Initialize output columns
    df = df.copy()  # Avoid modifying the input DataFrame
    df[['rsi_call_%', 'rsi_put_%', 'ma_deviation_pct%' , 'bb_call_%', 'bb_put_%']] = 0.0

    for idx, row in df.iterrows():
        price = row['price']
        rsi = row['rsi']
        bb_lower = row['bb_lower']
        bb_upper = row['bb_upper']
        ma200 = row['ma200']

        # --- RSI Logic ---
        # Calculate RSI-based call/put percentages and score in [-1, 1]
        if rsi >= 70:
            rsi_call_pct = 100.0
            rsi_put_pct = 0.0
        elif rsi <= 30:
            rsi_call_pct = 0.0
            rsi_put_pct = 100.0
        else:
            rsi_position = (rsi - 30) / 40  # Normalize RSI between 30 and 70
            rsi_call_pct = rsi_position * 100
            rsi_put_pct = (1 - rsi_position) * 100

        # --- Bollinger Bands Logic ---
        # Calculate BB-based call/put percentages and score in [-1, 1]
        band_width = max(bb_upper - bb_lower, 1e-6)  # Avoid division by zero
        if price >= bb_upper:
            bb_call_pct = 100.0
            bb_put_pct = 0.0
        elif price <= bb_lower:
            bb_call_pct = 0.0
            bb_put_pct = 100.0
        else:
            bb_position = (price - bb_lower) / band_width
            bb_call_pct = bb_position * 100
            bb_put_pct = (1 - bb_position) * 100

        # --- MA200 Logic ---
        # Calculate percentage deviation from MA200 and map to score in [-1, 1]
        if ma200 == 0:  # Avoid division by zero
            ma_deviation_pct = 0.0
        else:
            ma_deviation_pct = ((price - ma200) / ma200) * 100  # % deviation from MA200
            

        # --- Assign Values ---
        df.at[idx, 'rsi_call_%'] = rsi_call_pct
        df.at[idx, 'rsi_put_%'] = rsi_put_pct
        df.at[idx, 'ma_deviation_pct%'] = ma_deviation_pct
        df.at[idx, 'bb_call_%'] = bb_call_pct
        df.at[idx, 'bb_put_%'] = bb_put_pct

    return df

def calculate_call_put_probability(df):
    # Define weights
    weight_bb = 0.30
    weight_rsi = 0.40
    weight_ma = 0.30
    
    # Initialize columns for probabilities
    df['call_%'] = 0.0
    df['put_%'] = 0.0
    
    for index, row in df.iterrows():
        # Calculate call probability
        call_score = (row['bb_call_%'] * weight_bb + 
                      row['rsi_call_%'] * weight_rsi + 
                      max(0, row['ma_deviation_pct%']) * weight_ma)  # Positive deviation favors calls
        # Normalize to 0-100
        call_prob = min(100, max(0, call_score))
        
        # Calculate put probability
        put_score = (row['bb_put_%'] * weight_bb + 
                     row['rsi_put_%'] * weight_rsi + 
                     max(0, -row['ma_deviation_pct%']) * weight_ma)  # Negative deviation favors puts
        # Normalize to 0-100
        put_prob = min(100, max(0, put_score))
        
        # Ensure call_% and put_% sum to 100% (adjust if needed)
        total = call_prob + put_prob
        if total > 0:
            df.at[index, 'call_%'] = (call_prob / total) * 100
            df.at[index, 'put_%'] = (put_prob / total) * 100
        else:
            df.at[index, 'call_%'] = 50.0
            df.at[index, 'put_%'] = 50.0
    
    return df


# my_list = [
#     ['bb_s_high', 101 , 1, 80, 90, 100, 1],
#            ['bb_s_low', 79 , 1, 80, 90, 100, 1],
#            ['bb_high', 99 , 1, 80, 90, 100, 1],
#            ['bb_low', 81 , 1, 80, 90, 100, 1],
#            ['bb_mid_high', 91 , 0, 80, 90, 100, 1],
#            ['bb_mid_low', 89 , 0, 80, 90, 100, 1],

#            ['ma_s_high', 101 , 1, 1, 1, 1, 50],
#            ['ma_s_low', 1 , 1, 1, 1, 1, 50],
#            ['ma_high', 60 , 1, 1, 1, 1, 50],
#            ['ma_low', 40 , 1, 1, 1, 1, 50],
#            ['ma_low', 50 , 1, 1, 1, 1, 50],

#            ['rsi_low', 50 , 1, 1, 1, 1, 50],
#            ['rsi_high', 50 , 80, 1, 1, 1, 50],
#            ['rsi_mid', 50 , 45, 1, 1, 1, 50]
#            ]

# # Convert to DataFrame with column names
# columns = ['symbol', 'price', 'rsi', 'bb_lower', 'bb_middle', 'bb_upper', 'ma200']

# test_df = pd.DataFrame(my_list, columns=columns)


df = get_stock_info(['TSLA','QQQ'])
suggestion_df = getKpiRanges(df)
output_df = calculate_call_put_probability(suggestion_df)

# Display the suggestion DataFrame with no concatination
print("Suggestion DataFrame:")
print(suggestion_df)
