from ..brokers.broker_ibkr import IBKRBroker
from ..brokers.broker_quest import QuestradeBroker
from collections import defaultdict
import pandas as pd
import numpy as np
import json
from .serialization import convert_to_serializable
import pandas as pd
import logging
import numpy as np
from datetime import datetime, timedelta, date
from flask import session,current_app
from calendar import monthrange



def transform_data(df):
    df['tradeDate'] = pd.to_datetime(df['tradeDate'])
    df['tradePrice'] = pd.to_numeric(df['tradePrice']).abs()
    df['commission'] = pd.to_numeric(df['commission'])
    df['quantity'] = pd.to_numeric(df['quantity'])
    df['strike'] = pd.to_numeric(df['strike'])
    df['expiry'] = pd.to_datetime(df['expiry'])

    #if expiry is NaT then set it to 9999-12-31
    df['expiry'] = df['expiry'].fillna(pd.Timestamp('2099-12-31'))

    # make the openCloseIndicator more readable
    df['openCloseIndicator'] = df['openCloseIndicator'].map({
        'O': 'Open',
        'C': 'Close',
        'C;O': 'Open'
    })

    # make the putCall more readable
    df['putCall'] = df['putCall'].map({
        'P': 'Put',
        'C': 'Call',
        'Call': 'Call',
        'Put': 'Put'
    })

    # make the assetCategory more readable
    df['assetCategory'] = df['assetCategory'].map({
        'STK': 'Stock',
        'OPT': 'Option'
    })

        # make the assetCategory more readable
    df['buySell'] = df['buySell'].map({
        'B': 'BUY',
        'S': 'SELL',
        'BUY': 'BUY',
        'SELL': 'SELL'
    })

    # assert that buySell col only has BUY or SELL
    assert df['buySell'].isin(['BUY', 'SELL']).all()

    # for trade date get only the date part
    df['tradeDate'] = df['tradeDate'].dt.date
    df['expiry'] = df['expiry'].dt.date

    return df

def find_matching_key(trade_open_key, trade_close_dict):
    if len(trade_open_key) < 6:
        return None  # Invalid key format
    
    #(symbol, row["putCall"], row["strike"], row["expiry"], row['accountId'], row['tradeDate'])

    for close_key in trade_close_dict:
        if len(close_key) < 6:
            continue  # Skip invalid keys
        import datetime
        if close_key[3] == datetime.date(2099, 12, 31):
            if (
                trade_open_key[0] == close_key[0] # Check symbol
                and trade_open_key[1] == close_key[1] # Check putCall
                and trade_open_key[2] == close_key[2] # Check strike price
                and trade_open_key[3] >= close_key[5] # Check expiry date
                and close_key[5] >= trade_open_key[5] # validate that the close date is greater than the open date
                ):
                return close_key
        else:
            if (
                trade_open_key[0] == close_key[0] # Check symbol
                and trade_open_key[1] == close_key[1] # Check putCall
                and trade_open_key[2] == close_key[2] # Check strike price
                and trade_open_key[3] == close_key[3] # Check expiry date
                and trade_open_key[4] == close_key[4] # Check accountId
                and trade_open_key[6] != close_key[6] # Check buySell is different
                and close_key[5] >= trade_open_key[5] # validate that the close date is greater than the open date
                and close_key[5] <= trade_open_key[3] # validate that the close trades are before the expiry date
                ):
                return close_key  # Return the first matching key

    return None

def process_wheel_trades(df):
    try:
        import datetime
        import logging
        import numpy as np
        import pandas as pd

        # helper to match keys loosely (ignores buySell/quantity for cross-lookup)
        def find_matching_key(original_key, lookup_dict):
            # original_key format in processed_trades: (symbol, putCall, strike, expiry, accountId, tradeDate, buySell)
            sym, putcall, strike, expiry, acct, tdate, buysell = original_key
            for k in lookup_dict.keys():
                # For stock/assignment lookups we used slightly different key shapes, so guard
                try:
                    k_sym, k_putcall, k_strike, k_expiry, k_acct, k_tdate, k_buysell = k
                except Exception:
                    continue
                if (
                    k_sym == sym and
                    k_putcall == putcall and
                    k_expiry == expiry and
                    k_acct == acct
                ):
                    return k
            return None

        # --- start original preprocessing ---
        df = df.copy()
        df = df.fillna("")

        # compute total_premium similar to your original code
        df['total_premium'] = np.where(
            df['assetCategory'] == 'Option',
            (df['tradePrice'] * df['quantity'] * 100),
            (df['tradePrice'] * df['quantity'])
        )

        df['total_premium'] = (df['total_premium'].astype(float) * -1) + df['commission']

        df = df.groupby(['symbol','putCall','buySell','openCloseIndicator','strike','accountId','tradeDate','assetCategory','expiry']).agg(
            quantity=pd.NamedAgg(column='quantity', aggfunc='sum'),
            commission=pd.NamedAgg(column='commission', aggfunc='sum'),
            tradePrice=pd.NamedAgg(column='tradePrice', aggfunc='mean'),
            total_premium=pd.NamedAgg(column='total_premium', aggfunc='sum')
        ).reset_index()

        df["asset_priority"] = df["putCall"].apply(lambda x: 1 if x in ["Call", "Put"] else 2)
        df.sort_values(by=["asset_priority", "symbol", "tradeDate"], ascending=[True, True, True], inplace=True)
        df.drop(columns=["asset_priority"], inplace=True)

        # data structures to collect trades
        processed_trades = {}
        assigned_stocks = {}
        stock_sales = {}
        buybacks = {}
        sellbacks = {}

        # collect option-leg opens/closings to allow detection of spreads
        option_opens = []   # list of rows for Option Open
        option_closes = []  # list of rows for Option Close (buybacks)

        # Iterate over grouped trades, collect opens/closes and stocks as before
        for _, row in df.iterrows():
            symbol = row["symbol"]

            if row["assetCategory"] == "Option" and row["openCloseIndicator"] == "Open":
                # capture opens for potential spread detection
                option_opens.append(row.to_dict())

            elif row["assetCategory"] == "Option" and row["openCloseIndicator"] == "Close":
                # buyback/close
                option_closes.append(row.to_dict())

            elif row["assetCategory"] == "Stock" and row["buySell"] == "BUY":
                # Stock Assignment (from Put Option)
                key = (symbol, 'Put', row["tradePrice"], row["expiry"], row['accountId'], row['tradeDate'], row["buySell"])
                assigned_stocks[key] = {
                    "assign_price": row["tradePrice"],
                    "quantity": row["quantity"],
                    "tradeDate": row["tradeDate"],
                    "number_of_assign_contract": row["quantity"],
                    "net_assign_cost": row["total_premium"]
                }

            elif row["assetCategory"] == "Stock" and row["buySell"] == "SELL":
                # Stock Sale
                key = (symbol, 'Call', row["tradePrice"], row["expiry"], row['accountId'], row['tradeDate'], row["buySell"])
                stock_sales[key] = {
                    "sold_price": row["tradePrice"],
                    "quantity": row["quantity"],
                    "tradeDate": row["tradeDate"],
                    "number_of_sold_contract": row["quantity"],
                    "net_sold_cost": row["total_premium"]
                }

        # --- detect credit spreads among option_opens ---
        # Strategy:
        # For a given (symbol, putCall, expiry, accountId, tradeDate),
        # if we have exactly two legs with same abs(quantity) and opposite buySell (SELL + BUY),
        # treat it as a spread: short leg = SELL, long leg = BUY.
        used_open_indices = set()
        for i, r1 in enumerate(option_opens):
            if i in used_open_indices:
                continue
            for j in range(i+1, len(option_opens)):
                if j in used_open_indices:
                    continue
                r2 = option_opens[j]
                # matching keys for candidate spread
                if (
                    r1['symbol'] == r2['symbol'] and
                    r1['putCall'] == r2['putCall'] and
                    r1['expiry'] == r2['expiry'] and
                    r1['accountId'] == r2['accountId'] and
                    r1['tradeDate'] == r2['tradeDate'] and
                    abs(r1['quantity']) == abs(r2['quantity']) and
                    set([r1['buySell'], r2['buySell']]) == set(['SELL', 'BUY'])
                ):
                    # We have a spread open
                    short_leg = r1 if r1['buySell'] == 'SELL' else r2
                    long_leg = r1 if r1['buySell'] == 'BUY' else r2
                    contracts = int(abs(short_leg['quantity']))
                    net_premium = short_leg['total_premium'] + long_leg['total_premium']  # premium collected (negative) + paid (maybe positive)
                    short_strike = short_leg['strike']
                    long_strike = long_leg['strike']
                    # determine spread width and max loss
                    spread_width = abs(short_strike - long_strike)
                    max_loss = (spread_width * 100 * contracts) + net_premium  # net_premium is typically negative (collected) so this reduces max loss
                    # ensure positive max_loss
                    max_loss = abs(max_loss) if max_loss != 0 else (spread_width * 100 * contracts)
                    # ROI as percentage of max_loss paid: how much we collected vs what we can lose
                    if max_loss > 0:
                        roi_pct = (abs(net_premium) / max_loss) * 100
                    else:
                        roi_pct = 0.0

                    key = (short_leg['symbol'], short_leg['putCall'], f"SPREAD_{short_strike}_{long_strike}", short_leg['expiry'], short_leg['accountId'], short_leg['tradeDate'], 'SPREAD')
                    processed_trades[key] = {
                        'accountId': short_leg['accountId'],
                        "symbol": short_leg['symbol'],
                        "callorPut": short_leg['putCall'],
                        "buySell": "SPREAD",
                        "trade_open_date": short_leg['tradeDate'],
                        "expiry_date": short_leg['expiry'],
                        "strike_price": short_strike,  # short strike
                        "long_leg_strike": long_strike,
                        "spread_width": spread_width,
                        "number_of_contracts_sold": -contracts,  # negative to indicate sold quantity similar to your original
                        "premium_collected": net_premium,
                        "short_leg_premium": short_leg['total_premium'],
                        "long_leg_premium": long_leg['total_premium'],
                        "net_buyback_price": None,
                        "number_of_buyback": None,
                        "buyback_date": None,
                        "net_premium": net_premium,
                        "assign_price_per_share": None,
                        "assign_quantity": None,
                        "number_of_assign_contract": None,
                        "assign_date": None,
                        "net_assign_cost": None,
                        "sold_price_per_share": None,
                        "sold_quantity": None,
                        "number_of_sold_contract": None,
                        "sold_date": None,
                        "net_sold_cost": None,
                        "close_date": None,
                        "ROI": round(roi_pct, 2),
                        "status": "OPEN",
                        "max_loss": max_loss,
                        "collateral_used": max_loss
                    }
                    used_open_indices.add(i)
                    used_open_indices.add(j)
                    break

        # Any remaining single-leg option opens (not part of detected spreads) -> create same as original behavior
        for idx, r in enumerate(option_opens):
            if idx in used_open_indices:
                continue
            key = (r["symbol"], r["putCall"], r["strike"], r["expiry"], r['accountId'], r['tradeDate'], r["buySell"])
            processed_trades[key] ={
                'accountId': r['accountId'],
                "symbol": r["symbol"],
                "callorPut": r["putCall"],
                "buySell": r["buySell"],
                "trade_open_date": r["tradeDate"],
                "expiry_date": r["expiry"],
                "strike_price": r["strike"],
                "number_of_contracts_sold": r["quantity"],
                "premium_collected": r["total_premium"],
                "net_buyback_price": None,
                "number_of_buyback": None,
                "buyback_date": None,
                "net_premium": r["total_premium"],
                "assign_price_per_share": None,
                "assign_quantity": None,
                "number_of_assign_contract": None,
                "assign_date": None,
                "net_assign_cost": None,
                "sold_price_per_share": None,
                "sold_quantity": None,
                "number_of_sold_contract": None,
                "sold_date": None,
                "net_sold_cost": None,
                "close_date": None,
                "ROI": None,
                "status": "OPEN"
            }

        # Process option_closes -> try to close spreads first, then single legs
        used_close_indices = set()
        for i, r1 in enumerate(option_closes):
            if i in used_close_indices:
                continue
            for j in range(i+1, len(option_closes)):
                if j in used_close_indices:
                    continue
                r2 = option_closes[j]
                if (
                    r1['symbol'] == r2['symbol'] and
                    r1['putCall'] == r2['putCall'] and
                    r1['expiry'] == r2['expiry'] and
                    r1['accountId'] == r2['accountId'] and
                    r1['tradeDate'] == r2['tradeDate'] and
                    abs(r1['quantity']) == abs(r2['quantity']) and
                    set([r1['buySell'], r2['buySell']]) == set(['SELL', 'BUY'])
                ):
                    # This is a close of a spread (both legs closed same day)
                    short_leg = r1 if r1['buySell'] == 'SELL' else r2
                    long_leg = r1 if r1['buySell'] == 'BUY' else r2
                    contracts = int(abs(short_leg['quantity']))
                    net_close_premium = short_leg['total_premium'] + long_leg['total_premium']
                    short_strike = short_leg['strike']
                    long_strike = long_leg['strike']
                    # find matching open spread key
                    spread_key = (short_leg['symbol'], short_leg['putCall'], f"SPREAD_{short_strike}_{long_strike}", short_leg['expiry'], short_leg['accountId'], short_leg['tradeDate'], 'SPREAD')
                    if spread_key in processed_trades:
                        proc = processed_trades[spread_key]
                        proc['net_buyback_price'] = net_close_premium
                        proc['number_of_buyback'] = contracts
                        proc['buyback_date'] = short_leg['tradeDate']
                        proc['close_date'] = short_leg['tradeDate']
                        proc['status'] = "BOUGHT BACK"
                        # recompute ROI relative to max_loss
                        max_loss = proc.get('max_loss', (abs(short_strike - long_strike) * 100 * contracts))
                        net_premium_open = proc.get('net_premium', 0.0)
                        realized_pnl = -(net_premium_open + net_close_premium)  # negative of (collected + paid)
                        # ROI as percent of required collateral (max_loss)
                        proc['ROI'] = round((abs(realized_pnl) / max_loss) * 100, 2) if max_loss != 0 else 0.0
                    else:
                        # fallback: record a closing spread entry if no opening found
                        key = (short_leg['symbol'], short_leg['putCall'], f"SPREAD_{short_strike}_{long_strike}", short_leg['expiry'], short_leg['accountId'], short_leg['tradeDate'], 'SPREAD_CLOSE')
                        processed_trades[key] = {
                            'accountId': short_leg['accountId'],
                            "symbol": short_leg['symbol'],
                            "callorPut": short_leg['putCall'],
                            "buySell": "SPREAD_CLOSE",
                            "trade_open_date": short_leg['tradeDate'],
                            "expiry_date": short_leg['expiry'],
                            "strike_price": short_strike,
                            "long_leg_strike": long_strike,
                            "spread_width": abs(short_strike - long_strike),
                            "number_of_contracts_sold": None,
                            "premium_collected": None,
                            "short_leg_premium": short_leg['total_premium'],
                            "long_leg_premium": long_leg['total_premium'],
                            "net_buyback_price": net_close_premium,
                            "number_of_buyback": contracts,
                            "buyback_date": short_leg['tradeDate'],
                            "net_premium": None,
                            "close_date": short_leg['tradeDate'],
                            "ROI": None,
                            "status": "BOUGHT BACK",
                            "max_loss": abs(short_strike - long_strike) * 100 * contracts,
                            "collateral_used": abs(short_strike - long_strike) * 100 * contracts
                        }
                    used_close_indices.add(i)
                    used_close_indices.add(j)
                    break

        # Any remaining closes for single legs -> treat as buybacks mapped to single-leg opens
        for idx, r in enumerate(option_closes):
            if idx in used_close_indices:
                continue
            # try to find matching open single-leg
            key = (r["symbol"], r["putCall"], r["strike"], r["expiry"], r['accountId'], r['tradeDate'], r["buySell"])
            # note: for close, buySell may be 'BUY' (closing a prior SELL open). We'll search processed_trades ignoring buySell sign
            match_key = None
            for pt_key in processed_trades.keys():
                # match on symbol, putcall, strike, expiry, accountId
                try:
                    if (
                        pt_key[0] == r['symbol'] and
                        pt_key[1] == r['putCall'] and
                        float(pt_key[2]) == float(r['strike']) and
                        pt_key[3] == r['expiry'] and
                        pt_key[4] == r['accountId']
                    ):
                        match_key = pt_key
                        break
                except Exception:
                    continue
            if match_key:
                proc = processed_trades[match_key]
                proc["net_buyback_price"] = r["total_premium"]
                proc["number_of_buyback"] = r["quantity"]
                proc["buyback_date"] = r["tradeDate"]
                proc["close_date"] = r["tradeDate"]
                proc["status"] = "BOUGHT BACK"
                # ROI using original formula (approximate)
                try:
                    proc["net_premium"] = proc.get("net_premium", 0.0) + r["total_premium"]
                    proc["ROI"] = (proc["net_premium"] / (proc["strike_price"] * abs(proc["number_of_buyback"]) * 100)) * 100
                except Exception:
                    proc["ROI"] = None

        # Now incorporate assigned stocks and stock sales into processed_trades similar to your original logic
        # (Re-use your existing mapping logic)
        partial_trades = {}
        for trade_key in list(processed_trades.keys()):
            trade = processed_trades[trade_key]

            assigned_stocks_key = find_matching_key(trade_key, assigned_stocks)
            stock_sales_key = find_matching_key(trade_key, stock_sales)
            # buybacks handled earlier for spreads/single legs as we populated processed_trades directly

            if assigned_stocks_key:
                a = assigned_stocks[assigned_stocks_key]
                trade["assign_price_per_share"] = a['assign_price']
                trade["assign_quantity"] = a['quantity']
                trade["assign_date"] = a['tradeDate']
                trade["number_of_assign_contract"] = a['number_of_assign_contract']/100 if a.get('number_of_assign_contract') else None
                trade["net_assign_cost"] = a['net_assign_cost']
                trade["close_date"] = a['tradeDate']
                trade["status"] = "ASSIGNED"
                trade["ROI"] = 0
                assigned_stocks.pop(assigned_stocks_key)

            if stock_sales_key:
                s = stock_sales[stock_sales_key]
                trade["sold_price_per_share"] = s['sold_price']
                trade["sold_quantity"] = s['quantity']
                trade["sold_date"] = s['tradeDate']
                trade["number_of_sold_contract"] = s.get('number_of_sold_contract')/100 if s.get('number_of_sold_contract') else None
                trade["net_sold_cost"] = s['net_sold_cost']
                trade["close_date"] = s['tradeDate']
                trade["status"] = "TAKEN AWAY"
                trade["ROI"] = 0
                stock_sales.pop(stock_sales_key)

            # expiry handling for open trades
            today = pd.Timestamp.today().normalize().date()
            if trade.get("expiry_date") and trade["expiry_date"] != "":
                try:
                    expiry_date = trade["expiry_date"]
                    if isinstance(expiry_date, pd.Timestamp):
                        expiry_dt = expiry_date.date()
                    elif isinstance(expiry_date, datetime.date):
                        expiry_dt = expiry_date
                    else:
                        expiry_dt = pd.to_datetime(expiry_date).date()
                except Exception:
                    expiry_dt = None

                if expiry_dt and expiry_dt < today and trade.get("status") == "OPEN":
                    trade["status"] = "EXPIRED"
                    trade["close_date"] = trade["expiry_date"]
                    # For spreads, ROI already computed; for single legs compute similar to original
                    try:
                        trade["ROI"] = (trade["net_premium"] / (trade["strike_price"] * abs(trade["number_of_contracts_sold"]) * 100)) * 100
                    except Exception:
                        trade["ROI"] = None

        # Any remaining assigned_stocks or stock_sales that didn't match opens -> add as standalone records
        for key, value in assigned_stocks.items():
            processed_trades[key] ={
                    'accountId': key[4],
                    "symbol": key[0],
                    "callorPut": None,
                    "buySell": "BUY",
                    "trade_open_date": value["tradeDate"],
                    "expiry_date": value['tradeDate'],
                    "strike_price": None,
                    "number_of_contracts_sold": None,
                    "premium_collected": None,
                    "net_buyback_price": None,
                    "number_of_buyback": None,
                    "buyback_date": None,
                    "net_premium": None,
                    "assign_price_per_share": value["assign_price"],
                    "assign_quantity": value["number_of_assign_contract"],
                    "number_of_assign_contract": None,
                    "assign_date": value["tradeDate"],
                    "net_assign_cost": value["net_assign_cost"],
                    "sold_price_per_share": None,
                    "sold_quantity": None,
                    "number_of_sold_contract": None,
                    "net_sold_cost": None,
                    "sold_date": None,
                    "close_date": value["tradeDate"],
                    'status': "BOUGHT STOCK"
                }

        for key, value in stock_sales.items():
            processed_trades[key] ={
                    'accountId': key[4],
                    "symbol": key[0],
                    "callorPut": None,
                    "buySell": "SELL",
                    "trade_open_date": value['tradeDate'],
                    "expiry_date": value['tradeDate'],
                    "strike_price": None,
                    "number_of_contracts_sold": None,
                    "premium_collected": None,
                    "net_buyback_price": None,
                    "number_of_buyback": None,
                    "buyback_date": None,
                    "net_premium": None,
                    "assign_price_per_share": None,
                    "assign_quantity": None,
                    "number_of_assign_contract": None,
                    "assign_date": None,
                    "net_assign_cost": None,
                    "sold_price_per_share": value['sold_price'],
                    "sold_quantity": value['quantity'],
                    "number_of_sold_contract": None,
                    "net_sold_cost": value['net_sold_cost'],
                    "sold_date": value['tradeDate'],
                    "close_date": value['tradeDate'],
                    'status': "SOLD STOCK"
                }

        # final dataframe assembly and formatting
        df_out = pd.DataFrame(processed_trades).transpose()

        df_out = df_out.fillna(0)
        df_out = df_out.reset_index(drop=True)

        # ensure numeric conversions and rounding similar to your original
        df_out['net_sold_cost'] = pd.to_numeric(df_out.get('net_sold_cost', pd.Series([])), errors='coerce').fillna(0.0).astype(float) if 'net_sold_cost' in df_out.columns else 0.0
        df_out['net_assign_cost'] = pd.to_numeric(df_out.get('net_assign_cost', pd.Series([])), errors='coerce').fillna(0.0).astype(float) if 'net_assign_cost' in df_out.columns else 0.0
        df_out['net_premium'] = pd.to_numeric(df_out.get('net_premium', pd.Series([])), errors='coerce').fillna(0.0).astype(float)
        df_out['net_premium'] = df_out['net_premium'].apply(lambda x: round(x, 2))
        if 'ROI' in df_out.columns:
            df_out['ROI'] = df_out['ROI'].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

        # compute Colateral_used in a more general way: prefer explicit collateral_used (for spreads), else fallback to existing logic
        def compute_collateral(row):
            if row.get('status') in ['OPEN', 'ASSIGNED'] and row.get('collateral_used'):
                return row.get('collateral_used')
            # fallback single-leg collateral
            try:
                if row.get('close_date') != 0:
                    return row.get('assign_price_per_share', 0) * (row.get('assign_quantity', 0))
                else:
                    if row.get('buySell') == 'SELL' and row.get('number_of_contracts_sold'):
                        return row.get('number_of_contracts_sold') * 100 * (row.get('strike_price') or 0) * -1
            except Exception:
                return 0
            return 0

        df_out['Colateral_used'] = df_out.apply(compute_collateral, axis=1)

        # select / order columns similar to your original, adding new spread fields if present
        cols = ['accountId','symbol', 'callorPut', 'buySell', 'trade_open_date', 'expiry_date','strike_price',
                   'number_of_contracts_sold', 'premium_collected','net_buyback_price', 'number_of_buyback', 'buyback_date', 'net_premium',
                   'assign_price_per_share', 'assign_quantity','number_of_assign_contract', 'assign_date', 'net_assign_cost','sold_price_per_share',
                   'sold_quantity', 'number_of_sold_contract','net_sold_cost', 'sold_date', 'close_date','ROI','status','Colateral_used']

        # include spread-specific columns if present
        extra = ['long_leg_strike','spread_width','short_leg_premium','long_leg_premium','max_loss','collateral_used']
        for e in extra:
            if e in df_out.columns and e not in cols:
                cols.insert(cols.index('net_premium')+1, e)

        # keep only available columns
        cols = [c for c in cols if c in df_out.columns]
        df_out = df_out[cols]

        return df_out

    except Exception as e:
        logging.error(f"Error processing wheel trades: {e}")
        raise


def process_trade_data(email,token=None,broker_name=None,start_date=None,end_date=None,grouping='month'):
    try:
        # check if session['master_trade_data'] is populated
        # if so, return the data from the session
        # if not, fetch the data from the broker
        
        if (session.get('master_trade_data') is None) or session.get('adhoc_email'):
            current_app.logger.info('fetching data from broker')
            is_test = False
            if broker_name == 'IBKR':
                broker = IBKRBroker(token,is_test)
            elif broker_name == 'Quest':
                broker = QuestradeBroker(token,is_test)
            else:
                raise Exception(f"Broker '{broker_name}' is not supported.")
            trade_data = broker.get_data(email)


            df = pd.DataFrame([vars(trade) for trade in trade_data])
            current_app.logger.info('transforming data')
            raw_df = transform_data(df)
            current_app.logger.info('processing wheel trades')
            processed_data = process_wheel_trades(raw_df)
            session['master_trade_data'] = processed_data
            session['raw_df'] = raw_df
        else:
            current_app.logger.info('using data from session')
            processed_data = session['master_trade_data']
            raw_df = session['raw_df']

        stk_cost_per_share = processed_data.groupby(['accountId', 'symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[processed_data['status'] != 'OPEN'].sum()),
            total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[(processed_data['status'] == 'OPEN') & (processed_data['buySell'] == 'SELL')].sum()),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
            total_colateral_used=pd.NamedAgg(column='Colateral_used', aggfunc='sum')
        ).reset_index()

        stk_cost_per_share['cost_basis_per_share'] = (
                                    (abs(stk_cost_per_share["total_stock_assign_cost"]) - 
                                    (stk_cost_per_share.get("total_premium_collected", 0.0) + stk_cost_per_share.get("total_premium_collected_open", 0.0)))
                                    /stk_cost_per_share["total_assign_quantity"])
        
        stk_cost_per_share = stk_cost_per_share[['accountId', 'symbol', 'cost_basis_per_share']].copy()
        stk_cost_per_share = stk_cost_per_share.fillna(0.0)
        session['stk_cost_per_share'] = stk_cost_per_share

        # Apply time filter
        filtered_data = filter_by_time_period(processed_data, start_date, end_date)
        #raw_df = filter_by_time_period(raw_df, filter_type,"tradeDate")

        stock_summary = getStockSummary(filtered_data,stk_cost_per_share)
        session['stock_summary'] = stock_summary
        account_summary = getAccountSummary(stock_summary)
        total_summary = getTotalSummary(account_summary)
        profit_by_month = getProfitPerTimePeriod(filtered_data,stock_summary,grouping)


        # Prepare account-stock dictionary
        account_dict = defaultdict(list)
        for item in stock_summary.round(2).to_dict(orient='records'):
            account_dict[item['accountId']].append(item)

        if total_summary.empty:
            session['oldest_trade_date'] = '1990-01-01'
        else:
            min_date_value = total_summary['min_date'].values[0]

            if isinstance(min_date_value, float) and np.isnan(min_date_value):
                session['oldest_trade_date'] = '1990-01-01'  # Or assign a default value like 'N/A'
            else:
                session['oldest_trade_date'] = min_date_value.strftime('%Y-%m-%d')
        
        
        # Store filter_type in session
        if session.get('start_date') is None or session.get('end_date') is None:
            session['start_date'] = total_summary['min_date'].values[0].strftime('%Y-%m-%d')
            session['end_date'] = date.today().strftime('%Y-%m-%d')

        data = {
            'oldest_trade_date': total_summary['min_date'].values[0],
            'total_premium_collected': total_summary['total_premium_collected'].values[0],
            'total_premium_collected_open': total_summary['total_premium_collected_open'].values[0],
            "total_premium_formated": str(total_summary['total_premium_collected'].values[0]) + f"({total_summary['total_premium_collected_open'].values[0]})",
            'total_open_trades': total_summary['total_open_trades'].values[0],
            'total_profit': total_summary['net_profit'].values[0],
            'total_colateral_used': total_summary['colateral_used'].values[0],
            'p_l_stock': total_summary['realized_pnl'].values[0],
            'total_wins': total_summary['total_wins'].values[0],
            'total_loss': total_summary['total_lost_trades'].values[0],
            'avg_ROI': total_summary['avg_ROI'].values[0],
            'win_percentage': round((total_summary['total_wins'].values[0]/(total_summary['total_wins'].values[0] + total_summary['total_lost_trades'].values[0])) * 100,2),
            'account_summary': account_summary.to_dict(orient='records'),
            'account_stk_merge': dict(account_dict),
            'profit_data': profit_by_month.to_dict(orient='records'),
            'all_trades': format_processed_data(filtered_data).to_dict(orient='records')
        }

        # # Convert all NumPy types to Python native types
        out = convert_to_serializable(data)
        return out

    except Exception as e:
        raise Exception(f"An error occurred: {e}")

def format_processed_data(df):
    df['type'] = np.where(df['callorPut'] == 0, 'Stock', 'Option')
    df['key'] = np.where(
        df['callorPut'] == 0,
        df['symbol'],
        df['symbol'] + '_' + df['callorPut'].astype(str) + '_' + df['strike_price'].astype(str) + '_' + df['expiry_date'].astype(str) + '_' + df['buySell'].astype(str)
    )
    df['amt'] = np.where(df['callorPut'] == 0, np.where(df['net_assign_cost'] != 0, df['net_assign_cost'], df['net_sold_cost']), df['net_premium'])
    df['qty'] = np.where(df['callorPut'] == 0, np.where(df['sold_quantity'] != 0, df['sold_quantity'], df['assign_quantity']), df['number_of_contracts_sold'])


    df = df[['key','type','qty','ROI','status','trade_open_date','month_week','amt','Colateral_used']]

    # # if status is open set the trade_open_date to ''
    df.loc[df['status'] == 'OPEN', 'month_week'] = ''

    df.sort_values(by=['month_week'], ascending=False, inplace=True)

    # convert trade_open_date to a string
    df['trade_open_date'] = df['trade_open_date'].astype(str)

    #rename columns
    df.rename(columns={
        'qty': '# Qty',
        'amt': '$ Amount',
        'status': 'Status',
        'trade_open_date': 'Trade Open Date',
        'month_week':'Close week'
    }, inplace=True)

    return df


def getTotalSummary(df):
    try:
        # Group by accountId and sum all other numeric columns
        agg_df = df.copy()
        agg_df = agg_df.sum(numeric_only=True).to_frame().T
        agg_df['avg_ROI'] = df['avg_ROI'].mean()  # Add the mean of the ROI column
        agg_df['min_date'] = df['min_date'].min()  # Get the earliest trade date

        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing getTotalSummary : {e}")
        raise

def getProfitPerTimePeriod(df,stk_smry,grouping):
    try:
        # Function to get week of month
        def week_of_month(dt):
            if pd.isnull(dt):
                return np.nan
            first_day = dt.replace(day=1)
            dom = dt.day
            adjusted_dom = dom + first_day.weekday()  # adjust for the weekday of the 1st
            return int(np.ceil(adjusted_dom / 7.0))
    

        
        # Date-based aggregation
        df['close_date'] = pd.to_datetime(df['close_date'], errors='coerce')
        df['week_of_month'] = df['close_date'].apply(week_of_month)
        df['month_week'] = df['close_date'].dt.strftime('%Y-%m-') + df['week_of_month'].astype(str) + 'W'
        df['year_month'] = df['close_date'].dt.strftime('%Y-%m')
        weekly_data = df[df['status'] != 'OPEN'].copy() 

        if grouping == 'month':
            key = ['year_month','symbol','accountId']
        elif grouping == 'week':
            key = ['month_week','symbol','accountId']
        elif grouping == 'day':
            key = ['close_date','symbol','accountId']

        weekly_data_grp = weekly_data.groupby(key).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc='sum'),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum')
        ).reset_index()

        stk_smry['stk_avg_cost'] = (abs(stk_smry["total_stock_assign_cost"])/stk_smry["total_assign_quantity"])
        stk_cost_basis = stk_smry[['accountId','symbol','stk_avg_cost']].copy()
        weekly_data_grp_cost_merge = weekly_data_grp.merge(stk_cost_basis, on=['accountId','symbol'], how='left')
        weekly_data_grp_cost_merge['stk_avg_cost'] = weekly_data_grp_cost_merge['stk_avg_cost'].fillna(0.0)


        weekly_data_grp_cost_merge['total_cost_of_sold_shares'] = (weekly_data_grp_cost_merge['stk_avg_cost']) * weekly_data_grp_cost_merge["total_sold_quantity"]
        weekly_data_grp_cost_merge['realized_pnl'] = abs(weekly_data_grp_cost_merge['total_stock_sale_cost']) - abs(weekly_data_grp_cost_merge['total_cost_of_sold_shares'])

        

        weekly_data_grp_cost_merge["net_profit"] = weekly_data_grp_cost_merge['realized_pnl'] + weekly_data_grp_cost_merge['total_premium_collected']

        # rename the key column to month_week or year_month
        weekly_data_grp_cost_merge.rename(columns={key[0]: 'period'}, inplace=True)

        out_df = weekly_data_grp_cost_merge.groupby(['period']).agg(
            net_profit=pd.NamedAgg(column='net_profit', aggfunc='sum')
        ).reset_index()

        return out_df[['period','net_profit']]
    except Exception as e:
        logging.error(f"Error processing profit by month: {e}")
        raise

def getAccountSummary(df):
    try:
        agg_df = df.groupby('accountId', as_index=False).agg({
                    'avg_ROI': 'mean',  # Calculate the mean for the ROI column
                    'min_date': 'min',  # Get the earliest trade date
                    **{col: 'sum' for col in df.select_dtypes(include='number').columns if col != 'avg_ROI'}  # Sum for other numeric columns
                })

        # Round all numeric columns to 2 decimal places
        agg_df = agg_df.round(2)
        return agg_df
    except:
        logging.error(f"Error processing account summary: {e}")
        raise

def getStockSummary(df, stk_cost_per_share):
    try:
        # # Stock level aggregation
        stock_summary = df.groupby(['accountId', 'symbol']).agg(
            total_premium_collected=pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[df['status'] != 'OPEN'].sum()),
            total_premium_collected_open = pd.NamedAgg(column='net_premium', aggfunc=lambda x: x[(df['status'] == 'OPEN') & (df['buySell'] == 'SELL')].sum()),
            total_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[(df['callorPut'].isin(["Call", "Put"]))].count()),
            total_wins = pd.NamedAgg(
                            column='symbol',
                            aggfunc=lambda x: x[(df['net_premium'] > 0) & (df['status'].isin(['EXPIRED', 'BOUGHT BACK']))].count()
                        ),
            total_open_trades=pd.NamedAgg(column='symbol', aggfunc=lambda x: x[df['status'] == 'OPEN'].count()),
            total_stock_sale_cost=pd.NamedAgg(column='net_sold_cost', aggfunc='sum'),
            total_stock_assign_cost=pd.NamedAgg(column='net_assign_cost', aggfunc='sum'),
            total_assign_quantity=pd.NamedAgg(column='assign_quantity', aggfunc='sum'),
            total_sold_quantity=pd.NamedAgg(column='sold_quantity', aggfunc='sum'),
            avg_ROI=pd.NamedAgg(column='ROI', aggfunc=lambda x: x[df['status'] != 'OPEN'].mean()),
            min_date=pd.NamedAgg(column='trade_open_date', aggfunc='min'),
            colateral_used=pd.NamedAgg(column='Colateral_used', aggfunc='sum')
        ).reset_index()

        # Merge with stock cost per share
        stock_summary = stock_summary.merge(stk_cost_per_share, on=['accountId', 'symbol'], how='left')

        # populate all missing value for avg_ROI to 0
        stock_summary = stock_summary.fillna(0.0)

        stock_summary['total_lost_trades'] = stock_summary['total_trades'] - stock_summary['total_wins']
        stock_summary['net_assign_qty'] = stock_summary['total_assign_quantity'] + stock_summary['total_sold_quantity']


        
        stock_summary['total_cost_of_sold_shares'] = abs(stock_summary["cost_basis_per_share"]) * stock_summary["total_sold_quantity"]
        stock_summary['total_cost_of_sold_shares'] = stock_summary['total_cost_of_sold_shares'].fillna(0.0)
        stock_summary['realized_pnl'] = abs(stock_summary['total_stock_sale_cost']) - abs(stock_summary['total_cost_of_sold_shares'])
        stock_summary['total_cost_of_sold_shares'] = stock_summary['total_cost_of_sold_shares'].fillna(0.0)

        # IF THE total_assign_quantity is 0 then set the cost_basis_per_share to 0
        stock_summary['cost_basis_per_share'] = np.where(
            stock_summary['net_assign_qty'] > 0,
            stock_summary['cost_basis_per_share'],
            0.0
        )


        stock_summary['win_percent'] = np.where(
            stock_summary['total_trades'] > 0,
            (stock_summary['total_wins'] / stock_summary['total_trades']) * 100,
            0
        )

        stock_summary["net_profit"] = stock_summary['realized_pnl'] + stock_summary['total_premium_collected']

        
        return stock_summary.round(2)
    except Exception as e:
        logging.error(f"Error processing stock summary: {e}")
        raise


def filter_by_time_period(df, start_date=None,end_date=None, col_name='trade_open_date'):
    if start_date is None and end_date is None:
        return df
    else:
        # Ensure start_date is a date object
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
        elif isinstance(start_date, datetime):
            start_date = start_date.date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
        elif isinstance(end_date, datetime):
            end_date = end_date.date()

    condition_1 = (df[col_name] >= start_date) & (df[col_name] <= end_date)
    condition_2 = (df['close_date'].dt.date >= start_date) & (df['close_date'].dt.date <= end_date)
    return df[condition_1 | condition_2 ]

    
