from brokers.base_broker import BaseBroker
import requests
import json
import time
from entity.trade import Trade
import logging
from database import *  
from datetime import date, datetime,timedelta

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class QuestradeBroker(BaseBroker):
    def __init__(self, is_test=False):
        super().__init__()
        self.is_test = is_test
        self.refresh_token = "ctoGlRkqixa6Tts0s03yEpo5suSKn3zN0" 
        self.auth_url = "https://login.questrade.com/oauth2/token"


    def authenticate(self):
        """Authenticate and get access token and API server URL."""
        params = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}
        response = requests.get(self.auth_url, params=params).json()
        if "access_token" not in response:
            logging.error("Authentication failed. Check refresh token.")
            raise Exception("Failed to authenticate with Questrade API")
        
        self.access_token = response["access_token"]
        self.api_server = response["api_server"]
        self.refresh_token = response["refresh_token"]  # Update refresh token for next use
        logging.info("Successfully authenticated with Questrade API")
        return self.access_token, self.api_server

    def get_account_ids(self):
        """Fetch the first account ID available."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(f"{self.api_server}v1/accounts", headers=headers).json()
        if "accounts" not in response or not response["accounts"]:
            logging.error("No accounts found.")
            raise Exception("No accounts available")
        return response["accounts"]

    def send_request(self,db_max_date):
        """Fetch trade executions from Questrade API."""
        self.authenticate()  # Ensure fresh token
        account_ids = self.get_account_ids()
        headers = {"Authorization": f"Bearer {self.access_token}"}
        # Use a reasonable date range; adjust as needed
        start_date = (db_max_date + timedelta(days=1)).strftime("%Y-%m-%dT00:00:00-05:00")
        end_date = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%dT23:59:59-05:00")
        params = {"startTime": start_date, "endTime": end_date}
        
        trades = []
        for account_id in account_ids:
            url = f"{self.api_server}v1/accounts/{account_id}/executions"
            response = requests.get(url, headers=headers, params=params).json()
        
            if "executions" not in response:
                logging.error("Failed to fetch executions.")
                raise Exception("No execution data returned")
            else:
                trades.append(response)
        
        return trades

    def get_test_data(self):
        """Load test data from a JSON file."""
        with open('test_data/questtrade/all_data.json', 'r') as file:
            data = json.load(file)
        return data

    def parse_data(self, data, max_date=None):
        """Parse Questrade execution data into Trade objects."""
        if max_date is None:
            max_date = datetime(1900, 1, 1)
        else:
            max_date = datetime.strptime(max_date, "%Y%m%d")

        parsed_data = []
        for trade in data:
            # Handle timestamp with microseconds and timezone
            trade_date = datetime.strptime(trade["timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z")
            symbol = trade["symbol"]
            
            # Determine if it's an option or stock
            is_option = len(symbol.split()) > 1
            if is_option:
                parts = symbol.split()
                stock = parts[0]
                option_details = parts[1]
                expiry = f"20{option_details[:6]}"  # e.g., "250228" -> "20250228"
                put_call = "Call" if "C" in option_details else "Put"
                strike = option_details[7:]  # e.g., "175"
                asset_category = "OPT"
                option_id = symbol
            else:
                stock = symbol
                put_call = ""
                strike = ""
                expiry = ""
                asset_category = "STK"
                option_id = ""

            
            obj = Trade(
                    optionId=option_id,
                    tradeDate=trade_date.strftime("%Y%m%d"),
                    accountId=str(trade["accountId"]),  # Always use get_account_id() since no accountNumber in data
                    symbol=stock,
                    putCall=put_call,
                    buySell=str(trade["side"]).upper(),
                    openCloseIndicator=None,  # Not available in Questrade API
                    strike=strike,
                    expiry=expiry,
                    quantity=str(trade["quantity"]),  # Ensure string if your Trade class expects it
                    tradePrice=str(trade["price"]),
                    commission=str(trade["commission"]),
                    assetCategory=asset_category
                )
            parsed_data.append(obj)
        
        return parsed_data

    def get_data(self):
        """Main method to fetch and process trade data."""
        if self.is_test:
            test_json_data = self.get_test_data()
            data = self.parse_data(test_json_data)
        else:
            max_date = get_max_trade_date()
            if max_date is None :
                max_date = datetime(1900, 1, 1)
            else:
                max_date = datetime.strptime(max_date,"%Y%m%d") 

            delta_data_raw = self.send_request(max_date)
            delta_data = self.parse_data(delta_data_raw)
            if delta_data:
                insert_trades(delta_data)  # Assuming this is from your database module
            data = get_all_trades()  # Assuming this is from your database module
        
        return data
