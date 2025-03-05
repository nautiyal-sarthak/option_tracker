from .base_broker import BaseBroker
import requests
import json
import time
from app.models.trade import Trade
import logging
from database import *  
from datetime import date, datetime,timedelta
import re
from flask_login import login_user, logout_user, login_required, current_user
from flask import current_app

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class QuestradeBroker(BaseBroker):
    def __init__(self,token,is_test=False):
        super().__init__()
        self.is_test = is_test
        self.refresh_token = token
        self.auth_url = "https://login.questrade.com/oauth2/token"


    def authenticate(self):
        """Authenticate and get access token and API server URL."""
        params = {"grant_type": "refresh_token", "refresh_token": self.refresh_token}
        
        try:
            current_app.logger.info('authenticating with Questrade')
            response = requests.post(self.auth_url, params=params).json()  # Use POST instead of GET
            
            if "access_token" not in response:
                current_app.logger.error("Failed to authenticate with Questrade API")
                raise Exception("Failed to authenticate with Questrade API")

            # update the refrech token in the database
            update_refresh_token(current_user.email,response.get("refresh_token"))
            # ✅ Save the NEW refresh token for future use
            self.refresh_token = response.get("refresh_token", self.refresh_token)  
            self.access_token = response["access_token"]
            self.api_server = response["api_server"]

            current_app.logger.info('authentication successful')
            return self.access_token, self.api_server

        except requests.exceptions.RequestException as e:
            current_app.logger.error(f"Network error during authentication: {e}")
            raise Exception("Network error during authentication")

    def get_account_ids(self):
        try:
            """Fetch the first account ID available."""
            current_app.logger.info('fetching account IDs')
            headers = {"Authorization": f"Bearer {self.access_token}"}
            response = requests.get(f"{self.api_server}v1/accounts", headers=headers).json()
            if "accounts" not in response or not response["accounts"]:
                current_app.logger.error("No accounts available")
                raise Exception("No accounts available")
            current_app.logger.info('account IDs fetched' + str(response["accounts"]))
            return response["accounts"]
        except Exception as e:
            current_app.logger.error(f"Failed to get account IDs: {e}")
            raise



    def send_request(self, db_max_date):
        try:
            current_app.logger.info('sending request to Questrade to get the data after ' + str(db_max_date))
            """Fetch all historical trade executions from Questrade API."""
            self.authenticate()  # Ensure fresh token
            account_ids = self.get_account_ids()
            headers = {"Authorization": f"Bearer {self.access_token}"}

            trades = []

            # Start from the next day after the last recorded trade
            start_date = db_max_date + timedelta(days=1)
            end_date = datetime.today() - timedelta(days=1)  # Yesterday's date

            while start_date < end_date:
                # Get the next month's end date, ensuring it doesn't exceed today's date
                next_month = start_date + timedelta(days=30)
                if next_month > end_date:
                    next_month = end_date  # Ensure we don't go beyond the available data

                start_time_str = start_date.strftime("%Y-%m-%dT00:00:00-05:00")
                end_time_str = next_month.strftime("%Y-%m-%dT23:59:59-05:00")
                params = {"startTime": start_time_str, "endTime": end_time_str}
                current_app.logger.info('fetching data from ' + start_time_str + ' to ' + end_time_str)

                for account_id in account_ids:
                    url = f"{self.api_server}v1/accounts/{account_id['number']}/executions"
                    response = requests.get(url, headers=headers, params=params).json()
                    current_app.logger.info('Number of trades fetched for : ' + account_id + "and date " +  start_time_str + ' to ' + end_time_str + str(len(response.get('executions', []))))
                    if "executions" in response:
                        execution_elements = response['executions']
                        for element in execution_elements:
                            element['accountId'] = account_id['number']
                            trades.append(element)
                    else:
                        logging.warning(f"No trades found for {start_time_str} to {end_time_str}")

                # Move to the next month
                start_date = next_month + timedelta(days=1)

            return trades
        except Exception as e:
            current_app.logger.error(f"Failed to send request to Questrade: {e}")
            raise Exception(e)


    def get_test_data(self):
        """Load test data from a JSON file."""
        with open('test_data/questtrade/all_data.json', 'r') as file:
            data = json.load(file)
        return data
    
    def parse_option_symbol(self,symbol):
        # Regex pattern for extracting components from an option symbol
        option_pattern = r'^([A-Z]+)(\d{1,2}[A-Za-z]{3}\d{2})([CP])(\d+(\.\d{2})?)$'
        match = re.match(option_pattern, symbol)

        if not match:
            return None  # Not an option symbol

        underlying = match.group(1)   # Stock ticker
        exp_date = match.group(2)     # Expiration date in DDMMMYY format
        option_type = "Call" if match.group(3) == "C" else "Put"
        strike_price = float(match.group(4))  # Convert strike price to float

        # Convert date from DDMMMYY to YYYY-MM-DD format
        exp_date_obj = datetime.strptime(exp_date, "%d%b%y")
        formatted_exp_date = exp_date_obj.strftime("%Y-%m-%d")

        return {
            "underlying": underlying,
            "expiration_date": formatted_exp_date,
            "option_type": option_type,
            "strike_price": strike_price
        }


    def parse_data(self, data):
        """Parse Questrade execution data into Trade objects."""

        parsed_data = []
        for trade in data:
            # Handle timestamp with microseconds and timezone
            trade_date = datetime.strptime(trade["timestamp"], "%Y-%m-%dT%H:%M:%S.%f%z")
            symbol = trade["symbol"]
            
            # Determine if it's an option or stock
            details = self.parse_option_symbol(symbol)
            if details:
                #{'underlying': 'GOOG', 'expiration_date': '2025-06-20', 'option_type': 'Call', 'strike_price': 170.0}
                
                stock = details['underlying']
                expiry = details['expiration_date']
                put_call = details['option_type']
                strike = details['strike_price']
                asset_category = "OPT"
                option_id = details['underlying']
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
                    buySell=str(trade["side"][0]).upper(),
                    openCloseIndicator=str(trade["side"][2]).upper(),  # Not available in Questrade API
                    strike=strike,
                    expiry=expiry,
                    quantity=str(trade["quantity"]),  # Ensure string if your Trade class expects it
                    tradePrice=str(trade["price"]),
                    commission=str(trade["commission"]),
                    assetCategory=asset_category
                )
            parsed_data.append(obj)
        
        return parsed_data

    def get_data(self,email):
        try:
            """Main method to fetch and process trade data."""
            if self.is_test:
                test_json_data = self.get_test_data()
                data = self.parse_data(test_json_data)
            else:
                max_date = get_max_trade_date(email)
                if max_date is None :
                    max_date = datetime(2023, 1, 1)
                else:
                    max_date = datetime.strptime(max_date,"%Y%m%d") 

                delta_data_raw = self.send_request(max_date)
                delta_data = self.parse_data(delta_data_raw)
                if delta_data:
                    insert_trades(delta_data,email)  # Assuming this is from your database module
                data = get_all_trades(email)  # Assuming this is from your database module
            
            return data
        except Exception as e:
            logging.error(f"Failed to get data: {e}")
            raise Exception(e)