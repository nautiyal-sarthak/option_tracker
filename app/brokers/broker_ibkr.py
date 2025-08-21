from .base_broker import BaseBroker
import requests
import xml.etree.ElementTree as ET
import time
from app.models.trade import Trade
import logging
from supabase import *  
from datetime import date, datetime,timedelta
from flask import current_app,session

# Configure logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')



class IBKRBroker(BaseBroker):
    def __init__(self,token,is_test=False):
        super().__init__()
        self.is_test = is_test
        self.token = token
        self.query_id = "1144505"
        self.flex_version = 3
        self.requestBase = "https://ndcdyn.interactivebrokers.com/AccountManagement/FlexWebService"

    def send_request(self):
        try:
            current_app.logger.info('sending request to IBKR')
            token = self.token
            queryId = self.query_id
            flex_version = self.flex_version

            send_path = "/SendRequest"
            send_params = {
                "t":token, 
                "q":queryId, 
                "v":flex_version
            }

            response = requests.get(url=self.requestBase+send_path, params=send_params)
            tree = ET.ElementTree(ET.fromstring(response.text))
            root = tree.getroot()

            for child in root:
                if child.tag == "Status":
                    if child.text != "Success":
                        print(f"Failed to generate Flex statement. Stopping...")
                        exit()
                elif child.tag == "ReferenceCode":
                    refCode = child.text
                    return refCode
        except Exception as e:
            current_app.logger.error(f"Error sending request to IBKR: {e}")
            raise e

    def get_test_data(self):
        current_app.logger.info('fetching test data')
        # reat the file and return the data as a string
        with open('test_data/ibkr/all_data.xml', 'r') as file:
            data = file.read().replace('\n', '')
        return data

    def get_statement(self, refCode):
        try:
            current_app.logger.info('fetching statement from IBKR')
            receive_slug = "/GetStatement"
            receive_params = {
                "t":self.token, 
                "q":refCode, 
                "v":self.flex_version
            }

            receiveUrl = requests.get(url=self.requestBase+receive_slug, params=receive_params, allow_redirects=True)
            return receiveUrl.content
        except Exception as e:
            current_app.logger.error(f"Error fetching statement from IBKR: {e}")
            raise e
    
    def parse_data(self, data,max_date=None):
        try:
            current_app.logger.info('parsing data from IBKR')
            if max_date is None :
                max_date = datetime(1900, 1, 1)
            else:
                # subtract 5 days to get the last 5 days of data
                max_date = max_date - timedelta(days=1)

            root = ET.fromstring(data)
            parsed_data = []
            for FlexStatements in root:
                if FlexStatements.tag == "FlexStatements":
                    for statement in FlexStatements:
                        if statement.tag == "FlexStatement":
                            for trades in statement:
                                if trades.tag == "Trades":
                                    for trade_ele in trades:
                                        if (trade_ele.tag == "Trade" and trade_ele.attrib["symbol"] != "" and trade_ele.attrib["assetCategory"] in ['STK','OPT']):
                                            if trade_ele.attrib["assetCategory"] == "STK":
                                                stock = trade_ele.attrib["symbol"]
                                                option_id = trade_ele.attrib["tradeID"]
                                            else:
                                                stock = trade_ele.attrib["underlyingSymbol"]
                                                option_id = trade_ele.attrib["tradeID"]
                                            
                                            if (datetime.strptime(trade_ele.attrib["tradeDate"],"%Y%m%d") >= datetime.combine(max_date, datetime.min.time())):
                                                
                                                if float(trade_ele.attrib["ibCommission"]) > 0:
                                                    ibCommission = float(trade_ele.attrib["ibCommission"]) * -1
                                                else:
                                                    ibCommission = float(trade_ele.attrib["ibCommission"])

                                                if trade_ele.attrib["openCloseIndicator"] == 'C;O':
                                                    openCloseIndicator = 'O'
                                                else:
                                                    openCloseIndicator = trade_ele.attrib["openCloseIndicator"]
                                                
                                                obj = Trade(
                                                        option_id,trade_ele.attrib["tradeDate"], 
                                                        trade_ele.attrib["accountId"], 
                                                        stock, trade_ele.attrib["putCall"], 
                                                        trade_ele.attrib["buySell"],
                                                        openCloseIndicator, 
                                                        trade_ele.attrib["strike"], 
                                                        trade_ele.attrib["expiry"], 
                                                        trade_ele.attrib["quantity"], 
                                                        trade_ele.attrib["tradePrice"], 
                                                        ibCommission,  
                                                        trade_ele.attrib["assetCategory"],
                                                        datetime.strptime(trade_ele.attrib["dateTime"], "%Y%m%d;%H%M%S")
                                                    )
                                            
                                                parsed_data.append(obj)
                                        if (trade_ele.tag == "Order" and trade_ele.attrib["closePrice"] != "0" 
                                            and trade_ele.attrib["openCloseIndicator"] == "C" and trade_ele.attrib["tradePrice"] == "0" and trade_ele.attrib["underlyingSymbol"] == 'XSP') :

 
                                            if float(trade_ele.attrib["ibCommission"]) > 0:
                                                ibCommission = float(trade_ele.attrib["ibCommission"]) * -1
                                            else:
                                                ibCommission = float(trade_ele.attrib["ibCommission"])

                                            if trade_ele.attrib["openCloseIndicator"] == 'C;O':
                                                openCloseIndicator = 'O'
                                            else:
                                                openCloseIndicator = trade_ele.attrib["openCloseIndicator"]

                                            obj = Trade(
                                                        str(trade_ele.attrib["tradeDate"]) + str(trade_ele.attrib["underlyingSymbol"]) + str(trade_ele.attrib["quantity"]),
                                                        trade_ele.attrib["tradeDate"], 
                                                        trade_ele.attrib["accountId"], 
                                                        trade_ele.attrib["underlyingSymbol"], 
                                                        trade_ele.attrib["putCall"], 
                                                        trade_ele.attrib["buySell"],
                                                        openCloseIndicator, 
                                                        trade_ele.attrib["strike"], 
                                                        trade_ele.attrib["expiry"], 
                                                        trade_ele.attrib["quantity"], 
                                                        trade_ele.attrib["closePrice"], 
                                                        ibCommission,  
                                                        trade_ele.attrib["assetCategory"],
                                                        datetime.strptime(trade_ele.attrib["dateTime"], "%Y%m%d;%H%M%S")
                                                    )
                                            
                                            
                                            parsed_data.append(obj)
                                           
            
            return parsed_data
        except Exception as e:
            current_app.logger.error(f"Error parsing data from IBKR: {e}")
            raise e

    def get_data(self,email):
        try:
            if session.get('adhoc_email'):
                email = session.get('adhoc_email')
                data = get_all_trades(email)
            else:
                if self.is_test:
                    current_app.logger.info('fetching test data')
                    xml_data = self.get_test_data()
                    data = self.parse_data(xml_data)
                else:
                    current_app.logger.info('fetching data from IBKR')
                    refCode = self.send_request()
                    time.sleep(20)
                    xml_data = self.get_statement(refCode)
                    max_date = get_max_trade_date(email)
                    current_app.logger.info(f"older max_date: {max_date}")
                    delta_data = self.parse_data(xml_data,max_date)
                    if delta_data:
                        insert_trades(delta_data,email)
                    data = get_all_trades(email)
            
            return data
        except Exception as e:
            current_app.logger.error(f"Error fetching data from IBKR: {e}")
            raise e
