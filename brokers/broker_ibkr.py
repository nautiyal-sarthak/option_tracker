from brokers.base_broker import BaseBroker
import requests
import xml.etree.ElementTree as ET
import time
from entity.trade import Trade
import logging
from database import *  
from datetime import date, datetime

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

    def get_test_data(self):
        # reat the file and return the data as a string
        with open('test_data/ibkr/all_data.xml', 'r') as file:
            data = file.read().replace('\n', '')
        return data

    def get_statement(self, refCode):
        receive_slug = "/GetStatement"
        receive_params = {
            "t":self.token, 
            "q":refCode, 
            "v":self.flex_version
        }

        receiveUrl = requests.get(url=self.requestBase+receive_slug, params=receive_params, allow_redirects=True)
        return receiveUrl.content
    
    def parse_data(self, data,max_date=None):
        if max_date is None :
            max_date = datetime(1900, 1, 1)
        else:
            max_date = datetime.strptime(max_date,"%Y%m%d")

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
                                            option_id = ""
                                        else:
                                            stock = trade_ele.attrib["underlyingSymbol"]
                                            option_id = trade_ele.attrib["symbol"]
                                        
                                        if (datetime.strptime(trade_ele.attrib["tradeDate"],"%Y%m%d") > max_date and datetime.strptime(trade_ele.attrib["tradeDate"],"%Y%m%d") < datetime.today()):
                                            obj = Trade(
                                                    option_id,trade_ele.attrib["tradeDate"], 
                                                    trade_ele.attrib["accountId"], 
                                                    stock, trade_ele.attrib["putCall"], 
                                                    trade_ele.attrib["buySell"],
                                                    trade_ele.attrib["openCloseIndicator"], 
                                                    trade_ele.attrib["strike"], 
                                                    trade_ele.attrib["expiry"], 
                                                    trade_ele.attrib["quantity"], 
                                                    trade_ele.attrib["tradePrice"], 
                                                    trade_ele.attrib["ibCommission"],  
                                                    trade_ele.attrib["assetCategory"]
                                                )
                                        
                                            parsed_data.append(obj)
         
        return parsed_data

    def get_data(self,email):
        if self.is_test:
            xml_data = self.get_test_data()
            data = self.parse_data(xml_data)
        else:
            refCode = self.send_request()
            time.sleep(20)
            xml_data = self.get_statement(refCode)
            max_date = get_max_trade_date(email)
            delta_data = self.parse_data(xml_data,max_date)
            if delta_data:
                insert_trades(delta_data,email)
            data = get_all_trades(email)

            
        
        return data
