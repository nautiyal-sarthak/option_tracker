import unittest
import pandas as pd
from datetime import datetime
from app.utils.data import process_wheel_trades


class TestProcessWheelTrades(unittest.TestCase):

    def setUp(self):
        # Updated sample data with expiry_date and tradeDate as datetime.date
        self.sample_data = pd.DataFrame([
            {
                'optionId': 'OPT123', 'tradeDate': datetime(2023, 10, 1).date(), 'accountId': '12345', 'symbol': 'AAPL',
                'putCall': 'Call', 'buySell': 'SELL', 'openCloseIndicator': 'Open', 'strike': 150.0,
                'expiry': datetime(2023, 10, 15).date(), 'quantity': 1, 'tradePrice': 2.0, 'commission': 1.0,
                'assetCategory': 'Option', 'total_premium': 200.0
            }
        ])

    def test_process_wheel_trades_basic(self):
        # Test the basic functionality of process_wheel_trades
        result = process_wheel_trades(self.sample_data)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('symbol', result.columns)
        self.assertIn('status', result.columns)
        self.assertIn('ROI', result.columns)

    def test_open_trade_status(self):
        # Test that open trades are correctly marked as "OPEN"
        result = process_wheel_trades(self.sample_data)
        open_trades = result[result['status'] == 'OPEN']
        self.assertEqual(len(open_trades), 0)  # No open trades in this dataset

    def test_bought_back_status(self):
        # Test that buyback trades are correctly marked as "BOUGHT BACK"
        result = process_wheel_trades(self.sample_data)
        bought_back_trades = result[result['status'] == 'BOUGHT BACK']
        self.assertEqual(len(bought_back_trades), 1)
        self.assertEqual(bought_back_trades.iloc[0]['symbol'], 'AAPL')
        self.assertAlmostEqual(bought_back_trades.iloc[0]['net_premium'], 100.0)