"""Unit tests for the matcher service.

Run with: python -m pytest matcher/test_matcher.py -v
"""
import sys
import os
import time
import unittest
from unittest.mock import Mock, patch
from collections import defaultdict

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock database before importing matcher
sys.modules['matcher.database'] = Mock()


class TestNormalizeOrder(unittest.TestCase):
    """Tests for order normalization."""

    def setUp(self):
        # Import after mocking
        from matcher.matcher import normalize_order
        self.normalize_order = normalize_order

    def test_normalize_basic_order(self):
        """Test normalizing a basic order."""
        raw = {
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.5,
            'cl_ord_id': 'order-1'
        }
        order = self.normalize_order(raw)
        self.assertEqual(order['symbol'], 'ALPHA')
        self.assertEqual(order['side'], 'BUY')
        self.assertEqual(order['quantity'], 100)
        self.assertEqual(order['price'], 50.5)
        self.assertEqual(order['cl_ord_id'], 'order-1')

    def test_normalize_fix_tags(self):
        """Test normalizing FIX-style order with tag numbers."""
        raw = {
            '55': 'BETA',
            '54': '1',  # Buy
            '38': '200',
            '44': '25.0',
            '11': 'fix-order-1'
        }
        order = self.normalize_order(raw)
        self.assertEqual(order['symbol'], 'BETA')
        self.assertEqual(order['side'], 'BUY')
        self.assertEqual(order['quantity'], 200)
        self.assertEqual(order['price'], 25.0)

    def test_normalize_sell_side(self):
        """Test normalizing sell orders."""
        for side_value in ['2', 'sell', 'SELL', 's']:
            raw = {'symbol': 'TEST', 'side': side_value, 'quantity': 50, 'price': 10}
            order = self.normalize_order(raw)
            self.assertEqual(order['side'], 'SELL', f"Failed for side={side_value}")

    def test_normalize_market_order(self):
        """Test normalizing market order type."""
        raw = {
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'ord_type': '1'  # Market
        }
        order = self.normalize_order(raw)
        self.assertEqual(order['ord_type'], 'MARKET')

    def test_normalize_ioc_order(self):
        """Test normalizing IOC time-in-force."""
        raw = {
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50,
            'time_in_force': '3'  # IOC
        }
        order = self.normalize_order(raw)
        self.assertEqual(order['time_in_force'], 'IOC')


class TestMatchOrder(unittest.TestCase):
    """Tests for order matching logic."""

    def setUp(self):
        """Set up fresh order books for each test."""
        # Import and reset order books
        import matcher.matcher as matcher_module
        self.matcher = matcher_module
        self.matcher.order_books = defaultdict(lambda: {"bids": [], "asks": []})
        self.matcher.trades_log.clear()

    def test_no_match_empty_book(self):
        """Test that orders rest when book is empty."""
        order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }
        self.matcher.match_order(order, producer=None)

        # Order should be added to bids
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 1)
        self.assertEqual(self.matcher.order_books['ALPHA']['bids'][0]['quantity'], 100)

    def test_full_match(self):
        """Test full match between buy and sell."""
        # Add a resting sell order
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 100,
            'price': 50.0,
            'timestamp': time.time()
        })

        # Incoming buy matches
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_trade'):
            self.matcher.match_order(buy_order, producer=None)

        # Trade should be recorded
        self.assertEqual(len(self.matcher.trades_log), 1)
        trade = self.matcher.trades_log[0]
        self.assertEqual(trade['quantity'], 100)
        self.assertEqual(trade['price'], 50.0)

        # Both sides of book should be empty
        self.assertEqual(len(self.matcher.order_books['ALPHA']['asks']), 0)
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 0)

    def test_partial_fill(self):
        """Test partial fill when order qty exceeds available."""
        # Resting sell for 50
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 50,
            'price': 50.0,
            'timestamp': time.time()
        })

        # Buy for 100
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_trade'), \
             patch.object(self.matcher, 'save_order'):
            self.matcher.match_order(buy_order, producer=None)

        # Trade for 50
        self.assertEqual(len(self.matcher.trades_log), 1)
        self.assertEqual(self.matcher.trades_log[0]['quantity'], 50)

        # Remaining 50 rests on bid side
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 1)
        self.assertEqual(self.matcher.order_books['ALPHA']['bids'][0]['quantity'], 50)

    def test_no_match_price_mismatch(self):
        """Test no match when prices don't cross."""
        # Sell at 55
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 100,
            'price': 55.0,
            'timestamp': time.time()
        })

        # Buy at 50 - won't match
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_order'):
            self.matcher.match_order(buy_order, producer=None)

        # No trades
        self.assertEqual(len(self.matcher.trades_log), 0)

        # Both orders rest
        self.assertEqual(len(self.matcher.order_books['ALPHA']['asks']), 1)
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 1)

    def test_market_order_matches_any_price(self):
        """Test market order matches at any price."""
        # Sell at 100
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 100,
            'price': 100.0,
            'timestamp': time.time()
        })

        # Market buy
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': None,  # No price for market order
            'ord_type': 'MARKET',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_trade'):
            self.matcher.match_order(buy_order, producer=None)

        # Should match at 100
        self.assertEqual(len(self.matcher.trades_log), 1)
        self.assertEqual(self.matcher.trades_log[0]['price'], 100.0)

    def test_ioc_order_cancels_remainder(self):
        """Test IOC order cancels unfilled portion."""
        # Sell only 30
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 30,
            'price': 50.0,
            'timestamp': time.time()
        })

        # IOC buy for 100
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'IOC',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_trade'):
            self.matcher.match_order(buy_order, producer=None)

        # Trade for 30
        self.assertEqual(len(self.matcher.trades_log), 1)
        self.assertEqual(self.matcher.trades_log[0]['quantity'], 30)

        # Remaining 70 cancelled, not added to book
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 0)

    def test_fok_order_rejected_insufficient_qty(self):
        """Test FOK order rejected when full qty not available."""
        # Only 30 available
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-1',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 30,
            'price': 50.0,
            'timestamp': time.time()
        })

        # FOK buy for 100
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'FOK',
            'timestamp': time.time()
        }

        self.matcher.match_order(buy_order, producer=None)

        # No trades - order rejected
        self.assertEqual(len(self.matcher.trades_log), 0)

        # Sell order still there
        self.assertEqual(len(self.matcher.order_books['ALPHA']['asks']), 1)

    def test_price_time_priority(self):
        """Test orders match in price-time priority."""
        # Two sell orders at different prices
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-high',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 50,
            'price': 52.0,
            'timestamp': time.time()
        })
        self.matcher.order_books['ALPHA']['asks'].append({
            'cl_ord_id': 'sell-low',
            'symbol': 'ALPHA',
            'side': 'SELL',
            'quantity': 50,
            'price': 50.0,
            'timestamp': time.time() + 1
        })

        # Buy should match lower price first
        buy_order = {
            'cl_ord_id': 'buy-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 50,
            'price': 55.0,
            'ord_type': 'LIMIT',
            'time_in_force': 'DAY',
            'timestamp': time.time()
        }

        with patch.object(self.matcher, 'save_trade'):
            self.matcher.match_order(buy_order, producer=None)

        # Matched at 50 (better price)
        self.assertEqual(self.matcher.trades_log[0]['price'], 50.0)
        self.assertEqual(self.matcher.trades_log[0]['sell_id'], 'sell-low')


class TestCancelOrder(unittest.TestCase):
    """Tests for order cancellation."""

    def setUp(self):
        import matcher.matcher as matcher_module
        self.matcher = matcher_module
        self.matcher.order_books = defaultdict(lambda: {"bids": [], "asks": []})

    def test_cancel_existing_order(self):
        """Test cancelling an existing order."""
        # Add order to book
        self.matcher.order_books['ALPHA']['bids'].append({
            'cl_ord_id': 'order-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'timestamp': time.time()
        })

        cancel_msg = {
            'type': 'cancel',
            'orig_cl_ord_id': 'order-1',
            'symbol': 'ALPHA'
        }

        with patch.object(self.matcher, 'cancel_order'):
            self.matcher.handle_cancel(cancel_msg, producer=None)

        # Order should be removed
        self.assertEqual(len(self.matcher.order_books['ALPHA']['bids']), 0)


class TestAmendOrder(unittest.TestCase):
    """Tests for order amendment."""

    def setUp(self):
        import matcher.matcher as matcher_module
        self.matcher = matcher_module
        self.matcher.order_books = defaultdict(lambda: {"bids": [], "asks": []})

    def test_amend_price(self):
        """Test amending order price."""
        # Add order
        self.matcher.order_books['ALPHA']['bids'].append({
            'cl_ord_id': 'order-1',
            'symbol': 'ALPHA',
            'side': 'BUY',
            'quantity': 100,
            'price': 50.0,
            'timestamp': time.time()
        })

        amend_msg = {
            'type': 'amend',
            'orig_cl_ord_id': 'order-1',
            'symbol': 'ALPHA',
            'price': 55.0
        }

        self.matcher.handle_amend(amend_msg, producer=None)

        # Price should be updated
        self.assertEqual(self.matcher.order_books['ALPHA']['bids'][0]['price'], 55.0)


if __name__ == '__main__':
    unittest.main()
