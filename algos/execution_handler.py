
from __future__ import print_function

import datetime
import time

from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.opt import ibConnection, message


class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(
        self, order_routing="SMART", currency="USD"
    ):
        # initialize
        self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}

        self.order_id = self.create_initial_order_id()
        self.register_handlers()

    def _error_handler(self, msg):
        # error handling
        print("Server Error: %s" % msg)

    def _reply_handler(self, msg):
        # Handle open order orderId processing
        if msg.typeName == "openOrder" and \
            msg.orderId == self.order_id and \
            not self.fill_dict.has_key(msg.orderId):
            self.create_fill_dict_entry(msg)
        # Handle Fills
        if msg.typeName == "orderStatus" and \
            msg.status == "Filled" and \
            self.fill_dict[msg.orderId]["filled"] == False:
            self.create_fill(msg)
        print("Server Response: %s, %s\n" % (msg.typeName, msg))

    def create_initial_order_id(self):
        return 1

    def register_handlers(self):

        # Assign the error handling function
        self.tws_conn.register(self._error_handler, 'Error')

        # Assign all of the server reply messages
        self.tws_conn.registerAll(self._reply_handler)

    def create_contract(self, symbol, sec_type, exch, prim_exch, curr):
        """Create a Contract object defining what will
        be purchased, at which exchange and in which currency.

        symbol - The ticker symbol for the contract
        sec_type - The security type for the contract ('FUT' = Future)
        exch - The exchange to carry out the contract on
        prim_exch - The primary exchange to carry out the contract on
        curr - The currency in which to purchase the contract"""
        contract = Contract()
        contract.m_symbol = symbol
        contract.m_secType = sec_type
        contract.m_exchange = exch
        contract.m_primaryExch = prim_exch
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action):
        """Create an Order object (Market/Limit) to go long/short.

        order_type - 'MKT', 'LMT' for Market or Limit orders
        quantity - Integral number of assets to order
        action - 'BUY' or 'SELL'"""
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        return order

    def create_fill_dict_entry(self, msg):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides security information. This is
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        self.fill_dict[msg.orderId] = {
            "symbol": msg.contract.m_symbol,
            "exchange": msg.contract.m_exchange,
            "direction": msg.order.m_action,
            "filled": False
        }

    def create_fill(self, msg):
        """
        Places a fill in the fill dictionary
        """
        fd = self.fill_dict[msg.orderId]

        # Check for fill and no duplicates
        self.fill_dict[msg.orderId]["filled"] = True


    def execute_order(self, ib_contract, ib_order):
        """
        Execute the order through IB API
        """
        # send the order to IB
        self.tws_conn.placeOrder(
            self.order_id, ib_contract, ib_order
        )


        # order goes through!
        time.sleep(1)

        # Increment the order ID
        self.order_id += 1
