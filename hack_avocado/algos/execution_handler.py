
from __future__ import print_function
import datetime as dt
import time
import pickle
import os


from ib.ext.Contract import Contract
from ib.ext.Order import Order
from ib.ext.EClientSocket import EClientSocket
from ib.opt import ibConnection, message as ib_message_type
from ib.opt import Connection

class ExecutionHandler(object):
    """
    Handles order execution via the Interactive Brokers API
    """

    def __init__(
        self, ib_conn, currency="USD"
    ):
        # initialize
        self.ib_conn = ib_conn
        # self.order_routing = order_routing
        self.currency = currency
        self.fill_dict = {}
        self.fill_dict[0] = {
            "timestamp": dt.datetime.now(),
            "symbol": "Fuck",
            "exchange": "DCE",
            "direction": "NULL",
            "filled": True
        }
#is that bellow right with the pickle ??

#        self.register_handlers()
        self.load_pickle()
        # self.order_id = self.create_initial_order_id()



#this bellow needs to go in registration at HTFmodel
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
        return 750


    def create_contract(self, symbol, sec_type, exch, expiry, curr):
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
        contract.m_expiry = expiry
        contract.m_currency = curr
        return contract

    def create_order(self, order_type, quantity, action, lmt_price):
        """Create an Order object (Market/Limit) to go long/short.

        order_type - 'MKT', 'LMT' for Market or Limit orders
        quantity - Integral number of assets to order
        action - 'BUY' or 'SELL'"""
        order = Order()
        order.m_orderType = order_type
        order.m_totalQuantity = quantity
        order.m_action = action
        order.m_lmtPrice = lmt_price
        return order

    def create_trailing_order(self, quantity, action, trail_threshold, parent_id):
        """
        Creates the trailing order
        quantity: quantity of contracts to trade
        action: "BUY" or "SELL"
        trail_threshold: amount to trail the price by
        parent_id: order ID of the parent fill

        return: order object
        """
        order = Order()
        order.m_orderType = "TRAIL"  # "TRAIL" = Trailing Stop "TRAIL LIMIT" = Trailing stop limit
        order.m_totalQuantity = quantity
        order.m_auxPrice = trail_threshold  # trailing amount
        order.m_action = action
        order.m_triggerMethod = 8  # midpoint method
        #order.m_parentId = parent_id

        return order

    def create_fill_dict_entry(self, msg):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides security information. This is
        needed for the event-driven behaviour of the IB
        server message behaviour.
        """
        self.fill_dict[msg.orderId] = {
            "timestamp": dt.datetime.now(),
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
        self.ib_conn.placeOrder(
            self.order_id, ib_contract, ib_order
        )


        # order goes through!
        time.sleep(1)

        # Increment the order ID
        self.order_id += 1

    def save_pickle(self):
        pickle.dump(self.fill_dict, open(os.path.join(os.path.curdir, "fills.p"),"wb"))
        #horrible code
        pickle.dump(self.order_id, open(os.path.join(os.path.curdir, "orderid.p"), "wb"))



    def load_pickle(self):
        if os.path.exists(os.path.join(os.path.curdir, "fills.p")):

            self.fill_dict = pickle.load(open(os.path.join(os.path.curdir, "fills.p"), "rb"))
        if os.path.exists(os.path.join(os.path.curdir, "orderid.p")):
            self.order_id = pickle.load(open(os.path.join(os.path.curdir, "orderid.p"), "rb"))
        else:
            self.order_id = 1300

    def kill_em_all(self):
        EClientSocket(self.ib_conn).reqGlobalCancel()


    def req_positions(self):
        EClientSocket(self.ib_conn).reqPositions()


    def cancel_pos(self):
        EClientSocket(self.ib_conn).cancelPositions()

######3
#ALLTHIS ISSCAFFOLDING TOTEST THE ORDERLOGIC
# register Ib connection
# model_conn=ibConnection(host="localhost",port=4001, clientId=130)
# model_conn.connect()
#
#
# test = ExecutionHandler(model_conn)
#
#
#
# time.sleep(2)
# test_order = test.create_contract("CL","FUT","NYMEX","201606","USD")
# time.sleep(1)
# test_order_actual = test.create_order("MKT",1,"BUY","")
# time.sleep(1)
# test.execute_order(test_order,test_order_actual)
# time.sleep(1)
# test_lmt = test.create_order("LMT",1,"BUY",46)
# time.sleep(1)
# test.execute_order(test_order,test_lmt)
# test_trail = test.create_trailing_order(1,"SELL",0.1,test.order_id-1)
# time.sleep(1)
# test.execute_order(test_order, test_trail)
# time.sleep(60)
