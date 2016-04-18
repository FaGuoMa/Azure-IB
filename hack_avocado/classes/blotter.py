"""
trade blotter class to store trade and order information

Author: Derek Wong

Updated: 04/17/2016
"""

import random


class Blotter(self):
    """
    Blotter Class
    """

    def __init__(self):

        self.fill_dict = {}
        self.order_id = self.create_initial_order_id()

    def create_initial_order_id(self):
        """
        Generates a Random initial order id
        :return: integer
        """
        return random.randint(10000, 99999)

    def create_fill_dict_entry(self, order_id, timestamp, symbol, exchange, direction):
        """
        Creates an entry in the Fill Dictionary that lists
        orderIds and provides basic information.

        Params: order_id = incremented unique integer for reference
                timestamp = timedate object
                symbol = string of the contract
                exchange = string of exchange code
                direction = integer direction of the trade (-1 = short, 1 = long)
        """
        self.fill_dict[order_id] = {
            "timestamp": timestamp,
            "symbol": symbol,
            "exchange": exchange,
            "direction": direction,
            "filled": False
        }

    def create_fill(self, order_id):
        """
        Places a fill in the fill dictionary
        """

        self.fill_dict[order_id]["filled"] = True

        #increment order_id
        self.order_id += 1