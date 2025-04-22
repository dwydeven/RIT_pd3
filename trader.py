import requests
import time
from clients import Exchange_Client

with open('api_key.txt', 'r') as f:
    API_KEY = f.read().strip()

    
POSITION_LIMITS = {
    'GEM': 33000,
    'UB': 17500,
    'ETF': 33000 + 17500
}

class News:
    """
    A class to represent news data and handle news data to provide estimates for the GEM, UB, and ETF markets
    """
    def __init__(self):
        self.latest_news = []
        self.news_length = 1
        self.estimates = {'GEM': [20, 30], 'UB': [40, 60], 'ETF': [60, 90]}
        self.new_news = False

        self.full_process_news()

    def get_latest_news(self):
        """
        Returns the latest news data from the API as a list of all the news items
        """
        response = requests.get(f'http://localhost:10001/v1/news', headers={'X-API-key': API_KEY})
        if response.status_code == 200:
            self.latest_news = response.json()
            if len(self.latest_news) != self.news_length:
                self.new_news = True
                self.news_length = len(self.latest_news)
            else:
                self.new_news = False

            return self.latest_news
        else:
            print("Failed to fetch news data")
            return None       

    def parse_news_item(self, news_item):
        """
        Parses a single news item to extract and reutrn 
        Example: {'news_id': 3, 
                'period': 1, 
                'tick': 82, 
                'ticker': '', 
                'headline': 
                'Private Information #1 for UB', 
                'body': 'After 83 seconds, your private estimate is that the final value will be $43.25'}
            
        Returns: (time, ticker, estimate)
        """
        # Extract the ticker from the headline
        headline = news_item.get('headline', '')
        if 'GEM' in headline:
            ticker = 'GEM'
        elif 'UB' in headline:
            ticker = 'UB'
        else:
            ticker = None

        # Extract the estimate from the body
        body = news_item.get('body', '')
        if len(body) > 0:
            estimate = body.split('$')[-1].strip()
            if estimate:
                estimate = float(estimate.split()[0])
            else:
                estimate = None
        else:
            estimate = None

        tick = news_item.get('tick')
        time_ = tick + 1 # # Adding 1 to the tick to get the time in seconds

        return time_, ticker, estimate
    
    def calculate_estimate_interval(self, time, estimate):
        """
        Calculates the estimate interval for the given time, ticker, and estimate using the formula from the documentation
        Formula: estimate = actual + X(300 - time) / 50 where X is uniform[-1, 1]
        """
        estimate_interval = []
        coefficient = (300 - time) / 50
        minimum = estimate - coefficient
        maximum = estimate + coefficient
        estimate_interval = [round(minimum, 2), round(maximum, 2)]

        return estimate_interval
    
    def calculate_expected_values(self, estimates):
        """
        Returns a dictionary of the ticker with the value as the midpoint of the estimates
        """
        expected_value = {}
        for ticker, estimate in estimates.items():
            expected_value[ticker] = (estimate[0] + estimate[1]) / 2

        return expected_value
    
    def process_news(self, news_data):
        """
        Takes large news data -> sorts into ticker-specific news -> builds estimates based on the documentation function
        """
        geb_estimates = []
        ub_estimates = []

        for news in news_data:
            # First always pass over the news data with news_id of 1 or 12 since this is the introductory and ending news without any estimates
            if news['news_id'] == 1 or news['news_id'] == 12:
                continue

            # Parse the news item to extract relevant information
            time_, ticker, estimate = self.parse_news_item(news)
            estimate_interval = self.calculate_estimate_interval(time_, estimate)
            if ticker == 'GEM':
                geb_estimates.append(estimate_interval)
            elif ticker == 'UB':
                ub_estimates.append(estimate_interval)
            else:
                continue

        return geb_estimates, ub_estimates
    
    def get_processed_interval(self, geb_estimates, ub_estimates):
        """
        Takes the estimates and returns the processed intervals for each ticker
        The processed interval takes the maximum of the minimums and the minimum of the maximums for each ticker
        """
        if geb_estimates == []:
            geb_minimum = 20
            geb_maximum = 30
        else:
            geb_minimum = max(max([estimate[0] for estimate in geb_estimates]), 20)
            geb_maximum = min(min([estimate[1] for estimate in geb_estimates]), 30)

        if ub_estimates == []:
            ub_minimum = 40
            ub_maximum = 60
        else:
            ub_minimum = max(max([estimate[0] for estimate in ub_estimates]), 40)
            ub_maximum = min(min([estimate[1] for estimate in ub_estimates]), 60)

        return [geb_minimum, geb_maximum], [ub_minimum, ub_maximum]
    
    def full_process_news(self):
        """
        Takes the news data and processes it to get the estimates for each ticker

        Returns:
            dict: A dictionary with the estimates for each ticker with key as the ticker and value as the estimate interval as a list
        """
        news_data = self.get_latest_news()
        estimates = {}

        if self.new_news:
            geb_estimates, ub_estimates = self.process_news(news_data)
            geb_interval, ub_interval = self.get_processed_interval(geb_estimates, ub_estimates)

            # Store the estimates in the estimates dictionary
            # The ETF holds 1 of each asset, so we sum the estimates for each ticker
            # ETF = GEM + UB
            estimates['GEM'] = geb_interval
            estimates['UB'] = ub_interval
            estimates['ETF'] = [geb_interval[0] + ub_interval[0], geb_interval[1] + ub_interval[1]]
        
            self.estimates = estimates

        return self.estimates


class Quoter:
    """
    This class controls the market making logic to provide two sided quotes on all the markets
    """
    def __init__(self, exchange_client, news_handler):
        self.news_handler = news_handler
        self.exchange_client = exchange_client
             

    def competitive_quotes(self, consolidated_contra_book):
        """
        Calculates the competitive quotes for all tickers based on the contra orderbook and the estimates from the news handler.

        Args:
            consolidated_contra_book (dict): The orderbook with my quotes removed so I won't self-trade with orders consolidated by price and size
            It is in the format of {'ticker': {'bids': [[price, size], ...], 'asks': [[price, size], ...]}}

        Returns:
            dict: A dictionary with [ticker] = [[bid price, bid size], [ask price, ask size]].
        """
        COMMISSION = 0.02  # 2 cents per share
        comp_quotes = {}

        gem_best_bid, gem_best_bid_size = consolidated_contra_book['GEM']['bids'][0] if consolidated_contra_book['GEM']['bids'] else (20, 0)
        gem_best_ask, gem_best_ask_size = consolidated_contra_book['GEM']['asks'][0] if consolidated_contra_book['GEM']['asks'] else (30, 0)

        ub_best_bid, ub_best_bid_size = consolidated_contra_book['UB']['bids'][0] if consolidated_contra_book['UB']['bids'] else (40, 0)
        ub_best_ask, ub_best_ask_size = consolidated_contra_book['UB']['asks'][0] if consolidated_contra_book['UB']['asks'] else (60, 0)

        etf_best_bid, etf_best_bid_size = consolidated_contra_book['ETF']['bids'][0] if consolidated_contra_book['ETF']['bids'] else (60, 0)
        etf_best_ask, etf_best_ask_size = consolidated_contra_book['ETF']['asks'][0] if consolidated_contra_book['ETF']['asks'] else (90, 0)
        
        # Use the estimates from the news handler
        gem_bounds = self.news_handler.estimates['GEM']
        ub_bounds = self.news_handler.estimates['UB']
        etf_bounds = self.news_handler.estimates['ETF']

        # Calculate competitive quotes for each ticker
        # Adjust so that the bid is always at least my bottom estimate and the ask is always at most my top estimate
        # Setting a max size of 2,000 shares for all quotes
        MAX_SIZE = 2000

        gem_bid = [round(min(etf_best_bid - ub_best_ask - COMMISSION * 3, gem_bounds[1] - COMMISSION * 3 - .01), 2), min(ub_best_ask_size, etf_best_bid_size)]
        gem_bid = [round(max(gem_bid[0], gem_bounds[0] - COMMISSION * 3 - .01), 2), min(gem_bid[1], MAX_SIZE)]

        gem_ask = [round(max(etf_best_ask - ub_best_bid + COMMISSION * 3, gem_bounds[0] + COMMISSION * 3 + .01), 2), min(ub_best_bid_size, etf_best_ask_size)]
        gem_ask = [round(min(gem_ask[0], gem_bounds[1] + COMMISSION * 3 + .01), 2), min(gem_ask[1], MAX_SIZE)]

        ub_bid = [round(min(etf_best_bid - gem_best_ask - COMMISSION * 3, ub_bounds[1] - COMMISSION * 3 - .01), 2), min(gem_best_ask_size, etf_best_bid_size)]
        ub_bid = [round(max(ub_bid[0], ub_bounds[0] - COMMISSION * 3 - .01), 2), min(ub_bid[1], MAX_SIZE)]

        ub_ask = [round(max(etf_best_ask - gem_best_bid + COMMISSION * 3, ub_bounds[0] + COMMISSION * 3 + .01), 2), min(gem_best_bid_size, etf_best_ask_size)]
        ub_ask = [round(min(ub_ask[0], ub_bounds[1] + COMMISSION * 3 + .01), 2), min(ub_ask[1], MAX_SIZE)]

        etf_bid = [round(min(gem_best_bid + ub_best_bid - COMMISSION * 3, etf_bounds[1] - COMMISSION * 3 - .01), 2), min(gem_best_bid_size, ub_best_bid_size)]
        etf_bid = [round(max(etf_bid[0], etf_bounds[0] - COMMISSION * 3 - .01), 2), min(etf_bid[1], MAX_SIZE)]

        etf_ask = [round(max(gem_best_ask + ub_best_ask + COMMISSION * 3, etf_bounds[0] + COMMISSION * 3 + .01), 2), min(gem_best_ask_size, ub_best_ask_size)]
        etf_ask = [round(min(etf_ask[0], etf_bounds[1] + COMMISSION * 3 + .01), 2), min(etf_ask[1], MAX_SIZE)]

        # Store the quotes
        comp_quotes['GEM'] = [gem_bid, gem_ask]
        comp_quotes['UB'] = [ub_bid, ub_ask]
        comp_quotes['ETF'] = [etf_bid, etf_ask]

        return comp_quotes
    
    def optimize_quotes(self, competitive_quotes, consolidated_contra_book):
        """
        Optimizes the quotes making sure that I'm only top of the book and not overly competitive. If I'd buy at 20 but the best bid is 19.50, I should bid 19.51
        This function, like competitive quotes, is completely indifferent of my current quotes
        
        Args:
            competitive_quotes (dict): The most competitive quotes for all tickers that would still make me money
            full_orderbook (dict): The orderbook with all orders in the market, including the ones with my trader id
            
        Response:
            dict: A dictionary with [ticker] = [[bid price, bid size], [ask price, ask size]]     
        """
        optimized_quotes = {}

        # Get the best bid and ask price only
        gem_best_bid = consolidated_contra_book['GEM']['bids'][0][0] if consolidated_contra_book['GEM']['bids'] else 20
        gem_best_ask = consolidated_contra_book['GEM']['asks'][0][0] if consolidated_contra_book['GEM']['asks'] else 30

        ub_best_bid = consolidated_contra_book['UB']['bids'][0][0] if consolidated_contra_book['UB']['bids'] else 40
        ub_best_ask = consolidated_contra_book['UB']['asks'][0][0] if consolidated_contra_book['UB']['asks'] else 60

        etf_best_bid = consolidated_contra_book['ETF']['bids'][0][0] if consolidated_contra_book['ETF']['bids'] else 60
        etf_best_ask = consolidated_contra_book['ETF']['asks'][0][0] if consolidated_contra_book['ETF']['asks'] else 90

        # Extract my price information
        my_gem_bid = competitive_quotes['GEM'][0][0]
        my_gem_ask = competitive_quotes['GEM'][1][0]

        my_ub_bid = competitive_quotes['UB'][0][0]
        my_ub_ask = competitive_quotes['UB'][1][0]

        my_etf_bid = competitive_quotes['ETF'][0][0]
        my_etf_ask = competitive_quotes['ETF'][1][0]

        # Check where my quotes would compare to the best quotes in the market
        # If my bid is better than the best bid, I should be at the best bid + 0.01, vice versa for the ask
        # Otherwise, if the market is more competitive, I just leave the quotes as they are
        if my_gem_bid > gem_best_bid:
            competitive_quotes['GEM'][0][0] = round(gem_best_bid + 0.01, 2)

        if my_gem_ask < gem_best_ask:
            competitive_quotes['GEM'][1][0] = round(gem_best_ask - 0.01, 2)

        if my_ub_bid > ub_best_bid:
            competitive_quotes['UB'][0][0] = round(ub_best_bid + 0.01, 2)

        if my_ub_ask < ub_best_ask:
            competitive_quotes['UB'][1][0] = round(ub_best_ask - 0.01, 2)

        if my_etf_bid > etf_best_bid:
            competitive_quotes['ETF'][0][0] = round(etf_best_bid + 0.01, 2)

        if my_etf_ask < etf_best_ask:
            competitive_quotes['ETF'][1][0] = round(etf_best_ask - 0.01, 2)

        optimized_quotes['GEM'] = competitive_quotes['GEM']
        optimized_quotes['UB'] = competitive_quotes['UB']
        optimized_quotes['ETF'] = competitive_quotes['ETF']

        return optimized_quotes
    
    def adjust_quotes(self, optimized_quotes, positions):
        """
        Adjusts the quotes based on the positions in the market. If I am too long or too short, I should adjust my quotes accordingly.
        
        Args:
            optimized_quotes (dict): The quotes for the given ticker, dictionary of dictionaries with {'ticker': [[bid price, bid size], [ask price, ask size]], ...}.
            positions (dict): The positions by ticker key
            
        Returns:
            adjusted_quotes (dict): The adjusted quotes for the given ticker, dictionary of dictionaries with {'ticker': [[bid price, bid size], [ask price, ask size]], ...}.
            """
        adjusted_quotes = {}
        
        # Quotes are adjusted based on their relative positions to the limits on the positions
        def get_position_skew(ticker, position):
            # We first normalize on a 0-1 scale based on the position limits and then translate to a -1 to 1 scale
            if ticker == 'GEM':
                zero_one_normalized_position = (position + POSITION_LIMITS['GEM']) / (2 * POSITION_LIMITS['GEM'])
                neg_one_one_position = (zero_one_normalized_position - 0.5) * 2
                return neg_one_one_position
            elif ticker == 'UB':
                zero_one_normalized_position = (position + POSITION_LIMITS['UB']) / (2 * POSITION_LIMITS['UB'])
                neg_one_one_position = (zero_one_normalized_position - 0.5) * 2
                return neg_one_one_position
            elif ticker == 'ETF':
                zero_one_normalized_position = (position + POSITION_LIMITS['ETF']) / (2 * POSITION_LIMITS['ETF'])
                neg_one_one_position = (zero_one_normalized_position - 0.5) * 2
                return neg_one_one_position
            

        # I am going to scale my size by new_size = old size * (1 + position skew)
        # So if my skew is negative, my bid size will be smaller and my ask size will be larger
        # If my skew is positive, my bid size will be larger and my ask size will be smaller
        for ticker in optimized_quotes.keys():
            position = positions[ticker]
            position_skew = get_position_skew(ticker, position)

            bid_size = optimized_quotes[ticker][0][1]
            ask_size = optimized_quotes[ticker][1][1]

            new_bid_size = round(bid_size * (1 - position_skew))
            new_ask_size = round(ask_size * (1 + position_skew))

            adjusted_quotes[ticker] = [
                [optimized_quotes[ticker][0][0], new_bid_size],
                [optimized_quotes[ticker][1][0], new_ask_size]
            ]

        return adjusted_quotes
    
    def check_against_current_quotes(self, current_quotes, adjusted_quotes):
        """
        Checks the optimized quotes against the current quotes to see if they are better or worse than the current quotes.
        If they are better, we should send them to the exchange.

        Args:
            current_quotes (dict): The current quotes for the given ticker, once a side specified it's a list of orders with {'ticker': {'bids': [{'price':, 'size':, 'order_id':}, ...]}}.
            adjusted_quotes (dict): The adjusted quotes for the given ticker, dictionary of dictionaries with {'ticker': [[bid price, bid size], [ask price, ask size]], ...}.

        Returns:

            list: A list of orders to send to the exchange.
        """
        orders_to_send = []
        orders_to_cancel = []

        for ticker in adjusted_quotes.keys():
            # Get current quotes for the ticker, default to empty if not present
            current_ticker_quotes = current_quotes.get(ticker, {'bids': [], 'asks': []})    

            current_bid = current_ticker_quotes['bids'][0] if current_ticker_quotes['bids'] else None
            current_ask = current_ticker_quotes['asks'][0] if current_ticker_quotes['asks'] else None

            # Check bids
            if not current_bid:
                # No current bid, send the adjusted bid
                if adjusted_quotes[ticker][0][1] > 0:
                    orders_to_send.append({
                        'ticker': ticker,
                        'type': 'LIMIT',
                        'price': adjusted_quotes[ticker][0][0],
                        'quantity': adjusted_quotes[ticker][0][1],
                        'action': 'BUY'
                    })
            else:
                # Compare current bid with adjusted bid
                if adjusted_quotes[ticker][0][1] > 0 and adjusted_quotes[ticker][0][0] != current_bid['price']:
                    # Cancel the current bid and send the new one
                    orders_to_cancel.append(current_bid['order_id'])
                    orders_to_send.append({
                        'ticker': ticker,
                        'type': 'LIMIT',
                        'price': adjusted_quotes[ticker][0][0],
                        'quantity': adjusted_quotes[ticker][0][1],
                        'action': 'BUY'
                    })

            # Check asks
            if not current_ask:
                # No current ask, send the adjusted ask
                if adjusted_quotes[ticker][1][1] > 0:
                    orders_to_send.append({
                        'ticker': ticker,
                        'type': 'LIMIT',
                        'price': adjusted_quotes[ticker][1][0],
                        'quantity': adjusted_quotes[ticker][1][1],
                        'action': 'SELL'
                    })
            else:
                # Compare current ask with adjusted ask
                if adjusted_quotes[ticker][1][1] > 0 and adjusted_quotes[ticker][1][0] != current_ask['price']:
                    # Cancel the current ask and send the new one
                    orders_to_cancel.append(current_ask['order_id'])
                    orders_to_send.append({
                        'ticker': ticker,
                        'type': 'LIMIT',
                        'price': adjusted_quotes[ticker][1][0],
                        'quantity': adjusted_quotes[ticker][1][1],
                        'action': 'SELL'
                    })

        # Cancel the orders that are no longer valid
        if orders_to_cancel:
            self.exchange_client.cancel_all_orders(order_ids=orders_to_cancel)

        return orders_to_send
    
    def validate_orders(orders):
        """
        Validates orders to make sure they are not too large and are within the limits of the exchange
        """
        for order in orders:
            if order['quantity'] > 5000:
                order['quantity'] = 5000
            if order['price'] < 0:
                raise ValueError(f"Order price {order['price']} is invalid")
            
        return True
    
    def calculate_and_send_orders(self, contra_orderbook):
        """
        Calculates the quotes for the given orderbooks and estimates from the news handler
        
        Args:
            current_quotes (dict): The current quotes for the given ticker
            full_orderbook (dict): The orderbook with all orders in the marekt
            contra_orderbook (dict): The orderbook with my quotes removed

        Response:
            dict: A dictionary with [ticker] = [[bid price, bid size], [ask price, ask size]]     
        """
        current_quotes = self.exchange_client.get_quotes()

        competitive_quotes = self.competitive_quotes(contra_orderbook)
        optimized_quotes = self.optimize_quotes(competitive_quotes, contra_orderbook)
        adjusted_quotes = self.adjust_quotes(optimized_quotes, self.exchange_client.get_positions())

        orders_to_send = self.check_against_current_quotes(current_quotes, adjusted_quotes)

        for order in orders_to_send:
            if order['quantity'] > 5000:
                order['quantity'] = 5000
            self.exchange_client.create_order(order)


class Hitter:
    """
    This class controls the hitter which responds to news based events and hits on filled quotes to capture etf arbitrage opportunities
    """
    def __init__(self, exchange_client, news_handler):
        self.news_handler = news_handler
        self.exchange_client = exchange_client
        self.new_news = news_handler.new_news

        
    def check_orderbook_mispricing(self, consolidated_contra_book, estimates):
        """
        This function checks the orderbook for mispricing. It is called when there is new news.
        It should also be only called after my quotes are cancelled.

        Args:
            consolidated_contra_book (dict): The orderbook with my quotes removed so I won't self-trade.
            estimates (dict): The estimates for each ticker from the news handler.

        Returns:
            dict: A dictionary with the structure:
                {
                    'GEM': {'bid': False, 'ask': False},
                    'UB': {'bid': False, 'ask': False},
                    'ETF': {'bid': False, 'ask': False}
                }
        """
        mispricing = {
            'GEM': {'bid': False, 'ask': False},
            'UB': {'bid': False, 'ask': False},
            'ETF': {'bid': False, 'ask': False}
        }

        # Extract the best bid and ask prices for each ticker
        for ticker in ['GEM', 'UB', 'ETF']:
            best_bid = consolidated_contra_book[ticker]['bids'][0][0] if consolidated_contra_book[ticker]['bids'] else 20
            best_ask = consolidated_contra_book[ticker]['asks'][0][0] if consolidated_contra_book[ticker]['asks'] else 90

            bounds = estimates[ticker]
            lower_bound, upper_bound = bounds

            # Check for mispricing on the bid side
            if best_bid > upper_bound:
                mispricing[ticker]['bid'] = True

            # Check for mispricing on the ask side
            if best_ask < lower_bound:
                mispricing[ticker]['ask'] = True

        return mispricing
    
    def get_size_to_price(self, ticker, consolidated_contra_book, estimate, side):    
        """
        Takes an individual estimate and returns the size to price for the given side of the orderbook
        
        Args:
            consolidated_contra_book (dict): The orderbook with my quotes removed so I won't self-trade.
            estimate (list): The estimate for the given ticker from the news handler.
            side (str): The side of the orderbook to check, either 'bid' or 'ask'.
            
        Returns:
            int: Thet total size available outside of the estimate range
        """
        total_size = 0

        # Check the side of the orderbook to calculate the size to price
        if side == 'bid':
            bids = consolidated_contra_book[ticker]['bids'] if consolidated_contra_book[ticker]['bids'] else []
            for price, size in bids:
                if price < estimate:
                    break
                total_size += size
        elif side == 'ask':
            asks = consolidated_contra_book[ticker]['asks'] if consolidated_contra_book[ticker]['asks'] else []
            for price, size in asks:
                if price > estimate:
                    break
                total_size += size

        return total_size
    
    def get_total_size(self, consolidated_contra_book, estimates, mispricings):
        """
        This function calculates the total edge for each ticker based on the estimates and the orderbook.
        Total edge is (price - estimate) * size for each side of the orderbook up to price == estimate.
        
        Args:
            consolidated_contra_book (dict): The orderbook with my quotes removed so I won't self-trade.
            estimates (dict): The estimates for each ticker from the news handler.
            mispricings (dict): The mispricing for each ticker from the news handler.

        Returns:
            dict: A dictionary with the total size on each side of the orderbook for each ticker [ticker] = {'bid': size, 'ask': size}
        """
        total_size = {'GEM': {'bid': 0, 'ask': 0}, 'UB': {'bid': 0, 'ask': 0}, 'ETF': {'bid': 0, 'ask': 0}}

        # First check for each ticker if there is a ask below my bottom estimate or an bid above my top estimate
        # Then I would want to hit the bid or ask respectively
        for ticker in ['GEM', 'UB', 'ETF']:
            if mispricings[ticker]['bid']:
                # There is a mispriced bid, aka a bid above the upper estimate
                upper_estimate = estimates[ticker][1]

                size_available = self.get_size_to_price(ticker, consolidated_contra_book, upper_estimate, 'bid')

                total_size[ticker]['bid'] = size_available
            else:
                # There is no mispriced bid, aka a bid below the upper estimate
                total_size[ticker]['bid'] = 0

            if mispricings[ticker]['ask']:
                # There is a mispriced ask, aka an ask below the lower estimate
                lower_estimate = estimates[ticker][0]

                size_available = self.get_size_to_price(ticker, consolidated_contra_book, lower_estimate, 'ask')

                total_size[ticker]['ask'] = size_available
            else:
                # There is no mispriced ask, aka an ask above the lower estimate
                total_size[ticker]['ask'] = 0

        return total_size


    def hit_to_estimate_orders(self, total_size):
        """
        This function takes an orderbook and systematically creates a list of order to be fired into the market
        
        Args:
            total_size (dict): The total size for each ticker from the get_total_size function.
            It is in the format of {'GEM': {'bid': size, 'ask': size}, 'UB': {'bid': size, 'ask': size}, 'ETF': {'bid': size, 'ask': size}}

        Returns:
            orders (list): A list of orders to be sent to the exchange.            
        """
        orders = []

        for ticker in total_size.keys():
            # Get the size for the bid and ask
            bid_size = total_size[ticker]['bid']
            ask_size = total_size[ticker]['ask']

            if bid_size > 0:
                number_of_orders = int(bid_size // 5000)

                for i in range(number_of_orders):
                    order = {
                        'ticker': ticker,
                        'type': 'MARKET',
                        'quantity': 5000,
                        'action': 'SELL'
                    }
                    orders.append(order)

            if ask_size > 0:
                number_of_orders = int(ask_size // 5000)

                for i in range(number_of_orders):
                    order = {
                        'ticker': ticker,
                        'type': 'MARKET',
                        'quantity': 5000,
                        'action': 'BUY'
                    }
                    orders.append(order)    

        if orders == []:
            return None
        
        return orders

    
    def hit_to_estimates(self, orders):
        """
        This function takes the orders and sends them to the exchange as long as they wouldn't be rejected
        
        Args:
            orders (list): A list of orders to be sent to the exchange.
        """

        if orders is not None:
            for order in orders:
                # First, get my positions so I stay within the position limits
                positions = self.exchange_client.get_positions()
                gross_positions = sum(abs(value) for value in positions.values())  

                ticker = order['ticker']
                action = order['action']
                quantity = order['quantity']

                # Calculate the new gross position if this order is executed
                if action == 'BUY':
                    # Buying increases gross position if already long, decreases if short
                    new_gross_positions = gross_positions + quantity if positions[ticker] >= 0 else gross_positions - abs(quantity)
                elif action == 'SELL':
                    # Selling decreases gross position if already long, increases if short
                    new_gross_positions = gross_positions - quantity if positions[ticker] > 0 else gross_positions + abs(quantity)

                # Check gross position limit
                if new_gross_positions > 100000:
                    continue

                if action == 'BUY':
                    if positions[ticker] + quantity > POSITION_LIMITS[ticker]:
                        continue

                elif action == 'SELL':
                    if positions[ticker] - quantity < -POSITION_LIMITS[ticker]:
                        continue

                self.exchange_client.create_order(order)


    def run(self, consolidated_contra_book, estimates):
        """
        This function runs the hitter logic. It checks for mispricing and hits to the market if there is any.
        
        Args:
            consolidated_contra_book (dict): The orderbook with my quotes removed so I won't self-trade.
            estimates (dict): The estimates for each ticker from the news handler.
        """
        mispricing = self.check_orderbook_mispricing(consolidated_contra_book, estimates)
        total_size = self.get_total_size(consolidated_contra_book, estimates, mispricing)

        orders = self.hit_to_estimate_orders(total_size)
        self.hit_to_estimates(orders)

    def hit_to_market(self):
        """
        This function hits to the market. It is called when there is no new news.
        """
        pass



class Controller:

    """
    This class controls the main logic of the trader and is responsible for managing the quoter and hitter
    It can help switch between the two modes of trading
    """
    def __init__(self, exchange_client, news_handler):
        self.exchange_client = exchange_client
        self.news_handler = news_handler
        self.quoter = Quoter(exchange_client, news_handler)
        self.hitter = Hitter(exchange_client, news_handler)

        self.mode = 'quoter' # 'quoter' or 'hitter'
        self.trading_state = 'active'  # active or inactive


    def turn_off(self):
        """
        Turns off the trader by cancelling all orders and setting the trading state to inactive
        """
        self.exchange_client.cancel_all_orders(all_orders=True)
        self.trading_state = 'inactive'

    def turn_on(self):
        """
        Turns on the trader by setting the trading state to active
        """
        self.trading_state = 'active'

    def switch_mode(self):
        """
        Switches the mode of the trader between quoter and hitter
        """
        if self.mode == 'quoter':
            self.mode = 'hitter'
        else:
            self.mode = 'quoter'

    def get_ratio(self, expected_values):
        """
        This function takes in the expected values and returns the ratio of the positions to take in each market
        """
        # Get the expected values from the news handler
        gem_expected_value = expected_values['GEM']
        ub_expected_value = expected_values['UB']
        etf_expected_value = expected_values['ETF']

        sum = gem_expected_value + ub_expected_value + etf_expected_value

        if sum == 0:
            return {'GEM': 0, 'UB': 0, 'ETF': 0}
        
        ratio = {
            'GEM': round(gem_expected_value / sum, 2),
            'UB': round(ub_expected_value / sum, 2),
            'ETF': round(etf_expected_value / sum, 2)
        }

        return ratio
    
    def remedy_skew(self, position_skew, ratio):  
        """
        This function takes in a skew and returns the necessary change in position to resolve the skew as a dict
        """
        remedy = {'GEM': 0, 'UB': 0, 'ETF': 0}
        
        skew_ratio = {
            'GEM': ratio['GEM'] * position_skew,
            'UB': ratio['UB'] * position_skew,
            'ETF': ratio['ETF'] * position_skew
        }

        # The action to remedy is the opposite of the current skew ratio
        remedy['GEM'] = - round(skew_ratio['GEM'])
        remedy['UB'] = - round(skew_ratio['UB'])
        remedy['ETF'] = round(skew_ratio['ETF'])

        return remedy
    

    def run(self):
        """
        Runs the main logic of the trader. It checks for new news and switches between the quoter and hitter modes.
        """
        trading_state = self.exchange_client.get_status()
        while trading_state:
            # Get estimate intervals and expected values
            estimates = self.news_handler.full_process_news()

            # Check if there is any orderbook mispricing, this gets first priority
            consolidated_contra_book = self.exchange_client.get_consolidated_orderbook(self.exchange_client.get_contra_orderbooks())
        
            self.hitter.run(consolidated_contra_book, estimates)

            self.quoter.calculate_and_send_orders(consolidated_contra_book)

            # time.sleep(.2)
            trading_state = self.exchange_client.get_status()
            if not trading_state:
                print('Trader is inactive')
                break
            



exchange_client = Exchange_Client(API_KEY)
news_handler = News()


controller = Controller(exchange_client, news_handler)
controller.run()

