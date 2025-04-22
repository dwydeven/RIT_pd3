import requests
import time
import asyncio
import aiohttp
import datetime

with open('api_key.txt', 'r') as f:
    API_KEY = f.read().strip()


class RIT_Client:
    """
    A class to interact with the STYNCLLC API.
    """
    def __init__(self, api_key: str, port: int = 10001, base_url: str = 'http://localhost'):
        self.api_key = api_key
        self.port = port
        self.base_url = base_url
        self.host = f'{base_url}:{port}/v1'
        
        # Create a session with the API key
        self.session = requests.Session()
        self.session.headers.update({'X-API-key': api_key})

        # Rate limiter value
        self.last_request_time = datetime.datetime.now()

    def rate_limit(self):
        """
        Each security has a rate limit of 100 requests per second
        This rate limiter is lazy for now, and will always implement the rate limit of 100 requests per second
        """
        THRESHOLD = 100  # requests per second

        now = datetime.datetime.now()

        if now - self.last_request_time < datetime.timedelta(seconds= (1 / THRESHOLD)):
            # If the time since the last request is less than the threshold, wait
            wait_time = (1 / THRESHOLD) - (now - self.last_request_time).total_seconds()
            if wait_time > 0:
                time.sleep(wait_time)
        
        # Update the last request time
        self.last_request_time = datetime.datetime.now()

    def get(self, endpoint: str, params: dict = None):
        """
        Send a GET request to the API.

        Args:
            endpoint (str): The API endpoint to call (e.g., '/case').
            params (dict, optional): Query parameters to include in the request.

        Returns:
            dict: The JSON response from the API.
        """
        self.rate_limit()  # Call the rate limiter before making the request

        url = f'{self.host}{endpoint}'
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"GET request failed: {e}")
            return None

    def post(self, endpoint: str, data: dict = None):
        """
        Send a POST request to the API.

        Args:
            endpoint (str): The API endpoint to call (e.g., '/orders').
            data (dict, optional): The JSON payload to include in the request.

        Returns:
            dict: The JSON response from the API.
        """
        self.rate_limit()  # Call the rate limiter before making the request

        url = f'{self.host}{endpoint}'
        try:
            response = self.session.post(url, json=data)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"POST request failed: {e}")
            return None

    def delete(self, endpoint: str):
        """
        Send a DELETE request to the API.

        Args:
            endpoint (str): The API endpoint to call (e.g., '/orders/{order_id}').

        Returns:
            dict: The JSON response from the API.
        """
        self.rate_limit()  # Call the rate limiter before making the request

        url = f'{self.host}{endpoint}'
        try:
            response = self.session.delete(url)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"DELETE request failed: {e}")
            return None
        
    def query_generation(self, params: dict) -> str:
        """
        Generate a query string from a dictionary of parameters.

        Args:
            params (dict): A dictionary of query parameters.

        Returns:
            str: A query string (e.g., '?key1=value1&key2=value2').
        """
        if not params:
            return ''
        
        # Filter out parameters with None values
        relevant_params = {k: v for k, v in params.items() if v is not None}
        
        # Construct the query string
        query = '&'.join(f'{k}={v}' for k, v in relevant_params.items())
        return f'?{query}' if query else ''

class Exchange_Client(RIT_Client):
    """
    A class to interact with the STYNCLLC API for exchange operations.
    """
    def __init__(self, api_key: str, port: int = 10001, base_url: str = 'http://localhost'):
        super().__init__(api_key, port, base_url)
        self.api_key = api_key
        self.port = port

        self.case_url = '/case'
        self.news_url = '/news'
        self.securities_url = '/securities'
        self.orders_url = '/orders'

        self.tickers = ['GEM', 'UB', 'ETF']


    # CASE ENDPOINT
    # Get tick from the json response of /case: from ['tick'] key of the json response
    # Get trading status from the json response of /case: from the ['status'] key of the json response
    def get_tick(self):
        response = self.get(self.case_url)
        if response:
            return response.get('tick')
        return None
    
    def get_status(self):
        response = self.get(self.case_url)
        if response:
            status = response.get('status')
            if status == 'ACTIVE':
                return True
        return None

    # NEWS ENDPOINT
    # Some sort of parsing to get the respective data
    # What's important to get is the tick (which is it's own key), the ticker (which is in the headline), and the estimate (which is in the body)
    def get_news(self):
        response = self.get(self.news_url)
        if response:
            return response.get('news')
        return None

    # SECURITIES ENDPOINT
    # Uses the /orderbook endpoint to get the order book for a given security
    async def fetch_orderbook(self, session, ticker: str, limit: int = 200):
        """
        Fetch the orderbook for a given security asynchronously.

        Args:
            session (aiohttp.ClientSession): The aiohttp session to use.
            ticker (str): The ticker symbol of the security.
            limit (int): The maximum number of orders to fetch.

        Returns:
            dict: The orderbook data for the given security.
        """
        endpoint = f'{self.securities_url}/book'
        params = {'ticker': ticker, 'limit': limit}
        query_string = self.query_generation(params)
        url = f'{self.host}{endpoint}{query_string}'

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"Failed to fetch orderbook for {ticker}: {e}")
            return None

    async def get_orderbooks_async(self, tickers: list, limit: int = 200):
        """
        Fetch the orderbooks for multiple securities asynchronously.

        Args:
            tickers (list): A list of ticker symbols.
            limit (int): The maximum number of orders to fetch.

        Returns:
            dict: A dictionary containing orderbook data for each security.
        """
        async with aiohttp.ClientSession(headers={'X-API-key': self.api_key}) as session:
            tasks = [self.fetch_orderbook(session, ticker, limit) for ticker in tickers]
            results = await asyncio.gather(*tasks)
            return {tickers[i]: results[i] for i in range(len(tickers))}

    def get_orderbooks(self, limit: int = 200):
        """
        Fetch the orderbooks for all securities using asynchronous requests.

        Args:
            limit (int): The maximum number of orders to fetch.

        Returns:
            dict: A dictionary containing orderbook data for each security.
        """
        return asyncio.run(self.get_orderbooks_async(self.tickers, limit))
    
    def get_contra_orderbooks(self, trader_id: str = 'user15'):
        """
        Gets the regular orderbook and filters out all the orders that aren't mine according to the trader id.
        The trader id comes in user## format
        
        Args:
            trader_id (str): The user id of this trader to filter out of the orderbook
            
        Returns:
            dict: The filtered orderbook data for the given security.
        """
        orderbook = self.get_orderbooks()
        filtered_orderbook = {'GEM': {'bids': [], 'asks': []}, 'UB': {'bids': [], 'asks': []}, 'ETF': {'bids': [], 'asks': []}}
        
        for ticker, data in orderbook.items():
            bids = data.get('bids', [])
            asks = data.get('asks', [])
            
            # Filter bids and asks based on trader_id
            filtered_bids = [bid for bid in bids if bid.get('trader_id') != trader_id] if trader_id else bids
            filtered_asks = [ask for ask in asks if ask.get('trader_id') != trader_id] if trader_id else asks
            
            filtered_orderbook[ticker]['bids'] = filtered_bids
            filtered_orderbook[ticker]['asks'] = filtered_asks

        return filtered_orderbook
    
    def get_consolidated_orderbook(self, orderbook):
        """
        This function takes an orderbook and turns it into price, size notation only
        
        Args:
            orderbook (dict): The orderbook data for a given security.
            
        Returns:
            dict: A dictionary containing the consolidated orderbook data.
        """
        consolidated_orderbook = {}

        for ticker, data in orderbook.items():
            bids = data.get('bids', [])
            asks = data.get('asks', [])

            # Consolidate bids
            consolidated_bids = {}
            for bid in bids:
                price = bid['price']
                quantity = bid['quantity'] - bid.get('quantity_filled', 0)
                if price in consolidated_bids:
                    consolidated_bids[price] += quantity
                else:
                    consolidated_bids[price] = quantity

            # Consolidate asks
            consolidated_asks = {}
            for ask in asks:
                price = ask['price']
                quantity = ask['quantity'] - ask.get('quantity_filled', 0)
                if price in consolidated_asks:
                    consolidated_asks[price] += quantity
                else:
                    consolidated_asks[price] = quantity

            # Convert consolidated bids and asks back to list format
            consolidated_orderbook[ticker] = {
                'bids': [[price, quantity] for price, quantity in sorted(consolidated_bids.items(), reverse=True)],
                'asks': [[price, quantity] for price, quantity in sorted(consolidated_asks.items())]
            }

        return consolidated_orderbook
    
    def get_nbbo_book(self, orderbook):
        """
        Get the NBBO (National Best Bid and Offer) from the orderbook.

        Args:
            orderbook (dict): The orderbook data for a given security.

        Returns:
            dict: A dictionary containing the best bid and ask prices and sizes.
        """
        nbbo = {}

        for ticker, data in orderbook.items():
            # Best is always first elemetn of the inner list
            best_bid = data.get('bids')[0] if data.get('bids') else None
            best_ask = data.get('asks')[0] if data.get('asks') else None

            nbbo[ticker] = {
                'bids': [best_bid.get('price'), best_bid.get('quantity') - best_bid.get('quantity_filled')] if best_bid else None,
                'asks': [best_ask.get('price'), best_ask.get('quantity') - best_ask.get('quantity_filled')] if best_ask else None
            }

        return nbbo

    def get_positions(self):
        """
        Fetch the position for a given security.

        Args:
            ticker (str): The ticker symbol of the security.

        Returns:
            dict: The position data for the given security.
        """
        positions = {}

        endpoint = self.securities_url

        # Make the GET request to fetch the list of securitiy information
        response = self.get(endpoint)
        for dictr in response:
            ticker = dictr.get('ticker')
            position = dictr.get('position', 0)
            if ticker in self.tickers:
                positions[ticker] = position

        return positions
    

    def get_positions_skew(self, positions):
        """
        Fetch the position skew for a given security.

        Args:
            None:

        Returns:
            int: The (gem position + ub position) - etf position
        """
        gem_position = positions.get('GEM', 0)
        ub_position = positions.get('UB', 0)
        etf_position = positions.get('ETF', 0)

        return (gem_position + ub_position) - etf_position
    
    def get_gross_position(self, positions):
        """
        Fetch the gross position for a given security.

        Args:
            positions (dict): A dictionary containing the positions for each security.

        Returns:
            int: The gross position, sum(abs(position)) for all securities.
        """
        gross_position = 0
        for position in positions.values():
            gross_position += abs(position)
        return gross_position

    # ORDERS ENDPOINT
    # Has the get, post, and delete methods uses a get_orders, create_orders, and delete method to get, create, and delete orders respectively
    # The post get and delete methods require and order id to be passed in as a parameter, it's passed as another endpoint
    def get_orders(self):
        """
        Fetch all the resting orders for the trader

        Returns:
            list: List of dictionaries containing order information.
        """
        endpoint = self.orders_url
        return self.get(endpoint)
    
    def get_order(self, order_id):
        """
        Fetch a specific order by its ID.

        Args:
            order_id (str): The ID of the order to fetch.

        Returns:
            dict: The order data for the given ID.
        """
        endpoint = f'{self.orders_url}/{order_id}'
        return self.get(endpoint)

    def get_quotes(self):
        """
        Fetch all the quotes for the trader

        Returns:
            dict: a ticker keyed dictionary with ['bids']: [price, size], ['asks']: [price, size] notation
        """
        quotes = {'GEM': {'bids': [], 'asks': []}, 'UB': {'bids': [], 'asks': []}, 'ETF': {'bids': [], 'asks': []}}
        
        orders = self.get_orders()
        
        for order in orders:
            ticker = order.get('ticker')
            price = order.get('price')
            size = order.get('quantity') - order.get('quantity_filled', 0)
            
            # Determine if it's a bid or ask and add to the respective list
            if order.get('action') == 'BUY':
                order_id = order.get('order_id')
                quotes[ticker]['bids'].append({'price': price, 'size': size, 'order_id': order_id})
            elif order.get('action') == 'SELL':
                order_id = order.get('order_id')
                quotes[ticker]['asks'].append({'price': price, 'size': size, 'order_id': order_id})

        return quotes
    
    def create_order(self, params: dict):
        """
        Create a new order.

        Args:
            params (dict): A dictionary containing the order parameters.
            All inserted as a query string in the url
            ticker, type, quantity, action are required
            price is required if type is 'LIMIT'


        Returns:
            dict: The response data from the create operation.
        """
        endpoint = self.orders_url
        query_string = self.query_generation(params)
        url = f'{self.host}{endpoint}{query_string}'
        # Make the POST request to create the order
        try:
            response = self.session.post(url, json=params)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"POST request failed: {e}")
            return None
    
    def cancel_order(self, order_id):
        """
        Delete a specific order by its ID.

        Args:
            order_id (str): The ID of the order to delete.

        Returns:
            dict: The response data from the delete operation.
        """
        endpoint = f'{self.orders_url}/{order_id}'
        return self.delete(endpoint)
    
    def cancel_all_orders(self, all_orders: bool = False, ticker: str = None, direction: str = None, order_ids: list = None):
        """
        Cancel open orders based on the specified parameters.

        Args:
            all_orders (bool, optional): Set to True to cancel all open orders. Defaults to False.
            ticker (str, optional): Cancel all open orders for a specific security. Defaults to None.
            direction (str, optional): Specify 'buy' or 'sell' to cancel only buy or sell orders for the given ticker. Defaults to None.

        The documentation allows for more specific query paramaters but I only included the ones relevant for this program

        Returns:
            dict: The response data from the cancel operation.
        """
        # Validate input
        if all_orders and (ticker or direction) and (order_ids is None):
            raise ValueError("Cannot specify 'all_orders' with 'ticker' or 'direction' or 'order_id.")
        if direction and direction.lower() not in ['buy', 'sell']:
            raise ValueError("Invalid direction. Must be 'buy' or 'sell'.")

        # Construct query parameters
        if all_orders:
            params = {'all': 1}
        elif ticker:
            if direction:
                # Construct query for ticker and direction
                volume = '<0' if direction.lower() == 'sell' else '>0'
                query = f"Ticker='{ticker}' AND Volume{volume}"
                params = {'query': query}
            else:
                # Cancel all orders for the ticker
                params = {'ticker': ticker}
        elif order_ids: 
            # Cancel specific orders by ID
            params = {'ids': ','.join(str(order_id) for order_id in order_ids)}
        else:
            raise ValueError("Must specify either 'all_orders', 'ticker', or 'order_ids'.")
        
        # Generate the query string
        query_string = self.query_generation(params)

        # Construct the endpoint
        endpoint = f'/commands/cancel{query_string}'

        # Send the POST request
        return self.post(endpoint)
    
    def get_fills(self):
        """
        Fetch all the fills for the trader

        Returns:
            list: List of dictionaries containing fill information.
        """
        query_string = self.query_generation({'status': 'TRANSACTED'})
        endpoint = f'{self.orders_url}{query_string}'
        return self.get(endpoint)
    


exchange_client = Exchange_Client(API_KEY)

base_url = 'http://localhost:10001/v1'
mock_endpoint = '/securities'
mock_url = 'http://localhost:10001/v1/securities/book/UB'
response = requests.get(base_url + mock_endpoint, headers={'X-API-key': API_KEY})
#print(response.json())
# print(exchange_client.get_consolidated_orderbook(exchange_client.get_contra_orderbooks()))
