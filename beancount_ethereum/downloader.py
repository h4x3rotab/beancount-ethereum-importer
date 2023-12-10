import json
import os
import datetime
import time
import requests
from decimal import Decimal
from requests.auth import HTTPBasicAuth
from requests.exceptions import HTTPError

DEFAULT_CURRENCY = 'ETH'
MINER = '0xffffffffffffffffffffffffffffffffffffffff'
WEI = 10 ** 18
NO_TRANSACTIONS = [
    'No transactions found',
    'No internal transactions found',
    'No token transfers found',
]


class BlockExplorerApi:

    def __init__(self, api_url: str, api_key: str, delay: float = 0.0, base_currency: str = DEFAULT_CURRENCY):
        self.api_url = api_url
        self.api_key = api_key
        self.delay = delay
        self._last_request_timestamp = 0.0
        self.base_currency = base_currency
        self.session = requests.Session()  # Using a session for connection pooling
        self.session.headers.update({'User-Agent': 'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)'})

    def _make_api_request(self, address: str, action: str) -> list:
        """
        Load data from block explorer API
        """
        last_request_delta = time.time() - self._last_request_timestamp
        if last_request_delta < self.delay:
            time.sleep(self.delay - last_request_delta)
        params = {
            'module': 'account',
            'action': action,
            'address': address,
            'sort': 'asc',
            'apikey': self.api_key  # Assuming that the API key is always provided
        }
        try:
            response = self.session.get(self.api_url, params=params)
            response.raise_for_status()  # Raise an HTTPError if the HTTP request returned an unsuccessful status code
            data = response.json()
            self._last_request_timestamp = time.time()
            if int(data['status']) == 1 or data['message'] in NO_TRANSACTIONS:
                return data['result']
            else:
                raise RuntimeError(f"API Error: {data['message']}")
        except HTTPError as http_err:
            raise RuntimeError(f"HTTP error occurred: {http_err}")  # HTTP error
        except Exception as err:
            raise RuntimeError(f"An error occurred: {err}")  # Other errors

    def get_normal_transactions(self, address: str) -> list:
        transactions = []
        for item in self._make_api_request(address, 'txlist'):
            if int(item['isError']) == 0:
                transaction = {
                    'tx_id': item['hash'],
                    'time': int(item['timeStamp']),
                    'from': item['from'],
                    'to': item['to'],
                    'currency': self.base_currency,
                    'value': Decimal(item['value']) / WEI,
                }
                transactions.append(transaction)
            if item['from'].lower() == address.lower():
                transaction_fee = {
                    'tx_id': item['hash'],
                    'time': int(item['timeStamp']),
                    'from': item['from'],
                    'to': MINER,
                    'currency': self.base_currency,
                    'value': (Decimal(item['gasUsed']) *
                              Decimal(item['gasPrice']) /
                              WEI),
                }
                transactions.append(transaction_fee)
        return transactions

    def get_internal_transactions(self, address: str) -> list:
        transactions = []
        for item in self._make_api_request(address, 'txlistinternal'):
            transaction = {
                # Blockscout uses 'transactionHash' instead of 'hash'
                'tx_id': (item['hash'] if 'hash' in item
                          else item['transactionHash']),
                'time': int(item['timeStamp']),
                'from': item['from'],
                'to': item['to'],
                'currency': self.base_currency,
                'value': Decimal(item['value']) / WEI,
            }
            transactions.append(transaction)
        return transactions

    def get_erc20_transfers(self, address: str) -> list:
        transactions = []
        for item in self._make_api_request(address, 'tokentx'):
            if item['tokenDecimal'] == '':
                # Skip NFTs (Blockscout)
                continue
            transaction = {
                'tx_id': item['hash'],
                'time': int(item['timeStamp']),
                'from': item['from'],
                'to': item['to'],
                'currency': item['tokenSymbol'],
                'value': (Decimal(item['value']) /
                          10 ** Decimal(item['tokenDecimal'])),
                'contract': item['contractAddress'],
            }
            transactions.append(transaction)
        return transactions

    def get_base_currency_balances(self, address: str) -> list:
        balances = []
        balance = {
            'time': int(datetime.datetime.now().timestamp()),
            'address': address.lower(),
            'currency': self.base_currency,
            'balance': Decimal(self._make_api_request(address, 'balance')) / WEI,
        }
        balances.append(balance)
        return balances

class WalletApi:
    def __init__(self, network: str, api_key: str) -> None:
        self.api_url = f'https://api.covalenthq.com/v1/{network}/address/{}/balances_v2/?no-spam=true'
        self.auth = HTTPBasicAuth(api_key, '')

    def get_asset_balances(self, address: str) -> list:
        balances = []
        url = self.api_url.format(address)
        try:
            response = requests.get(url, headers={'Content-Type': 'application/json'}, auth=self.auth)
            response.raise_for_status()
            r = response.json()
            if r['error']:
                raise RuntimeError(f"API Error: {r['error_message']}")
            data = r['data']
        except HTTPError as http_err:
            raise RuntimeError(f"HTTP error occurred: {http_err}")  # HTTP error
        except Exception as err:
            raise RuntimeError(f"An error occurred: {err}")  # Other errors

        time_str_trimmed = data['updated_at'][:23]
        dt = datetime.datetime.strptime(time_str_trimmed, "%Y-%m-%dT%H:%M:%S.%f")
        dt = dt.replace(tzinfo=datetime.timezone.utc)
        timestamp = int(dt.timestamp())
        valid_assets = [
            i for i in data['items']
            if isinstance(i['quote'], (int, float)) and i['quote'] > 0]
        for i in valid_assets:
            balances.append({
                'time': timestamp,
                'address': address.lower(),
                'currency': i['contract_ticker_symbol'],
                'contract': i['contract_address'],
                'balance': Decimal(i['balance']) / (10 ** (i['contract_decimals'])),
                'rate': i['quote_rate'],
            })
        return balances

def download(config: dict, output_dir: str):
    name = config['name']
    addresses = config['account_map'].keys()
    api = BlockExplorerApi(
        config['block_explorer_api_url'],
        config['block_explorer_api_key'],
        config.get('block_explorer_api_request_delay', 0.0),
        config.get('base_currency', DEFAULT_CURRENCY),
    )
    wallet_api = WalletApi(config['covalent_network'], config['covalent_api_key'])
    transactions = []
    balances = []
    for address in addresses:
        transactions += api.get_normal_transactions(address)
        transactions += api.get_internal_transactions(address)
        transactions += api.get_erc20_transfers(address)
        balances += wallet_api.get_asset_balances(address)
    os.makedirs(output_dir, exist_ok=True)
    output_file_path = os.path.join(output_dir, f'{name}.json')
    with open(output_file_path, 'w') as output_file:
        json.dump(transactions, output_file, indent=4, default=str)
    print(f'Transactions saved to {output_file_path}')

    output_file_path = os.path.join(output_dir, f'{name}-balances.json')
    with open(output_file_path, 'w') as output_file:
        json.dump(balances, output_file, indent=4, default=str)
    print(f'Balances saved to {output_file_path}')
