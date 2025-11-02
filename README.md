# Backpack Exchange Python SDK

A comprehensive Python SDK for the Backpack Exchange API, providing both REST and WebSocket functionality for trading and market data access.

## Features

- 🚀 **Complete API Coverage**: Full support for all Backpack Exchange API endpoints
- 🔐 **Secure Authentication**: ED25519 signature-based authentication
- 📡 **Real-time Data**: WebSocket support for live market data
- ⚡ **Async Support**: Both synchronous and asynchronous operations
- 🛡️ **Type Safety**: Full type hints and Pydantic models
- 📊 **Market Data**: Order books, tickers, trades, and more
- 💼 **Trading**: Order management, positions, and account operations

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

### Public API (No Authentication Required)

```python
from backpack import ApiClient, Configuration
from backpack.api.markets_api import MarketsApi

# Create configuration
config = Configuration(host="https://api.backpack.exchange")
api_client = ApiClient(config)

# Get markets
markets_api = MarketsApi(api_client)
markets = markets_api.get_markets()
print(f"Found {len(markets)} markets")

# Get market info
market_info = markets_api.get_market("SOL_USDC")
print(f"SOL_USDC info: {market_info}")

# Get order book
depth = markets_api.get_depth("SOL_USDC", limit=10)
print(f"Order book: {depth}")
```

### Authenticated API

```python
from backpack import SignerClient

# Create signer client with your API credentials
signer_client = SignerClient(
    api_key="your_api_key",
    api_secret="your_api_secret"
)

# Get account information
account = signer_client.get_account()
print(f"Account: {account}")

# Get balances
balances = signer_client.get_balances()
print(f"Balances: {balances}")

# Create an order
order = signer_client.create_order(
    symbol="SOL_USDC",
    side="Bid",
    order_type="Limit",
    quantity="1.0",
    price="20.50"
)
print(f"Order created: {order}")
```

### WebSocket Real-time Data

```python
from backpack import WsClient

def on_message(data):
    print(f"Received: {data}")

def on_error(error):
    print(f"Error: {error}")

# Create WebSocket client
ws_client = WsClient(
    on_message=on_message,
    on_error=on_error
)

# Connect and subscribe
if ws_client.connect():
    # Subscribe to order book updates
    ws_client.subscribe("depth.SOL_USDC")
    
    # Subscribe to ticker updates
    ws_client.subscribe("ticker.SOL_USDC")
    
    # Run the client
    ws_client.run()
```

### Async Operations

```python
import asyncio
from backpack import ApiClient, Configuration
from backpack.api.markets_api import MarketsApi

async def main():
    config = Configuration(host="https://api.backpack.exchange")
    api_client = ApiClient(config)
    markets_api = MarketsApi(api_client)
    
    # Get markets asynchronously
    markets = await markets_api.get_markets(async_req=True)
    print(f"Found {len(markets)} markets")
    
    await api_client.close()

# Run async function
asyncio.run(main())
```

## API Modules

### Public APIs
- **MarketsApi**: Market data, order books, trades
- **AssetsApi**: Asset information
- **SystemApi**: System status
- **TradesApi**: Public trade data

### Authenticated APIs
- **AccountApi**: Account information and balances
- **OrderApi**: Order management
- **PositionApi**: Position management
- **CapitalApi**: Capital operations
- **HistoryApi**: Historical data
- **FundingApi**: Funding rates
- **BorrowLendApi**: Borrowing and lending

## WebSocket Streams

### Public Streams
- `ticker.<symbol>`: 24hr ticker statistics
- `depth.<symbol>`: Order book depth updates
- `bookTicker.<symbol>`: Best bid/ask prices
- `trade.<symbol>`: Public trade data
- `kline.<interval>.<symbol>`: K-line/candlestick data

### Private Streams (Requires Authentication)
- `account.orderUpdate`: Order updates
- `account.positionUpdate`: Position updates
- `account.rfqUpdate`: RFQ updates

## Examples

### Price Monitor
```python
python examples/backpack_price_monitor.py
```

### Basic API Usage
```python
python examples/backpack_example.py
```

### Async Price Monitor
```python
python examples/backpack_price_monitor.py async
```

## Configuration

### Environment Variables
```bash
export BACKPACK_API_KEY="your_api_key"
export BACKPACK_API_SECRET="your_api_secret"
export BACKPACK_HOST="https://api.backpack.exchange"
```

### Configuration Object
```python
from backpack import Configuration

config = Configuration(
    host="https://api.backpack.exchange",
    api_key="your_api_key",
    api_secret="your_api_secret",
    timeout=30,
    retry_count=3
)
```

## Authentication

The Backpack Exchange API uses ED25519 signature-based authentication:

1. **API Key**: Your public key (base64 encoded)
2. **API Secret**: Your private key for signing requests
3. **Signing**: All authenticated requests must be signed with ED25519

### Request Signing Process

1. Build the signing string with instruction, parameters, timestamp, and window
2. Sign the string with your private key using ED25519
3. Include the signature in the `X-Signature` header

## Error Handling

```python
from backpack.exceptions import ApiException, AuthenticationError

try:
    account = signer_client.get_account()
except AuthenticationError as e:
    print(f"Authentication failed: {e}")
except ApiException as e:
    print(f"API error: {e}")
```

## Rate Limits

The API has rate limits that vary by endpoint:
- Public endpoints: 120 requests per minute
- Authenticated endpoints: 60 requests per minute
- WebSocket connections: 1 connection per API key

## WebSocket Connection Management

```python
# Automatic reconnection
ws_client = WsClient(
    on_message=handle_message,
    on_error=handle_error,
    on_close=handle_close
)

# Manual connection management
if ws_client.connect():
    ws_client.subscribe("ticker.SOL_USDC")
    ws_client.run()
```

## Development

### Running Tests
```bash
pytest tests/
```

### Code Formatting
```bash
black backpack/
flake8 backpack/
```

### Type Checking
```bash
mypy backpack/
```

## License

This project is licensed under the MIT License.

## Support

For issues and questions:
- GitHub Issues: [Create an issue](https://github.com/your-repo/backpack-python-sdk/issues)
- Documentation: [Backpack Exchange API Docs](https://docs.backpack.exchange)

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Changelog

### v1.0.0
- Initial release
- Full API coverage
- WebSocket support
- Async operations
- Type safety with Pydantic