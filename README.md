# Automated Trading System
This repository contains a Python-based automated trading system built around Hyperliquid’s API. 
It:

Acquires 1-minute candle data and aggregates them into hourly candles.
Generates trade signals based on a simple IBS (Internal Bar Strength) strategy.
Executes trades (limit or market) using the Hyperliquid testnet or mainnet.

## Project Structure

```
automated_trading_system/
├── config/
│   └── .env                 # Environment variables (keys, addresses, etc.)
├── database/
│   └── trading.db           # SQLite database storing candle data and trade signals
├── logs/
│   ├── data_acquisition.log
│   ├── decision_making.log
│   └── trade_execution.log
├── scripts/
│   ├── data_acquisition.py  # Acquires minute candles via WebSocket, aggregates hourly
│   ├── decision_making.py   # Checks new hourly candles, calculates IBS, creates trade signals
│   ├── trade_execution_logic.py  # Executes trades from signals, handles leverage & order placement
│   └── maintenance.py       # (Optional) script to prune old logs and DB records
├── tests/
│   └── ...                  # Tests for data acquisition, decision making, etc.
├── requirements.txt         # Python dependencies
└── README.md                # This readme
```

## Installation & Setup

1. Clone the Repository

```
git clone https://github.com/yourusername/automated_trading_system.git
cd automated_trading_system
```

2. Create a Virtual Environment (Recommended)

```
python3 -m venv venv
source venv/bin/activate
```

3. Install Dependencies

```
pip install --upgrade pip
pip install -r requirements.txt
```

4. Configure Environment Variables
In ```config/.env```, set:

```
ACCOUNT_ADDRESS=0xYourMainAccount
HYPERLIQUID_API_KEY=0xYourAPIWalletAddress
HYPERLIQUID_API_SECRET=0xYourAPIWalletPrivateKey
SQLITE_DB_PATH=./database/trading.db
SYMBOL=BTC
LEVERAGE_BASE=YourLeverageBase
LEVERAGE_EXPONENT=YourLeverageExponent
WS_URL=wss://api.hyperliquid.xyz/ws
```

## Usage

1. Data Acquisition
Run ```data_acquisition.py``` to subscribe to minute candles from Hyperliquid’s WebSocket and aggregate them into hourly candles stored in ```trading.db```:

```
python scripts/data_acquisition.py
```

This script loops indefinitely, inserting new hourly records as time passes.

2. Decision Making
Separately run ```decision_making.py``` to poll the hourly_candles table, compute IBS, and create “open” or “close” trade signals in ```trade_signals```:

```
python scripts/decision_making.py
```
When IBS < 0.2, the script issues a “long” trade signal; after 1 hour, it closes that position.

3. Trade Execution
```decision_making.py``` calls ```execute_pending_signals(...)``` in ```trade_execution_logic.py``` to place or close orders. You can customize: 
Order Type (limit, market, etc.)
Leverage and position sizing logic
Safety buffer to avoid margin rejections
The order and responses are logged in ```logs/trade_execution.log```

### Disclaimer

Educational Purposes Only: The included IBS-based strategy is simplistic and not guaranteed to be profitable.
Use on Testnet first. Be careful with real funds on mainnet.
No Warranty: The maintainers are not responsible for losses or damages from using this code.
