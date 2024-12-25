# scripts/decision_making.py

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

# Import the function to execute signals
from trade_execution_logic import execute_pending_signals

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../config/.env'))

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "../database/trading.db")
LEVERAGE_BASE = float(os.getenv("LEVERAGE_BASE", 5))
LEVERAGE_EXPONENT = float(os.getenv("LEVERAGE_EXPONENT", 7))
SYMBOL = os.getenv("SYMBOL", "BTC")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/decision_making.log", mode='a')
    ]
)

def calculate_ibs(close: float, low: float, high: float) -> float:
    if high == low:
        return 0.5
    ibs = (close - low) / (high - low)
    return max(0.0, min(ibs, 1.0))

def determine_leverage(ibs: float) -> int:
    leverage = LEVERAGE_BASE * (1 - ibs) ** LEVERAGE_EXPONENT
    leverage = min(leverage, LEVERAGE_BASE)
    leverage = max(1, leverage)
    return int(round(leverage))

def format_trade_signal(action: str, timestamp: str, symbol: str, side: str, price: float, leverage: Optional[float]=None) -> dict:
    signal = {
        "action": action,
        "timestamp": timestamp,
        "symbol": symbol,
        "side": side,
        "price": price
    }
    if action == "open" and leverage is not None:
        signal["leverage"] = leverage
    return signal

def has_active_trade(db_path: str) -> bool:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT COUNT(*) FROM trade_signals
        WHERE action = 'open' AND executed = 0
    ''')
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

def initialize_database(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS trade_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            action TEXT,
            symbol TEXT,
            side TEXT,
            price REAL,
            leverage REAL,
            executed INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS hourly_candles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT UNIQUE,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL
        )
    ''')
    conn.commit()
    conn.close()
    logging.info(f"Initialized SQLite database at {db_path}.")

def get_latest_hourly_candle(db_path: str, last_processed_id: Optional[int]) -> Optional[dict]:
    """
    Fetch the next unprocessed candle from hourly_candles.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    if last_processed_id:
        cursor.execute('''
            SELECT id, timestamp, open, high, low, close, volume
            FROM hourly_candles
            WHERE id > ?
            ORDER BY id ASC
            LIMIT 1
        ''', (last_processed_id,))
    else:
        cursor.execute('''
            SELECT id, timestamp, open, high, low, close, volume
            FROM hourly_candles
            ORDER BY id ASC
            LIMIT 1
        ''')

    row = cursor.fetchone()
    conn.close()
    if row:
        return {
            'id': row[0],
            'timestamp': row[1],
            'open': row[2],
            'high': row[3],
            'low': row[4],
            'close': row[5],
            'volume': row[6]
        }
    return None

def insert_trade_signal(db_path: str, signal: dict):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO trade_signals (timestamp, action, symbol, side, price, leverage)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', (
        signal['timestamp'],
        signal['action'],
        signal['symbol'],
        signal['side'],
        signal['price'],
        signal.get('leverage', 1.0)
    ))
    conn.commit()
    conn.close()
    logging.info(f"Inserted trade signal: {signal}")

def mark_open_trade_executed(db_path: str, symbol: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE trade_signals
        SET executed = 1
        WHERE action = 'open' AND symbol = ? AND executed = 0
    ''', (symbol,))
    conn.commit()
    conn.close()
    logging.info(f"Marked corresponding open trade for {symbol} as executed.")

class TradingLogic:
    def __init__(self):
        self.trade_active = False
        self.entry_price = 0.0
        self.leverage = 1.0
        self.entry_time: Optional[datetime] = None
        self.last_processed_id: Optional[int] = None

    async def process_candle(self, candle: dict) -> None:
        try:
            timestamp_str = candle.get('timestamp')
            if not timestamp_str:
                logging.warning("Candle missing 'timestamp' field.")
                return

            timestamp = datetime.fromisoformat(timestamp_str)
            open_price = float(candle.get('open', 0.0))
            high_price = float(candle.get('high', 0.0))
            low_price = float(candle.get('low', 0.0))
            close_price = float(candle.get('close', 0.0))

            if high_price < low_price:
                logging.warning(f"Invalid candle data: high ({high_price}) < low ({low_price}).")
                return

            # Calculate IBS & log
            ibs = calculate_ibs(close_price, low_price, high_price)
            logging.info(f"Candle at {timestamp_str} => IBS={ibs:.4f}")

            # If we currently have no open trade
            if not self.trade_active:
                logging.info(f"Using database at {SQLITE_DB_PATH}")

                # If there's already an active open trade, skip
                if has_active_trade(SQLITE_DB_PATH):
                    logging.info("Active trade found in DB, skipping new open.")
                    return

                if ibs < 0.2:
                    self.leverage = determine_leverage(ibs)
                    self.entry_price = close_price
                    self.entry_time = timestamp
                    side = "long"

                    trade_signal = format_trade_signal(
                        action="open",
                        timestamp=timestamp_str,
                        symbol=SYMBOL,
                        side=side,
                        price=self.entry_price,
                        leverage=self.leverage
                    )
                    insert_trade_signal(SQLITE_DB_PATH, trade_signal)
                    logging.info(f"Opened trade: {trade_signal}")

                    # Immediately attempt to execute the new signal
                    execute_pending_signals(SQLITE_DB_PATH)
                    self.trade_active = True

            else:
                # If we've had a trade open for >= 1 hr, close
                time_elapsed = timestamp - self.entry_time
                if time_elapsed >= timedelta(hours=1):
                    side = "long"
                    trade_close_signal = format_trade_signal(
                        action="close",
                        timestamp=timestamp_str,
                        symbol=SYMBOL,
                        side=side,
                        price=close_price
                    )
                    insert_trade_signal(SQLITE_DB_PATH, trade_close_signal)
                    mark_open_trade_executed(SQLITE_DB_PATH, SYMBOL)
                    logging.info(f"Closed trade: {trade_close_signal}")

                    execute_pending_signals(SQLITE_DB_PATH)
                    self.trade_active = False
                    self.entry_price = 0.0
                    self.leverage = 1.0
                    self.entry_time = None

        except Exception as e:
            logging.error(f"Error processing candle: {e}")

async def decision_making_loop(trading_logic: TradingLogic):
    while True:
        candle = get_latest_hourly_candle(SQLITE_DB_PATH, trading_logic.last_processed_id)
        if candle:
            await trading_logic.process_candle(candle)
            trading_logic.last_processed_id = candle['id']
        await asyncio.sleep(10)

if __name__ == "__main__":
    try:
        initialize_database(SQLITE_DB_PATH)
        trading_logic = TradingLogic()
        asyncio.run(decision_making_loop(trading_logic))
    except KeyboardInterrupt:
        logging.info("Decision making stopped manually.")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")