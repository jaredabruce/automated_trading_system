# scripts/data_acquisition.py

import asyncio
import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../config/.env'))

WS_URL = os.getenv("WS_URL", "wss://api.hyperliquid.xyz/ws")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "./database/trading.db")

SUBSCRIPTION_MESSAGE = {
    "method": "subscribe",
    "subscription": {
        "type": "candle",
        "coin": "BTC",
        "interval": "1m"
    }
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/data_acquisition.log", mode='a')
    ]
)

def initialize_database(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
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

def insert_hourly_candle(db_path: str, candle: dict):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO hourly_candles (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            candle['timestamp'],
            candle['open'],
            candle['high'],
            candle['low'],
            candle['close'],
            candle['volume']
        ))
        conn.commit()
        logging.info(f"Inserted hourly candle at {candle['timestamp']}.")
    except sqlite3.IntegrityError:
        logging.warning(f"Hourly candle at {candle['timestamp']} already exists. Skipping insertion.")
    except Exception as e:
        logging.error(f"Failed to insert hourly candle: {e}")
    finally:
        conn.close()

def initialize_hourly_candle(timestamp: datetime, price: float) -> dict:
    candle_start_time = timestamp.replace(minute=0, second=0, microsecond=0)
    candle_end_time = candle_start_time + timedelta(hours=1)
    return {
        'timestamp': candle_end_time.isoformat(),
        'open': price,
        'high': price,
        'low': price,
        'close': price,
        'volume': 0.0
    }

def update_hourly_candle(hourly_candle: dict, minute_candle: dict) -> dict:
    hourly_candle['high'] = max(hourly_candle['high'], minute_candle['high'])
    hourly_candle['low'] = min(hourly_candle['low'], minute_candle['low'])
    hourly_candle['close'] = minute_candle['close']
    hourly_candle['volume'] += minute_candle['volume']
    return hourly_candle

async def subscribe_candles(websocket):
    await websocket.send(json.dumps(SUBSCRIPTION_MESSAGE))
    logging.info("Subscribed to BTC 1-minute candles.")

async def receive_and_aggregate_candles():
    initialize_database(SQLITE_DB_PATH)
    current_hourly_candle: Optional[dict] = None

    import websockets
    while True:
        try:
            async with websockets.connect(WS_URL) as websocket:
                await subscribe_candles(websocket)
                logging.info("Waiting for minute candle data...")

                async for message in websocket:
                    data = json.loads(message)
                    if data.get("channel") == "subscriptionResponse":
                        subscription = data.get("data", {})
                        logging.info(f"Subscription Response: {subscription}")

                    elif data.get("channel") == "candle":
                        candle = data.get("data")
                        if not candle:
                            logging.warning("Received candle data without 'data' field.")
                            continue

                        try:
                            minute_close_time = datetime.utcfromtimestamp(candle['T'] / 1000)
                            open_price = float(candle['o'])
                            high_price = float(candle['h'])
                            low_price = float(candle['l'])
                            close_price = float(candle['c'])
                            volume = float(candle['v'])
                        except (KeyError, ValueError, TypeError) as e:
                            logging.error(f"Invalid candle data format: {e}")
                            continue

                        minute_candle = {
                            'timestamp': minute_close_time,
                            'open': open_price,
                            'high': high_price,
                            'low': low_price,
                            'close': close_price,
                            'volume': volume
                        }

                        if current_hourly_candle is None:
                            current_hourly_candle = initialize_hourly_candle(minute_close_time, open_price)
                            continue

                        if minute_close_time < datetime.fromisoformat(current_hourly_candle['timestamp']):
                            current_hourly_candle = update_hourly_candle(current_hourly_candle, minute_candle)
                        else:
                            insert_hourly_candle(SQLITE_DB_PATH, current_hourly_candle)
                            current_hourly_candle = initialize_hourly_candle(minute_close_time, open_price)

        except websockets.ConnectionClosed as e:
            logging.warning(f"WebSocket connection closed: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f"An error occurred: {e}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(receive_and_aggregate_candles())
    except KeyboardInterrupt:
        logging.info("Data acquisition stopped manually.")
