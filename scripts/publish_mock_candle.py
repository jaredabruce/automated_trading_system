# scripts/publish_mock_candle.py

import os
import sqlite3
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../config/.env'))
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "../database/trading.db")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/data_acquisition.log", mode='a')
    ]
)

def insert_mock_hourly_candle(db_path: str, timestamp: datetime, open_p: float, high_p: float, low_p: float, close_p: float, volume: float):
    """
    Insert a mock hourly candle into the hourly_candles table.
    If a candle with the same timestamp already exists, it will skip insertion.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.execute('''
            INSERT INTO hourly_candles (timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            timestamp.isoformat(),
            open_p,
            high_p,
            low_p,
            close_p,
            volume
        ))
        conn.commit()
        logging.info(f"Inserted mock hourly candle at {timestamp.isoformat()}.")
    except sqlite3.IntegrityError:
        logging.warning(f"Hourly candle at {timestamp.isoformat()} already exists. Skipping.")
    except Exception as e:
        logging.error(f"Failed to insert mock hourly candle: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    # Example: Insert a candle that should trigger a buy
    # Current time as a candle end time
    now = datetime.utcnow()

    # Adjust these values as desired for testing:
    open_price = 100.0
    high_price = 110.0
    low_price = 100.0
    close_price = 101.0  # Close near low to achieve IBS < 0.2
    volume = 500.0

    insert_mock_hourly_candle(
        db_path=SQLITE_DB_PATH,
        timestamp=now,  # This will be used as the candle's ending timestamp
        open_p=open_price,
        high_p=high_price,
        low_p=low_price,
        close_p=close_price,
        volume=volume
    )

    logging.info("Mock candle published successfully.")
