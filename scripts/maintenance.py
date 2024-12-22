# scripts/maintenance.py

import os
import sqlite3
import logging
import shutil
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../config/.env'))

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "../database/trading.db")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/maintenance.log", mode='a')
    ]
)

def prune_old_candles(db_path: str, days: int = 30):
    """
    Remove hourly candles older than 'days' days.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    rows_deleted = 0
    try:
        cursor.execute("SELECT id, timestamp FROM hourly_candles")
        rows = cursor.fetchall()
        for r in rows:
            candle_id = r[0]
            ts_str = r[1]

            # Safely parse datetime; if it fails, skip that candle.
            try:
                candle_dt = datetime.fromisoformat(ts_str)
            except ValueError as e:
                logging.error(f"Failed to parse candle timestamp '{ts_str}': {e}")
                continue

            if candle_dt < cutoff:
                cursor.execute("DELETE FROM hourly_candles WHERE id=?", (candle_id,))
                rows_deleted += 1

        conn.commit()
    except Exception as e:
        logging.error(f"Error pruning old candles: {e}")
    finally:
        conn.close()

    if rows_deleted > 0:
        logging.info(f"Pruned {rows_deleted} old candles older than {days} days.")

def prune_old_signals(db_path: str, days: int = 30):
    """
    Remove trade signals older than 'days' days from trade_signals.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    cutoff_str = cutoff.isoformat()

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    rows_deleted = 0
    try:
        # Requires 'created_at' (datetime) in 'trade_signals' table.
        cursor.execute("DELETE FROM trade_signals WHERE created_at < DATETIME(?, 'localtime')", (cutoff_str,))
        rows_deleted = cursor.rowcount
        conn.commit()
    except Exception as e:
        logging.error(f"Error pruning old signals: {e}")
    finally:
        conn.close()

    if rows_deleted > 0:
        logging.info(f"Pruned {rows_deleted} old trade signals older than {days} days.")

def clear_old_logs(logs_dir: str, days: int = 30):
    """
    Remove log files older than 'days' days from a logs directory,
    or simply rotate them.
    """
    cutoff = datetime.utcnow() - timedelta(days=days)
    for filename in os.listdir(logs_dir):
        filepath = os.path.join(logs_dir, filename)
        if os.path.isfile(filepath):
            mtime = datetime.utcfromtimestamp(os.path.getmtime(filepath))
            if mtime < cutoff:
                try:
                    os.remove(filepath)
                    logging.info(f"Removed old log file: {filepath}")
                except Exception as e:
                    logging.error(f"Could not remove file {filepath}: {e}")

if __name__ == "__main__":
    logging.info("Starting weekly maintenance tasks...")

    # 1. Prune old candles/signals older than 30 days
    prune_old_candles(SQLITE_DB_PATH, days=30)
    prune_old_signals(SQLITE_DB_PATH, days=30)

    # 2. Clear old logs in ../logs older than 30 days
    logs_path = os.path.join(os.path.dirname(__file__), '../logs')
    clear_old_logs(logs_path, days=30)

    logging.info("Maintenance tasks completed.")
