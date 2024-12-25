# scripts/trade_execution_logic.py

import os
import sqlite3
import logging
from datetime import datetime
from dotenv import load_dotenv
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../config/.env'))

ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS")
API_WALLET_ADDRESS = os.getenv("HYPERLIQUID_API_KEY")
API_SECRET = os.getenv("HYPERLIQUID_API_SECRET")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "../database/trading.db")

if not ACCOUNT_ADDRESS or not API_SECRET or not API_WALLET_ADDRESS:
    raise ValueError("Missing ACCOUNT_ADDRESS, HYPERLIQUID_API_KEY, or HYPERLIQUID_API_SECRET")

api_wallet = Account.from_key(API_SECRET)
if api_wallet.address.lower() != API_WALLET_ADDRESS.lower():
    raise ValueError("API wallet private key does not match the API wallet address")

info = Info(constants.MAINNET_API_URL, skip_ws=True)
exchange = Exchange(
    wallet=api_wallet,
    base_url=constants.MAINNET_API_URL,
    account_address=ACCOUNT_ADDRESS
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/trade_execution.log", mode='a')
    ]
)

def get_unexecuted_signals(db_path: str):
    """
    Fetch signals that are unexecuted (executed=0).
    (No start-time filtering)
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, timestamp, action, symbol, side, price, leverage
        FROM trade_signals
        WHERE executed = 0
        ORDER BY id ASC
    ''')
    rows = cursor.fetchall()
    conn.close()

    results = []
    for row in rows:
        results.append({
            'id': row[0],
            'timestamp': row[1],
            'action': row[2],
            'symbol': row[3],
            'side': row[4],
            'price': float(row[5]),
            'leverage': int(row[6]) if row[6] else 1
        })
    return results

def mark_signal_executed(db_path: str, signal_id: int):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE trade_signals
        SET executed = 1
        WHERE id = ?
    ''', (signal_id,))
    conn.commit()
    conn.close()
    logging.info(f"Marked signal {signal_id} as executed.")

def mark_signal_failed(db_path: str, signal_id: int):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE trade_signals
        SET executed = 2
        WHERE id = ?
    ''', (signal_id,))
    conn.commit()
    conn.close()
    logging.info(f"Marked signal {signal_id} as failed.")

def get_size_decimals():
    meta_data = info.meta()
    btc_info = next((x for x in meta_data["universe"] if x["name"] == "BTC"), None)
    if not btc_info:
        raise ValueError("BTC not found in meta data")
    return btc_info["szDecimals"]

def set_leverage(leverage: int):
    resp = exchange.update_leverage(leverage, "BTC", is_cross=True)
    if resp.get("status") == "err":
        logging.error(f"Failed to set leverage to {leverage}: {resp}")
    else:
        logging.info(f"Set leverage to {leverage}x successfully.")

def place_limit_order(side: str, size: float, price: float, reduce_only=False):
    is_buy = (side == "long")
    order_type = {"limit": {"tif": "Gtc"}}
    order_resp = exchange.order(
        name="BTC",
        is_buy=is_buy,
        sz=size,
        limit_px=price,
        order_type=order_type,
        reduce_only=reduce_only
    )
    logging.info(f"Placed limit order: side={side}, size={size}, price={price}, reduce_only={reduce_only}")
    logging.info(f"Order Response: {order_resp}")
    return order_resp

def execute_pending_signals(db_path: str):
    """
    Process all unexecuted signals, no time filter. 
    If order fails, mark as failed. Otherwise, mark executed.
    """
    sz_decimals = get_size_decimals()
    signals = get_unexecuted_signals(db_path)
    if not signals:
        logging.info("No unexecuted trade signals found.")
        return

    for sig in signals:
        signal_id = sig["id"]
        action = sig["action"]
        side = sig["side"]
        leverage = sig["leverage"]
        price = sig["price"]  # not currently used but included

        logging.info(f"Executing signal {signal_id} => {sig}")

        if action == "open":
            # Attempt to set leverage
            set_leverage(leverage)

            user_state = info.user_state(ACCOUNT_ADDRESS)
            withdrawable_str = user_state.get("withdrawable", "0")
            withdrawable = float(withdrawable_str)

            all_mids = info.all_mids()
            btc_mid_str = all_mids.get("BTC")
            if not btc_mid_str:
                logging.error("BTC mid price not found, marking failed.")
                mark_signal_failed(db_path, signal_id)
                continue
            btc_mid = float(btc_mid_str)

            # small buffer to avoid margin issues
            BUFFER_FACTOR = 0.98
            trade_size = (withdrawable * leverage) / btc_mid
            trade_size *= BUFFER_FACTOR
            trade_size = round(trade_size, sz_decimals)

            if trade_size <= 0:
                logging.error(f"Trade size <= 0 for signal {signal_id}, skipping.")
                mark_signal_failed(db_path, signal_id)
                continue

            rounded_price = int(round(btc_mid))
            resp = place_limit_order(side, trade_size, rounded_price, reduce_only=False)
            if resp.get("status") == "err":
                mark_signal_failed(db_path, signal_id)
            else:
                statuses = resp["response"]["data"].get("statuses", [])
                if any("error" in s for s in statuses):
                    mark_signal_failed(db_path, signal_id)
                else:
                    mark_signal_executed(db_path, signal_id)

        elif action == "close":
            user_state = info.user_state(ACCOUNT_ADDRESS)
            positions = user_state.get("assetPositions", [])
            position_size = 0.0
            for p in positions:
                pos = p["position"]
                if pos["coin"] == "BTC":
                    position_size = float(pos["szi"])
                    break

            if position_size == 0:
                logging.info(f"No position found to close for signal {signal_id}, marking executed.")
                mark_signal_executed(db_path, signal_id)
                continue

            close_size = round(abs(position_size), sz_decimals)

            all_mids = info.all_mids()
            btc_mid_str = all_mids.get("BTC")
            if not btc_mid_str:
                logging.error("BTC mid price not found, marking failed.")
                mark_signal_failed(db_path, signal_id)
                continue
            btc_mid = float(btc_mid_str)

            rounded_price = int(round(btc_mid))
            opposite_side = "short" if side == "long" else "long"
            resp = place_limit_order(opposite_side, close_size, rounded_price, reduce_only=True)
            if resp.get("status") == "err":
                mark_signal_failed(db_path, signal_id)
            else:
                statuses = resp["response"]["data"].get("statuses", [])
                if any("error" in s for s in statuses):
                    mark_signal_failed(db_path, signal_id)
                else:
                    mark_signal_executed(db_path, signal_id)