# scripts/trade_execution_logic.py

import os
import sqlite3
import logging
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
    raise ValueError("Missing ACCOUNT_ADDRESS, HYPERLIQUID_API_KEY, or HYPERLIQUID_API_SECRET in .env")

api_wallet = Account.from_key(API_SECRET)
if api_wallet.address.lower() != API_WALLET_ADDRESS.lower():
    raise ValueError("API wallet private key does not match the API wallet address")

info = Info(constants.TESTNET_API_URL, skip_ws=True)
exchange = Exchange(
    wallet=api_wallet,
    base_url=constants.TESTNET_API_URL,
    account_address=ACCOUNT_ADDRESS
)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("../logs/trade_execution.log", mode='a')
    ]
)

def get_unexecuted_signals(db_path: str):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT id, timestamp, action, symbol, side, price, leverage
        FROM trade_signals
        WHERE executed = 0
        ORDER BY id ASC
    ''')
    signals = cursor.fetchall()
    conn.close()

    results = []
    for row in signals:
        results.append({
            'id': row[0],
            'timestamp': row[1],
            'action': row[2],
            'symbol': row[3],
            'side': row[4],
            'price': float(row[5]),
            'leverage': int(row[6]) if row[6] else 1
        })

    logging.debug(f"Fetched unexecuted signals: {results}")
    return results

def mark_signal_executed(db_path: str, signal_id: int):
    logging.debug(f"Marking signal {signal_id} as executed.")
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

def get_position_size():
    user_state = info.user_state(ACCOUNT_ADDRESS)
    positions = user_state.get("assetPositions", [])
    logging.debug(f"User state positions: {positions}")
    for pos in positions:
        p = pos["position"]
        if p["coin"] == "BTC":
            return float(p["szi"])
    return 0.0

def get_size_decimals():
    meta_data = info.meta()
    btc_info = next((x for x in meta_data["universe"] if x["name"] == "BTC"), None)
    if not btc_info:
        raise ValueError("BTC not found in meta data")
    return btc_info["szDecimals"]

def set_leverage(leverage: int):
    logging.debug(f"Setting leverage: {leverage}")
    resp = exchange.update_leverage(leverage, "BTC", is_cross=True)
    if resp.get("status") == "err":
        logging.error(f"Failed to set leverage to {leverage}: {resp}")
    else:
        logging.info(f"Set leverage to {leverage}x successfully.")

def place_limit_order(side: str, size: float, price: float, reduce_only: bool = False):
    logging.debug(f"Placing limit order: side={side}, size={size}, price={price}, reduce_only={reduce_only}")

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

    if order_resp.get("status") == "ok":
        return order_resp
    else:
        logging.error(f"Limit order failed: {order_resp}")
        return order_resp

def execute_pending_signals(db_path: str):
    sz_decimals = get_size_decimals()
    signals = get_unexecuted_signals(db_path)

    if not signals:
        logging.debug("No signals to execute.")

    for sig in signals:
        action = sig["action"]
        side = sig["side"]
        leverage = sig["leverage"]

        logging.debug(f"Executing signal: {sig}")

        if action == "open":
            set_leverage(leverage)

            user_state = info.user_state(ACCOUNT_ADDRESS)
            withdrawable_str = user_state.get("withdrawable", "0")
            withdrawable = float(withdrawable_str)

            all_mids = info.all_mids()
            btc_mid_str = all_mids.get("BTC")
            if not btc_mid_str:
                logging.error("BTC mid price not found; cannot place limit order.")
                mark_signal_executed(db_path, sig['id'])
                continue
            btc_mid = float(btc_mid_str)

            trade_size = (withdrawable * leverage) / btc_mid
            trade_size = round(trade_size, sz_decimals)
            logging.debug(
                f"Trade size calculated: {trade_size} BTC using withdrawable={withdrawable}, "
                f"leverage={leverage}, btc_mid={btc_mid}"
            )

            if trade_size <= 0:
                logging.error("Trade size is zero or negative. Skipping signal.")
                mark_signal_executed(db_path, sig['id'])
                continue

            # Round price to integer to avoid tick-size issues
            rounded_price = int(round(btc_mid))

            resp = place_limit_order(side=side, size=trade_size, price=rounded_price, reduce_only=False)
            if resp.get("status") == "ok":
                mark_signal_executed(db_path, sig['id'])
            else:
                logging.error("Limit order placement failed; not marking signal as executed.")

        elif action == "close":
            position_size = get_position_size()
            if position_size == 0:
                logging.info("No position found to close. Marking as executed.")
                mark_signal_executed(db_path, sig['id'])
                continue

            close_size = abs(position_size)
            close_size = round(close_size, sz_decimals)
            opposite_side = "short" if side == "long" else "long"

            logging.debug(
                f"Closing position: current size={position_size}, "
                f"close_size={close_size}, opposite_side={opposite_side}"
            )

            all_mids = info.all_mids()
            btc_mid_str = all_mids.get("BTC")
            if not btc_mid_str:
                logging.error("BTC mid price not found; cannot place limit order.")
                mark_signal_executed(db_path, sig['id'])
                continue
            btc_mid = float(btc_mid_str)

            rounded_price = int(round(btc_mid))

            resp = place_limit_order(side=opposite_side, size=close_size, price=rounded_price, reduce_only=True)
            if resp.get("status") == "ok":
                mark_signal_executed(db_path, sig['id'])
            else:
                logging.error("Close limit order placement failed; not marking signal as executed.")
