import os
import sqlite3
import logging
import time
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

# Make sure you use the correct environment (Mainnet vs Testnet)
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

# -------------------------------------------------------------------------
# Database / signal helpers
# -------------------------------------------------------------------------
def get_unexecuted_signals(db_path: str):
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

# -------------------------------------------------------------------------
# Position / Fill Helpers
# -------------------------------------------------------------------------
def get_btc_position() -> float:
    """Return the user's current BTC position size (szi)."""
    user_state = info.user_state(ACCOUNT_ADDRESS)
    positions = user_state.get("assetPositions", [])
    for p in positions:
        pos = p["position"]
        if pos["coin"] == "BTC":
            return float(pos["szi"])
    return 0.0

def check_position_change(old_pos: float, new_pos: float, side: str, trade_size: float) -> bool:
    """
    If side='long', expect new_pos ~ old_pos + trade_size.
    If side='short', expect new_pos ~ old_pos - trade_size (closing a long or opening a short).
    We'll allow 10% tolerance.
    """
    tolerance = trade_size * 0.1
    if side == "long":
        expected = old_pos + trade_size
    else:
        expected = old_pos - trade_size
    return abs(new_pos - expected) < tolerance

# -------------------------------------------------------------------------
# Chasing Function: Use open_orders + user_fills + fallback
# -------------------------------------------------------------------------
def place_limit_order_with_chase_openorders(
    side: str,
    size: float,
    initial_price: float,
    reduce_only: bool = False,
    max_requotes: int = 5,
    sleep_seconds: float = 2.0
):
    """
    Places a limit order and tries to chase using OID, but only checks
    open_orders + user_fills + position fallback.

    1) Record old BTC position.
    2) Place the order => parse OID.
       - If instantly filled, done.
    3) Sleep + loop up to max_requotes times:
       a) open_orders => if OID found => "resting"
          => optionally modify to chase a new price
       b) if OID not in open_orders => check user_fills for OID => if found => filled
          if not found => check position fallback => if changed => treat as filled
          else => keep going or fail
    4) Return an OK or ERR style dict with "response.data.statuses"

    This approach does not call query_order_by_oid, as that can sometimes fail to
    find the OID quickly.
    """
    is_buy = (side == "long")
    old_pos = get_btc_position()

    logging.info(f"Placing OID-based limit order. side={side}, size={size}, price={initial_price}, reduce_only={reduce_only}")

    order_type = {"limit": {"tif": "Gtc"}}
    # Place the order
    resp = exchange.order(
        name="BTC",
        is_buy=is_buy,
        sz=size,
        limit_px=initial_price,
        order_type=order_type,
        reduce_only=reduce_only
    )
    if resp.get("status") == "err":
        logging.error(f"Initial order placement failed: {resp}")
        return {
            "status": "err",
            "response": {"data": {"statuses": ["error: initial order placement failed"]}}
        }

    # Parse OID
    statuses_info = resp.get("response", {}).get("data", {}).get("statuses", [])
    if not statuses_info:
        logging.error(f"No 'statuses' in order response. Full resp: {resp}")
        return {
            "status": "err",
            "response": {"data": {"statuses": ["error: no statuses in response"]}}
        }

    first_status = statuses_info[0]
    if "error" in first_status:
        logging.error(f"Order returned error in statuses: {first_status}")
        return {
            "status": "err",
            "response": {"data": {"statuses": ["error: order placement returned error"]}}
        }

    # If instantly filled, we might see something like {"filled": {...}}
    if "filled" in first_status:
        logging.info("Order was instantly filled on placement.")
        return {
            "status": "ok",
            "response": {"data": {"statuses": ["filled-immediate"]}}
        }

    if "resting" not in first_status:
        logging.warning(f"Unexpected immediate status: {first_status}")
        return {
            "status": "err",
            "response": {"data": {"statuses": ["error: unknown immediate status"]}}
        }

    oid = first_status["resting"]["oid"]
    logging.info(f"Order is resting with OID={oid}.")

    # Let the exchange register the OID in open_orders
    time.sleep(2.0)

    current_price = initial_price

    for attempt in range(max_requotes + 1):
        time.sleep(sleep_seconds)

        # (a) check open_orders
        all_open = info.open_orders(ACCOUNT_ADDRESS)
        if isinstance(all_open, dict) and all_open.get("status") == "err":
            # If for some reason open_orders also fails, we can do a quick fallback
            logging.warning("open_orders returned err; continuing fallback checks.")

        # parse open orders
        open_orders_list = []
        if isinstance(all_open, list):
            open_orders_list = all_open
        elif isinstance(all_open, dict) and "response" in all_open:
            # some older versions might return a dict with response/data. 
            # Typically though open_orders is just a list. 
            # We'll unify to open_orders_list if you see a different structure.
            pass

        found_oid = None
        for o in open_orders_list:
            # o might look like: {"coin":"BTC","limitPx":"10000","oid":12345,"side":"A","sz":"0.01","timestamp":1692217107273}
            if "oid" in o and o["oid"] == oid:
                found_oid = o
                break

        if found_oid:
            # It's still resting or partially filled
            logging.info(f"OID={oid} is still in open_orders => resting or partial fill.")
            if attempt < max_requotes:
                # chase by modifying
                all_mids = info.all_mids()
                btc_mid_str = all_mids.get("BTC")
                if btc_mid_str:
                    new_mid = float(btc_mid_str)
                    current_price = int(round(new_mid))

                logging.info(f"[Chase Attempt {attempt+1}] Modifying OID={oid} to new price={current_price}")
                modify_resp = exchange.modify_order(
                    oid,
                    "BTC",
                    is_buy,
                    size,
                    current_price,
                    order_type,
                    reduce_only=reduce_only
                )
                if modify_resp.get("status") == "err":
                    logging.error(f"Modify order failed: {modify_resp}")
                    return {
                        "status": "err",
                        "response": {"data": {"statuses": ["error: modify_order failed"]}}
                    }
            else:
                logging.info(f"Max re-quotes reached for OID={oid}. Returning resting.")
                return {
                    "status": "ok",
                    "response": {
                        "data": {
                            "statuses": ["resting"],
                            "oid": oid
                        }
                    }
                }
        else:
            # Not in open_orders => maybe filled?
            logging.info(f"OID={oid} not found in open_orders => checking user_fills and position fallback...")

            # (b) check user_fills
            fills_resp = info.user_fills(ACCOUNT_ADDRESS)
            if isinstance(fills_resp, dict) and fills_resp.get("status") == "err":
                logging.warning("user_fills returned err; continuing fallback checks.")
                fills_list = []
            elif isinstance(fills_resp, list):
                fills_list = fills_resp
            else:
                # maybe "response" structure
                fills_list = fills_resp if isinstance(fills_resp, list) else []

            # see if any fill references our OID
            found_fill = any((f.get("oid") == oid) for f in fills_list)
            if found_fill:
                logging.info(f"OID={oid} found in user_fills => it was filled.")
                return {
                    "status": "ok",
                    "response": {"data": {"statuses": ["filled"]}}
                }

            # if not in user_fills, last fallback => position changed?
            new_pos = get_btc_position()
            if check_position_change(old_pos, new_pos, side, size):
                logging.info(f"Fallback => position changed => OID={oid} likely filled instantly.")
                return {
                    "status": "ok",
                    "response": {"data": {"statuses": ["filled-fallback"]}}
                }

            # else we can't find it => might have been canceled externally, or just no data
            logging.warning(f"OID={oid} not found in open_orders or user_fills, no position change. Possibly an error or external cancel.")
            return {
                "status": "err",
                "response": {
                    "data": {
                        "statuses": ["error: OID not open, not filled, no fallback fill"]
                    }
                }
            }

    # If we exit the loop, do a final fallback check
    new_pos = get_btc_position()
    if check_position_change(old_pos, new_pos, side, size):
        logging.info(f"Final fallback => position changed => OID={oid} must have filled.")
        return {
            "status": "ok",
            "response": {"data": {"statuses": ["filled-final-fallback"]}}
        }

    return {
        "status": "err",
        "response": {
            "data": {
                "statuses": ["error: chase loop ended, no fill detected"]
            }
        }
    }

# -------------------------------------------------------------------------
# Main Execution of Pending Signals
# -------------------------------------------------------------------------
def execute_pending_signals(db_path: str):
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
        price = sig["price"]  # not used except for logging

        logging.info(f"Executing signal {signal_id} => {sig}")

        if action == "open":
            # Set leverage
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

            BUFFER_FACTOR = 0.98
            trade_size = (withdrawable * leverage) / btc_mid
            trade_size *= BUFFER_FACTOR
            trade_size = round(trade_size, sz_decimals)

            if trade_size <= 0:
                logging.error(f"Trade size <= 0 for signal {signal_id}, skipping.")
                mark_signal_failed(db_path, signal_id)
                continue

            rounded_price = int(round(btc_mid))
            resp = place_limit_order_with_chase_openorders(
                side=side,
                size=trade_size,
                initial_price=rounded_price,
                reduce_only=False
            )

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
                logging.info(f"No BTC position to close for signal {signal_id}, marking executed.")
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

            resp = place_limit_order_with_chase_openorders(
                side=opposite_side,
                size=close_size,
                initial_price=rounded_price,
                reduce_only=True
            )

            if resp.get("status") == "err":
                mark_signal_failed(db_path, signal_id)
            else:
                statuses = resp["response"]["data"].get("statuses", [])
                if any("error" in s for s in statuses):
                    mark_signal_failed(db_path, signal_id)
                else:
                    mark_signal_executed(db_path, signal_id)
