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
from hyperliquid.utils.types import Cloid

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


# ------------------------------------------------------------------------------
# Helper to detect immediate fills by comparing old vs. new position size
# ------------------------------------------------------------------------------
def check_position_change(old_size: float, new_size: float, trade_side: str, trade_size: float) -> bool:
    """
    Returns True if the new_size is consistent with having 
    fully filled an order of size=trade_size on the given trade_side ('long' or 'short').

    This is a rough check to see if the user position changed by ~ trade_size.
    """
    # "Tolerance" for float mismatch
    tolerance = trade_size * 0.1  # 10% of trade_size for slippage or partial fills
    if trade_side == "long":
        # Expect new_size ~ old_size + trade_size
        expected = old_size + trade_size
        return abs(new_size - expected) < tolerance
    else:
        # For "short" we do the opposite: new_size should be smaller if closing
        # or negative if net short.  This example assumes we only track absolute size on BTC.
        # If we do net short, you'd interpret "new_size" as negative. 
        # But let's assume we only do reduce-only "short" to close a long position.
        expected = old_size - trade_size
        return abs(new_size - expected) < tolerance


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


def place_limit_order_with_chase_modify(
    side: str,
    size: float,
    initial_price: float,
    old_position: float,
    reduce_only: bool = False,
    max_requotes: int = 5,
    sleep_seconds: float = 1.0
):
    """
    Places a limit order with a unique CLOID and modifies its price if unfilled.
    If we cannot find it (due to fast fill, etc.), we do a fallback check on position.
    If the position changed accordingly, we treat it as "filled" instantly.

    side: 'long' or 'short'
    size: trade size
    initial_price: starting limit price
    old_position: the user's BTC position size *before* placing this trade
    reduce_only: whether or not the order is reduce-only
    """
    is_buy = (side == "long")
    cloid = Cloid.from_str("0x" + os.urandom(16).hex())
    logging.info(
        f"Placing limit order (chase/modify). side={side}, size={size}, "
        f"initial_price={initial_price}, reduce_only={reduce_only}, CLOID={cloid}"
    )

    order_type = {"limit": {"tif": "Gtc"}}

    # 1) Place the initial order
    resp = exchange.order(
        name="BTC",
        is_buy=is_buy,
        sz=size,
        limit_px=initial_price,
        order_type=order_type,
        reduce_only=reduce_only,
        cloid=cloid
    )
    if resp.get("status") == "err":
        logging.error(f"Initial order placement failed: {resp}")
        # Return a structured "err" with a response
        return {
            "status": "err",
            "response": {
                "data": {"statuses": ["error: initial order placement failed"]}
            }
        }

    # Brief sleep so the CLOID is recognized
    time.sleep(0.5)

    current_price = initial_price

    for attempt in range(max_requotes + 1):
        # Sleep before checking status
        time.sleep(sleep_seconds)

        # Query with small internal retry
        order_status_resp = None
        status_tries = 2
        for _ in range(status_tries):
            order_status_resp = info.query_order_by_cloid(ACCOUNT_ADDRESS, cloid)
            if order_status_resp.get("status") == "ok":
                break
            time.sleep(0.5)

        if not order_status_resp or order_status_resp.get("status") != "ok":
            # Possibly an instant fill or a delayed CLOID recognition
            logging.warning(f"Could not fetch order status (CLOID={cloid}) after {status_tries} tries. Checking position fallback...")

            # Fallback check: see if position changed to indicate a fill
            user_state = info.user_state(ACCOUNT_ADDRESS)
            new_positions = user_state.get("assetPositions", [])
            new_btc_position = 0.0
            for p in new_positions:
                pos = p["position"]
                if pos["coin"] == "BTC":
                    new_btc_position = float(pos["szi"])
                    break

            # If it looks like the position changed by ~size in the correct direction, we call it "filled"
            if check_position_change(old_position, new_btc_position, side, size):
                logging.info(f"Order CLOID={cloid} likely filled instantly based on position change.")
                return {
                    "status": "ok",
                    "response": {
                        "data": {
                            "statuses": ["filled (fallback)"],
                            "oldPos": str(old_position),
                            "newPos": str(new_btc_position)
                        }
                    }
                }
            else:
                # Not found, no immediate fill. Return an error for the chase logic
                return {
                    "status": "err",
                    "response": {
                        "data": {
                            "statuses": ["error: could not query order status by CLOID, no fallback fill"]
                        }
                    }
                }

        # If we *did* get an OK order_status_resp, parse it
        data = order_status_resp["response"]["data"]
        status_key = data.get("status", "")
        filled_sz = float(data.get("filledSz", "0"))
        remaining_sz = float(data.get("remainingSz", "0"))

        if status_key == "filled":
            logging.info(f"Order CLOID={cloid} fully filled after attempt {attempt+1}.")
            return {
                "status": "ok",
                "response": {
                    "data": {
                        "statuses": ["filled"],
                        "filledSz": str(filled_sz),
                        "remainingSz": str(remaining_sz)
                    }
                }
            }

        elif status_key == "canceled":
            logging.info(f"Order CLOID={cloid} was canceled externally. Stopping chase.")
            return {
                "status": "err",
                "response": {
                    "data": {"statuses": ["error: order was canceled externally"]}
                }
            }

        elif status_key == "resting":
            # We can chase by modifying
            if attempt < max_requotes:
                all_mids = info.all_mids()
                btc_mid_str = all_mids.get("BTC")
                if btc_mid_str:
                    new_mid = float(btc_mid_str)
                    current_price = int(round(new_mid))

                logging.info(f"[Chase Attempt {attempt+1}] Modifying order CLOID={cloid} to price={current_price}")
                modify_resp = exchange.modify_order(
                    cloid,
                    "BTC",
                    is_buy,
                    size,
                    current_price,
                    order_type,
                    reduce_only=reduce_only
                )
                if modify_resp.get("status") == "err":
                    logging.error(f"Order modify failed: {modify_resp}")
                    return {
                        "status": "err",
                        "response": {
                            "data": {
                                "statuses": ["error: order modify failed"]
                            }
                        }
                    }
            else:
                logging.info(f"Max re-quotes reached. Returning last known status for CLOID={cloid}.")
                return {
                    "status": "ok",
                    "response": {
                        "data": {
                            "statuses": ["resting"],
                            "filledSz": str(filled_sz),
                            "remainingSz": str(remaining_sz)
                        }
                    }
                }
        else:
            logging.info(f"Order CLOID={cloid} in unknown status '{status_key}'. Stopping chase.")
            return {
                "status": "err",
                "response": {
                    "data": {
                        "statuses": [f"error: unknown status={status_key}"]
                    }
                }
            }

    # If we exit the loop, do one final status check
    final_status_resp = info.query_order_by_cloid(ACCOUNT_ADDRESS, cloid)
    if final_status_resp.get("status") == "ok":
        return final_status_resp
    else:
        return {
            "status": "err",
            "response": {
                "data": {
                    "statuses": ["error: final status query not ok"]
                }
            }
        }


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
        price = sig["price"]

        logging.info(f"Executing signal {signal_id} => {sig}")

        if action == "open":
            set_leverage(leverage)

            user_state = info.user_state(ACCOUNT_ADDRESS)
            withdrawable_str = user_state.get("withdrawable", "0")
            withdrawable = float(withdrawable_str)

            # Grab the old BTC position before we place the order
            positions = user_state.get("assetPositions", [])
            old_btc_position = 0.0
            for p in positions:
                pos = p["position"]
                if pos["coin"] == "BTC":
                    old_btc_position = float(pos["szi"])
                    break

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

            # Place + chase/modify, with the old position as reference
            resp = place_limit_order_with_chase_modify(
                side=side,
                size=trade_size,
                initial_price=rounded_price,
                old_position=old_btc_position,
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
            old_btc_position = 0.0
            for p in positions:
                pos = p["position"]
                if pos["coin"] == "BTC":
                    old_btc_position = float(pos["szi"])
                    break

            if old_btc_position == 0:
                logging.info(f"No position found to close for signal {signal_id}, marking executed.")
                mark_signal_executed(db_path, signal_id)
                continue

            close_size = round(abs(old_btc_position), sz_decimals)

            all_mids = info.all_mids()
            btc_mid_str = all_mids.get("BTC")
            if not btc_mid_str:
                logging.error("BTC mid price not found, marking failed.")
                mark_signal_failed(db_path, signal_id)
                continue
            btc_mid = float(btc_mid_str)

            rounded_price = int(round(btc_mid))
            opposite_side = "short" if side == "long" else "long"

            resp = place_limit_order_with_chase_modify(
                side=opposite_side,
                size=close_size,
                initial_price=rounded_price,
                old_position=old_btc_position,
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
