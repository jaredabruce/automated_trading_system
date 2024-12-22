import os
from dotenv import load_dotenv
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

# Load environment variables
load_dotenv()

ACCOUNT_ADDRESS = os.getenv("ACCOUNT_ADDRESS")
API_WALLET_ADDRESS = os.getenv("HYPERLIQUID_API_KEY")
API_SECRET = os.getenv("HYPERLIQUID_API_SECRET")

if not ACCOUNT_ADDRESS or not API_SECRET or not API_WALLET_ADDRESS:
    raise ValueError("Missing environment variables. Please set ACCOUNT_ADDRESS, HYPERLIQUID_API_KEY, and HYPERLIQUID_API_SECRET.")

# Create LocalAccount for the API wallet
api_wallet = Account.from_key(API_SECRET)
if api_wallet.address.lower() != API_WALLET_ADDRESS.lower():
    raise ValueError("API wallet private key does not match the API wallet address")

# Initialize Info and Exchange with the API wallet
info = Info(constants.TESTNET_API_URL, skip_ws=True)
exchange_api = Exchange(
    wallet=api_wallet,
    base_url=constants.TESTNET_API_URL,
    account_address=ACCOUNT_ADDRESS
)

# Check your account balance and position
user_state = info.user_state(ACCOUNT_ADDRESS)
margin_summary = user_state.get("marginSummary", {})
account_value_str = margin_summary.get("accountValue", "0")
account_value = float(account_value_str)

if account_value <= 0:
    raise ValueError("Account value is zero. Ensure you have test funds.")

# Get BTC price and decimals
all_mids = info.all_mids()
btc_mid_str = all_mids.get("BTC")
if btc_mid_str is None:
    raise ValueError("BTC mid price not found.")
btc_mid = float(btc_mid_str)

meta_data = info.meta()
btc_info = next((x for x in meta_data["universe"] if x["name"] == "BTC"), None)
if not btc_info:
    raise ValueError("BTC not found in meta data")
sz_decimals = btc_info["szDecimals"]

# Calculate trade size
trade_size = account_value / btc_mid
trade_size = round(trade_size, sz_decimals)

if trade_size <= 0:
    raise ValueError("Trade size is zero after rounding.")

print(f"Placing market BUY order for {trade_size} BTC...")
buy_response = exchange_api.market_open("BTC", is_buy=True, sz=trade_size)
print("Buy Response:", buy_response)

print(f"Placing market SELL order for {trade_size} BTC...")
sell_response = exchange_api.market_open("BTC", is_buy=False, sz=trade_size)
print("Sell Response:", sell_response)

new_user_state = info.user_state(ACCOUNT_ADDRESS)
print("New User State after trades:", new_user_state)
