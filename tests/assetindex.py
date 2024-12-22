import os
import requests
from dotenv import load_dotenv
from hyperliquid.info import Info
from hyperliquid.utils import constants
import eth_account
import time

# Load environment variables
load_dotenv()
VAULT_ADDRESS = os.getenv("VAULT_ADDRESS")
HYPERLIQUID_API_SECRET = os.getenv("HYPERLIQUID_API_SECRET")

# Debugging: Verify credentials
if not VAULT_ADDRESS or not HYPERLIQUID_API_SECRET:
    raise ValueError("Error: VAULT_ADDRESS or HYPERLIQUID_API_SECRET is missing in the .env file.")

# Testnet URL
TESTNET_URL = "https://api.hyperliquid.xyz/info"

# Step 1: Verify connectivity to the testnet API
def test_api_connection():
    print("Testing connectivity to the testnet API...")
    payload = {"type": "meta"}
    try:
        response = requests.post(TESTNET_URL, json=payload)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.text[:500]}")  # Print the first 500 chars of response
        if response.status_code != 200:
            print("Error: Testnet API returned an unexpected status.")
        else:
            print("Testnet API is reachable.")
    except Exception as e:
        print(f"Error: Could not reach the testnet API. {e}")

# Step 2: Initialize Info client
def initialize_info_client():
    print("Initializing Info client...")
    try:
        info = Info(TESTNET_URL, skip_ws=True)
        print("Info client initialized successfully.")
        return info
    except Exception as e:
        print(f"Error initializing Info client: {e}")
        return None

# Step 3: Fetch USDC balance
def fetch_usdc_balance(info):
    print("Fetching USDC balance...")
    try:
        user_state = info.user_state(VAULT_ADDRESS)
        print(f"User State Response: {user_state}")
        usdc_balance = float(user_state["marginSummary"]["accountValue"])
        print(f"USDC Balance: {usdc_balance}")
        return usdc_balance
    except Exception as e:
        print(f"Error fetching USDC balance: {e}")
        return 0

# Step 4: Retry logic wrapper
def retry(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except Exception as e:
            print(f"Attempt {i + 1} failed: {e}")
            time.sleep(delay)
    raise Exception("All retry attempts failed.")

# Step 5: Main workflow
def main():
    # Test API connection
    test_api_connection()

    # Initialize the Info client
    info = initialize_info_client()
    if not info:
        print("Exiting due to client initialization failure.")
        return

    # Fetch USDC balance with retries
    usdc_balance = retry(lambda: fetch_usdc_balance(info))
    print(f"Final USDC Balance: {usdc_balance}")

if __name__ == "__main__":
    main()
