#!/usr/bin/env python3
"""Test authentication with auto proxy manager"""
import os
import sys
import time

# Disable manual proxy, enable auto proxy
os.environ.pop('PROXY_URL', None)
os.environ['ENABLE_AUTO_PROXY'] = 'true'
os.environ['MAX_WORKING_PROXIES'] = '10'  # Limit for faster testing

from cgtrader_http import CGTraderHTTPClient

def test_authentication():
    """Test authentication with auto proxy."""
    print("=" * 60)
    print("Testing CGTrader Authentication with Auto Proxy")
    print("=" * 60)
    print()
    
    print("Initializing HTTP client...")
    client = CGTraderHTTPClient()
    
    print(f"Proxy manager initialized: {hasattr(client, 'proxy_manager') and client.proxy_manager is not None}")
    if hasattr(client, 'proxy_manager') and client.proxy_manager:
        print(f"Working proxies in pool: {len(client.proxy_manager.working_proxies)}")
        current = client.proxy_manager.get_current_proxy()
        if current:
            print(f"Current proxy: {current.split('@')[-1] if '@' in current else current}")
    
    print()
    print("Attempting login...")
    print("-" * 60)
    
    try:
        success = client.login()
        if success:
            print()
            print("=" * 60)
            print("✅ LOGIN SUCCESSFUL!")
            print("=" * 60)
            
            # Verify login status
            print()
            print("Verifying login status...")
            if client.is_logged_in():
                print("✅ Login verified - user is authenticated")
            else:
                print("⚠️  Login reported success but verification failed")
            
            return True
        else:
            print()
            print("=" * 60)
            print("❌ LOGIN FAILED")
            print("=" * 60)
            return False
            
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ ERROR during login: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    max_attempts = 5
    for attempt in range(1, max_attempts + 1):
        print(f"\n{'=' * 60}")
        print(f"ATTEMPT {attempt}/{max_attempts}")
        print(f"{'=' * 60}\n")
        
        success = test_authentication()
        
        if success:
            print("\n✅ Authentication successful!")
            sys.exit(0)
        
        if attempt < max_attempts:
            print(f"\n⏳ Waiting 10 seconds before next attempt...")
            time.sleep(10)
    
    print("\n❌ All authentication attempts failed")
    sys.exit(1)
