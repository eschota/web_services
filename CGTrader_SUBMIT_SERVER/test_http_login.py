#!/usr/bin/env python3
"""Test HTTP login to CGTrader"""
from cgtrader_http import CGTraderHTTPClient

def test_login():
    client = CGTraderHTTPClient()
    
    print("Testing login...")
    success = client.login()
    
    if success:
        print("✅ Login successful!")
        
        # Check login status
        if client.is_logged_in():
            print("✅ Login verified!")
        else:
            print("⚠️  Login reported success but verification failed")
    else:
        print("❌ Login failed")
    
    return success

if __name__ == "__main__":
    test_login()
