#!/usr/bin/env python3
"""Test script for proxy and manual cookies"""
import os
import sys
from cgtrader_http import CGTraderHTTPClient
from config import parse_proxy_url

def test_proxy():
    """Test proxy configuration."""
    print("=" * 60)
    print("Testing Proxy Configuration")
    print("=" * 60)
    
    proxy_url = "http://tIQnauShHLxaNdf:byZ8A47X8y0jk2Y@194.79.14.3:45264"
    proxies = parse_proxy_url(proxy_url)
    
    if proxies:
        print(f"✅ Proxy parsed successfully")
        print(f"   HTTP: {proxies['http'].split('@')[0]}@***")
        print(f"   HTTPS: {proxies['https'].split('@')[0]}@***")
        
        # Test actual proxy connection
        import requests
        try:
            response = requests.get(
                "http://httpbin.org/ip",
                proxies=proxies,
                timeout=10
            )
            print(f"✅ Proxy connection test: Status {response.status_code}")
            print(f"   Response: {response.text[:100]}")
        except Exception as e:
            print(f"⚠️  Proxy connection failed: {e}")
    else:
        print("❌ Proxy parsing failed")
    
    print()

def test_manual_cookies():
    """Test manual cookies loading."""
    print("=" * 60)
    print("Testing Manual Cookies")
    print("=" * 60)
    
    client = CGTraderHTTPClient()
    
    # Test set_manual_auth
    print("\n1. Testing set_manual_auth()...")
    client.set_manual_auth(
        csrf_token="test_csrf_token_123",
        cookies={
            "_session_id": "test_session_123",
            "remember_token": "test_remember_123"
        }
    )
    print(f"   CSRF token: {client.csrf_token}")
    print(f"   Cookies count: {len(client.session.cookies)}")
    
    # Test load_manual_cookies
    print("\n2. Testing load_manual_cookies()...")
    test_cookies_file = "/tmp/test_cookies.json"
    test_cookies = {
        "_session_id": "file_session_123",
        "auth_token": "file_auth_123"
    }
    
    import json
    with open(test_cookies_file, "w") as f:
        json.dump(test_cookies, f)
    
    client2 = CGTraderHTTPClient()
    result = client2.load_manual_cookies(test_cookies_file)
    print(f"   Load result: {result}")
    print(f"   Cookies loaded: {len(client2.session.cookies)}")
    
    # Cleanup
    os.unlink(test_cookies_file)
    
    print()

def test_manual_auth_flow():
    """Test manual authentication flow."""
    print("=" * 60)
    print("Testing Manual Auth Flow")
    print("=" * 60)
    
    # Set proxy via env
    os.environ['PROXY_URL'] = 'http://tIQnauShHLxaNdf:byZ8A47X8y0jk2Y@194.79.14.3:45264'
    
    client = CGTraderHTTPClient()
    
    # Check if proxy is configured
    if hasattr(client.session, 'proxies') and client.session.proxies:
        print(f"✅ Proxy configured")
    else:
        print("⚠️  Proxy not configured")
    
    # Check manual cookies
    if len(client.session.cookies) > 0:
        print(f"✅ Cookies loaded: {len(client.session.cookies)} cookies")
        for cookie in client.session.cookies:
            print(f"   - {cookie.name}")
    else:
        print("ℹ️  No cookies loaded (this is OK if manual cookies not set)")
    
    # Check CSRF token
    if client.csrf_token:
        print(f"✅ CSRF token set: {client.csrf_token[:20]}...")
    else:
        print("ℹ️  No CSRF token set (will be fetched on login)")
    
    print()

if __name__ == "__main__":
    test_proxy()
    test_manual_cookies()
    test_manual_auth_flow()
    print("=" * 60)
    print("Testing complete!")
    print("=" * 60)
