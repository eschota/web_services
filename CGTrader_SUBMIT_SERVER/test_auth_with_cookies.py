#!/usr/bin/env python3
"""Test authentication with manually provided cookies"""
import os
import sys
import json
from cgtrader_http import CGTraderHTTPClient

def test_with_cookies_file():
    """Test authentication using cookies from file."""
    cookies_file = "db/cgtrader_cookies_manual.json"
    
    if not os.path.exists(cookies_file):
        print(f"❌ Cookies file not found: {cookies_file}")
        print("\nСоздайте файл с cookies одним из способов:")
        print("1. Скопируйте cookies из DevTools -> Application -> Cookies")
        print("2. Или используйте формат из COOKIES_EXPORT_GUIDE.md")
        return False
    
    print(f"📁 Loading cookies from {cookies_file}...")
    
    try:
        with open(cookies_file, "r") as f:
            cookies_data = json.load(f)
        print(f"✅ Cookies file loaded")
    except Exception as e:
        print(f"❌ Error loading cookies file: {e}")
        return False
    
    # Create client
    print("\n🔧 Creating HTTP client...")
    client = CGTraderHTTPClient()
    
    # Load cookies
    print("🔑 Loading cookies into session...")
    if isinstance(cookies_data, dict):
        # Simple format: {"cookie_name": "value"}
        for name, value in cookies_data.items():
            client.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
        print(f"✅ Loaded {len(cookies_data)} cookies")
    elif isinstance(cookies_data, list):
        # Array format: [{"name": "...", "value": "...", ...}]
        for cookie in cookies_data:
            client.session.cookies.set(
                cookie.get("name"),
                cookie.get("value"),
                domain=cookie.get("domain", ".cgtrader.com"),
                path=cookie.get("path", "/")
            )
        print(f"✅ Loaded {len(cookies_data)} cookies")
    else:
        print("❌ Invalid cookies format")
        return False
    
    # Test authentication
    print("\n🔐 Testing authentication...")
    try:
        is_logged_in = client.is_logged_in()
        if is_logged_in:
            print("✅ SUCCESS! Authentication works!")
            print("\n📊 Session info:")
            print(f"   Cookies in session: {len(client.session.cookies)}")
            print(f"   Has session_id: {bool(client.session.cookies.get('_cgtrader_session_id'))}")
            print(f"   Has user_id: {bool(client.session.cookies.get('user_id'))}")
            return True
        else:
            print("❌ FAILED: Not authenticated")
            print("\nВозможные причины:")
            print("1. Cookies устарели (нужно войти заново)")
            print("2. Не все необходимые cookies скопированы")
            print("3. Cookies недействительны")
            return False
    except Exception as e:
        print(f"❌ Error testing authentication: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_with_cookie_string():
    """Test authentication using cookie string from Network tab."""
    print("\n" + "="*60)
    print("Тест с Cookie строкой из Network tab")
    print("="*60)
    
    cookie_string = input("\nВставьте Cookie строку из Network -> Headers -> Request Headers -> Cookie:\n> ").strip()
    
    if not cookie_string:
        print("❌ Cookie string is empty")
        return False
    
    print("\n🔧 Creating HTTP client...")
    client = CGTraderHTTPClient()
    
    # Parse cookie string
    print("🔑 Parsing cookies...")
    cookies_dict = {}
    for cookie_pair in cookie_string.split(';'):
        cookie_pair = cookie_pair.strip()
        if '=' in cookie_pair:
            name, value = cookie_pair.split('=', 1)
            name = name.strip()
            value = value.strip()
            cookies_dict[name] = value
            client.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
    
    print(f"✅ Parsed {len(cookies_dict)} cookies")
    
    # Test authentication
    print("\n🔐 Testing authentication...")
    try:
        is_logged_in = client.is_logged_in()
        if is_logged_in:
            print("✅ SUCCESS! Authentication works!")
            return True
        else:
            print("❌ FAILED: Not authenticated")
            return False
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("="*60)
    print("CGTrader Authentication Test")
    print("="*60)
    
    if len(sys.argv) > 1 and sys.argv[1] == "string":
        # Test with cookie string
        test_with_cookie_string()
    else:
        # Test with cookies file
        success = test_with_cookies_file()
        
        if not success:
            print("\n" + "="*60)
            print("Альтернативный способ: тест с Cookie строкой")
            print("="*60)
            print("Запустите: python3 test_auth_with_cookies.py string")
            print("И вставьте Cookie строку из Network tab")
