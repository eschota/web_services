#!/usr/bin/env python3
"""Quick auth test"""
import os
import sys
import json
from cgtrader_http import CGTraderHTTPClient

# Set CSRF token
os.environ['CGTRADER_CSRF_TOKEN'] = 'qsH2HAzaJG0Vs7-gXCAsp_EcP6lyUNujBuMP7W5KqkZWaPz2JXfX8JnkQenJoxq4mdmlFyiYoEKNzywrEv4AhA'

# Reload config
import importlib
import config
importlib.reload(config)

print("="*60)
print("CGTrader Authentication Test")
print("="*60)

# Load cookies
cookies_file = "db/cgtrader_cookies_manual.json"
print(f"\n📁 Loading cookies from {cookies_file}...")

with open(cookies_file, "r") as f:
    cookies_data = json.load(f)

print(f"✅ Cookies loaded: {list(cookies_data.keys())}")

# Create client
print("\n🔧 Creating HTTP client...")
client = CGTraderHTTPClient()

# Load cookies manually
print("🔑 Loading cookies into session...")
for name, value in cookies_data.items():
    client.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
    print(f"   ✅ {name}")

print(f"\n📊 Session info:")
print(f"   Total cookies: {len(client.session.cookies)}")
print(f"   Has _cgtrader_session_id: {bool(client.session.cookies.get('_cgtrader_session_id'))}")
print(f"   Has user_id: {bool(client.session.cookies.get('user_id'))}")
print(f"   CSRF token: {client.csrf_token[:20] if client.csrf_token else 'None'}...")

# Test authentication
print("\n🔐 Testing authentication...")
try:
    is_logged_in = client.is_logged_in()
    if is_logged_in:
        print("\n✅✅✅ SUCCESS! Authentication works! ✅✅✅")
        print("\n🎉 You are logged in to CGTrader!")
    else:
        print("\n❌ FAILED: Not authenticated")
        print("\nВозможные причины:")
        print("1. Cookies устарели (нужно войти заново)")
        print("2. Не все необходимые cookies скопированы")
        print("3. Проблемы с прокси/соединением")
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()
