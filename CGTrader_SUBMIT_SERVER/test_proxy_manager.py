#!/usr/bin/env python3
"""Test script for proxy manager"""
import os
import time
from proxy_manager import ProxyManager, ProxyChecker, get_proxy_manager

def test_proxy_parsing():
    """Test proxy line parsing."""
    print("=" * 60)
    print("Testing Proxy Parsing")
    print("=" * 60)
    
    manager = ProxyManager()
    
    test_lines = [
        "192.168.1.1:8080",
        "user:pass@192.168.1.1:8080",
        "http://192.168.1.1:8080",
        "socks5://192.168.1.1:1080",
        "192.168.1.1:8080 # comment",
        "# comment only",
        "",
        "invalid",
    ]
    
    for line in test_lines:
        result = manager._parse_proxy_line(line)
        print(f"  '{line}' -> {result}")
    
    print()

def test_proxy_checker():
    """Test proxy checker with a known proxy (will likely fail, but test structure)."""
    print("=" * 60)
    print("Testing Proxy Checker")
    print("=" * 60)
    
    # Test with a fake proxy (will fail, but tests the checker)
    test_proxy = "http://1.2.3.4:8080"
    print(f"Testing proxy: {test_proxy}")
    is_working, error = ProxyChecker.check_proxy(test_proxy)
    print(f"  Result: {'✅ Working' if is_working else '❌ Failed'}")
    if error:
        print(f"  Error: {error}")
    print()

def test_proxy_manager_basic():
    """Test basic proxy manager functionality."""
    print("=" * 60)
    print("Testing Proxy Manager (Basic)")
    print("=" * 60)
    
    manager = ProxyManager(max_working_proxies=5, check_interval=60)
    print(f"✅ Manager created")
    print(f"   Sources: {len(manager.PROXY_SOURCES)}")
    print(f"   Max working proxies: {manager.max_working_proxies}")
    print(f"   Check interval: {manager.check_interval}s")
    print()

def test_fetch_proxies():
    """Test fetching proxies from one source."""
    print("=" * 60)
    print("Testing Proxy Fetching (one source)")
    print("=" * 60)
    
    manager = ProxyManager()
    # Test with one source
    test_url = manager.PROXY_SOURCES[0]
    print(f"Fetching from: {test_url}")
    
    proxies = manager.fetch_proxies_from_url(test_url)
    print(f"✅ Fetched {len(proxies)} proxies")
    if proxies:
        print(f"   Sample: {proxies[:3]}")
    print()

def test_refresh_working_proxies():
    """Test refreshing working proxies (limit to 3 for speed)."""
    print("=" * 60)
    print("Testing Proxy Refresh (limited to 3 working)")
    print("=" * 60)
    print("This will take a while...")
    
    manager = ProxyManager(max_working_proxies=3)
    count = manager.refresh_working_proxies(max_proxies=3)
    print(f"✅ Found {count} working proxies")
    if manager.working_proxies:
        print(f"   Working proxies: {[p.split('@')[-1] if '@' in p else p for p in manager.working_proxies[:3]]}")
    print()

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "full":
        # Run full test (slow)
        test_proxy_parsing()
        test_proxy_checker()
        test_proxy_manager_basic()
        test_fetch_proxies()
        test_refresh_working_proxies()
    else:
        # Quick tests only
        print("Running quick tests (use 'python3 test_proxy_manager.py full' for full test)")
        print()
        test_proxy_parsing()
        test_proxy_checker()
        test_proxy_manager_basic()
        test_fetch_proxies()
        print("Skipping slow refresh test. Run with 'full' argument to test proxy refresh.")
