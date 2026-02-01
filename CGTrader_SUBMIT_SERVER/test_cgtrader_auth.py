#!/usr/bin/env python3
"""
Test script for CGTrader authentication
"""
import sys
import time
from cgtrader_automation import CGTraderAutomation

def test_login():
    """Test CGTrader login."""
    print("=" * 60)
    print("CGTrader Authentication Test")
    print("=" * 60)
    
    automation = CGTraderAutomation()
    
    try:
        print("\n[1] Starting browser...")
        automation.start()
        
        print("\n[2] Attempting login...")
        success = automation.login()
        
        if success:
            print("\n✅ LOGIN SUCCESSFUL!")
            
            print("\n[3] Verifying login status...")
            if automation.is_logged_in():
                print("✅ Login verified - user is logged in")
            else:
                print("⚠️  Login reported success but verification failed")
            
            print("\n[4] Checking current URL...")
            current_url = automation.driver.current_url
            print(f"Current URL: {current_url}")
            
            # Try to navigate to profile
            print("\n[5] Navigating to profile page...")
            automation.driver.get("https://www.cgtrader.com/profile")
            time.sleep(3)
            
            profile_url = automation.driver.current_url
            print(f"Profile URL: {profile_url}")
            
            if "/profile" in profile_url and "/login" not in profile_url:
                print("✅ Profile page accessible - login confirmed")
            else:
                print("⚠️  Could not access profile page")
            
            # Check for user menu or profile elements
            print("\n[6] Checking for user elements on page...")
            try:
                page_source_snippet = automation.driver.page_source[:2000]
                if "logout" in page_source_snippet.lower() or "sign out" in page_source_snippet.lower():
                    print("✅ Found logout/sign out elements - user is logged in")
                else:
                    print("⚠️  Could not find logout elements")
            except Exception as e:
                print(f"⚠️  Error checking page: {e}")
            
        else:
            print("\n❌ LOGIN FAILED!")
            print("\n[3] Checking current URL...")
            current_url = automation.driver.current_url
            print(f"Current URL: {current_url}")
            
            # Check for error messages
            print("\n[4] Checking for error messages...")
            try:
                page_source = automation.driver.page_source
                if "error" in page_source.lower() or "incorrect" in page_source.lower():
                    print("⚠️  Found error messages on page")
                # Try to get screenshot or page source snippet
                print(f"Page title: {automation.driver.title}")
            except Exception as e:
                print(f"Error: {e}")
        
        print("\n" + "=" * 60)
        return success
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        print("\n[7] Closing browser...")
        automation.stop()
        print("Browser closed")

if __name__ == "__main__":
    success = test_login()
    sys.exit(0 if success else 1)
