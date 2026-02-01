#!/usr/bin/env python3
"""Test Chrome with webdriver-manager"""
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

options = Options()
options.binary_location = "/snap/bin/chromium"
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")

print("Getting chromedriver from webdriver-manager...")
service = Service(ChromeDriverManager().install())

print("Creating driver...")
try:
    driver = webdriver.Chrome(service=service, options=options)
    print("Driver created!")
    
    print("Navigating to Google...")
    driver.get("https://www.google.com")
    print(f"Title: {driver.title}")
    print("✅ Chrome works!")
    
    driver.quit()
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
