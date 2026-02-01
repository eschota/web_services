"""
CGTrader browser automation module.
Handles login, file upload, and form filling using Selenium.
"""
import os
import json
import time
from pathlib import Path
from typing import Dict, Any, Optional, List

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, NoSuchElementException

from config import (
    CHROME_BINARY_PATH, CHROMEDRIVER_PATH, CHROME_OPTIONS_WITH_JS,
    CGTRADER_EMAIL, CGTRADER_PASSWORD, CGTRADER_LOGIN_URL, 
    CGTRADER_UPLOAD_URL, COOKIES_PATH
)


class CGTraderAutomation:
    """CGTrader website automation using Selenium."""
    
    def __init__(self):
        self.driver: Optional[webdriver.Chrome] = None
        self.wait: Optional[WebDriverWait] = None
    
    def _create_driver(self) -> webdriver.Chrome:
        """Create memory-optimized Chrome driver."""
        options = Options()
        
        # Set Chrome binary path
        options.binary_location = CHROME_BINARY_PATH
        
        # Apply memory-optimized options (remove --single-process as it can cause issues)
        for opt in CHROME_OPTIONS_WITH_JS:
            if opt != "--single-process":  # Skip single-process for stability
                options.add_argument(opt)
        
        # Additional preferences
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Disable images except when uploading
            "disk-cache-size": 52428800,  # 50MB cache
        }
        options.add_experimental_option("prefs", prefs)
        
        # Remove user data dir to avoid permission issues
        options.add_argument("--user-data-dir=/tmp/chrome-user-data")
        
        # Create service - try webdriver-manager if chromedriver fails
        try:
            service = Service(executable_path=CHROMEDRIVER_PATH)
        except:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
        
        # Create driver
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(60)
        
        return driver
    
    def start(self):
        """Start the browser."""
        if self.driver is None:
            self.driver = self._create_driver()
            self.wait = WebDriverWait(self.driver, 30)
            print("[CGTrader] Browser started")
    
    def stop(self):
        """Stop the browser and free memory."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception as e:
                print(f"[CGTrader] Error stopping browser: {e}")
            finally:
                self.driver = None
                self.wait = None
            print("[CGTrader] Browser stopped")
    
    def _save_cookies(self):
        """Save session cookies for reuse."""
        if self.driver:
            cookies = self.driver.get_cookies()
            with open(COOKIES_PATH, "w") as f:
                json.dump(cookies, f)
            print(f"[CGTrader] Cookies saved to {COOKIES_PATH}")
    
    def _load_cookies(self) -> bool:
        """Load saved cookies if available."""
        if not os.path.exists(COOKIES_PATH):
            return False
        
        try:
            with open(COOKIES_PATH) as f:
                cookies = json.load(f)
            
            # Navigate to CGTrader to set domain
            self.driver.get("https://www.cgtrader.com")
            time.sleep(2)
            
            # Add cookies
            for cookie in cookies:
                try:
                    # Remove problematic fields
                    cookie.pop("sameSite", None)
                    cookie.pop("expiry", None)
                    self.driver.add_cookie(cookie)
                except Exception as e:
                    print(f"[CGTrader] Cookie error: {e}")
            
            print("[CGTrader] Cookies loaded")
            return True
        except Exception as e:
            print(f"[CGTrader] Failed to load cookies: {e}")
            return False
    
    def is_logged_in(self) -> bool:
        """Check if currently logged in."""
        try:
            self.driver.get("https://www.cgtrader.com/profile")
            time.sleep(3)
            
            # Check if redirected to login
            if "/login" in self.driver.current_url:
                return False
            
            # Check for profile elements
            try:
                self.driver.find_element(By.CSS_SELECTOR, "[data-testid='user-menu']")
                return True
            except NoSuchElementException:
                pass
            
            # Alternative: check for dashboard link
            try:
                self.driver.find_element(By.LINK_TEXT, "Dashboard")
                return True
            except NoSuchElementException:
                pass
            
            return False
        except Exception as e:
            print(f"[CGTrader] Login check error: {e}")
            return False
    
    def login(self) -> bool:
        """Login to CGTrader."""
        self.start()
        
        # Try cookies first
        if self._load_cookies():
            if self.is_logged_in():
                print("[CGTrader] Already logged in via cookies")
                return True
        
        print("[CGTrader] Performing fresh login...")
        
        try:
            # Navigate to login page
            self.driver.get(CGTRADER_LOGIN_URL)
            time.sleep(3)
            
            # Accept cookies if banner appears
            try:
                cookie_btn = self.driver.find_element(By.ID, "onetrust-accept-btn-handler")
                cookie_btn.click()
                time.sleep(1)
            except NoSuchElementException:
                pass
            
            # Find and fill email
            email_input = self.wait.until(
                EC.presence_of_element_located((By.NAME, "email"))
            )
            email_input.clear()
            email_input.send_keys(CGTRADER_EMAIL)
            
            # Find and fill password
            password_input = self.driver.find_element(By.NAME, "password")
            password_input.clear()
            password_input.send_keys(CGTRADER_PASSWORD)
            
            # Click login button
            login_btn = self.driver.find_element(By.CSS_SELECTOR, "button[type='submit']")
            login_btn.click()
            
            # Wait for redirect (successful login)
            time.sleep(5)
            
            # Check if login was successful
            if "/login" not in self.driver.current_url:
                print("[CGTrader] Login successful")
                self._save_cookies()
                return True
            else:
                # Check for error message
                try:
                    error = self.driver.find_element(By.CSS_SELECTOR, ".alert-danger, .error-message")
                    print(f"[CGTrader] Login failed: {error.text}")
                except NoSuchElementException:
                    print("[CGTrader] Login failed: Unknown error")
                return False
                
        except TimeoutException:
            print("[CGTrader] Login timeout")
            return False
        except Exception as e:
            print(f"[CGTrader] Login error: {e}")
            return False
    
    def upload_files(self, folder_path: str) -> bool:
        """
        Upload files to CGTrader batch upload page.
        
        Args:
            folder_path: Path to the extracted model folder
        """
        try:
            # Navigate to batch upload
            self.driver.get(CGTRADER_UPLOAD_URL)
            time.sleep(5)
            
            # Accept cookies if needed
            try:
                cookie_btn = self.driver.find_element(By.ID, "onetrust-accept-btn-handler")
                cookie_btn.click()
                time.sleep(1)
            except NoSuchElementException:
                pass
            
            # Find the file input element
            # CGTrader uses a hidden file input that we can interact with
            file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            
            if not file_inputs:
                print("[CGTrader] No file input found, trying alternative method...")
                # Try to find dropzone and trigger file dialog
                dropzone = self.driver.find_element(By.CSS_SELECTOR, "[data-dropzone], .dropzone, .upload-area")
                # Make hidden input visible via JS
                self.driver.execute_script("""
                    var inputs = document.querySelectorAll('input[type="file"]');
                    inputs.forEach(function(input) {
                        input.style.display = 'block';
                        input.style.opacity = '1';
                        input.style.position = 'relative';
                    });
                """)
                time.sleep(1)
                file_inputs = self.driver.find_elements(By.CSS_SELECTOR, "input[type='file']")
            
            if not file_inputs:
                raise Exception("Could not find file input element")
            
            file_input = file_inputs[0]
            
            # Collect all files from the folder
            folder = Path(folder_path)
            files_to_upload = []
            
            for file_path in folder.rglob("*"):
                if file_path.is_file():
                    files_to_upload.append(str(file_path.absolute()))
            
            if not files_to_upload:
                raise Exception(f"No files found in {folder_path}")
            
            print(f"[CGTrader] Uploading {len(files_to_upload)} files...")
            
            # Upload files (join paths with newline for multiple files)
            file_input.send_keys("\n".join(files_to_upload))
            
            # Wait for upload to complete
            time.sleep(10)  # Initial wait
            
            # Wait for progress to finish (look for progress bars)
            max_wait = 300  # 5 minutes max
            start_time = time.time()
            
            while time.time() - start_time < max_wait:
                try:
                    # Check if upload is still in progress
                    progress = self.driver.find_elements(By.CSS_SELECTOR, ".progress, .uploading, [data-uploading='true']")
                    if not progress:
                        break
                    print("[CGTrader] Upload in progress...")
                except:
                    pass
                time.sleep(5)
            
            print("[CGTrader] Files uploaded")
            return True
            
        except Exception as e:
            print(f"[CGTrader] Upload error: {e}")
            raise
    
    def fill_form(self, metadata: Dict[str, Any]) -> bool:
        """
        Fill the model details form with metadata.
        
        Args:
            metadata: Dictionary with title, description, tags, etc.
        """
        try:
            time.sleep(3)
            
            # Title
            title_input = self.wait.until(
                EC.presence_of_element_located((By.NAME, "title"))
            )
            title_input.clear()
            title_input.send_keys(metadata.get("title", "3D Model"))
            
            # Description
            desc_inputs = self.driver.find_elements(By.NAME, "description")
            if desc_inputs:
                desc_inputs[0].clear()
                desc_inputs[0].send_keys(metadata.get("description", "High-quality 3D model."))
            else:
                # Try textarea
                desc_textarea = self.driver.find_element(By.CSS_SELECTOR, "textarea[name='description'], textarea.description")
                desc_textarea.clear()
                desc_textarea.send_keys(metadata.get("description", "High-quality 3D model."))
            
            # Tags
            tags = metadata.get("tags", ["3d-model"])
            tags_input = self.driver.find_elements(By.CSS_SELECTOR, "input[name='tags'], .tags-input input")
            if tags_input:
                for tag in tags[:10]:  # Max 10 tags
                    tags_input[0].send_keys(tag)
                    tags_input[0].send_keys("\n")
                    time.sleep(0.3)
            
            # Polygons
            poly_input = self.driver.find_elements(By.NAME, "polygons")
            if poly_input:
                poly_input[0].clear()
                poly_input[0].send_keys(str(metadata.get("polygons", 100000)))
            
            # Vertices
            vert_input = self.driver.find_elements(By.NAME, "vertices")
            if vert_input:
                vert_input[0].clear()
                vert_input[0].send_keys(str(metadata.get("vertices", 100000)))
            
            # Category dropdown
            try:
                category = metadata.get("category", "Character")
                category_select = self.driver.find_element(By.NAME, "category")
                for option in category_select.find_elements(By.TAG_NAME, "option"):
                    if category.lower() in option.text.lower():
                        option.click()
                        break
            except NoSuchElementException:
                print("[CGTrader] Category select not found")
            
            # Subcategory
            try:
                subcategory = metadata.get("subcategory", "Man")
                time.sleep(1)  # Wait for subcategory to load
                subcat_select = self.driver.find_element(By.NAME, "subcategory")
                for option in subcat_select.find_elements(By.TAG_NAME, "option"):
                    if subcategory.lower() in option.text.lower():
                        option.click()
                        break
            except NoSuchElementException:
                print("[CGTrader] Subcategory select not found")
            
            # Price
            price_input = self.driver.find_elements(By.NAME, "price")
            if price_input:
                price_input[0].clear()
                price_input[0].send_keys(str(metadata.get("suggested_price", 37)))
            
            # License (usually a dropdown or radio)
            try:
                license_options = self.driver.find_elements(
                    By.CSS_SELECTOR, 
                    "input[name='license'], select[name='license'] option"
                )
                for opt in license_options:
                    if "royalty" in opt.get_attribute("value", "").lower() or "royalty" in opt.text.lower():
                        opt.click()
                        break
            except:
                pass
            
            print("[CGTrader] Form filled")
            return True
            
        except Exception as e:
            print(f"[CGTrader] Form fill error: {e}")
            raise
    
    def publish(self) -> Optional[str]:
        """
        Click publish/submit button and return the product URL.
        
        Returns:
            Product URL if successful, None otherwise
        """
        try:
            # Find publish button
            publish_selectors = [
                "button[type='submit']",
                "button.publish",
                "button.submit",
                "input[type='submit']",
                "[data-action='publish']",
                "button:contains('Publish')",
                "button:contains('Submit')",
            ]
            
            publish_btn = None
            for selector in publish_selectors:
                try:
                    btns = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for btn in btns:
                        if "publish" in btn.text.lower() or "submit" in btn.text.lower():
                            publish_btn = btn
                            break
                    if publish_btn:
                        break
                except:
                    pass
            
            if not publish_btn:
                # Use XPath as fallback
                publish_btn = self.driver.find_element(
                    By.XPATH, 
                    "//button[contains(text(), 'Publish')] | //button[contains(text(), 'Submit')] | //button[@type='submit']"
                )
            
            # Click publish
            publish_btn.click()
            
            # Wait for redirect to product page
            time.sleep(10)
            
            # Get the product URL
            current_url = self.driver.current_url
            
            # Check if we're on a product page
            if "/3d-models/" in current_url or "/profile/models" in current_url:
                print(f"[CGTrader] Published successfully: {current_url}")
                return current_url
            
            # Check for success message
            try:
                success = self.driver.find_element(By.CSS_SELECTOR, ".alert-success, .success-message")
                print(f"[CGTrader] Published: {success.text}")
                return current_url
            except NoSuchElementException:
                pass
            
            print(f"[CGTrader] Publish completed, URL: {current_url}")
            return current_url
            
        except Exception as e:
            print(f"[CGTrader] Publish error: {e}")
            raise
    
    def full_upload_flow(self, folder_path: str, metadata: Dict[str, Any]) -> Optional[str]:
        """
        Complete upload flow: login, upload, fill form, publish.
        
        Args:
            folder_path: Path to extracted model folder
            metadata: Model metadata
            
        Returns:
            Product URL if successful
        """
        try:
            # Login if needed
            if not self.login():
                raise Exception("Failed to login to CGTrader")
            
            # Upload files
            if not self.upload_files(folder_path):
                raise Exception("Failed to upload files")
            
            # Fill form
            if not self.fill_form(metadata):
                raise Exception("Failed to fill form")
            
            # Publish
            product_url = self.publish()
            
            return product_url
            
        finally:
            # Always close browser to free memory
            self.stop()


# Singleton instance
_automation: Optional[CGTraderAutomation] = None


def get_automation() -> CGTraderAutomation:
    """Get or create automation instance."""
    global _automation
    if _automation is None:
        _automation = CGTraderAutomation()
    return _automation
