"""
CGTrader HTTP API client.
Replaces Selenium automation with direct HTTP requests.
"""
import os
import json
import re
import time
from pathlib import Path
from typing import Optional, Dict, Any, List
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Try to import cloudscraper for Cloudflare bypass
try:
    import cloudscraper
    HAS_CLOUDSCRAPER = True
except ImportError:
    HAS_CLOUDSCRAPER = False

from config import (
    CGTRADER_EMAIL, CGTRADER_PASSWORD,
    CGTRADER_LOGIN_URL, CGTRADER_UPLOAD_URL,
    COOKIES_PATH, MANUAL_COOKIES_PATH,
    PROXY_URL, parse_proxy_url,
    CGTRADER_CSRF_TOKEN, CGTRADER_SESSION_COOKIE, CGTRADER_AUTH_TOKEN,
    ENABLE_AUTO_PROXY, PROXY_CHECK_INTERVAL, MAX_WORKING_PROXIES
)
from proxy_manager import get_proxy_manager


class CGTraderHTTPClient:
    """HTTP client for CGTrader API operations."""
    
    def __init__(self):
        self.base_url = "https://www.cgtrader.com"
        
        # Configure proxy
        proxy_config = None
        proxy_url = None
        
        # Priority: 1) Manual PROXY_URL, 2) Auto proxy manager
        if PROXY_URL:
            proxy_config = parse_proxy_url(PROXY_URL)
            if proxy_config:
                proxy_url = PROXY_URL
                print(f"[CGTrader HTTP] Using manual proxy: {PROXY_URL.split('@')[0]}@***")
            else:
                print(f"[CGTrader HTTP] Warning: Invalid proxy URL format: {PROXY_URL}")
        elif ENABLE_AUTO_PROXY:
            # Initialize proxy manager
            self.proxy_manager = get_proxy_manager(
                check_interval=PROXY_CHECK_INTERVAL,
                max_working_proxies=MAX_WORKING_PROXIES
            )
            # Ensure we have a working proxy
            if self.proxy_manager.ensure_working_proxy():
                proxy_url = self.proxy_manager.get_current_proxy()
                if proxy_url:
                    proxy_config = parse_proxy_url(proxy_url)
                    print(f"[CGTrader HTTP] Using auto proxy: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
            # Start background checking
            self.proxy_manager.start()
        else:
            self.proxy_manager = None
        
        # Use cloudscraper if available (bypasses Cloudflare)
        # Note: cloudscraper may not support proxies well, use requests if proxy is needed
        if HAS_CLOUDSCRAPER and not proxy_config:
            self.session = cloudscraper.create_scraper()
            print("[CGTrader HTTP] Using cloudscraper for Cloudflare bypass")
        else:
            self.session = requests.Session()
            if proxy_config:
                self.session.proxies.update(proxy_config)
                print("[CGTrader HTTP] Using requests with proxy (cloudscraper skipped when proxy is used)")
            else:
                print("[CGTrader HTTP] Using requests (cloudscraper not available or proxy used)")
        
        self.csrf_token: Optional[str] = None
        self._proxy_url = proxy_url  # Store for rotation
        
        # Set realistic browser headers
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        })
        
        # Load manual cookies first (if provided)
        self._load_manual_cookies()
        
        # Set manual CSRF token if provided
        if CGTRADER_CSRF_TOKEN:
            self.csrf_token = CGTRADER_CSRF_TOKEN
            print("[CGTrader HTTP] Using manual CSRF token from config")
        
        # Load regular cookies if available (fallback)
        self._load_cookies()
    
    def _load_manual_cookies(self) -> bool:
        """Load manually provided cookies from file or env (private method called from __init__)."""
        # Try to load from file first
        if os.path.exists(MANUAL_COOKIES_PATH):
            try:
                with open(MANUAL_COOKIES_PATH, "r") as f:
                    cookies_data = json.load(f)
                
                # Handle different formats
                if isinstance(cookies_data, list):
                    # Format: [{"name": "...", "value": "...", ...}, ...]
                    for cookie in cookies_data:
                        self.session.cookies.set(
                            cookie.get("name"),
                            cookie.get("value"),
                            domain=cookie.get("domain", ".cgtrader.com"),
                            path=cookie.get("path", "/")
                        )
                elif isinstance(cookies_data, dict):
                    # Format: {"cookie_name": "value", ...}
                    for name, value in cookies_data.items():
                        self.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
                
                print(f"[CGTrader HTTP] Loaded manual cookies from {MANUAL_COOKIES_PATH}")
                return True
            except Exception as e:
                print(f"[CGTrader HTTP] Failed to load manual cookies from file: {e}")
        
        # Try to load from env variable (single session cookie)
        if CGTRADER_SESSION_COOKIE:
            try:
                # Try to parse as JSON first
                try:
                    cookie_dict = json.loads(CGTRADER_SESSION_COOKIE)
                    if isinstance(cookie_dict, dict):
                        for name, value in cookie_dict.items():
                            self.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
                    else:
                        # Single cookie value, assume common names
                        self.session.cookies.set("_session_id", CGTRADER_SESSION_COOKIE, domain=".cgtrader.com", path="/")
                except (json.JSONDecodeError, ValueError):
                    # Not JSON, assume it's a single cookie value
                    self.session.cookies.set("_session_id", CGTRADER_SESSION_COOKIE, domain=".cgtrader.com", path="/")
                
                print("[CGTrader HTTP] Loaded manual session cookie from env")
                return True
            except Exception as e:
                print(f"[CGTrader HTTP] Failed to load manual cookie from env: {e}")
        
        return False
    
    def _load_cookies(self) -> bool:
        """Load saved cookies from file."""
        if not os.path.exists(COOKIES_PATH):
            return False
        
        try:
            with open(COOKIES_PATH, "r") as f:
                cookies = json.load(f)
            
            # Convert cookie dict to CookieJar
            for cookie in cookies:
                self.session.cookies.set(
                    cookie.get("name"),
                    cookie.get("value"),
                    domain=cookie.get("domain", ".cgtrader.com"),
                    path=cookie.get("path", "/")
                )
            
            print(f"[CGTrader HTTP] Loaded cookies from {COOKIES_PATH}")
            return True
        except Exception as e:
            print(f"[CGTrader HTTP] Failed to load cookies: {e}")
            return False
    
    def _save_cookies(self):
        """Save session cookies to file."""
        try:
            cookies = []
            for cookie in self.session.cookies:
                cookies.append({
                    "name": cookie.name,
                    "value": cookie.value,
                    "domain": cookie.domain,
                    "path": cookie.path,
                    "secure": cookie.secure,
                })
            
            with open(COOKIES_PATH, "w") as f:
                json.dump(cookies, f, indent=2)
            
            print(f"[CGTrader HTTP] Saved cookies to {COOKIES_PATH}")
        except Exception as e:
            print(f"[CGTrader HTTP] Failed to save cookies: {e}")
    
    def set_manual_auth(self, csrf_token: Optional[str] = None, cookies: Optional[Dict[str, str]] = None):
        """
        Set manual authentication data (CSRF token and/or cookies).
        
        Args:
            csrf_token: CSRF token string
            cookies: Dictionary of cookie name -> value pairs
        """
        if csrf_token:
            self.csrf_token = csrf_token
            print("[CGTrader HTTP] Manual CSRF token set")
        
        if cookies:
            for name, value in cookies.items():
                self.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
            print(f"[CGTrader HTTP] Manual cookies set: {len(cookies)} cookies")
    
    def load_manual_cookies(self, cookies_file: str) -> bool:
        """
        Load cookies from a JSON file (exported from browser).
        
        Args:
            cookies_file: Path to JSON file with cookies
            
        Returns:
            True if loaded successfully
        """
        if not os.path.exists(cookies_file):
            print(f"[CGTrader HTTP] Cookies file not found: {cookies_file}")
            return False
        
        try:
            with open(cookies_file, "r") as f:
                cookies_data = json.load(f)
            
            # Handle different formats
            if isinstance(cookies_data, list):
                # Format: [{"name": "...", "value": "...", ...}, ...]
                for cookie in cookies_data:
                    self.session.cookies.set(
                        cookie.get("name"),
                        cookie.get("value"),
                        domain=cookie.get("domain", ".cgtrader.com"),
                        path=cookie.get("path", "/")
                    )
            elif isinstance(cookies_data, dict):
                # Format: {"cookie_name": "value", ...}
                for name, value in cookies_data.items():
                    self.session.cookies.set(name, value, domain=".cgtrader.com", path="/")
            
            print(f"[CGTrader HTTP] Loaded cookies from {cookies_file}")
            return True
            
        except Exception as e:
            print(f"[CGTrader HTTP] Failed to load cookies from {cookies_file}: {e}")
            return False
    
    def get_csrf_token(self) -> Optional[str]:
        """Get CSRF token from login page."""
        # If manual token is already set, use it
        if self.csrf_token:
            return self.csrf_token
        
        # Update proxy before request
        self._update_proxy()
        
        try:
            print("[CGTrader HTTP] Getting CSRF token...")
            response = self.session.get(CGTRADER_LOGIN_URL, timeout=30)
            response.raise_for_status()
            
            # Parse HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Try different methods to find CSRF token
            csrf_token = None
            
            # Method 1: Look for input[name="authenticity_token"] or similar
            csrf_inputs = soup.find_all("input", {
                "name": re.compile(r".*csrf.*|.*token.*|.*authenticity.*", re.I)
            })
            if csrf_inputs:
                csrf_token = csrf_inputs[0].get("value")
                print(f"[CGTrader HTTP] Found CSRF token in input: {csrf_token[:20]}...")
            
            # Method 2: Look for meta tag
            if not csrf_token:
                meta_csrf = soup.find("meta", {"name": re.compile(r".*csrf.*", re.I)})
                if meta_csrf:
                    csrf_token = meta_csrf.get("content")
                    print(f"[CGTrader HTTP] Found CSRF token in meta tag: {csrf_token[:20]}...")
            
            # Method 3: Look in script tags (some sites generate tokens in JS)
            if not csrf_token:
                scripts = soup.find_all("script")
                for script in scripts:
                    if script.string:
                        # Look for patterns like csrf_token: "value" or CSRF_TOKEN = "value"
                        match = re.search(r'csrf[_-]?token["\']?\s*[:=]\s*["\']([^"\']+)', script.string, re.I)
                        if match:
                            csrf_token = match.group(1)
                            print(f"[CGTrader HTTP] Found CSRF token in script: {csrf_token[:20]}...")
                            break
            
            # Method 4: Look in response headers
            if not csrf_token:
                for header_name in response.headers:
                    if "csrf" in header_name.lower() or "token" in header_name.lower():
                        csrf_token = response.headers[header_name]
                        print(f"[CGTrader HTTP] Found CSRF token in header {header_name}: {csrf_token[:20]}...")
                        break
            
            if csrf_token:
                self.csrf_token = csrf_token
                return csrf_token
            else:
                print("[CGTrader HTTP] Warning: Could not find CSRF token, will try without it")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"[CGTrader HTTP] Error getting CSRF token: {e}")
            # Try to rotate proxy and retry once
            if self._handle_proxy_error(e):
                try:
                    response = self.session.get(CGTRADER_LOGIN_URL, timeout=30)
                    response.raise_for_status()
                    # Continue parsing...
                    soup = BeautifulSoup(response.text, "html.parser")
                    csrf_token = None
                    # ... (rest of parsing logic would go here)
                except Exception as retry_error:
                    print(f"[CGTrader HTTP] Retry after proxy rotation failed: {retry_error}")
            return None
        except Exception as e:
            print(f"[CGTrader HTTP] Error getting CSRF token: {e}")
            return None
    
    def login(self) -> bool:
        """Login to CGTrader via HTTP."""
        try:
            # Check if we have valid manual cookies first
            if self._has_valid_cookies():
                print("[CGTrader HTTP] Valid manual cookies found, skipping automatic login")
                return True
            
            # Get CSRF token (will use manual token if set)
            self.get_csrf_token()
            
            print("[CGTrader HTTP] Attempting login...")
            
            # Prepare login data
            login_data = {
                "email": CGTRADER_EMAIL,
                "password": CGTRADER_PASSWORD,
            }
            
            # Add CSRF token if found
            if self.csrf_token:
                # Try common field names
                login_data["authenticity_token"] = self.csrf_token
                login_data["_token"] = self.csrf_token
                login_data["csrf_token"] = self.csrf_token
            
            # Update headers for POST
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": CGTRADER_LOGIN_URL,
                "Origin": self.base_url,
            }
            
            # Try POST to /login or /sessions
            login_urls = [
                CGTRADER_LOGIN_URL,
                urljoin(self.base_url, "/sessions"),
                urljoin(self.base_url, "/api/login"),
                urljoin(self.base_url, "/users/sign_in"),
            ]
            
            for login_url in login_urls:
                try:
                    print(f"[CGTrader HTTP] Trying POST to {login_url}...")
                    response = self.session.post(
                        login_url,
                        data=login_data,
                        headers=headers,
                        timeout=30,
                        allow_redirects=False  # Don't follow redirects, check status
                    )
                    
                    # Check if login was successful
                    # Usually 302 redirect means success, 200 might mean error page
                    if response.status_code == 302:
                        # Check redirect location
                        location = response.headers.get("Location", "")
                        if "/login" not in location.lower():
                            print(f"[CGTrader HTTP] Login successful! Redirected to: {location}")
                            self._save_cookies()
                            return True
                    
                    # If we get cookies, might be successful
                    if self.session.cookies.get("session") or self.session.cookies.get("_session_id"):
                        print("[CGTrader HTTP] Login successful (session cookie found)")
                        self._save_cookies()
                        return True
                    
                    # Check response content for errors
                    if "error" in response.text.lower() or "invalid" in response.text.lower():
                        print(f"[CGTrader HTTP] Login failed: Error in response")
                        continue
                        
                except requests.exceptions.RequestException as e:
                    print(f"[CGTrader HTTP] Request to {login_url} failed: {e}")
                    # Try to rotate proxy if error
                    if self._handle_proxy_error(e):
                        continue  # Retry with new proxy
                    continue
            
            print("[CGTrader HTTP] Login failed: All attempts failed")
            return False
            
        except Exception as e:
            print(f"[CGTrader HTTP] Login error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def is_logged_in(self) -> bool:
        """Check if currently logged in by accessing profile page."""
        # Update proxy before request
        self._update_proxy()
        
        try:
            response = self.session.get(
                urljoin(self.base_url, "/profile"),
                timeout=30,
                allow_redirects=False
            )
            
            # If redirected to login, we're not logged in
            if response.status_code == 302:
                location = response.headers.get("Location", "")
                if "/login" in location.lower():
                    return False
            
            # If we get the profile page (200), we're logged in
            if response.status_code == 200:
                # Check if page contains profile elements
                if "profile" in response.text.lower() and "logout" in response.text.lower():
                    return True
            
            return False
            
        except Exception as e:
            print(f"[CGTrader HTTP] Error checking login status: {e}")
            return False
    
    def _has_valid_cookies(self) -> bool:
        """Check if we have valid authentication cookies."""
        # Check for common session cookie names
        session_cookies = [
            "_session_id",
            "session",
            "_session",
            "remember_token",
            "auth_token",
        ]
        
        for cookie_name in session_cookies:
            if self.session.cookies.get(cookie_name):
                # Try to verify by checking login status
                if self.is_logged_in():
                    return True
        
        return False
    
    def _update_proxy(self):
        """Update proxy from proxy manager if auto proxy is enabled."""
        if hasattr(self, 'proxy_manager') and self.proxy_manager and ENABLE_AUTO_PROXY:
            proxy_url = self.proxy_manager.get_current_proxy()
            if proxy_url and proxy_url != self._proxy_url:
                proxy_config = parse_proxy_url(proxy_url)
                if proxy_config:
                    self.session.proxies.update(proxy_config)
                    self._proxy_url = proxy_url
                    print(f"[CGTrader HTTP] Proxy rotated to: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
    
    def _handle_proxy_error(self, error: Exception) -> bool:
        """
        Handle proxy-related errors by rotating proxy.
        
        Returns:
            True if proxy was rotated, False otherwise
        """
        error_str = str(error).lower()
        if any(keyword in error_str for keyword in ['proxy', 'connection', 'timeout', 'refused', '403', 'forbidden']):
            if hasattr(self, 'proxy_manager') and self.proxy_manager and ENABLE_AUTO_PROXY:
                logger.warning(f"[CGTrader HTTP] Proxy error detected, rotating proxy...")
                if self.proxy_manager.check_and_rotate_proxy():
                    self._update_proxy()
                    return True
        return False
    
    def upload_files(self, folder_path: str) -> Optional[str]:
        """
        Upload files to CGTrader batch upload.
        
        Args:
            folder_path: Path to prepared folder (with preview images and zip archives)
            
        Returns:
            Draft ID or upload ID if successful, None otherwise
        """
        # Update proxy before request
        self._update_proxy()
        
        try:
            print(f"[CGTrader HTTP] Starting file upload from {folder_path}")
            
            # First, get the upload page to understand the API
            response = self.session.get(CGTRADER_UPLOAD_URL, timeout=30)
            response.raise_for_status()
            
            # Parse the page to find upload endpoint
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Try to find upload form or API endpoint
            # Common patterns: data-upload-url, action attribute, or API endpoint in JS
            
            # Method 1: Check for data attributes
            upload_form = soup.find("form", {"data-upload-url": True}) or soup.find("div", {"data-upload-url": True})
            upload_url = None
            if upload_form:
                upload_url = upload_form.get("data-upload-url")
            
            # Method 2: Look in script tags for API endpoints
            if not upload_url:
                scripts = soup.find_all("script")
                for script in scripts:
                    if script.string:
                        # Look for patterns like uploadUrl: "/api/upload" or apiEndpoint: "..."
                        matches = re.findall(r'(?:upload[_-]?url|api[_-]?endpoint|upload[_-]?endpoint)\s*[:=]\s*["\']([^"\']+)', script.string, re.I)
                        if matches:
                            upload_url = matches[0]
                            if not upload_url.startswith("http"):
                                upload_url = urljoin(self.base_url, upload_url)
                            break
            
            # Default to common API endpoints if not found
            if not upload_url:
                possible_urls = [
                    urljoin(self.base_url, "/api/upload/batch"),
                    urljoin(self.base_url, "/api/uploads"),
                    urljoin(self.base_url, "/api/models/upload"),
                    CGTRADER_UPLOAD_URL,  # Fallback to form submission
                ]
            else:
                possible_urls = [upload_url]
            
            # Collect all files from the folder
            folder = Path(folder_path)
            files_to_upload = []
            
            for file_path in folder.rglob("*"):
                if file_path.is_file():
                    files_to_upload.append(file_path)
            
            if not files_to_upload:
                raise Exception(f"No files found in {folder_path}")
            
            print(f"[CGTrader HTTP] Found {len(files_to_upload)} files to upload")
            
            # Try uploading to each possible endpoint
            for upload_url in possible_urls:
                file_handles = []
                try:
                    print(f"[CGTrader HTTP] Trying upload to {upload_url}...")
                    
                    # Prepare multipart/form-data (keep file handles open)
                    files_data = []
                    for file_path in files_to_upload:
                        f = open(file_path, "rb")
                        file_handles.append(f)  # Keep reference for cleanup
                        files_data.append(
                            ("files[]", (file_path.name, f, "application/octet-stream"))
                        )
                    
                    # Prepare form data
                    form_data = {}
                    if self.csrf_token:
                        form_data["authenticity_token"] = self.csrf_token
                        form_data["_token"] = self.csrf_token
                        form_data["csrf_token"] = self.csrf_token
                    
                    # Headers for upload
                    headers = {
                        "Referer": CGTRADER_UPLOAD_URL,
                        "Origin": self.base_url,
                    }
                    
                    response = self.session.post(
                        upload_url,
                        data=form_data,
                        files=files_data,
                        headers=headers,
                        timeout=600,  # 10 minutes for large files
                        allow_redirects=False
                    )
                    
                    # Close file handles
                    for f in file_handles:
                        try:
                            f.close()
                        except:
                            pass
                    file_handles = []
                    
                    # Check response
                    if response.status_code in (200, 201, 302):
                        # Try to parse response for draft_id or upload_id
                        try:
                            json_response = response.json()
                            draft_id = json_response.get("id") or json_response.get("draft_id") or json_response.get("upload_id")
                            if draft_id:
                                print(f"[CGTrader HTTP] Upload successful! Draft ID: {draft_id}")
                                return draft_id
                        except (ValueError, KeyError):
                            pass
                        
                        # Check Location header for redirect with ID
                        location = response.headers.get("Location", "")
                        if location:
                            # Extract ID from URL
                            match = re.search(r'/(\d+)|/([a-f0-9-]+)', location)
                            if match:
                                draft_id = match.group(1) or match.group(2)
                                print(f"[CGTrader HTTP] Upload successful! Draft ID: {draft_id}")
                                return draft_id
                        
                        # If we get here, upload might be successful but ID not found
                        print("[CGTrader HTTP] Upload completed (status OK), but could not extract draft ID")
                        return "unknown"  # Placeholder
                    
                except requests.exceptions.RequestException as e:
                    print(f"[CGTrader HTTP] Upload to {upload_url} failed: {e}")
                    # Try to rotate proxy on error
                    self._handle_proxy_error(e)
                    continue
            
            raise Exception("All upload endpoints failed")
            
        except Exception as e:
            print(f"[CGTrader HTTP] Upload error: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def submit_metadata(self, draft_id: str, metadata: Dict[str, Any]) -> bool:
        """
        Submit metadata for uploaded model.
        
        Args:
            draft_id: ID of the draft/model
            metadata: Dictionary with title, description, tags, etc.
            
        Returns:
            True if successful
        """
        try:
            print(f"[CGTrader HTTP] Submitting metadata for draft {draft_id}")
            
            # Possible endpoints for metadata submission
            possible_urls = [
                urljoin(self.base_url, f"/api/models/{draft_id}/metadata"),
                urljoin(self.base_url, f"/api/models/{draft_id}"),
                urljoin(self.base_url, f"/api/drafts/{draft_id}/metadata"),
                urljoin(self.base_url, f"/profile/models/{draft_id}/update"),
            ]
            
            # Prepare metadata payload
            metadata_payload = {
                "title": metadata.get("title", ""),
                "description": metadata.get("description", ""),
                "tags": ",".join(metadata.get("tags", [])),
                "category": metadata.get("category", ""),
                "subcategory": metadata.get("subcategory", ""),
                "price": metadata.get("suggested_price", 37),
                "license": metadata.get("license", "royalty-free"),
                "polygons": metadata.get("polygons", 100000),
                "vertices": metadata.get("vertices", 100000),
            }
            
            # Add CSRF token if available
            if self.csrf_token:
                metadata_payload["authenticity_token"] = self.csrf_token
                metadata_payload["_token"] = self.csrf_token
            
            headers = {
                "Content-Type": "application/json",
                "Referer": urljoin(self.base_url, f"/profile/models/{draft_id}"),
            }
            
            # Try JSON API first
            for url in possible_urls:
                try:
                    print(f"[CGTrader HTTP] Trying JSON POST to {url}...")
                    response = self.session.patch(
                        url,
                        json=metadata_payload,
                        headers=headers,
                        timeout=60,
                        allow_redirects=False
                    )
                    
                    if response.status_code in (200, 201, 204):
                        print("[CGTrader HTTP] Metadata submitted successfully (JSON)")
                        return True
                    
                    # If 404 or method not allowed, try POST
                    if response.status_code in (404, 405):
                        response = self.session.post(
                            url,
                            json=metadata_payload,
                            headers=headers,
                            timeout=60,
                            allow_redirects=False
                        )
                        if response.status_code in (200, 201, 204):
                            print("[CGTrader HTTP] Metadata submitted successfully (JSON POST)")
                            return True
                        
                except requests.exceptions.RequestException as e:
                    self._handle_proxy_error(e)
                    continue
            
            # Try form-data as fallback
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            for url in possible_urls:
                try:
                    print(f"[CGTrader HTTP] Trying form-data POST to {url}...")
                    response = self.session.post(
                        url,
                        data=metadata_payload,
                        headers=headers,
                        timeout=60,
                        allow_redirects=False
                    )
                    
                    if response.status_code in (200, 201, 204, 302):
                        print("[CGTrader HTTP] Metadata submitted successfully (form-data)")
                        return True
                        
                except requests.exceptions.RequestException as e:
                    self._handle_proxy_error(e)
                    continue
            
            print("[CGTrader HTTP] Metadata submission failed: All endpoints failed")
            return False
            
        except requests.exceptions.RequestException as e:
            print(f"[CGTrader HTTP] Metadata submission error: {e}")
            self._handle_proxy_error(e)
            return False
        except Exception as e:
            print(f"[CGTrader HTTP] Metadata submission error: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def publish(self, draft_id: str) -> Optional[str]:
        """
        Publish the model.
        
        Args:
            draft_id: ID of the draft/model
            
        Returns:
            Product URL if successful, None otherwise
        """
        try:
            print(f"[CGTrader HTTP] Publishing draft {draft_id}")
            
            # Possible endpoints for publishing
            possible_urls = [
                urljoin(self.base_url, f"/api/models/{draft_id}/publish"),
                urljoin(self.base_url, f"/api/models/{draft_id}/submit"),
                urljoin(self.base_url, f"/api/drafts/{draft_id}/publish"),
                urljoin(self.base_url, f"/profile/models/{draft_id}/publish"),
            ]
            
            # Prepare publish payload
            publish_data = {}
            if self.csrf_token:
                publish_data["authenticity_token"] = self.csrf_token
                publish_data["_token"] = self.csrf_token
            
            headers = {
                "Content-Type": "application/json",
                "Referer": urljoin(self.base_url, f"/profile/models/{draft_id}"),
            }
            
            # Try JSON API first
            for url in possible_urls:
                try:
                    print(f"[CGTrader HTTP] Trying POST to {url}...")
                    response = self.session.post(
                        url,
                        json=publish_data,
                        headers=headers,
                        timeout=60,
                        allow_redirects=False
                    )
                    
                    if response.status_code in (200, 201, 302):
                        # Check for product URL in response
                        product_url = None
                        
                        # Try to parse JSON response
                        try:
                            json_response = response.json()
                            product_url = json_response.get("url") or json_response.get("product_url") or json_response.get("model_url")
                        except (ValueError, KeyError):
                            pass
                        
                        # Check Location header
                        if not product_url:
                            location = response.headers.get("Location", "")
                            if location:
                                product_url = location
                        
                        # Check response URL (if redirected)
                        if not product_url and response.url:
                            product_url = response.url
                        
                        if product_url:
                            # Make absolute URL if needed
                            if not product_url.startswith("http"):
                                product_url = urljoin(self.base_url, product_url)
                            print(f"[CGTrader HTTP] Published successfully! URL: {product_url}")
                            return product_url
                        
                        print("[CGTrader HTTP] Published (status OK), but URL not found in response")
                        return urljoin(self.base_url, f"/3d-models/{draft_id}")
                        
                except requests.exceptions.RequestException as e:
                    self._handle_proxy_error(e)
                    continue
            
            # Try form-data as fallback
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            for url in possible_urls:
                try:
                    response = self.session.post(
                        url,
                        data=publish_data,
                        headers=headers,
                        timeout=60,
                        allow_redirects=True  # Follow redirects for form submission
                    )
                    
                    if response.status_code == 200:
                        # Check if we're on a product page
                        current_url = response.url
                        if "/3d-models/" in current_url:
                            print(f"[CGTrader HTTP] Published successfully! URL: {current_url}")
                            return current_url
                            
                except requests.exceptions.RequestException as e:
                    self._handle_proxy_error(e)
                    continue
            
            print("[CGTrader HTTP] Publish failed: All endpoints failed")
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"[CGTrader HTTP] Publish error: {e}")
            self._handle_proxy_error(e)
            return None
        except Exception as e:
            print(f"[CGTrader HTTP] Publish error: {e}")
            import traceback
            traceback.print_exc()
            return None
