"""
Proxy Manager for CGTrader Submit Server

Automatically fetches, tests, and manages working proxies from public lists.
Maintains a pool of working proxies and rotates them when needed.
"""
import os
import re
import time
import threading
import requests
from typing import List, Optional, Dict, Tuple
from urllib.parse import urlparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProxyChecker:
    """Checks if a proxy is working for CGTrader.com"""
    
    TEST_URL = "https://www.cgtrader.com"
    TEST_TIMEOUT = 10
    
    @staticmethod
    def check_proxy(proxy_url: str) -> Tuple[bool, Optional[str]]:
        """
        Check if proxy is working.
        
        Args:
            proxy_url: Proxy URL in format http://user:pass@host:port or http://host:port
            
        Returns:
            Tuple of (is_working, error_message)
        """
        try:
            proxies = {
                "http": proxy_url,
                "https": proxy_url,
            }
            
            # Try to connect to CGTrader
            response = requests.get(
                ProxyChecker.TEST_URL,
                proxies=proxies,
                timeout=ProxyChecker.TEST_TIMEOUT,
                allow_redirects=False
            )
            
            # Check if we got a response (even 403 is better than connection error)
            if response.status_code in [200, 301, 302, 403]:
                return True, None
            elif response.status_code == 403:
                # 403 might indicate Cloudflare blocking, but proxy works
                return True, None
            else:
                return False, f"HTTP {response.status_code}"
                
        except requests.exceptions.ProxyError as e:
            return False, f"Proxy error: {str(e)}"
        except requests.exceptions.ConnectTimeout:
            return False, "Connection timeout"
        except requests.exceptions.ConnectionError as e:
            return False, f"Connection error: {str(e)}"
        except Exception as e:
            return False, f"Unknown error: {str(e)}"


class ProxyManager:
    """Manages proxy pool - fetches, tests, and rotates proxies"""
    
    PROXY_SOURCES = [
        "https://raw.githubusercontent.com/jetkai/proxy-list/main/online-proxies/txt/proxies.txt",
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks4.txt",
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/http.txt",
        "https://raw.githubusercontent.com/TheSpeedX/SOCKS-List/master/socks5.txt",
        "https://raw.githubusercontent.com/MuRongPIG/Proxy-Master/main/http.txt",
    ]
    
    def __init__(self, check_interval: int = 300, max_working_proxies: int = 50):
        """
        Initialize proxy manager.
        
        Args:
            check_interval: Interval in seconds to check current proxy (default 5 minutes)
            max_working_proxies: Maximum number of working proxies to keep (default 50)
        """
        self.check_interval = check_interval
        self.max_working_proxies = max_working_proxies
        
        self.working_proxies: List[str] = []
        self.current_proxy: Optional[str] = None
        self.current_proxy_lock = threading.Lock()
        self.working_proxies_lock = threading.Lock()
        
        self.running = False
        self.check_thread: Optional[threading.Thread] = None
        
        logger.info("[ProxyManager] Initialized")
    
    def _parse_proxy_line(self, line: str) -> Optional[str]:
        """Parse a line from proxy list file."""
        line = line.strip()
        if not line or line.startswith("#"):
            return None
        
        # Remove any comments
        if "#" in line:
            line = line.split("#")[0].strip()
        
        # Handle different formats:
        # host:port
        # user:pass@host:port
        # http://host:port
        # socks4://host:port
        # socks5://host:port
        
        # If it already has a scheme, return as is
        if "://" in line:
            return line
        
        # Check if it has auth
        if "@" in line:
            # Format: user:pass@host:port
            return f"http://{line}"
        
        # Format: host:port (assume HTTP)
        # Validate format
        parts = line.split(":")
        if len(parts) == 2:
            host, port = parts
            try:
                int(port)  # Validate port is numeric
                return f"http://{host}:{port}"
            except ValueError:
                return None
        
        return None
    
    def fetch_proxies_from_url(self, url: str) -> List[str]:
        """Fetch and parse proxies from a URL."""
        proxies = []
        try:
            logger.info(f"[ProxyManager] Fetching proxies from {url}")
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                lines = response.text.strip().split("\n")
                for line in lines:
                    proxy = self._parse_proxy_line(line)
                    if proxy:
                        proxies.append(proxy)
                logger.info(f"[ProxyManager] Fetched {len(proxies)} proxies from {url}")
            else:
                logger.warning(f"[ProxyManager] Failed to fetch {url}: HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"[ProxyManager] Error fetching {url}: {e}")
        
        return proxies
    
    def fetch_all_proxies(self) -> List[str]:
        """Fetch proxies from all sources."""
        all_proxies = []
        for url in self.PROXY_SOURCES:
            proxies = self.fetch_proxies_from_url(url)
            all_proxies.extend(proxies)
            time.sleep(0.5)  # Be nice to GitHub
        
        # Remove duplicates while preserving order
        seen = set()
        unique_proxies = []
        for proxy in all_proxies:
            if proxy not in seen:
                seen.add(proxy)
                unique_proxies.append(proxy)
        
        logger.info(f"[ProxyManager] Total unique proxies fetched: {len(unique_proxies)}")
        return unique_proxies
    
    def test_proxy(self, proxy_url: str) -> bool:
        """Test a single proxy."""
        is_working, error = ProxyChecker.check_proxy(proxy_url)
        if is_working:
            logger.info(f"[ProxyManager] ✅ Proxy working: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
            return True
        else:
            logger.debug(f"[ProxyManager] ❌ Proxy failed: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url} - {error}")
            return False
    
    def refresh_working_proxies(self, max_proxies: Optional[int] = None) -> int:
        """
        Fetch proxies, test them, and update working list.
        
        Args:
            max_proxies: Maximum number of working proxies to collect (default: self.max_working_proxies)
            
        Returns:
            Number of working proxies found
        """
        if max_proxies is None:
            max_proxies = self.max_working_proxies
        
        logger.info("[ProxyManager] Starting proxy refresh...")
        
        # Fetch all proxies
        all_proxies = self.fetch_all_proxies()
        if not all_proxies:
            logger.warning("[ProxyManager] No proxies fetched from sources")
            return 0
        
        # Test proxies
        working = []
        tested = 0
        
        logger.info(f"[ProxyManager] Testing {len(all_proxies)} proxies (max {max_proxies} working)...")
        for proxy in all_proxies:
            if len(working) >= max_proxies:
                break
            
            tested += 1
            if self.test_proxy(proxy):
                working.append(proxy)
            
            # Progress update every 50 proxies
            if tested % 50 == 0:
                logger.info(f"[ProxyManager] Tested {tested}/{len(all_proxies)}, found {len(working)} working")
        
        # Update working list
        with self.working_proxies_lock:
            self.working_proxies = working
        
        logger.info(f"[ProxyManager] ✅ Proxy refresh complete: {len(working)} working proxies out of {tested} tested")
        return len(working)
    
    def get_current_proxy(self) -> Optional[str]:
        """Get current working proxy."""
        with self.current_proxy_lock:
            return self.current_proxy
    
    def set_current_proxy(self, proxy_url: Optional[str]):
        """Set current proxy."""
        with self.current_proxy_lock:
            self.current_proxy = proxy_url
            if proxy_url:
                logger.info(f"[ProxyManager] Current proxy set: {proxy_url.split('@')[-1] if '@' in proxy_url else proxy_url}")
            else:
                logger.info("[ProxyManager] Current proxy cleared")
    
    def get_next_working_proxy(self) -> Optional[str]:
        """Get next working proxy from pool, rotating if needed."""
        with self.working_proxies_lock:
            if not self.working_proxies:
                return None
            
            current = self.get_current_proxy()
            
            # If no current proxy, get first one
            if not current:
                proxy = self.working_proxies[0]
                self.set_current_proxy(proxy)
                return proxy
            
            # Find current in list
            try:
                current_idx = self.working_proxies.index(current)
                # Get next one (wrap around)
                next_idx = (current_idx + 1) % len(self.working_proxies)
                proxy = self.working_proxies[next_idx]
            except ValueError:
                # Current not in list, get first one
                proxy = self.working_proxies[0]
            
            self.set_current_proxy(proxy)
            return proxy
    
    def check_and_rotate_proxy(self) -> bool:
        """
        Check current proxy and rotate if not working.
        
        Returns:
            True if proxy is working or was successfully rotated, False otherwise
        """
        current = self.get_current_proxy()
        
        if not current:
            # No current proxy, try to get one
            proxy = self.get_next_working_proxy()
            return proxy is not None
        
        # Check current proxy
        is_working = self.test_proxy(current)
        
        if is_working:
            return True
        
        # Current proxy failed, try to rotate
        logger.warning(f"[ProxyManager] Current proxy failed, rotating...")
        proxy = self.get_next_working_proxy()
        
        if proxy and proxy != current:
            logger.info(f"[ProxyManager] Rotated to new proxy")
            return True
        elif not self.working_proxies:
            logger.warning("[ProxyManager] No working proxies available, need refresh")
            return False
        else:
            # Same proxy (only one in list), check if it's actually working
            if self.test_proxy(proxy):
                return True
            return False
    
    def _check_loop(self):
        """Background thread loop to periodically check proxy."""
        while self.running:
            try:
                time.sleep(self.check_interval)
                if self.running:
                    self.check_and_rotate_proxy()
            except Exception as e:
                logger.error(f"[ProxyManager] Error in check loop: {e}")
    
    def start(self):
        """Start background proxy checking."""
        if self.running:
            return
        
        self.running = True
        self.check_thread = threading.Thread(target=self._check_loop, daemon=True)
        self.check_thread.start()
        logger.info(f"[ProxyManager] Background checking started (interval: {self.check_interval}s)")
    
    def stop(self):
        """Stop background proxy checking."""
        self.running = False
        if self.check_thread:
            self.check_thread.join(timeout=5)
        logger.info("[ProxyManager] Background checking stopped")
    
    def ensure_working_proxy(self) -> bool:
        """
        Ensure we have a working proxy. Refresh pool if needed.
        
        Returns:
            True if working proxy is available, False otherwise
        """
        # Check if we have working proxies
        with self.working_proxies_lock:
            has_working = len(self.working_proxies) > 0
        
        if not has_working:
            logger.info("[ProxyManager] No working proxies, refreshing...")
            self.refresh_working_proxies()
        
        # Check/rotate current proxy
        return self.check_and_rotate_proxy()


# Global proxy manager instance
_proxy_manager: Optional[ProxyManager] = None
_proxy_manager_lock = threading.Lock()


def get_proxy_manager(check_interval: int = 300, max_working_proxies: int = 50) -> ProxyManager:
    """Get or create global proxy manager instance."""
    global _proxy_manager
    with _proxy_manager_lock:
        if _proxy_manager is None:
            _proxy_manager = ProxyManager(check_interval=check_interval, max_working_proxies=max_working_proxies)
        return _proxy_manager
