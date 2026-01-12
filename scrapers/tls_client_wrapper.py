#!/usr/bin/env python3
"""
TLS Client wrapper with fingerprint rotation and retry logic.
Provides browser-like TLS fingerprints to bypass WAF detection (JA3/JA4).
"""

import logging
import random
import time
from typing import Dict, Optional, List, Any

try:
    import tls_client
    TLS_CLIENT_AVAILABLE = True
except ImportError:
    TLS_CLIENT_AVAILABLE = False
    logging.warning("tls-client not installed. Install with: pip install tls-client")

# Fallback to httpx if tls-client not available
try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False


# Available browser fingerprints in tls-client
# These impersonate real browser TLS signatures to bypass JA3/JA4 fingerprinting
BROWSER_FINGERPRINTS = [
    # Chrome versions (most common)
    "chrome_120", "chrome_119", "chrome_118", "chrome_117",
    "chrome_116", "chrome_115", "chrome_114", "chrome_113",
    "chrome_112", "chrome_111", "chrome_110",
    # Firefox versions
    "firefox_120", "firefox_117", "firefox_110", "firefox_108",
    # Safari versions
    "safari_ios_17_0", "safari_16_0", "safari_15_6_1",
    # Edge
    "edge_99", "edge_101",
    # Opera
    "opera_91", "opera_90", "opera_89",
]


class TLSClientWrapper:
    """
    Wrapper around tls-client with fingerprint rotation and retry logic.

    Features:
    - Browser TLS fingerprint impersonation (Chrome, Firefox, Safari)
    - Automatic fingerprint rotation on 403 errors
    - Browser-like header ordering
    - HTTP/2 support
    - Proxy integration
    """

    def __init__(self, config: Dict, proxy_manager=None):
        """
        Initialize TLS client wrapper.

        Args:
            config: TLS configuration dict with keys:
                - client_identifier: Primary fingerprint to use (e.g., "chrome_120")
                - fallback_identifiers: List of fallback fingerprints
                - randomize_fingerprint: Whether to randomize on init
            proxy_manager: Optional ProxyManager instance for proxy rotation
        """
        self.config = config
        self.proxy_manager = proxy_manager

        # Get fingerprint configuration
        self.primary_identifier = config.get('client_identifier', 'chrome_120')
        self.fallbacks = config.get('fallback_identifiers', BROWSER_FINGERPRINTS[:5])
        self.randomize = config.get('randomize_fingerprint', True)

        # Select initial fingerprint
        if self.randomize:
            self.current_identifier = random.choice([self.primary_identifier] + self.fallbacks)
        else:
            self.current_identifier = self.primary_identifier

        self.fallback_index = 0
        self.session = None
        self._use_tls_client = TLS_CLIENT_AVAILABLE

        self._create_session()

    def _create_session(self):
        """Create new session with current fingerprint and proxy settings."""
        logging.info(f"Creating TLS session with fingerprint: {self.current_identifier}")

        if self._use_tls_client and TLS_CLIENT_AVAILABLE:
            try:
                self.session = tls_client.Session(
                    client_identifier=self.current_identifier,
                    random_tls_extension_order=True  # Helps bypass JA4 detection
                )

                # Set proxy if available
                if self.proxy_manager:
                    proxy = self.proxy_manager.get_proxy()
                    if proxy:
                        self.session.proxies = proxy
                        logging.info(f"Session configured with proxy")

            except Exception as e:
                logging.warning(f"Failed to create tls-client session: {e}")
                logging.info("Falling back to httpx")
                self._use_tls_client = False
                self._create_httpx_session()
        else:
            self._create_httpx_session()

    def _create_httpx_session(self):
        """Create httpx session as fallback."""
        if not HTTPX_AVAILABLE:
            raise RuntimeError("Neither tls-client nor httpx available")

        proxy_url = None
        if self.proxy_manager:
            proxy_url = self.proxy_manager.get_proxy_url()

        self.session = httpx.Client(
            http2=True,
            timeout=30.0,
            proxy=proxy_url,
            follow_redirects=True
        )
        logging.info("Using httpx session (fallback mode)")

    def rotate_fingerprint(self):
        """Rotate to next fingerprint in fallback list."""
        if self.fallback_index < len(self.fallbacks):
            self.current_identifier = self.fallbacks[self.fallback_index]
            self.fallback_index += 1
        else:
            # Exhausted fallbacks, pick random from full list
            self.current_identifier = random.choice(BROWSER_FINGERPRINTS)
            self.fallback_index = 0

        logging.info(f"Rotated TLS fingerprint to: {self.current_identifier}")
        self._create_session()

    def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        json: Optional[Dict] = None,
        data: Optional[Any] = None,
        timeout: int = 30,
        **kwargs
    ) -> Any:
        """
        Make HTTP request with current session.

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Request URL
            headers: Request headers (will be ordered for browser impersonation)
            json: JSON body for POST requests
            data: Form data for POST requests
            timeout: Request timeout in seconds
            **kwargs: Additional arguments

        Returns:
            Response object (tls_client.response.Response or httpx.Response)
        """
        if headers:
            # Ensure correct header order for browser impersonation
            ordered_headers = self._order_headers(headers)
        else:
            ordered_headers = None

        if self._use_tls_client and TLS_CLIENT_AVAILABLE:
            return self.session.execute_request(
                method=method.upper(),
                url=url,
                headers=ordered_headers,
                json=json,
                data=data,
                timeout_seconds=timeout,
                **kwargs
            )
        else:
            # httpx path
            return self.session.request(
                method=method.upper(),
                url=url,
                headers=ordered_headers,
                json=json,
                data=data,
                timeout=timeout,
                **kwargs
            )

    def get(self, url: str, **kwargs) -> Any:
        """Convenience method for GET requests."""
        return self.request('GET', url, **kwargs)

    def post(self, url: str, **kwargs) -> Any:
        """Convenience method for POST requests."""
        return self.request('POST', url, **kwargs)

    def _order_headers(self, headers: Dict) -> Dict:
        """
        Order headers to match Chrome browser patterns.
        Header order is analyzed by WAFs to detect bots.
        """
        # Chrome's typical header order
        priority_order = [
            'Host',
            'Connection',
            'Content-Length',
            'sec-ch-ua',
            'sec-ch-ua-mobile',
            'sec-ch-ua-platform',
            'Upgrade-Insecure-Requests',
            'User-Agent',
            'Accept',
            'Content-Type',
            'Sec-Fetch-Site',
            'Sec-Fetch-Mode',
            'Sec-Fetch-User',
            'Sec-Fetch-Dest',
            'Referer',
            'Origin',
            'Accept-Encoding',
            'Accept-Language',
            'Cookie',
            # Algolia-specific headers
            'x-algolia-api-key',
            'x-algolia-application-id',
            'x-algolia-agent',
        ]

        ordered = {}

        # Add priority headers first (case-insensitive matching)
        lower_headers = {k.lower(): (k, v) for k, v in headers.items()}

        for priority_key in priority_order:
            lower_key = priority_key.lower()
            if lower_key in lower_headers:
                original_key, value = lower_headers[lower_key]
                ordered[priority_key] = value
                del lower_headers[lower_key]

        # Add remaining headers
        for lower_key, (original_key, value) in lower_headers.items():
            ordered[original_key] = value

        return ordered

    def update_proxy(self):
        """Update session with new proxy from proxy manager."""
        if self.proxy_manager and self._use_tls_client:
            proxy = self.proxy_manager.get_proxy()
            if proxy and self.session:
                self.session.proxies = proxy
                logging.debug("Updated session proxy")

    def close(self):
        """Close the session and release resources."""
        if self.session:
            if not self._use_tls_client and HTTPX_AVAILABLE:
                self.session.close()
            self.session = None

    def get_status(self) -> Dict:
        """
        Get current wrapper status.

        Returns:
            Dict with status information
        """
        return {
            'using_tls_client': self._use_tls_client,
            'current_fingerprint': self.current_identifier,
            'fallback_index': self.fallback_index,
            'has_proxy': self.proxy_manager is not None and self.proxy_manager.enabled
        }

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
