#!/usr/bin/env python3
"""
Proxy rotation manager for scrapers.
Supports loading proxies from environment variables, files, or direct configuration.
"""

import os
import random
import logging
import time
from typing import List, Optional, Dict
from pathlib import Path
from dataclasses import dataclass, field


@dataclass
class ProxyConfig:
    """Proxy configuration with authentication and health tracking."""
    url: str
    failures: int = 0
    last_used: float = 0.0

    def to_dict(self) -> Dict[str, str]:
        """Convert to dict format for tls-client."""
        return {"http": self.url, "https": self.url}


class ProxyManager:
    """
    Manages proxy rotation with health tracking and failure handling.

    Supports:
    - Loading proxies from environment variables
    - Loading proxies from files
    - Round-robin or random rotation
    - Failure tracking and automatic rotation
    """

    def __init__(self, config: Dict):
        """
        Initialize proxy manager from configuration.

        Args:
            config: Proxy configuration dict with keys:
                - enabled: bool
                - source: "env" | "file" | "list"
                - env_var: Environment variable name (if source="env")
                - file_path: Path to proxy list file (if source="file")
                - proxies: List of proxy URLs (if source="list")
                - rotation_strategy: "round_robin" | "random"
                - max_failures_before_rotate: int
        """
        self.enabled = config.get('enabled', False)
        self.max_failures = config.get('max_failures_before_rotate', 3)
        self.rotation_strategy = config.get('rotation_strategy', 'round_robin')
        self.proxies: List[ProxyConfig] = []
        self.current_index = 0

        if self.enabled:
            self._load_proxies(config)
            if self.proxies:
                logging.info(f"ProxyManager initialized with {len(self.proxies)} proxies")
            else:
                logging.warning("ProxyManager enabled but no proxies loaded - running without proxy")
                self.enabled = False

    def _load_proxies(self, config: Dict):
        """Load proxies from configured source."""
        source = config.get('source', 'env')

        if source == 'env':
            env_var = config.get('env_var', 'PROXY_URL')
            proxy_url = os.environ.get(env_var)
            if proxy_url:
                # Support comma-separated list in env var
                urls = [u.strip() for u in proxy_url.split(',') if u.strip()]
                self.proxies = [ProxyConfig(url=u) for u in urls]
                logging.info(f"Loaded {len(self.proxies)} proxies from {env_var}")
            else:
                logging.warning(f"Proxy enabled but {env_var} environment variable not set")

        elif source == 'file':
            file_path = config.get('file_path')
            if file_path and Path(file_path).exists():
                with open(file_path, 'r') as f:
                    urls = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                self.proxies = [ProxyConfig(url=u) for u in urls]
                logging.info(f"Loaded {len(self.proxies)} proxies from {file_path}")
            else:
                logging.warning(f"Proxy file not found: {file_path}")

        elif source == 'list':
            urls = config.get('proxies', [])
            self.proxies = [ProxyConfig(url=u) for u in urls]
            logging.info(f"Loaded {len(self.proxies)} proxies from config list")

    def get_proxy(self) -> Optional[Dict[str, str]]:
        """
        Get current proxy for use with tls-client or httpx.

        Returns:
            Dict with 'http' and 'https' keys, or None if no proxies available
        """
        if not self.enabled or not self.proxies:
            return None

        proxy = self.proxies[self.current_index]
        proxy.last_used = time.time()
        return proxy.to_dict()

    def get_proxy_url(self) -> Optional[str]:
        """
        Get current proxy URL string.

        Returns:
            Proxy URL string or None if no proxies available
        """
        if not self.enabled or not self.proxies:
            return None

        return self.proxies[self.current_index].url

    def rotate(self):
        """Rotate to next proxy based on rotation strategy."""
        if not self.proxies:
            return

        old_index = self.current_index

        if self.rotation_strategy == 'round_robin':
            self.current_index = (self.current_index + 1) % len(self.proxies)
        elif self.rotation_strategy == 'random':
            # Avoid selecting the same proxy if we have multiple
            if len(self.proxies) > 1:
                available = [i for i in range(len(self.proxies)) if i != self.current_index]
                self.current_index = random.choice(available)

        logging.info(f"Rotated proxy: index {old_index} -> {self.current_index}")

    def report_failure(self):
        """Report a failure on current proxy, rotate if threshold reached."""
        if not self.proxies:
            return

        proxy = self.proxies[self.current_index]
        proxy.failures += 1

        logging.debug(f"Proxy {self.current_index} failure count: {proxy.failures}/{self.max_failures}")

        if proxy.failures >= self.max_failures:
            logging.warning(f"Proxy {self.current_index} exceeded failure threshold ({self.max_failures}), rotating")
            self.rotate()
            # Reset failure count on new proxy
            self.proxies[self.current_index].failures = 0

    def report_success(self):
        """Report successful request, reset failure count for current proxy."""
        if self.proxies:
            self.proxies[self.current_index].failures = 0

    def get_status(self) -> Dict:
        """
        Get current proxy manager status.

        Returns:
            Dict with status information
        """
        return {
            'enabled': self.enabled,
            'total_proxies': len(self.proxies),
            'current_index': self.current_index,
            'current_proxy': self.get_proxy_url(),
            'rotation_strategy': self.rotation_strategy
        }
