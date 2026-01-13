#!/usr/bin/env python3
"""
Sitemap parser utility for XML sitemap parsing.
Supports sitemap index files, gzip compression, and incremental updates via lastmod.
"""

import gzip
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Generator, List, Optional, Set
from xml.etree import ElementTree as ET

import requests

logger = logging.getLogger(__name__)


# XML namespaces used in sitemaps
SITEMAP_NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    'xhtml': 'http://www.w3.org/1999/xhtml'
}


@dataclass
class SitemapEntry:
    """Represents a single URL entry from a sitemap."""
    loc: str
    lastmod: Optional[datetime] = None
    changefreq: Optional[str] = None
    priority: Optional[float] = None

    @property
    def is_product_page(self) -> bool:
        """Check if URL appears to be a product page (Walmart format: /en/ip/...)."""
        return '/ip/' in self.loc or '/produit/' in self.loc


class SitemapParser:
    """
    Parser for XML sitemaps with support for:
    - Sitemap index files (nested sitemaps)
    - Gzip compression
    - Incremental updates via lastmod filtering
    - robots.txt sitemap discovery
    """

    def __init__(self, session: Optional[requests.Session] = None, user_agent: Optional[str] = None):
        """
        Initialize sitemap parser.

        Args:
            session: Optional requests session (creates new if not provided)
            user_agent: User agent string for requests
        """
        self.session = session or requests.Session()
        if user_agent:
            self.session.headers['User-Agent'] = user_agent
        else:
            self.session.headers['User-Agent'] = (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

    def fetch_robots_txt(self, base_url: str) -> str:
        """
        Fetch robots.txt content from a URL.

        Args:
            base_url: Base URL of the site (e.g., https://www.walmart.ca)

        Returns:
            robots.txt content as string
        """
        robots_url = f"{base_url.rstrip('/')}/robots.txt"
        logger.info(f"Fetching robots.txt from {robots_url}")

        response = self.session.get(robots_url, timeout=30)
        response.raise_for_status()
        return response.text

    def extract_sitemaps_from_robots(self, robots_content: str) -> List[str]:
        """
        Extract sitemap URLs from robots.txt content.

        Args:
            robots_content: robots.txt file content

        Returns:
            List of sitemap URLs
        """
        sitemaps = []
        for line in robots_content.split('\n'):
            line = line.strip()
            if line.lower().startswith('sitemap:'):
                sitemap_url = line.split(':', 1)[1].strip()
                sitemaps.append(sitemap_url)

        logger.info(f"Found {len(sitemaps)} sitemaps in robots.txt")
        return sitemaps

    def discover_sitemaps(self, base_url: str) -> List[str]:
        """
        Discover sitemap URLs from robots.txt.

        Args:
            base_url: Base URL of the site

        Returns:
            List of discovered sitemap URLs
        """
        try:
            robots_content = self.fetch_robots_txt(base_url)
            return self.extract_sitemaps_from_robots(robots_content)
        except Exception as e:
            logger.warning(f"Failed to discover sitemaps from robots.txt: {e}")
            return []

    def _fetch_sitemap_content(self, url: str) -> bytes:
        """
        Fetch sitemap content, handling gzip compression.

        Args:
            url: Sitemap URL

        Returns:
            Raw XML bytes
        """
        logger.debug(f"Fetching sitemap: {url}")
        response = self.session.get(url, timeout=60)
        response.raise_for_status()

        content = response.content

        # Handle gzip compression (common for large sitemaps)
        if url.endswith('.gz') or response.headers.get('Content-Encoding') == 'gzip':
            try:
                content = gzip.decompress(content)
            except Exception:
                # Already decompressed or not gzipped
                pass

        return content

    def _parse_datetime(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse ISO 8601 datetime string."""
        if not date_str:
            return None

        # Common formats in sitemaps
        formats = [
            '%Y-%m-%dT%H:%M:%S%z',
            '%Y-%m-%dT%H:%M:%S.%f%z',
            '%Y-%m-%dT%H:%M:%SZ',
            '%Y-%m-%d',
        ]

        for fmt in formats:
            try:
                return datetime.strptime(date_str.replace('+00:00', 'Z').replace('Z', '+0000'), fmt)
            except ValueError:
                continue

        # Try basic date parse
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except ValueError:
            return None

    def _parse_sitemap_xml(self, content: bytes) -> tuple[List[str], List[SitemapEntry]]:
        """
        Parse sitemap XML content.

        Args:
            content: Raw XML bytes

        Returns:
            Tuple of (sitemap_urls, url_entries)
        """
        sitemap_urls = []
        url_entries = []

        try:
            root = ET.fromstring(content)
        except ET.ParseError as e:
            logger.error(f"Failed to parse sitemap XML: {e}")
            return sitemap_urls, url_entries

        # Determine root tag (sitemapindex or urlset)
        root_tag = root.tag.lower()

        # Handle sitemap index (contains references to other sitemaps)
        if 'sitemapindex' in root_tag:
            for sitemap in root.findall('.//sm:sitemap', SITEMAP_NS):
                loc = sitemap.find('sm:loc', SITEMAP_NS)
                if loc is not None and loc.text:
                    sitemap_urls.append(loc.text.strip())

            # Also try without namespace (some sitemaps don't use it)
            if not sitemap_urls:
                for sitemap in root.findall('.//sitemap'):
                    loc = sitemap.find('loc')
                    if loc is not None and loc.text:
                        sitemap_urls.append(loc.text.strip())

            logger.info(f"Sitemap index contains {len(sitemap_urls)} nested sitemaps")

        # Handle urlset (contains actual URLs)
        elif 'urlset' in root_tag:
            for url_elem in root.findall('.//sm:url', SITEMAP_NS):
                loc = url_elem.find('sm:loc', SITEMAP_NS)
                if loc is None or not loc.text:
                    continue

                lastmod_elem = url_elem.find('sm:lastmod', SITEMAP_NS)
                changefreq_elem = url_elem.find('sm:changefreq', SITEMAP_NS)
                priority_elem = url_elem.find('sm:priority', SITEMAP_NS)

                entry = SitemapEntry(
                    loc=loc.text.strip(),
                    lastmod=self._parse_datetime(lastmod_elem.text if lastmod_elem is not None else None),
                    changefreq=changefreq_elem.text if changefreq_elem is not None else None,
                    priority=float(priority_elem.text) if priority_elem is not None else None
                )
                url_entries.append(entry)

            # Also try without namespace
            if not url_entries:
                for url_elem in root.findall('.//url'):
                    loc = url_elem.find('loc')
                    if loc is None or not loc.text:
                        continue

                    lastmod_elem = url_elem.find('lastmod')
                    changefreq_elem = url_elem.find('changefreq')
                    priority_elem = url_elem.find('priority')

                    entry = SitemapEntry(
                        loc=loc.text.strip(),
                        lastmod=self._parse_datetime(lastmod_elem.text if lastmod_elem is not None else None),
                        changefreq=changefreq_elem.text if changefreq_elem is not None else None,
                        priority=float(priority_elem.text) if priority_elem is not None else None
                    )
                    url_entries.append(entry)

            logger.info(f"Parsed {len(url_entries)} URLs from sitemap")

        return sitemap_urls, url_entries

    def parse_sitemap(self, url: str, recursive: bool = True,
                      since: Optional[datetime] = None,
                      url_filter: Optional[callable] = None,
                      max_urls: Optional[int] = None) -> Generator[SitemapEntry, None, None]:
        """
        Parse a sitemap URL and yield entries.

        Args:
            url: Sitemap URL to parse
            recursive: If True, follow nested sitemap references
            since: Only return entries modified after this datetime
            url_filter: Optional function to filter URLs (returns True to include)
            max_urls: Maximum number of URLs to return

        Yields:
            SitemapEntry objects
        """
        urls_yielded = 0
        visited_sitemaps: Set[str] = set()
        sitemap_queue = [url]

        while sitemap_queue:
            current_url = sitemap_queue.pop(0)

            if current_url in visited_sitemaps:
                continue
            visited_sitemaps.add(current_url)

            try:
                content = self._fetch_sitemap_content(current_url)
                nested_sitemaps, entries = self._parse_sitemap_xml(content)

                # Add nested sitemaps to queue if recursive
                if recursive and nested_sitemaps:
                    sitemap_queue.extend(nested_sitemaps)

                # Yield URL entries
                for entry in entries:
                    # Apply lastmod filter
                    if since and entry.lastmod and entry.lastmod < since:
                        continue

                    # Apply custom filter
                    if url_filter and not url_filter(entry.loc):
                        continue

                    yield entry
                    urls_yielded += 1

                    if max_urls and urls_yielded >= max_urls:
                        return

            except Exception as e:
                logger.error(f"Failed to parse sitemap {current_url}: {e}")
                continue

    def get_product_urls(self, sitemap_url: str,
                        max_urls: Optional[int] = None,
                        since: Optional[datetime] = None) -> List[SitemapEntry]:
        """
        Get product page URLs from a sitemap.

        Args:
            sitemap_url: Sitemap URL to parse
            max_urls: Maximum number of URLs to return
            since: Only return entries modified after this datetime

        Returns:
            List of SitemapEntry objects for product pages
        """
        entries = list(self.parse_sitemap(
            url=sitemap_url,
            recursive=True,
            since=since,
            url_filter=lambda url: '/ip/' in url or '/produit/' in url,
            max_urls=max_urls
        ))

        logger.info(f"Found {len(entries)} product URLs")
        return entries


def filter_walmart_product_urls(url: str) -> bool:
    """
    Filter function for Walmart Canada product URLs.
    Returns True if URL is a valid product page.

    Walmart product URLs follow pattern: /en/ip/[slug]/[item_id]
    """
    # Must be a product page (item page)
    if '/ip/' not in url and '/produit/' not in url:
        return False

    # Exclude disallowed patterns from robots.txt
    disallowed_patterns = [
        '/search',
        '/recherche',
        '/cart',
        '/panier',
        '/sign-in',
        '/account',
        '/kiosk/',
        '+',  # Faceted navigation
        '?f=',  # Filter params
    ]

    for pattern in disallowed_patterns:
        if pattern in url:
            return False

    return True
