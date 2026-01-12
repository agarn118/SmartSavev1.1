#!/usr/bin/env python3
"""
Sobeys API Scraper - Enhanced Version with Anti-Bot Bypass

Features:
- TLS fingerprint impersonation (JA3/JA4 bypass)
- Residential proxy rotation
- Multi-store rotation for regional pricing (Alberta stores)
- Adaptive backoff with jitter
- Browser-like header ordering

Uses Algolia Search API for fast, reliable product data extraction.
Follows the Rooney Method: API First with protocol-level browser impersonation.
"""

import logging
import time
import json
import random
import re
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from pathlib import Path

from scrapers.base import ProductRecord, BaseScraper
from scrapers.common import get_iso_timestamp, setup_logging_with_rotation, backup_data_file
from scrapers.proxy_manager import ProxyManager
from scrapers.tls_client_wrapper import TLSClientWrapper
from scrapers.store_rotator import StoreRotator


class SobeysAPIScraper(BaseScraper):
    """
    API-based scraper for Sobeys using Algolia Search with anti-bot bypass.

    Features:
    - TLS fingerprint impersonation to bypass JA3/JA4 detection
    - Proxy rotation for IP-based rate limit bypass
    - Store rotation across 8 Alberta locations for regional pricing
    - Adaptive exponential backoff with jitter
    """

    # Algolia API configuration (discovered from Network tab inspection)
    ALGOLIA_APP_ID = "ACSYSHF8AU"
    ALGOLIA_API_KEY = "fe555974f588b3e76ad0f1c548113b22"
    ALGOLIA_BASE_URL = "https://acsyshf8au-dsn.algolia.net"
    ALGOLIA_INDEX = "dxp_product_en"

    def __init__(self, config_path, project_root, headless=True, debug=True):
        """
        Initialize the Sobeys API scraper with anti-bot bypass components.

        Args:
            config_path: Path to sobeys.json configuration file
            project_root: Project root directory
            headless: Ignored (API scraper doesn't use browser)
            debug: Enable debug snapshots of API requests/responses
        """
        super().__init__(config_path, project_root)
        self.debug = debug

        # Initialize proxy manager
        proxy_config = self.config.get('proxy', {'enabled': False})
        self.proxy_manager = ProxyManager(proxy_config)

        # Initialize TLS client wrapper with fingerprint impersonation
        tls_config = self.config.get('tls', {'client_identifier': 'chrome_120'})
        self.tls_client = TLSClientWrapper(tls_config, self.proxy_manager)

        # Initialize store rotator for multi-store regional pricing
        stores_config = self.config.get('stores', None)
        self.store_rotator = StoreRotator(stores_config, rotation_mode='all')

        # Error handling configuration
        error_config = self.config.get('error_handling', {})
        self.max_retries = error_config.get('max_retries', 5)
        self.retry_status_codes = error_config.get('retry_on_status_codes', [403, 429, 500, 502, 503, 504])
        self.rotate_proxy_on_403 = error_config.get('rotate_proxy_on_403', True)
        self.rotate_fingerprint_on_403 = error_config.get('rotate_fingerprint_on_403', True)
        self.backoff_base = error_config.get('backoff_base', 2.0)
        self.max_backoff = error_config.get('max_backoff_seconds', 300)
        self.jitter_range = error_config.get('jitter_range', [0.8, 1.2])

        # Setup debug directories
        if self.debug:
            self.debug_dir = project_root / 'data' / 'debug' / 'sobeys'
            self.debug_dir.mkdir(parents=True, exist_ok=True)
            logging.info(f"Debug mode enabled. Snapshots saved to: {self.debug_dir}")

        # Log initialization status
        logging.info(f"Scraper initialized:")
        logging.info(f"  - Proxy enabled: {self.proxy_manager.enabled}")
        logging.info(f"  - TLS fingerprint: {self.tls_client.current_identifier}")
        logging.info(f"  - Stores configured: {len(self.store_rotator.stores)} Alberta locations")

    def _fetch_with_retry(self, method: str, url: str, **kwargs) -> Any:
        """
        Fetch URL with retry logic, adaptive backoff, and fingerprint rotation.

        Implements the error handling strategy from the research document:
        - Exponential backoff with jitter
        - Fingerprint rotation on 403 (bot detection)
        - Proxy rotation on repeated failures

        Args:
            method: HTTP method (GET, POST)
            url: URL to fetch
            **kwargs: Additional arguments for request

        Returns:
            Response object

        Raises:
            Exception: If all retries exhausted
        """
        last_exception = None

        for attempt in range(self.max_retries):
            try:
                response = self.tls_client.request(method, url, **kwargs)

                # Check response status
                status_code = getattr(response, 'status_code', 200)

                if status_code in self.retry_status_codes:
                    raise Exception(f"HTTP {status_code}")

                # Success - report to proxy manager
                if self.proxy_manager.enabled:
                    self.proxy_manager.report_success()

                return response

            except Exception as e:
                last_exception = e
                error_str = str(e)

                logging.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {error_str}")

                # Handle 403 Forbidden - likely bot detection
                if '403' in error_str:
                    logging.warning("403 Forbidden detected - likely TLS/bot detection")

                    # Rotate proxy if configured
                    if self.rotate_proxy_on_403 and self.proxy_manager.enabled:
                        self.proxy_manager.report_failure()
                        self.tls_client.update_proxy()

                    # Rotate TLS fingerprint if configured
                    if self.rotate_fingerprint_on_403:
                        self.tls_client.rotate_fingerprint()

                # Handle 429 Too Many Requests - rate limiting
                elif '429' in error_str:
                    logging.warning("429 Too Many Requests - rate limited")

                # Adaptive backoff with jitter (from research doc)
                backoff = min(
                    self.backoff_base ** (attempt + 1),
                    self.max_backoff
                )
                jitter = random.uniform(*self.jitter_range)
                wait_time = backoff * jitter

                logging.info(f"Waiting {wait_time:.2f}s before retry...")
                time.sleep(wait_time)

        # All retries exhausted
        logging.error(f"All {self.max_retries} attempts failed for {url}")
        raise last_exception

    def _get_headers(self) -> Dict[str, str]:
        """
        Get headers for API requests with browser-like ordering.

        Header order matters for WAF detection - this matches Chrome's pattern.
        """
        return {
            "Host": "acsyshf8au-dsn.algolia.net",
            "sec-ch-ua": '"Google Chrome";v="120", "Not;A=Brand";v="8", "Chromium";v="120"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "x-algolia-api-key": self.ALGOLIA_API_KEY,
            "x-algolia-application-id": self.ALGOLIA_APP_ID,
            "x-algolia-agent": "Algolia for JavaScript (5.46.2); Search (5.46.2); Browser",
            "Content-Type": "application/json",
            "Sec-Fetch-Site": "cross-site",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
            "Origin": "https://www.sobeys.com",
            "Referer": "https://www.sobeys.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-CA,en-US;q=0.9,en;q=0.8",
        }

    def _calculate_unit_price(self, price: float, size_text: str) -> Tuple[Optional[float], Optional[str]]:
        """
        Calculate unit price from total price and size text.

        Args:
            price: Total price in CAD
            size_text: Size text like "0.605 KG", "2 L", "500 ml", etc.

        Returns:
            Tuple of (unit_price, unit_price_uom) or (None, None) if cannot calculate
        """
        if not price or not size_text:
            return None, None

        try:
            size_clean = size_text.strip().upper()

            # Handle multi-pack formats like "12 x 355 ml"
            if '×' in size_clean or 'X' in size_clean:
                match = re.search(r'(\d+(?:\.\d+)?)\s*[×X]\s*(\d+(?:\.\d+)?)\s*([A-Z]+)', size_clean)
                if match:
                    count = float(match.group(1))
                    unit_size = float(match.group(2))
                    uom = match.group(3)
                    quantity = count * unit_size
                else:
                    return None, None
            else:
                match = re.search(r'(\d+(?:\.\d+)?)\s*([A-Z]+)', size_clean)
                if not match:
                    return None, None

                quantity = float(match.group(1))
                uom = match.group(2)

            # Normalize UOM to standard units
            if uom in ['ML', 'MILLILITER', 'MILLILITRE']:
                quantity = quantity / 1000
                uom = 'L'
            elif uom in ['G', 'GRAM', 'GRAMS']:
                quantity = quantity / 1000
                uom = 'KG'
            elif uom in ['L', 'LITER', 'LITRE']:
                uom = 'L'
            elif uom in ['KG', 'KILOGRAM', 'KILOGRAMS']:
                uom = 'KG'
            elif uom in ['EA', 'EACH', 'UNIT']:
                uom = 'EA'

            if quantity > 0:
                unit_price = round(price / quantity, 2)
                return unit_price, uom

            return None, None

        except (ValueError, AttributeError, ZeroDivisionError) as e:
            logging.debug(f"Could not calculate unit price from '{size_text}': {e}")
            return None, None

    def _save_debug_snapshot(self, name: str, data: Any, snapshot_type: str = "json"):
        """Save debug snapshot of request/response data."""
        if not self.debug:
            return

        timestamp = int(time.time())
        filename = f"{name}_{timestamp}.json"
        filepath = self.debug_dir / filename

        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                if isinstance(data, (dict, list)):
                    json.dump(data, f, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))

            logging.debug(f"[DEBUG_SNAPSHOT] Saved {snapshot_type} snapshot: {filename}")
        except Exception as e:
            logging.error(f"Failed to save debug snapshot {filename}: {e}")

    def search_products(self, query: str, max_pages: int = None, query_category: str = None) -> List[ProductRecord]:
        """
        Search for products using Algolia API across multiple Alberta stores.

        Implements store rotation from the research document to capture
        regional pricing variations across all configured stores.

        Args:
            query: Search term (e.g., "milk", "bread")
            max_pages: Maximum number of pages to scrape per store
            query_category: Category label for this search query

        Returns:
            List of ProductRecord objects with store-level pricing
        """
        if query_category is None:
            query_category = query

        all_products = []

        # Get all stores to query (Alberta only)
        stores = self.store_rotator.get_stores_for_query()
        logging.info(f"Querying {len(stores)} Alberta stores for '{query}'")

        for store in stores:
            store_id = store.id
            logging.info(f"\n{'='*60}")
            logging.info(f"Searching '{query}' at {store.name} ({store.city}, {store.province})")
            logging.info(f"Store ID: {store_id}")
            logging.info(f"{'='*60}")

            try:
                products = self._search_store(
                    query=query,
                    store_id=store_id,
                    max_pages=max_pages,
                    query_category=query_category
                )

                logging.info(f"Found {len(products)} products at {store.name}")
                all_products.extend(products)

            except Exception as e:
                logging.error(f"Failed to search store {store.name}: {e}")
                continue

            # Polite delay between stores
            delay = random.uniform(
                self.config.get('min_delay_seconds', 5.0),
                self.config.get('max_delay_seconds', 8.0)
            )
            logging.debug(f"Waiting {delay:.2f}s before next store...")
            time.sleep(delay)

        # Deduplicate by (UPC, store_id) - preserves store-level pricing
        unique_products = {}
        for product in all_products:
            store_id = product.raw_source.get('storeId', 'unknown')
            key = (product.external_id, store_id)

            if key not in unique_products:
                unique_products[key] = product
            else:
                existing = unique_products[key]
                # Log price variations (useful for monitoring)
                if existing.price != product.price:
                    logging.debug(
                        f"Price variation for {product.name} at store {store_id}: "
                        f"${existing.price} vs ${product.price}"
                    )
                # Keep the most recent one
                if product.scrape_ts > existing.scrape_ts:
                    unique_products[key] = product

        logging.info(f"Total unique products across all stores: {len(unique_products)}")
        return list(unique_products.values())

    def _search_store(self, query: str, store_id: str, max_pages: int = None,
                      query_category: str = None) -> List[ProductRecord]:
        """
        Search products at a specific store.

        Args:
            query: Search term
            store_id: Store identifier for regional pricing
            max_pages: Maximum pages to fetch
            query_category: Category label

        Returns:
            List of ProductRecord objects for this store
        """
        products = []
        page = 0
        total_pages_available = None

        while True:
            try:
                if total_pages_available is None:
                    page_info_str = "?"
                else:
                    page_info_str = str(total_pages_available)

                logging.info(f"Fetching page {page + 1}/{page_info_str} for '{query}' at store {store_id}...")

                page_products, page_info = self._search_algolia(
                    query, page, store_id=store_id, query_category=query_category
                )

                # Update total pages from first response
                if total_pages_available is None and page_info:
                    total_pages_available = page_info.get('nbPages', 0)
                    total_hits = page_info.get('nbHits', 0)
                    logging.info(f"Query '{query}' has {total_hits:,} products across {total_pages_available} pages")

                    # Auto-limit to reasonable number of pages
                    if max_pages is None:
                        max_pages = min(total_pages_available, 10)
                        logging.info(f"Auto-limiting to {max_pages} pages")

                if not page_products:
                    logging.info(f"No more products on page {page + 1}")
                    break

                logging.info(f"Extracted {len(page_products)} products from page {page + 1}")
                products.extend(page_products)

                page += 1

                if max_pages and page >= max_pages:
                    logging.info(f"Reached max_pages limit ({max_pages})")
                    break

                if total_pages_available and page >= total_pages_available:
                    logging.info("Reached end of available pages")
                    break

                # Polite delay between pages
                delay = random.uniform(
                    self.config.get('min_delay_seconds', 5.0),
                    self.config.get('max_delay_seconds', 8.0)
                )
                time.sleep(delay)

            except Exception as e:
                logging.error(f"Error fetching page {page + 1}: {e}")
                self._save_debug_snapshot(
                    f"error_{query}_store_{store_id}_page_{page}",
                    {"error": str(e), "query": query, "store_id": store_id, "page": page},
                    "error"
                )
                break

        return products

    def _search_algolia(self, query: str, page: int = 0, hits_per_page: int = 24,
                        store_id: str = None, query_category: str = None) -> Tuple[List[ProductRecord], dict]:
        """
        Search Algolia index for products.

        Args:
            query: Search term
            page: Page number (0-indexed)
            hits_per_page: Results per page
            store_id: Store ID for regional pricing context
            query_category: Category label

        Returns:
            Tuple of (List of ProductRecord, page_info dict)
        """
        if query_category is None:
            query_category = query
        if store_id is None:
            store_id = self.store_rotator.get_current_store().id

        url = f"{self.ALGOLIA_BASE_URL}/1/indexes/*/queries"

        # Build Algolia request
        params = f"query={query}&hitsPerPage={hits_per_page}&page={page}"

        body = {
            "requests": [
                {
                    "indexName": self.ALGOLIA_INDEX,
                    "params": params
                }
            ]
        }

        logging.debug(f"Algolia request: {body}")

        self._save_debug_snapshot(
            f"algolia_request_{query}_store_{store_id}_page_{page}",
            {"url": url, "body": body, "store_id": store_id},
            "json"
        )

        # Use TLS client with fingerprint impersonation
        response = self._fetch_with_retry(
            'POST', url,
            headers=self._get_headers(),
            json=body,
            timeout=30
        )

        data = response.json()

        self._save_debug_snapshot(
            f"algolia_response_{query}_store_{store_id}_page_{page}",
            data,
            "json"
        )

        if "results" in data and len(data["results"]) > 0:
            result = data["results"][0]
            hits = result.get("hits", [])
            logging.info(f"Algolia returned {len(hits)} hits")

            page_info = {
                'nbHits': result.get('nbHits', 0),
                'nbPages': result.get('nbPages', 0),
                'page': result.get('page', 0),
                'hitsPerPage': result.get('hitsPerPage', 24)
            }

            products = []
            for hit in hits:
                # Add store_id to raw_source for deduplication
                hit['storeId'] = store_id
                product = self._parse_algolia_product(hit, query_category=query_category)
                if product:
                    products.append(product)

            return products, page_info

        return [], {}

    def _parse_algolia_product(self, hit: Dict[str, Any], query_category: str = None) -> Optional[ProductRecord]:
        """
        Parse an Algolia hit into a ProductRecord.

        Args:
            hit: Product data from Algolia API
            query_category: Category label for the search query

        Returns:
            ProductRecord or None if parsing fails
        """
        try:
            name = hit.get("name") or hit.get("title") or hit.get("pageSlug", "Unknown")

            price = hit.get("price")
            if price is not None:
                price = float(price)

            brand = hit.get("brand") or hit.get("manufacturer")

            size_text = hit.get("weight") or hit.get("size") or hit.get("priceQuantity")
            if size_text and hit.get("uom"):
                size_text = f"{size_text} {hit.get('uom')}"

            unit_price = None
            unit_price_uom = None
            if hit.get("unitPrice"):
                unit_price = float(hit.get("unitPrice"))
                unit_price_uom = hit.get("uom")
            elif price and size_text:
                unit_price, unit_price_uom = self._calculate_unit_price(price, size_text)

            category_path = None
            if "hierarchicalCategories" in hit:
                hier_cats = hit["hierarchicalCategories"]
                for level in ["lvl2", "lvl1", "lvl0"]:
                    if level in hier_cats and hier_cats[level]:
                        cat = hier_cats[level]
                        category_path = cat[0] if isinstance(cat, list) else cat
                        break
            elif "categories" in hit:
                cats = hit["categories"]
                if cats:
                    category_path = " > ".join(cats) if isinstance(cats, list) else str(cats)

            in_stock = hit.get("inStock", True)
            availability = "in_stock" if in_stock else "out_of_stock"

            upc_raw = hit.get("upc") or hit.get("gtin") or hit.get("articleNumber")
            if upc_raw and isinstance(upc_raw, str) and ',' in upc_raw:
                external_id = upc_raw.split(',')[0].strip()
            else:
                external_id = upc_raw

            image_url = None
            if "images" in hit and hit["images"]:
                image_url = hit["images"][0] if isinstance(hit["images"], list) else hit["images"]
            elif "image" in hit:
                image_url = hit["image"]

            source_url = "https://www.sobeys.com"
            if "pageSlug" in hit:
                source_url = f"https://www.sobeys.com/product/{hit['pageSlug']}"

            record = ProductRecord(
                store="Sobeys",
                site_slug="sobeys",
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=external_id,
                name=name,
                brand=brand,
                size_text=size_text,
                price=price,
                currency="CAD",
                unit_price=unit_price,
                unit_price_uom=unit_price_uom,
                image_url=image_url,
                category_path=category_path,
                availability=availability,
                query_category=query_category,
                raw_source=hit
            )

            if not record.name or record.name == "Unknown":
                logging.debug(f"Skipping product with no name: {hit}")
                return None

            return record

        except Exception as e:
            logging.error(f"Error parsing product: {e}")
            logging.debug(f"Problem hit: {hit}")
            return None

    def scrape_product_page(self, product_url: str) -> Optional[ProductRecord]:
        """API scraper doesn't scrape individual product pages."""
        raise NotImplementedError("API scraper gets all data from search endpoint")

    def scrape_category(self, category_url: str, max_pages: int = 1) -> List[ProductRecord]:
        """API scraper doesn't use category URLs."""
        raise NotImplementedError("Use search_products() with a query instead")

    def scrape_search(self, query: str, max_pages: int = 1) -> List[ProductRecord]:
        """Scrape search results (wrapper for search_products)."""
        return self.search_products(query, max_pages)


def main():
    """Run comprehensive scraping test with store rotation."""
    setup_logging_with_rotation('sobeys', level=logging.INFO)

    project_root = Path(__file__).parent.parent.parent
    config_path = project_root / "configs" / "sobeys.json"

    scraper = SobeysAPIScraper(config_path, project_root)

    # Log configuration status
    logging.info(f"\n{'='*60}")
    logging.info("SOBEYS SCRAPER - CONFIGURATION")
    logging.info(f"{'='*60}")
    logging.info(f"Proxy enabled: {scraper.proxy_manager.enabled}")
    logging.info(f"TLS fingerprint: {scraper.tls_client.current_identifier}")
    logging.info(f"Stores configured: {len(scraper.store_rotator.stores)}")
    for store in scraper.store_rotator.stores:
        logging.info(f"  - {store}")

    all_products = []
    queries = ["milk", "bread", "eggs"]

    for query in queries:
        logging.info(f"\n{'='*60}")
        logging.info(f"Searching for: {query}")
        logging.info(f"{'='*60}")

        products = scraper.search_products(query, max_pages=5)
        logging.info(f"Found {len(products)} unique products for '{query}'")
        all_products.extend(products)

    # Final cross-query deduplication by (UPC, store_id)
    unique_products = {}
    for product in all_products:
        store_id = product.raw_source.get('storeId', 'unknown')
        key = (product.external_id, store_id)
        if key not in unique_products:
            unique_products[key] = product

    final_products = list(unique_products.values())

    logging.info(f"\n{'='*60}")
    logging.info(f"TOTAL UNIQUE PRODUCTS: {len(final_products)}")
    logging.info(f"{'='*60}")

    # Show sample products
    if final_products:
        logging.info("\nSample products (first 5):")
        for product in final_products[:5]:
            logging.info(f"\n  Name: {product.name}")
            logging.info(f"  Brand: {product.brand}")
            logging.info(f"  Price: ${product.price} {product.currency}")
            logging.info(f"  Store ID: {product.raw_source.get('storeId', 'unknown')}")
            logging.info(f"  UPC: {product.external_id}")

    # Save output
    output_dir = Path("data/raw/sobeys")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "sobeys_products.jsonl"

    backup_data_file(output_file)

    with open(output_file, 'w', encoding='utf-8') as f:
        for product in final_products:
            f.write(json.dumps(product.to_dict()) + '\n')

    logging.info(f"\n{'='*60}")
    logging.info("SCRAPING COMPLETE")
    logging.info(f"{'='*60}")
    logging.info(f"Saved {len(final_products)} products to: {output_file}")
    logging.info(f"File size: {output_file.stat().st_size / 1024:.2f} KB")


if __name__ == "__main__":
    main()
