#!/usr/bin/env python3
"""
Walmart Canada Scraper - Sitemap-based with Playwright
Uses XML sitemaps for product discovery and Playwright for page rendering.
Extracts data from __NEXT_DATA__ JSON for reliability.
Includes CAPTCHA solving integration for PerimeterX challenges.
"""

import json
import logging
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext, TimeoutError as PlaywrightTimeout

from scrapers.base import BaseScraper, ProductRecord
from scrapers.common import get_iso_timestamp
from scrapers.utils.sitemap_parser import SitemapParser, SitemapEntry, filter_walmart_product_urls
from scrapers.utils.captcha_solver import CaptchaSolverManager


class WalmartCanadaScraper(BaseScraper):
    """
    Scraper for Walmart Canada (walmart.ca) using:
    - Sitemap-based product discovery (robots.txt compliant)
    - Playwright for JavaScript rendering
    - __NEXT_DATA__ JSON extraction
    - CAPTCHA solving for PerimeterX challenges
    """

    # PerimeterX/Akamai detection indicators
    CAPTCHA_INDICATORS = [
        "press & hold",
        "press and hold",
        "px-captcha",
        "perimeterx",
        "distil_r_captcha",
        "_incapsula_resource",
        "blocked",
        "access denied",
    ]

    # PerimeterX specific selectors
    PX_CAPTCHA_SELECTORS = [
        "#px-captcha",
        "[data-testid='px-captcha']",
        ".px-captcha-container",
        "iframe[src*='captcha']",
    ]

    def __init__(self, config_path: Path, project_root: Path, headless: bool = True, fresh_start: bool = False):
        """
        Initialize Walmart Canada scraper.

        Args:
            config_path: Path to config JSON file
            project_root: Project root directory
            headless: Run browser in headless mode
            fresh_start: Clear existing data and start fresh
        """
        super().__init__(config_path, project_root, fresh_start=fresh_start)

        self.headless = headless
        self.base_url = self.config['base_url']

        # Playwright components
        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        # Sitemap parser
        self.sitemap_parser = SitemapParser()

        # CAPTCHA solver
        captcha_config = self.config.get('captcha_solver', {})
        self.captcha_solver = CaptchaSolverManager(captcha_config)
        self.captcha_solve_attempts = 0
        self.captcha_solve_successes = 0

        # Session state
        self.session_warmed_up = False
        self.consecutive_errors = 0
        self.max_consecutive_errors = 5

        # Current query category for tracking
        self.current_category = None

        # Log CAPTCHA solver status
        if self.captcha_solver.is_available():
            logging.info("CAPTCHA solver is enabled and configured")
            balances = self.captcha_solver.get_balances()
            for provider, balance in balances.items():
                logging.info(f"  {provider} balance: ${balance:.2f}")
        else:
            logging.warning("CAPTCHA solver is not configured - blocks will not be bypassed")

    def _get_browser_config(self) -> Dict:
        """Get browser configuration from config file."""
        browser_config = self.config.get('browser', {})
        return {
            'viewport': {
                'width': browser_config.get('viewport_width', 1920),
                'height': browser_config.get('viewport_height', 1080)
            },
            'locale': browser_config.get('locale', 'en-CA'),
            'timezone_id': browser_config.get('timezone', 'America/Toronto'),
            'user_agent': browser_config.get('user_agent',
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            ),
        }

    def start_browser(self):
        """Initialize Playwright browser with stealth configuration."""
        if self.browser:
            return

        logging.info("Starting Playwright browser...")
        self.playwright = sync_playwright().start()

        # Launch browser
        self.browser = self.playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox',
                '--disable-setuid-sandbox',
            ]
        )

        # Create context with fingerprint
        config = self._get_browser_config()
        self.context = self.browser.new_context(
            viewport=config['viewport'],
            locale=config['locale'],
            timezone_id=config['timezone_id'],
            user_agent=config['user_agent'],
            extra_http_headers={
                'Accept-Language': 'en-CA,en-US;q=0.9,en;q=0.8',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            }
        )

        # Apply stealth scripts to evade detection
        self.context.add_init_script("""
            // Override navigator.webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });

            // Override navigator.plugins
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Override navigator.languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-CA', 'en-US', 'en']
            });

            // Override chrome runtime
            window.chrome = {
                runtime: {}
            };
        """)

        self.page = self.context.new_page()
        logging.info("Browser started successfully")

    def stop_browser(self):
        """Close browser and cleanup."""
        if self.page:
            self.page.close()
            self.page = None
        if self.context:
            self.context.close()
            self.context = None
        if self.browser:
            self.browser.close()
            self.browser = None
        if self.playwright:
            self.playwright.stop()
            self.playwright = None
        logging.info("Browser stopped")

    def _human_delay(self, min_seconds: float = 0.5, max_seconds: float = 2.0):
        """Add human-like random delay."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def _scroll_page(self):
        """Simulate human scrolling behavior."""
        if not self.page:
            return

        try:
            # Get page height
            height = self.page.evaluate("document.body.scrollHeight")

            # Scroll in increments
            current = 0
            increment = random.randint(300, 500)

            while current < height * 0.7:  # Scroll to ~70% of page
                current += increment
                self.page.evaluate(f"window.scrollTo(0, {current})")
                self._human_delay(0.1, 0.3)

            # Scroll back up a bit (human behavior)
            if random.random() < 0.3:
                scroll_back = random.randint(100, 300)
                self.page.evaluate(f"window.scrollTo(0, {current - scroll_back})")

        except Exception as e:
            logging.debug(f"Scroll failed: {e}")

    def _is_blocked(self) -> bool:
        """Check if current page shows a CAPTCHA or block."""
        if not self.page:
            return False

        try:
            content = self.page.content().lower()
            for indicator in self.CAPTCHA_INDICATORS:
                if indicator in content:
                    logging.warning(f"Block detected: found '{indicator}' in page")
                    return True
            return False
        except Exception:
            return False

    def _extract_px_data(self) -> Optional[str]:
        """
        Extract PerimeterX data blob from the page.
        This data is needed by CAPTCHA solving services.

        Returns:
            PerimeterX data blob string, or None if not found
        """
        if not self.page:
            return None

        try:
            # Try to get _px3 cookie
            cookies = self.context.cookies()
            for cookie in cookies:
                if cookie.get('name') == '_px3':
                    return cookie.get('value')

            # Try to extract from page scripts
            px_data = self.page.evaluate("""
                () => {
                    // Look for PerimeterX data in window object
                    if (window._pxUuid) return window._pxUuid;
                    if (window._pxVid) return window._pxVid;

                    // Look in script tags
                    const scripts = document.querySelectorAll('script');
                    for (const script of scripts) {
                        const text = script.textContent || '';
                        const match = text.match(/["']([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})["']/i);
                        if (match) return match[1];
                    }
                    return null;
                }
            """)

            return px_data

        except Exception as e:
            logging.debug(f"Failed to extract PX data: {e}")
            return None

    def _find_captcha_element(self):
        """Find the CAPTCHA element on the page."""
        if not self.page:
            return None

        for selector in self.PX_CAPTCHA_SELECTORS:
            try:
                element = self.page.query_selector(selector)
                if element:
                    return element
            except Exception:
                continue

        return None

    def _handle_captcha(self) -> bool:
        """
        Attempt to solve PerimeterX CAPTCHA challenge.

        Returns:
            True if CAPTCHA was solved successfully, False otherwise
        """
        if not self.captcha_solver.is_available():
            logging.warning("CAPTCHA solver not configured, cannot bypass block")
            return False

        self.captcha_solve_attempts += 1
        current_url = self.page.url

        logging.info(f"Attempting to solve PerimeterX CAPTCHA (attempt #{self.captcha_solve_attempts})")

        # Extract PerimeterX data blob
        px_data = self._extract_px_data()
        user_agent = self._get_browser_config().get('user_agent')

        # Call CAPTCHA solver
        result = self.captcha_solver.solve_perimeterx(
            site_url=current_url,
            data_blob=px_data,
            user_agent=user_agent
        )

        if not result.success:
            logging.error(f"CAPTCHA solving failed: {result.error}")
            return False

        logging.info(f"CAPTCHA solved in {result.solve_time:.1f}s")
        self.captcha_solve_successes += 1

        # Inject the solution token
        if result.token:
            try:
                # Method 1: Set cookie with token
                self.context.add_cookies([{
                    'name': '_px3',
                    'value': result.token,
                    'domain': '.walmart.ca',
                    'path': '/'
                }])

                # Method 2: Try to inject via JavaScript
                self.page.evaluate(f"""
                    (token) => {{
                        // Try setting various PerimeterX related variables
                        if (window._pxParam1) window._pxParam1 = token;
                        if (window._pxCaptchaSolved) window._pxCaptchaSolved = true;

                        // Dispatch custom event that PerimeterX might listen for
                        window.dispatchEvent(new CustomEvent('px-captcha-solved', {{
                            detail: {{ token: token }}
                        }}));
                    }}
                """, result.token)

                # Wait a moment for the solution to be processed
                self._human_delay(1, 2)

                # Reload the page to apply the new session
                self.page.reload(wait_until='domcontentloaded', timeout=30000)
                self._human_delay(2, 3)

                # Check if block is cleared
                if not self._is_blocked():
                    logging.info("CAPTCHA bypass successful!")
                    return True
                else:
                    logging.warning("CAPTCHA token injected but page still blocked")
                    return False

            except Exception as e:
                logging.error(f"Failed to inject CAPTCHA solution: {e}")
                return False

        return False

    def _handle_block(self, url: str) -> bool:
        """
        Handle a detected block/CAPTCHA.

        Args:
            url: The URL that was blocked

        Returns:
            True if block was bypassed, False otherwise
        """
        logging.warning(f"Block detected on: {url}")

        # Try CAPTCHA solving if available
        if self.captcha_solver.is_available():
            if self._handle_captcha():
                return True

        # If CAPTCHA solving failed or not available, apply backoff
        logging.info("Applying backoff delay...")
        self._human_delay(10, 20)

        return False

    def warmup_session(self):
        """
        Warm up the browser session by visiting non-target pages.
        This helps build trust score with anti-bot systems.
        """
        if self.session_warmed_up:
            return

        if not self.config.get('warmup', {}).get('enabled', True):
            self.session_warmed_up = True
            return

        logging.info("Warming up session...")
        warmup_pages = self.config.get('warmup', {}).get('pages', ['/'])

        for path in warmup_pages:
            try:
                url = f"{self.base_url}{path}" if path.startswith('/') else path
                logging.info(f"Visiting: {url}")

                self.page.goto(url, wait_until='domcontentloaded', timeout=30000)
                self._human_delay(2, 4)

                if self._is_blocked():
                    logging.warning("Block detected during warmup!")
                    self._human_delay(5, 10)
                    continue

                # Simulate browsing
                self._scroll_page()
                self._human_delay(1, 3)

            except Exception as e:
                logging.warning(f"Warmup page failed: {e}")

        self.session_warmed_up = True
        logging.info("Session warmup complete")

    def _extract_next_data(self) -> Optional[Dict]:
        """
        Extract __NEXT_DATA__ JSON from page.
        This is the most reliable way to get product data from Walmart.
        """
        if not self.page:
            return None

        try:
            # Find the __NEXT_DATA__ script tag
            script = self.page.query_selector('script#__NEXT_DATA__')
            if script:
                json_text = script.inner_text()
                return json.loads(json_text)
        except Exception as e:
            logging.debug(f"Failed to extract __NEXT_DATA__: {e}")

        return None

    def _parse_product_from_next_data(self, next_data: Dict, source_url: str) -> Optional[ProductRecord]:
        """
        Parse product data from __NEXT_DATA__ JSON.

        Args:
            next_data: Parsed __NEXT_DATA__ JSON
            source_url: Product page URL

        Returns:
            ProductRecord if successful, None otherwise
        """
        try:
            # Navigate to product data in the JSON structure
            # Structure: props.pageProps.initialData.data.product
            props = next_data.get('props', {})
            page_props = props.get('pageProps', {})
            initial_data = page_props.get('initialData', {})
            data = initial_data.get('data', {})
            product = data.get('product', {})

            if not product:
                # Try alternate paths
                product = page_props.get('product', {})

            if not product:
                logging.debug("No product data found in __NEXT_DATA__")
                return None

            # Extract product fields
            name = product.get('name') or product.get('title', '')
            if not name:
                return None

            # Extract item ID from URL
            item_id = None
            url_match = re.search(r'/ip/[^/]+/(\d+)', source_url)
            if url_match:
                item_id = url_match.group(1)

            # Price extraction
            price = None
            price_info = product.get('priceInfo', {})
            if price_info:
                current_price = price_info.get('currentPrice', {})
                price = current_price.get('price') or current_price.get('priceValue')
            if price is None:
                price = product.get('price')

            # Brand
            brand = product.get('brand') or product.get('brandName')

            # Size/weight
            size_text = product.get('size') or product.get('weight') or product.get('quantity')

            # Unit price
            unit_price = None
            unit_price_uom = None
            if price_info:
                unit_price_info = price_info.get('unitPrice', {})
                unit_price = unit_price_info.get('price')
                unit_price_uom = unit_price_info.get('unit')

            # Image URL
            image_url = None
            images = product.get('images', [])
            if images:
                image_url = images[0].get('url') or images[0].get('src')
            elif product.get('imageUrl'):
                image_url = product.get('imageUrl')

            # Category
            category_path = None
            categories = product.get('categories', [])
            if categories:
                category_names = [c.get('name', '') for c in categories if c.get('name')]
                if category_names:
                    category_path = ' > '.join(category_names)

            # Availability
            availability = 'unknown'
            avail_status = product.get('availabilityStatus') or product.get('availability')
            if avail_status:
                avail_lower = str(avail_status).lower()
                if 'in_stock' in avail_lower or avail_lower == 'available':
                    availability = 'in_stock'
                elif 'out_of_stock' in avail_lower or avail_lower == 'unavailable':
                    availability = 'out_of_stock'

            # UPC/GTIN
            external_id = item_id or product.get('upc') or product.get('gtin') or product.get('sku')

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=external_id,
                name=name,
                brand=brand,
                size_text=size_text,
                price=float(price) if price else None,
                currency='CAD',
                unit_price=float(unit_price) if unit_price else None,
                unit_price_uom=unit_price_uom,
                image_url=image_url,
                category_path=category_path,
                availability=availability,
                query_category=self.current_category,
                raw_source=None  # Don't store raw to save space
            )

        except Exception as e:
            logging.error(f"Failed to parse product from __NEXT_DATA__: {e}")
            return None

    def scrape_product_page(self, product_url: str) -> Optional[ProductRecord]:
        """
        Scrape a single product detail page.

        Args:
            product_url: Product page URL

        Returns:
            ProductRecord if successful, None otherwise
        """
        if not self.page:
            self.start_browser()
            self.warmup_session()

        try:
            # Apply rate limiting
            self.rate_limiter.adaptive_wait(self.consecutive_errors)

            logging.info(f"Scraping: {product_url}")
            self.page.goto(product_url, wait_until='domcontentloaded', timeout=45000)

            # Wait for page to settle
            self._human_delay(1, 2)

            # Check for blocks
            if self._is_blocked():
                logging.warning(f"Blocked on product page: {product_url}")

                # Try to handle the block (CAPTCHA solving)
                if self._handle_block(product_url):
                    logging.info("Block bypassed, continuing extraction")
                else:
                    self.consecutive_errors += 1
                    self.stats['errors'] += 1

                    if self.consecutive_errors >= self.max_consecutive_errors:
                        logging.error("Too many consecutive errors, stopping")
                        return None

                    return None

            # Reset error counter on success
            self.consecutive_errors = 0

            # Try to extract __NEXT_DATA__
            next_data = self._extract_next_data()
            if next_data:
                product = self._parse_product_from_next_data(next_data, product_url)
                if product:
                    return product

            # Fallback to DOM extraction if enabled
            if self.config.get('extraction', {}).get('fallback_to_dom', False):
                logging.debug("Attempting DOM fallback extraction")
                return self._extract_from_dom(product_url)

            return None

        except PlaywrightTimeout:
            logging.warning(f"Timeout loading: {product_url}")
            self.consecutive_errors += 1
            self.stats['errors'] += 1
            return None

        except Exception as e:
            logging.error(f"Error scraping {product_url}: {e}")
            self.consecutive_errors += 1
            self.stats['errors'] += 1
            return None

    def _extract_from_dom(self, source_url: str) -> Optional[ProductRecord]:
        """
        Fallback: Extract product data from DOM elements.
        Less reliable than __NEXT_DATA__ but works as backup.
        """
        try:
            # Try common selectors
            name_elem = self.page.query_selector('h1[data-testid="product-title"]') or \
                       self.page.query_selector('h1.product-title') or \
                       self.page.query_selector('h1')
            name = name_elem.inner_text().strip() if name_elem else None

            if not name:
                return None

            # Price
            price = None
            price_elem = self.page.query_selector('[data-testid="price-current"]') or \
                        self.page.query_selector('.price-current') or \
                        self.page.query_selector('[itemprop="price"]')
            if price_elem:
                price_text = price_elem.inner_text()
                price_match = re.search(r'[\d,.]+', price_text.replace(',', ''))
                if price_match:
                    price = float(price_match.group())

            # Item ID from URL
            item_id = None
            url_match = re.search(r'/ip/[^/]+/(\d+)', source_url)
            if url_match:
                item_id = url_match.group(1)

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=item_id,
                name=name,
                brand=None,
                size_text=None,
                price=price,
                currency='CAD',
                unit_price=None,
                unit_price_uom=None,
                image_url=None,
                category_path=None,
                availability='unknown',
                query_category=self.current_category,
                raw_source=None
            )

        except Exception as e:
            logging.error(f"DOM extraction failed: {e}")
            return None

    def scrape_from_sitemap(self, sitemap_url: Optional[str] = None,
                           max_products: Optional[int] = None) -> int:
        """
        Scrape products discovered from sitemap.

        Args:
            sitemap_url: Sitemap URL (uses config if not provided)
            max_products: Maximum products to scrape

        Returns:
            Number of products scraped
        """
        if not sitemap_url:
            sitemaps = self.config.get('sitemaps', {})
            sitemap_url = sitemaps.get('product_1p_en')

        if not sitemap_url:
            logging.error("No sitemap URL configured")
            return 0

        logging.info(f"Discovering products from sitemap: {sitemap_url}")

        # Start browser and warm up
        self.start_browser()
        self.warmup_session()

        # Get product URLs from sitemap
        try:
            product_entries = self.sitemap_parser.get_product_urls(
                sitemap_url=sitemap_url,
                max_urls=max_products
            )
        except Exception as e:
            logging.error(f"Failed to parse sitemap: {e}")
            return 0

        if not product_entries:
            logging.warning("No product URLs found in sitemap")
            return 0

        logging.info(f"Found {len(product_entries)} product URLs")
        scraped_count = 0

        for entry in product_entries:
            if max_products and scraped_count >= max_products:
                break

            # Skip if URL doesn't pass filter
            if not filter_walmart_product_urls(entry.loc):
                continue

            product = self.scrape_product_page(entry.loc)
            if product:
                if self.save_record(product):
                    scraped_count += 1
                    logging.info(f"Scraped {scraped_count}: {product.name}")

            # Checkpoint periodically
            if scraped_count > 0 and scraped_count % 10 == 0:
                self.save_checkpoint({'last_url': entry.loc})

        return scraped_count

    def scrape_category(self, category_url: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from a category page.
        Note: This is primarily for interface compatibility.
        For Walmart, sitemap-based scraping is preferred.
        """
        logging.warning("Category scraping not implemented. Use scrape_from_sitemap() instead.")
        return 0

    def scrape_search(self, query: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from search results.
        Note: robots.txt disallows /search/* so this is not implemented.
        """
        logging.error("Search scraping is disallowed by robots.txt. Use scrape_from_sitemap() instead.")
        return 0

    def run_demo(self) -> int:
        """
        Run a demo scrape with limited products.

        Returns:
            Number of products scraped
        """
        demo_config = self.config.get('demo', {})
        max_products = demo_config.get('max_products', 20)

        logging.info(f"Running demo scrape (max {max_products} products)...")

        try:
            count = self.scrape_from_sitemap(max_products=max_products)
            self.print_stats()
            self.export_to_csv()
            return count
        finally:
            self.stop_browser()


def main():
    """Main entry point for demo."""
    import sys

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )

    # Paths
    project_root = Path(__file__).parent.parent.parent
    config_path = project_root / 'configs' / 'walmart_canada.json'

    if not config_path.exists():
        logging.error(f"Config file not found: {config_path}")
        sys.exit(1)

    logging.info("=" * 70)
    logging.info("Walmart Canada Scraper - Demo")
    logging.info("=" * 70)

    # Run scraper
    scraper = WalmartCanadaScraper(
        config_path=config_path,
        project_root=project_root,
        headless=True,  # Set to False to see browser
        fresh_start=True
    )

    try:
        count = scraper.run_demo()
        logging.info(f"\nDemo complete! Scraped {count} products.")
    except KeyboardInterrupt:
        logging.info("\nScraping interrupted by user")
    except Exception as e:
        logging.error(f"Scraping failed: {e}")
        raise
    finally:
        scraper.stop_browser()


if __name__ == '__main__':
    main()
