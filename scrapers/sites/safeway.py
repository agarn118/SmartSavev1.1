#!/usr/bin/env python3
"""
Safeway scraper (Sobeys Network) - Enhanced Stealth Version
Uses Algolia API as primary extraction method with Playwright fallback.
Requires JavaScript rendering via Playwright for fallback.
"""

import json
import logging
import random
import time
import re
import requests
from typing import Optional, List, Dict, Tuple
from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

from scrapers.base import BaseScraper, ProductRecord
from scrapers.common import get_iso_timestamp


class SafewayScraper(BaseScraper):
    """Scraper for Safeway (Sobeys network) using Algolia API with Playwright fallback."""

    # Algolia API configuration (shared across Sobeys network)
    ALGOLIA_APP_ID = "ACSYSHF8AU"
    ALGOLIA_API_KEY = "fe555974f588b3e76ad0f1c548113b22"
    ALGOLIA_BASE_URL = "https://acsyshf8au-dsn.algolia.net"
    ALGOLIA_INDEX = "dxp_product_en"

    def __init__(self, config_path, project_root, headless=True, fresh_start=False):
        super().__init__(config_path, project_root, fresh_start=fresh_start)

        self.base_url = self.config['base_url']
        self.headless = headless
        self.store_id = self.config.get('store_id')
        self.store_postal_code = self.config.get('store_postal_code')
        self.store_name_filter = self.config.get('store_name_filter')

        self.playwright = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.store_selected = False
        self.current_query = None  # Track current search query for ProductRecord

        # Google navigation settings
        self.use_google_navigation = self.config.get('use_google_navigation', True)
        self.search_term = self.config.get('google_search_term', 'safeway')

        # Algolia session for API requests
        self.algolia_session = requests.Session()
        self.algolia_session.headers.update(self._get_algolia_headers())

    def _get_algolia_headers(self) -> Dict[str, str]:
        """Get headers for Algolia API requests."""
        # Algolia requires sobeys.com referer (shared backend for Sobeys network)
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "x-algolia-api-key": self.ALGOLIA_API_KEY,
            "x-algolia-application-id": self.ALGOLIA_APP_ID,
            "x-algolia-agent": "Algolia for JavaScript (5.46.2); Search (5.46.2); Browser",
            "Content-Type": "application/json",
            "Origin": "https://www.sobeys.com",
            "Referer": "https://www.sobeys.com/",
        }

    def _search_algolia(self, query: str, page: int = 0, hits_per_page: int = 48) -> Tuple[List[ProductRecord], Dict]:
        """
        Search products using Algolia API.

        Args:
            query: Search term
            page: Page number (0-indexed)
            hits_per_page: Results per page

        Returns:
            Tuple of (list of ProductRecords, page_info dict)
        """
        url = f"{self.ALGOLIA_BASE_URL}/1/indexes/*/queries"

        body = {
            "requests": [
                {
                    "indexName": self.ALGOLIA_INDEX,
                    "params": f"query={query}&hitsPerPage={hits_per_page}&page={page}"
                }
            ]
        }

        try:
            self.rate_limiter.wait()
            response = self.algolia_session.post(url, json=body, timeout=30)
            response.raise_for_status()
            data = response.json()

            if "results" in data and len(data["results"]) > 0:
                result = data["results"][0]
                hits = result.get("hits", [])
                logging.info(f"Algolia returned {len(hits)} hits for '{query}' (page {page + 1})")

                page_info = {
                    'nbHits': result.get('nbHits', 0),
                    'nbPages': result.get('nbPages', 0),
                    'page': result.get('page', 0),
                    'hitsPerPage': result.get('hitsPerPage', hits_per_page)
                }

                products = []
                for hit in hits:
                    product = self._parse_algolia_hit(hit)
                    if product:
                        products.append(product)

                return products, page_info

        except Exception as e:
            logging.error(f"Algolia search failed: {e}")

        return [], {}

    def _parse_algolia_hit(self, hit: Dict) -> Optional[ProductRecord]:
        """Parse an Algolia hit into a ProductRecord."""
        try:
            name = hit.get("name") or hit.get("title") or hit.get("pageSlug", "")
            if not name:
                return None

            price = hit.get("price")
            if price is not None:
                price = float(price)

            brand = hit.get("brand") or hit.get("manufacturer")

            size_text = hit.get("weight") or hit.get("size") or hit.get("priceQuantity")
            if size_text and hit.get("uom"):
                size_text = f"{size_text} {hit.get('uom')}"

            # Calculate unit price
            unit_price = None
            unit_price_uom = None
            if hit.get("unitPrice"):
                unit_price = float(hit.get("unitPrice"))
                unit_price_uom = hit.get("uom")

            # Extract category
            category_path = None
            if "hierarchicalCategories" in hit:
                hier_cats = hit["hierarchicalCategories"]
                for level in ["lvl2", "lvl1", "lvl0"]:
                    if level in hier_cats and hier_cats[level]:
                        cat = hier_cats[level]
                        category_path = cat[0] if isinstance(cat, list) else cat
                        break

            # Availability
            in_stock = hit.get("inStock", True)
            availability = "in_stock" if in_stock else "out_of_stock"

            # External ID (UPC)
            external_id = hit.get("upc") or hit.get("gtin") or hit.get("articleNumber")
            if external_id and isinstance(external_id, str) and ',' in external_id:
                external_id = external_id.split(',')[0].strip()

            # Image URL
            image_url = None
            if "images" in hit and hit["images"]:
                image_url = hit["images"][0] if isinstance(hit["images"], list) else hit["images"]
            elif "image" in hit:
                image_url = hit["image"]

            # Source URL
            source_url = self.base_url
            if "pageSlug" in hit:
                source_url = f"{self.base_url}/product/{hit['pageSlug']}"

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
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
                query_category=self.current_query,
                raw_source={'type': 'algolia', 'data': hit}
            )

        except Exception as e:
            logging.warning(f"Failed to parse Algolia hit: {e}")
            return None

    def _human_delay(self, min_seconds=0.5, max_seconds=2.0):
        """Random delay to simulate human behavior."""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)

    def _random_mouse_movement(self, page: Page):
        """Simulate random mouse movements."""
        try:
            viewport = page.viewport_size
            if viewport:
                x = random.randint(100, viewport['width'] - 100)
                y = random.randint(100, viewport['height'] - 100)
                page.mouse.move(x, y)
                self._human_delay(0.1, 0.3)
        except Exception as e:
            logging.debug(f"Mouse movement failed: {e}")

    def _launch_browser(self):
        """Launch Playwright browser with enhanced stealth settings."""
        if self.playwright is None:
            self.playwright = sync_playwright().start()

        if self.browser is None:
            logging.info(f"Launching stealth browser (headless={self.headless})...")
            
            # Launch with enhanced stealth args
            self.browser = self.playwright.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--disable-web-security',
                    '--disable-features=IsolateOrigins,site-per-process',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-infobars',
                    '--window-position=0,0',
                    '--ignore-certificate-errors',
                    '--ignore-certificate-errors-spki-list',
                    '--disable-blink-features=AutomationControlled',
                    '--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
                ]
            )

            # Create context with realistic settings
            self.context = self.browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
                locale='en-CA',
                timezone_id='America/Edmonton',
                permissions=['geolocation'],
                geolocation={'latitude': 51.2938, 'longitude': -113.9918},
                has_touch=False,
                is_mobile=False,
                color_scheme='light',
                extra_http_headers={
                    'Accept-Language': 'en-CA,en-US;q=0.9,en;q=0.8',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                    'Sec-Fetch-Dest': 'document',
                    'Sec-Fetch-Mode': 'navigate',
                    'Sec-Fetch-Site': 'none',
                    'Sec-Fetch-User': '?1',
                    'Cache-Control': 'max-age=0',
                }
            )
            
            # Add comprehensive stealth scripts
            self.context.add_init_script("""
                // Remove webdriver flag
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                
                // Override the navigator.plugins to avoid detection
                Object.defineProperty(navigator, 'plugins', {
                    get: () => {
                        return [
                            {
                                0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format"},
                                description: "Portable Document Format",
                                filename: "internal-pdf-viewer",
                                length: 1,
                                name: "Chrome PDF Plugin"
                            },
                            {
                                0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format"},
                                description: "Portable Document Format", 
                                filename: "mhjfbmdgcfjbbpaeojofohoefgiehjai",
                                length: 1,
                                name: "Chrome PDF Viewer"
                            },
                            {
                                description: "Portable Document Format",
                                filename: "internal-pdf-viewer",
                                length: 1,
                                name: "PDF Viewer"
                            }
                        ];
                    }
                });
                
                // Mock Chrome runtime
                window.chrome = {
                    runtime: {},
                    loadTimes: function() {},
                    csi: function() {},
                    app: {}
                };
                
                // Override permissions
                const originalQuery = window.navigator.permissions.query;
                window.navigator.permissions.query = (parameters) => (
                    parameters.name === 'notifications' ?
                        Promise.resolve({ state: Notification.permission }) :
                        originalQuery(parameters)
                );
                
                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-CA', 'en-US', 'en']
                });
                
                // Add vendor
                Object.defineProperty(navigator, 'vendor', {
                    get: () => 'Google Inc.'
                });
                
                // Mock maxTouchPoints
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    get: () => 0
                });
                
                // Mock platform
                Object.defineProperty(navigator, 'platform', {
                    get: () => 'Win32'
                });
                
                // Mock productSub
                Object.defineProperty(navigator, 'productSub', {
                    get: () => '20030107'
                });
            """)

            logging.info("Stealth browser launched successfully")

    def _close_browser(self):
        """Close Playwright browser."""
        if self.context:
            self.context.close()
            self.context = None

        if self.browser:
            self.browser.close()
            self.browser = None

        if self.playwright:
            self.playwright.stop()
            self.playwright = None

        logging.info("Browser closed")

    def _detect_captcha(self, page: Page) -> bool:
        """Detect if page is showing a CAPTCHA."""
        try:
            captcha_indicators = [
                'recaptcha',
                'i\'m not a robot',
                'unusual traffic',
                'captcha',
                'robot'
            ]
            
            page_text = page.content().lower()
            for indicator in captcha_indicators:
                if indicator in page_text:
                    return True
            
            # Check for reCAPTCHA iframe
            captcha_frame = page.query_selector('iframe[src*="recaptcha"], iframe[title*="recaptcha"]')
            if captcha_frame:
                return True
                
            return False
        except:
            return False

    def _navigate_via_google(self, page: Page, search_term: str = None) -> bool:
        """
        Navigate to target site via Google Search (stealth technique).
        
        Args:
            page: Playwright page
            search_term: What to search for (defaults to config value)
            
        Returns:
            True if successfully navigated, False otherwise
        """
        try:
            if search_term is None:
                search_term = self.search_term
            
            logging.info(f"Starting Google navigation for '{search_term}'...")
            
            # Step 1: Go to Google homepage
            logging.info("Loading Google homepage...")
            page.goto('https://www.google.com', wait_until='domcontentloaded', timeout=30000)
            page.wait_for_timeout(2000)
            
            # Check for CAPTCHA immediately
            if self._detect_captcha(page):
                logging.warning("Google CAPTCHA detected! Skipping Google navigation...")
                return False
            
            self._human_delay(1.5, 3.0)
            
            # Random mouse movement on Google homepage
            self._random_mouse_movement(page)
            
            # Step 2: Handle Google location popup if present
            self._dismiss_google_popups(page)
            
            # Step 3: Find and use search box
            logging.info(f"Searching for '{search_term}'...")
            search_selectors = [
                'textarea[name="q"]',
                'input[name="q"]',
                'textarea[title="Search"]',
                'input[title="Search"]'
            ]
            
            search_box = None
            for selector in search_selectors:
                try:
                    search_box = page.wait_for_selector(selector, state='visible', timeout=5000)
                    if search_box:
                        logging.info(f"Found search box: {selector}")
                        break
                except:
                    continue
            
            if not search_box:
                logging.error("Could not find Google search box")
                return False
            
            # Type search query with human-like delays
            search_box.click()
            self._human_delay(0.3, 0.7)
            
            for char in search_term:
                search_box.type(char, delay=random.randint(50, 150))
            
            self._human_delay(0.5, 1.0)
            
            # Submit search
            search_box.press('Enter')
            page.wait_for_timeout(2000)
            
            # Check for CAPTCHA after search
            if self._detect_captcha(page):
                logging.warning("Google CAPTCHA detected after search! Skipping Google navigation...")
                return False
            
            try:
                page.wait_for_load_state('domcontentloaded', timeout=10000)
            except:
                page.wait_for_timeout(3000)
                
            logging.info("Search submitted, waiting for results...")
            
            self._human_delay(1.5, 2.5)
            
            # Step 4: Dismiss Google location popup on results page if present
            self._dismiss_google_popups(page)
            
            # Step 5: Click on the first organic result (not sponsored)
            logging.info("Looking for organic search result...")
            
            # Wait a bit for results to fully load
            self._human_delay(1.0, 2.0)
            
            # Find the main results container
            result_selectors = [
                'div#search a[href*="safeway.ca"]:not([data-rw])',  # Organic results, not ads
                'div.g a[href*="safeway.ca"]',
                'a[href*="safeway.ca"]',
            ]
            
            clicked = False
            for selector in result_selectors:
                try:
                    # Get all matching links
                    links = page.query_selector_all(selector)
                    
                    for link in links:
                        href = link.get_attribute('href')
                        if not href:
                            continue
                        
                        # Skip ads (they contain 'aclk' or are in sponsored sections)
                        parent_text = link.evaluate('el => el.closest("div")?.textContent || ""').lower()
                        if 'sponsored' in parent_text or 'ad' in href or 'aclk' in href:
                            logging.debug(f"Skipping sponsored link: {href[:100]}")
                            continue
                        
                        # This looks like an organic result
                        logging.info(f"Clicking organic result: {href[:100]}")
                        
                        # Scroll to element
                        link.scroll_into_view_if_needed()
                        self._human_delay(0.5, 1.0)
                        
                        # Random mouse movement near the link
                        self._random_mouse_movement(page)
                        
                        # Click the link
                        link.click()
                        clicked = True
                        
                        # Wait for navigation
                        try:
                            page.wait_for_load_state('networkidle', timeout=30000)
                        except:
                            page.wait_for_timeout(5000)
                        
                        logging.info(f"Successfully navigated to: {page.url}")
                        break
                    
                    if clicked:
                        break
                        
                except Exception as e:
                    logging.debug(f"Selector {selector} failed: {e}")
                    continue
            
            if not clicked:
                logging.warning("Could not find organic search result, skipping Google navigation...")
                return False
            
            self._human_delay(2.0, 3.0)
            return True
            
        except Exception as e:
            logging.warning(f"Google navigation encountered an issue: {e}")
            return False

    def _dismiss_google_popups(self, page: Page):
        """
        Dismiss Google popups (location, privacy, etc.).
        
        Args:
            page: Playwright page
        """
        try:
            page.wait_for_timeout(1000)
            
            # Location popup - "Not now" or "Block"
            not_now_selectors = [
                'button:has-text("Not now")',
                'button:has-text("Block")',
                'button:has-text("No thanks")',
                'button[aria-label="No"]',
            ]
            
            for selector in not_now_selectors:
                try:
                    button = page.wait_for_selector(selector, state='visible', timeout=2000)
                    if button and button.is_visible():
                        logging.info(f"Dismissing Google popup: {selector}")
                        button.click()
                        page.wait_for_timeout(1000)
                        return
                except:
                    continue
            
            # Privacy/cookie popups
            accept_selectors = [
                'button:has-text("Accept all")',
                'button:has-text("I agree")',
                '#L2AGLb',  # Google's "I agree" button
            ]
            
            for selector in accept_selectors:
                try:
                    button = page.query_selector(selector)
                    if button and button.is_visible():
                        logging.info(f"Accepting privacy terms: {selector}")
                        button.click()
                        page.wait_for_timeout(1000)
                        return
                except:
                    continue
                    
        except Exception as e:
            logging.debug(f"No Google popups to dismiss: {e}")

    def _dismiss_site_popups(self, page: Page):
        """
        Dismiss Safeway/Sobeys site popups (location, cookies, etc.).
        CRITICAL: Must close popup FIRST before any other interaction.

        Args:
            page: Playwright page
        """
        try:
            page.wait_for_timeout(2000)
            
            # PRIORITY 1: Click X button to close location popup
            close_selectors = [
                'button[aria-label="Close"]',
                '[aria-label="Close"]',
                'button.close',
                '.close',
                'button[class*="close"]',
                '[class*="modal"] button[class*="close"]',
                'button:has-text("×")',
                '[role="dialog"] button',
            ]
            
            popup_closed = False
            for selector in close_selectors:
                try:
                    close_btn = page.wait_for_selector(selector, state='visible', timeout=2000)
                    if close_btn and close_btn.is_visible():
                        logging.info(f"Clicking popup close button: {selector}")
                        close_btn.click(force=True)
                        popup_closed = True
                        page.wait_for_timeout(1000)
                        break
                except:
                    continue
            
            # PRIORITY 2: If X didn't work, try location permission buttons
            if not popup_closed:
                location_selectors = [
                    'button:has-text("Never allow")',
                    'button:has-text("Allow this time")',
                    'button:has-text("Block")',
                    'button:has-text("Not now")',
                ]
                
                for selector in location_selectors:
                    try:
                        button = page.query_selector(selector)
                        if button and button.is_visible():
                            logging.info(f"Dismissing location popup: {selector}")
                            button.click()
                            popup_closed = True
                            page.wait_for_timeout(1000)
                            break
                    except:
                        continue
            
            # Clear any remaining overlays
            if popup_closed:
                logging.info("Clearing overlays with page interactions...")
                page.mouse.wheel(0, 300)
                page.wait_for_timeout(500)
                page.mouse.wheel(0, -300)
                page.wait_for_timeout(500)
                
                try:
                    page.evaluate("document.body.click()")
                except:
                    pass
                
                logging.info("Page is now interactive")
            
            # Cookie consent
            cookie_selectors = [
                'button:has-text("Accept")',
                'button:has-text("Accept all")',
                '#accept-cookies',
            ]
            
            for selector in cookie_selectors:
                try:
                    button = page.query_selector(selector)
                    if button and button.is_visible():
                        button.click()
                        logging.info(f"Accepted cookies: {selector}")
                        page.wait_for_timeout(1000)
                        break
                except:
                    continue
            
            page.wait_for_load_state('networkidle', timeout=10000)
                    
        except Exception as e:
            logging.error(f"Error dismissing site popups: {e}", exc_info=True)

    def _select_store(self, page: Page):
        """
        Select store location for accurate pricing.
        
        Args:
            page: Playwright page
        """
        if self.store_selected:
            logging.info("Store already selected")
            return True
            
        if not self.store_postal_code and not self.store_name_filter:
            logging.info("No store selection configured")
            return True
        
        try:
            logging.info(f"Selecting store: {self.store_name_filter or self.store_postal_code}")
            
            # Wait for page to fully load first
            page.wait_for_timeout(2000)
            
            # Find store selector button - try many variations
            store_selectors = [
                'button:has-text("Store")',
                'button:has-text("store")',
                'a:has-text("Store")',
                'a:has-text("store")',
                'button:has-text("My Store")',
                'button:has-text("Select Store")',
                'button:has-text("Change Store")',
                '[aria-label*="store" i]',
                '[aria-label*="location" i]',
                '.store-selector',
                '#store-selector',
                'button[data-testid*="store" i]',
                'a[href*="store" i]',
            ]
            
            store_button = None
            for selector in store_selectors:
                try:
                    store_button = page.wait_for_selector(selector, state='visible', timeout=2000)
                    if store_button and store_button.is_visible():
                        logging.info(f"Found store selector: {selector}")
                        break
                    else:
                        store_button = None
                except:
                    continue
            
            if not store_button:
                logging.warning("Could not find store selector button")
                logging.info("Attempting to continue without explicit store selection...")
                # Don't fail completely - maybe store is already set or not required
                return False
            
            # Click store selector
            self._human_delay(0.5, 1.0)
            store_button.click()
            page.wait_for_timeout(2000)
            
            # Find search input
            input_selectors = [
                'input[placeholder*="postal" i]',
                'input[placeholder*="code" i]',
                'input[placeholder*="city" i]',
                'input[placeholder*="location" i]',
                'input[type="search"]',
            ]
            
            search_input = None
            for selector in input_selectors:
                try:
                    search_input = page.wait_for_selector(selector, state='visible', timeout=3000)
                    if search_input:
                        logging.info(f"Found search input: {selector}")
                        break
                except:
                    continue
            
            if not search_input:
                logging.warning("Could not find store search input")
                return False
            
            # Enter search term
            search_term = self.store_postal_code or self.store_name_filter or "Airdrie"
            logging.info(f"Searching for: {search_term}")
            
            search_input.click()
            self._human_delay(0.3, 0.6)
            search_input.fill(search_term)
            self._human_delay(0.5, 1.0)
            search_input.press('Enter')
            page.wait_for_timeout(3000)
            
            # Find and select store
            store_items = page.query_selector_all('[class*="store" i], [class*="location" i]')
            
            if not store_items:
                logging.warning("No store results found")
                return False
            
            logging.info(f"Found {len(store_items)} store results")
            
            # Filter by name if specified
            target_store = None
            if self.store_name_filter:
                for item in store_items:
                    text = item.inner_text().lower()
                    if self.store_name_filter.lower() in text:
                        target_store = item
                        logging.info(f"Found matching store: {text[:100]}")
                        break
            
            if not target_store and store_items:
                target_store = store_items[0]
                logging.info("Using first store in results")
            
            if not target_store:
                return False
            
            # Click select button or store item
            select_btn = target_store.query_selector('button:has-text("Select"), button:has-text("Choose")')
            if select_btn:
                logging.info("Clicking select button")
                select_btn.click()
            else:
                logging.info("Clicking store item")
                target_store.click()
            
            page.wait_for_timeout(2000)
            self.store_selected = True
            logging.info("Store selected successfully")
            return True
            
        except Exception as e:
            logging.error(f"Store selection failed: {e}", exc_info=True)
            return False

    def _scroll_page(self, page: Page):
        """Scroll page to trigger lazy loading."""
        try:
            previous_height = 0
            max_scrolls = 5

            for _ in range(max_scrolls):
                current_height = page.evaluate("document.body.scrollHeight")
                if current_height == previous_height:
                    break

                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1000)
                previous_height = current_height

        except Exception as e:
            logging.warning(f"Scrolling error: {e}")

    def _diagnose_page(self, page: Page):
        """Diagnose page content for debugging."""
        try:
            title = page.title()
            url = page.url
            logging.info(f"Page diagnosis:")
            logging.info(f"  URL: {url}")
            logging.info(f"  Title: {title}")

            # Check for errors
            error_indicators = ['404', 'not found', 'error', 'access denied']
            try:
                page_text = page.inner_text('body').lower()
                for indicator in error_indicators:
                    if indicator in page_text:
                        logging.error(f"  Page contains error indicator: '{indicator}'")
            except:
                pass

            # Count product elements with comprehensive selectors
            product_selectors = [
                '[data-component="product-tile"]',
                '.product-tile',
                '.product-card',
                '[class*="ProductTile"]',
                '[class*="ProductCard"]',
                '[data-testid*="product"]',
                'article[class*="product" i]',
            ]

            total_products = 0
            for selector in product_selectors:
                count = page.locator(selector).count()
                if count > 0:
                    logging.info(f"  Found {count} elements: '{selector}'")
                    total_products = max(total_products, count)

            if total_products == 0:
                logging.warning("  No product elements found with any selector")

            # Check for __NEXT_DATA__
            has_next_data = page.query_selector('script#__NEXT_DATA__') is not None
            logging.info(f"  Has __NEXT_DATA__: {has_next_data}")

            if has_next_data:
                try:
                    next_data_str = page.evaluate("JSON.stringify(window.__NEXT_DATA__ || {})")
                    next_data = json.loads(next_data_str)
                    page_props = next_data.get('props', {}).get('pageProps', {})

                    # Log what keys are available
                    if page_props:
                        logging.info(f"  __NEXT_DATA__ pageProps keys: {list(page_props.keys())[:10]}")
                except:
                    pass

        except Exception as e:
            logging.warning(f"Diagnosis error: {e}")

    def _extract_products_from_page(self, page: Page, source_url: str) -> List[ProductRecord]:
        """Extract products from rendered page."""
        products = []
        
        self._diagnose_page(page)

        # Try __NEXT_DATA__ extraction
        try:
            next_data_str = page.evaluate("JSON.stringify(window.__NEXT_DATA__ || {})")
            if next_data_str and next_data_str != '{}':
                next_data = json.loads(next_data_str)
                page_props = next_data.get('props', {}).get('pageProps', {})

                # Look for products in various locations
                product_keys = ['products', 'productList', 'searchResults', 'categoryProducts', 'items']
                product_list = []
                
                for key in product_keys:
                    if key in page_props:
                        product_list = page_props[key]
                        logging.info(f"Found {len(product_list)} products in {key}")
                        break

                # Check nested structures
                if not product_list:
                    initial_data = page_props.get('initialData', {})
                    for key in product_keys:
                        if key in initial_data:
                            product_list = initial_data[key]
                            logging.info(f"Found {len(product_list)} products in initialData.{key}")
                            break

                if product_list:
                    for product_data in product_list:
                        record = self._normalize_product(product_data, source_url)
                        if record:
                            products.append(record)
                    
                    logging.info(f"Extracted {len(products)} from __NEXT_DATA__")
                    return products

        except Exception as e:
            logging.debug(f"__NEXT_DATA__ extraction failed: {e}")

        # Fallback: DOM scraping
        try:
            logging.info("Fallback to DOM scraping...")

            # Try multiple selectors
            selectors = [
                '[data-component="product-tile"]',
                '.product-tile',
                '.product-card',
                '[class*="ProductTile"]',
                '[class*="ProductCard"]',
                '[data-testid*="product"]',
                'article[class*="product" i]',
            ]

            elements = []
            for selector in selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    logging.info(f"Found {len(elements)} elements with selector: {selector}")
                    break

            if not elements:
                logging.warning("No product elements found with any DOM selector")

            for element in elements:
                try:
                    record = self._extract_product_from_element(element, source_url)
                    if record:
                        products.append(record)
                except Exception as e:
                    logging.warning(f"Failed to extract product: {e}")

            logging.info(f"Extracted {len(products)} from DOM")

        except Exception as e:
            logging.error(f"DOM scraping failed: {e}")

        return products

    def _extract_product_from_element(self, element, source_url: str) -> Optional[ProductRecord]:
        """Extract product from DOM element."""
        try:
            name_elem = element.query_selector('.product-name, h3, h4, [class*="Title"]')
            name = name_elem.inner_text().strip() if name_elem else None

            price_elem = element.query_selector('.price, [class*="price"]')
            price_text = price_elem.inner_text().strip() if price_elem else None

            brand_elem = element.query_selector('.brand, [class*="brand"]')
            brand = brand_elem.inner_text().strip() if brand_elem else None

            image_elem = element.query_selector('img')
            image_url = image_elem.get_attribute('src') if image_elem else None

            link_elem = element.query_selector('a[href]')
            product_url = link_elem.get_attribute('href') if link_elem else None

            if not name:
                return None

            # Parse price
            price = None
            if price_text:
                import re
                match = re.search(r'[\d,]+\.?\d*', price_text)
                if match:
                    price = float(match.group().replace(',', ''))

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=None,
                name=name,
                brand=brand,
                size_text=None,
                price=price,
                currency='CAD',
                unit_price=None,
                unit_price_uom=None,
                image_url=image_url,
                category_path=None,
                availability='unknown',
                query_category=self.current_query,
                raw_source={'type': 'dom', 'url': product_url}
            )

        except Exception as e:
            logging.warning(f"Element extraction failed: {e}")
            return None

    def _normalize_product(self, raw_data: Dict, source_url: str) -> Optional[ProductRecord]:
        """Normalize product data from various sources."""
        try:
            name = raw_data.get('name') or raw_data.get('productName') or raw_data.get('title')
            if not name:
                return None

            price_data = raw_data.get('price', {})
            if isinstance(price_data, dict):
                price = price_data.get('amount') or price_data.get('value')
            else:
                price = price_data

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=raw_data.get('id') or raw_data.get('productId') or raw_data.get('code'),
                name=name,
                brand=raw_data.get('brand'),
                size_text=raw_data.get('size') or raw_data.get('packageSize'),
                price=float(price) if price else None,
                currency='CAD',
                unit_price=raw_data.get('unitPrice'),
                unit_price_uom=raw_data.get('unitPriceUom'),
                image_url=raw_data.get('imageUrl') or raw_data.get('image'),
                category_path=raw_data.get('category') or raw_data.get('categoryPath'),
                availability=self._parse_stock_status(
                    raw_data.get('inStock'),
                    raw_data.get('availability')
                ),
                query_category=self.current_query,
                raw_source={'type': 'json', 'data': raw_data}
            )

        except Exception as e:
            logging.warning(f"Product normalization failed: {e}")
            return None

    def _parse_stock_status(self, in_stock, availability) -> str:
        """Parse stock status to standard format."""
        if in_stock is True:
            return 'in_stock'
        elif in_stock is False:
            return 'out_of_stock'
        elif isinstance(availability, str):
            availability_lower = availability.lower()
            if 'in' in availability_lower and 'stock' in availability_lower:
                return 'in_stock'
            elif 'out' in availability_lower:
                return 'out_of_stock'
        return 'unknown'

    def scrape_category(self, category_url: str, max_pages: Optional[int] = None) -> int:
        """Scrape products from category page using Google navigation."""
        total_scraped = 0

        self._launch_browser()

        try:
            page = self.context.new_page()
            
            # Try Google navigation first
            google_success = False
            if self.use_google_navigation:
                google_success = self._navigate_via_google(page)
                if google_success:
                    self._human_delay(1.0, 2.0)
                else:
                    logging.info("Google navigation failed or was skipped, using direct navigation...")
            
            # If Google navigation failed or was disabled, navigate directly
            if not google_success:
                logging.info(f"Navigating directly to: {self.base_url}")
                try:
                    page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)
                    page.wait_for_timeout(3000)
                except Exception as e:
                    logging.error(f"Direct navigation failed: {e}")
                    page.close()
                    return 0
            
            # Dismiss popups
            self._dismiss_site_popups(page)
            
            # Select store
            self._select_store(page)
            page.wait_for_timeout(1000)

            # Now navigate to category
            page_num = 0
            while True:
                if max_pages and page_num >= max_pages:
                    logging.info(f"Reached max_pages: {max_pages}")
                    break

                full_url = category_url if category_url.startswith('http') else f"{self.base_url}{category_url}"
                if page_num > 0:
                    separator = '&' if '?' in full_url else '?'
                    full_url = f"{full_url}{separator}page={page_num}"

                logging.info(f"Scraping category page {page_num + 1}: {full_url}")

                try:
                    self.rate_limiter.wait()
                    page.goto(full_url, wait_until='networkidle', timeout=60000)

                    self._scroll_page(page)

                    products = self._extract_products_from_page(page, full_url)

                    if not products:
                        logging.warning(f"No products on page {page_num + 1}")
                        break

                    saved = self.save_records_batch(products)
                    total_scraped += saved
                    logging.info(f"Page {page_num + 1}: Saved {saved}/{len(products)}")

                    self.stats['pages_processed'] += 1
                    page_num += 1

                    # Check for next page
                    next_button = page.query_selector(
                        'a[aria-label="Next"], button[aria-label="Next"], .pagination-next'
                    )
                    if not next_button or next_button.is_disabled():
                        logging.info("No more pages")
                        break

                except Exception as e:
                    logging.error(f"Error on page {page_num + 1}: {e}", exc_info=True)
                    self.stats['errors'] += 1
                    break

            page.close()

        finally:
            self._close_browser()

        return total_scraped

    def scrape_search(self, query: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from search results.
        Uses Algolia API as primary method with Playwright fallback.
        """
        self.current_query = query  # Track query for ProductRecord
        logging.info(f"Searching for: {query}")

        # Try Algolia API first (faster and more reliable)
        total_scraped = self._scrape_search_algolia(query, max_pages)
        if total_scraped > 0:
            return total_scraped

        logging.warning("Algolia API failed, falling back to Playwright...")
        return self._scrape_search_playwright(query, max_pages)

    def _scrape_search_algolia(self, query: str, max_pages: Optional[int] = None) -> int:
        """Scrape search results using Algolia API."""
        total_scraped = 0
        page = 0

        while True:
            products, page_info = self._search_algolia(query, page)

            if not products:
                if page == 0:
                    logging.warning(f"No products found via Algolia for '{query}'")
                break

            saved = self.save_records_batch(products)
            total_scraped += saved
            logging.info(f"Algolia page {page + 1}: Saved {saved}/{len(products)} products")

            self.stats['pages_processed'] += 1
            page += 1

            # Check pagination limits
            total_pages = page_info.get('nbPages', 0)
            if max_pages and page >= max_pages:
                logging.info(f"Reached max_pages limit ({max_pages})")
                break
            if page >= total_pages:
                logging.info("Reached end of Algolia results")
                break

            # Polite delay
            time.sleep(random.uniform(1.0, 2.0))

        return total_scraped

    def _scrape_search_playwright(self, query: str, max_pages: Optional[int] = None) -> int:
        """Fallback: Scrape search results using Playwright browser."""
        self._launch_browser()
        
        try:
            page = self.context.new_page()
            
            # Try Google navigation first
            google_success = False
            if self.use_google_navigation:
                google_success = self._navigate_via_google(page)
                if google_success:
                    self._human_delay(1.0, 2.0)
                else:
                    logging.info("Google navigation failed or was skipped, using direct navigation...")
            
            # If Google navigation failed or was disabled, navigate directly
            if not google_success:
                logging.info(f"Navigating directly to: {self.base_url}")
                try:
                    page.goto(self.base_url, wait_until='domcontentloaded', timeout=60000)
                    # Wait a bit more for page to settle
                    page.wait_for_timeout(3000)
                except Exception as e:
                    logging.error(f"Direct navigation failed: {e}")
                    page.close()
                    return 0
            
            # Dismiss popups
            self._dismiss_site_popups(page)
            
            # Select store
            self._select_store(page)
            page.wait_for_timeout(1000)
            
            # Find search box on Safeway site
            logging.info("Looking for search box...")
            search_selectors = [
                'input[type="search"]',
                'input[placeholder*="Search" i]',
                'input[aria-label*="Search" i]',
                '#search-input',
                '.search-input',
                'input[name="search"]',
                'input[data-testid*="search" i]',
            ]
            
            search_input = None
            for selector in search_selectors:
                try:
                    search_input = page.wait_for_selector(selector, state='visible', timeout=5000)
                    if search_input and search_input.is_enabled() and search_input.is_visible():
                        logging.info(f"Found search input: {selector}")
                        search_input.scroll_into_view_if_needed()
                        page.wait_for_timeout(500)
                        break
                    else:
                        search_input = None
                except:
                    continue
            
            if not search_input:
                logging.error("Could not find search input")
                page.close()
                return 0
            
            # Click and type search with retry
            max_attempts = 3
            search_success = False
            
            for attempt in range(max_attempts):
                try:
                    logging.info(f"Typing search query (attempt {attempt + 1})...")
                    
                    # Click to focus
                    search_input.click(force=True, timeout=3000)
                    page.wait_for_timeout(500)
                    
                    # Clear any existing text
                    search_input.fill('')
                    page.wait_for_timeout(200)
                    
                    # Type with delays
                    for char in query:
                        search_input.type(char, delay=random.randint(50, 150))
                    
                    page.wait_for_timeout(500)
                    
                    # Submit - try multiple methods
                    # Method 1: Press Enter
                    search_input.press('Enter')
                    page.wait_for_timeout(2000)
                    
                    # Check if URL changed (search executed)
                    current_url = page.url
                    if 'search' in current_url.lower() or query.lower() in current_url.lower():
                        search_success = True
                        logging.info(f"Search submitted successfully: {current_url}")
                        break
                    
                    # Method 2: Click search button if Enter didn't work
                    search_button_selectors = [
                        'button[type="submit"]',
                        'button[aria-label*="Search" i]',
                        'button[data-testid*="search" i]',
                        '.search-button',
                        '#search-button',
                    ]
                    
                    for btn_selector in search_button_selectors:
                        try:
                            button = page.query_selector(btn_selector)
                            if button and button.is_visible():
                                button.click()
                                page.wait_for_timeout(2000)
                                
                                current_url = page.url
                                if 'search' in current_url.lower() or query.lower() in current_url.lower():
                                    search_success = True
                                    logging.info(f"Search submitted via button: {current_url}")
                                    break
                        except:
                            continue
                    
                    if search_success:
                        break
                    
                    logging.warning(f"Search attempt {attempt + 1} did not change URL, retrying...")
                    page.wait_for_timeout(1000)
                    
                except Exception as e:
                    logging.warning(f"Search attempt {attempt + 1} failed: {e}")
                    if attempt < max_attempts - 1:
                        page.wait_for_timeout(2000)
            
            if not search_success:
                logging.error("Failed to execute search after multiple attempts")
                page.close()
                return 0

            # CRITICAL: Wait for search results to actually render (not just URL change)
            logging.info("Waiting for search results to render...")

            # Wait for page to start loading
            try:
                page.wait_for_load_state('domcontentloaded', timeout=15000)
            except:
                page.wait_for_timeout(3000)

            # Now wait for actual product elements to appear (JavaScript rendering)
            product_selectors = [
                '[data-component="product-tile"]',
                '.product-tile',
                '.product-card',
                '[class*="ProductTile"]',
                '[data-testid*="product"]',
                'article[class*="product" i]',
                'div[class*="product" i] img',  # Last resort: any product div with an image
            ]

            products_loaded = False
            for selector in product_selectors:
                try:
                    logging.info(f"Waiting for products: {selector}")
                    # Wait up to 45 seconds for products to appear
                    page.wait_for_selector(selector, state='visible', timeout=45000)

                    # Double-check we have multiple products (not just one stray element)
                    count = page.locator(selector).count()
                    logging.info(f"Found {count} elements matching {selector}")

                    if count > 0:
                        products_loaded = True
                        logging.info(f"Products successfully loaded! Found {count} items")
                        break
                except Exception as e:
                    logging.debug(f"Selector '{selector}' not found: {e}")
                    continue

            if not products_loaded:
                logging.error("Products did not load after search submission!")
                logging.error(f"Current URL: {page.url}")
                logging.error(f"Page title: {page.title()}")

                # Take screenshot for debugging
                try:
                    screenshot_path = self.output_dir / f"debug_no_products_{int(time.time())}.png"
                    page.screenshot(path=str(screenshot_path))
                    logging.error(f"Screenshot saved to: {screenshot_path}")
                except:
                    pass

                # Dump page HTML for analysis
                try:
                    html_path = self.output_dir / f"debug_page_{int(time.time())}.html"
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(page.content())
                    logging.error(f"Page HTML saved to: {html_path}")
                except:
                    pass

                page.close()
                return 0

            # Additional wait for lazy loading
            page.wait_for_timeout(2000)

            logging.info(f"Search complete: {page.url}")
            self._diagnose_page(page)

            # Extract products from first page
            products = self._extract_products_from_page(page, page.url)
            
            if not products:
                logging.warning("No products found")
                page.close()
                return 0
            
            saved = self.save_records_batch(products)
            total_scraped = saved
            logging.info(f"Page 1: Saved {saved}/{len(products)}")
            self.stats['pages_processed'] += 1
            
            # Handle pagination
            page_num = 2
            while max_pages and page_num <= max_pages:
                next_button = page.query_selector(
                    'a[aria-label="Next"], button[aria-label="Next"], .pagination-next'
                )
                
                if not next_button or next_button.is_disabled():
                    logging.info("No more pages")
                    break
                
                logging.info(f"Navigating to page {page_num}...")
                self.rate_limiter.wait()
                next_button.click()
                page.wait_for_load_state('networkidle', timeout=30000)
                
                products = self._extract_products_from_page(page, page.url)
                
                if not products:
                    logging.warning(f"No products on page {page_num}")
                    break
                
                saved = self.save_records_batch(products)
                total_scraped += saved
                logging.info(f"Page {page_num}: Saved {saved}/{len(products)}")
                self.stats['pages_processed'] += 1
                
                page_num += 1
            
            page.close()
            return total_scraped
            
        except Exception as e:
            logging.error(f"Search error: {e}", exc_info=True)
            return 0
            
        finally:
            self._close_browser()

    def scrape_product_page(self, product_url: str) -> Optional[ProductRecord]:
        """Scrape single product page."""
        self._launch_browser()

        try:
            page = self.context.new_page()
            
            # Navigate via Google
            if self.use_google_navigation:
                self._navigate_via_google(page)
                self._human_delay(1.0, 2.0)
            else:
                page.goto(self.base_url, wait_until='networkidle', timeout=60000)
            
            self._dismiss_site_popups(page)
            self._select_store(page)
            page.wait_for_timeout(1000)

            full_url = product_url if product_url.startswith('http') else f"{self.base_url}{product_url}"
            logging.info(f"Scraping product: {full_url}")

            self.rate_limiter.wait()
            page.goto(full_url, wait_until='networkidle', timeout=60000)

            products = self._extract_products_from_page(page, full_url)
            page.close()

            return products[0] if products else None

        except Exception as e:
            logging.error(f"Product page scraping failed: {e}")
            return None

        finally:
            self._close_browser()

    def __del__(self):
        """Cleanup browser on deletion."""
        self._close_browser()