#!/usr/bin/env python3
"""
Sobeys scraper (Sobeys Network).
Inherits from Safeway scraper with Sobeys-specific DOM extraction.
"""

import logging
import json
from typing import Optional, List
from playwright.sync_api import Page, ElementHandle

from scrapers.sites.safeway import SafewayScraper
from scrapers.base import ProductRecord
from scrapers.common import get_iso_timestamp


class SobeysScraper(SafewayScraper):
    """
    Scraper for Sobeys.
    Uses same Playwright setup as Safeway but with Sobeys-specific element extraction.
    """

    def _extract_product_from_element(self, element: ElementHandle, source_url: str) -> Optional[ProductRecord]:
        """
        Extract product from Sobeys DOM element.
        Sobeys uses different selectors than the generic Safeway structure.
        """
        try:
            # Debug: Log element HTML structure
            try:
                element_html = element.evaluate('el => el.outerHTML')
                logging.debug(f"Extracting from element:\n{element_html[:500]}")
            except:
                pass

            # Try to get all text content from element for debugging
            try:
                all_text = element.inner_text()
                logging.debug(f"Element text content: {all_text[:200]}")
            except:
                pass

            # Extract name - try multiple selectors
            name = None
            name_selectors = [
                '[data-testid="product-title"]',
                '[class*="product-title" i]',
                '[class*="ProductTitle" i]',
                '[class*="product-name" i]',
                '[class*="ProductName" i]',
                'h3',
                'h4',
                'h2',
                'a[href*="product"]',  # Product links often contain the name
                'span[class*="name" i]',
                'div[class*="title" i]',
            ]

            for selector in name_selectors:
                try:
                    name_elem = element.query_selector(selector)
                    if name_elem:
                        name = name_elem.inner_text().strip()
                        if name:
                            logging.debug(f"Found name with selector '{selector}': {name}")
                            break
                except:
                    continue

            # If no name found with selectors, try getting first text node
            if not name:
                try:
                    # Get all text and take first meaningful line
                    all_text = element.inner_text().strip()
                    lines = [line.strip() for line in all_text.split('\n') if line.strip()]
                    if lines:
                        # First line is usually the product name
                        name = lines[0]
                        logging.debug(f"Extracted name from first text line: {name}")
                except:
                    pass

            # Extract price - try multiple selectors
            price = None
            price_text = None
            price_selectors = [
                '[data-testid="product-price"]',
                '[class*="price" i]',
                '[class*="Price" i]',
                'span[class*="amount" i]',
                'div[class*="cost" i]',
            ]

            for selector in price_selectors:
                try:
                    price_elem = element.query_selector(selector)
                    if price_elem:
                        price_text = price_elem.inner_text().strip()
                        if price_text:
                            logging.debug(f"Found price with selector '{selector}': {price_text}")
                            break
                except:
                    continue

            # Parse price from text
            if price_text:
                import re
                # Match patterns like $4.99, 4.99, $4,999.99
                match = re.search(r'\$?\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)', price_text.replace(',', ''))
                if match:
                    try:
                        price = float(match.group(1))
                        logging.debug(f"Parsed price: {price}")
                    except:
                        pass

            # Extract image
            image_url = None
            try:
                image_elem = element.query_selector('img')
                if image_elem:
                    image_url = image_elem.get_attribute('src') or image_elem.get_attribute('data-src')
                    logging.debug(f"Found image: {image_url}")
            except:
                pass

            # Extract product URL
            product_url = None
            try:
                link_elem = element.query_selector('a[href*="product"]')
                if link_elem:
                    product_url = link_elem.get_attribute('href')
                    logging.debug(f"Found product URL: {product_url}")
            except:
                pass

            # Extract brand if available
            brand = None
            brand_selectors = [
                '[data-testid="product-brand"]',
                '[class*="brand" i]',
                'span[class*="brand" i]',
            ]

            for selector in brand_selectors:
                try:
                    brand_elem = element.query_selector(selector)
                    if brand_elem:
                        brand = brand_elem.inner_text().strip()
                        if brand:
                            logging.debug(f"Found brand: {brand}")
                            break
                except:
                    continue

            # Extract product ID from data attributes or URL
            external_id = None
            try:
                # Try data attributes
                for attr in ['data-product-id', 'data-id', 'data-sku', 'id']:
                    external_id = element.get_attribute(attr)
                    if external_id:
                        logging.debug(f"Found product ID from {attr}: {external_id}")
                        break

                # Try extracting from URL
                if not external_id and product_url:
                    import re
                    id_match = re.search(r'/(\d+)/?$', product_url)
                    if id_match:
                        external_id = id_match.group(1)
                        logging.debug(f"Extracted ID from URL: {external_id}")
            except:
                pass

            # Validation: Must have at least a name
            if not name:
                logging.warning("Could not extract product name - skipping element")
                return None

            logging.info(f"Successfully extracted product: {name} - ${price}")

            return ProductRecord(
                store=self.store_name,
                site_slug=self.site_slug,
                source_url=source_url,
                scrape_ts=get_iso_timestamp(),
                external_id=external_id,
                name=name,
                brand=brand,
                size_text=None,  # Could be extracted if needed
                price=price,
                currency='CAD',
                unit_price=None,  # Could be extracted if needed
                unit_price_uom=None,
                image_url=image_url,
                category_path=None,
                availability='unknown',
                query_category=self.current_query,
                raw_source={'type': 'dom', 'url': product_url}
            )

        except Exception as e:
            logging.error(f"Element extraction failed: {e}", exc_info=True)
            return None

    def _extract_products_from_page(self, page: Page, source_url: str) -> List[ProductRecord]:
        """
        Override to add better debugging for Sobeys.
        """
        products = []

        # Log current page state
        try:
            logging.info(f"Extracting from page: {page.url}")
            logging.info(f"Page title: {page.title()}")
        except:
            pass

        # Try __NEXT_DATA__ extraction first (same as parent)
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
                        logging.info(f"Found {len(product_list)} products in __NEXT_DATA__.{key}")
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

        # Fallback: DOM scraping with Sobeys-specific selectors
        try:
            logging.info("Fallback to DOM scraping...")

            # Sobeys-specific selectors (prioritized)
            selectors = [
                '[data-testid*="product"]',  # This one worked in the logs
                '[data-testid="product-tile"]',
                '[data-testid="product-card"]',
                '[data-component="product-tile"]',
                '.product-tile',
                '.product-card',
                '[class*="ProductTile"]',
                '[class*="ProductCard"]',
                'article[class*="product" i]',
                'div[class*="product-item" i]',
            ]

            elements = []
            used_selector = None

            for selector in selectors:
                elements = page.query_selector_all(selector)
                if elements:
                    used_selector = selector
                    logging.info(f"Found {len(elements)} elements with selector: {selector}")
                    break

            if not elements:
                logging.warning("No product elements found with any DOM selector")

                # Save debug HTML
                try:
                    html_path = self.output_dir / f"debug_no_elements_{page.url.split('/')[-1]}.html"
                    with open(html_path, 'w', encoding='utf-8') as f:
                        f.write(page.content())
                    logging.error(f"Debug HTML saved to: {html_path}")
                except Exception as e:
                    logging.error(f"Could not save debug HTML: {e}")

                return products

            # Extract from each element
            for i, element in enumerate(elements):
                try:
                    logging.debug(f"Processing element {i+1}/{len(elements)}")
                    record = self._extract_product_from_element(element, source_url)
                    if record:
                        products.append(record)
                    else:
                        logging.warning(f"Element {i+1} extraction returned None")
                except Exception as e:
                    logging.warning(f"Failed to extract product from element {i+1}: {e}")

                    # Save debug HTML for failed element
                    if i == 0:  # Save first failed element for debugging
                        try:
                            element_html = element.evaluate('el => el.outerHTML')
                            html_path = self.output_dir / f"debug_failed_element_{i}.html"
                            with open(html_path, 'w', encoding='utf-8') as f:
                                f.write(element_html)
                            logging.error(f"Failed element HTML saved to: {html_path}")
                        except:
                            pass

            logging.info(f"Extracted {len(products)} from {len(elements)} DOM elements")

        except Exception as e:
            logging.error(f"DOM scraping failed: {e}", exc_info=True)

        return products
