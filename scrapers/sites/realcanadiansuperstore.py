#!/usr/bin/env python3
"""
Real Canadian Superstore scraper (Loblaw Network).
Uses embedded JSON data from __NEXT_DATA__ and JSON-LD.
No JavaScript rendering required.
"""

import json
import logging
import requests
from typing import Optional, List, Dict
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper, ProductRecord
from scrapers.common import get_iso_timestamp, retry_on_exception


class RealcanadiansuperstoreScraper(BaseScraper):
    """Scraper for Real Canadian Superstore (Loblaw network)."""

    def __init__(self, config_path, project_root, headless=True, fresh_start=False):
        super().__init__(config_path, project_root, fresh_start=fresh_start)

        self.base_url = self.config['base_url']
        self.session = requests.Session()
        self.session.headers.update(self.config.get('headers', {}))
        self.current_query = None  # Track current search query for ProductRecord

    @retry_on_exception(max_retries=3, exceptions=(requests.RequestException,))
    def _fetch_page(self, url: str) -> Optional[str]:
        """
        Fetch page HTML with retry logic.

        Args:
            url: URL to fetch

        Returns:
            HTML content or None on failure
        """
        full_url = url if url.startswith('http') else f"{self.base_url}{url}"

        try:
            self.rate_limiter.wait()
            response = self.session.get(full_url, timeout=30)
            response.raise_for_status()

            logging.debug(f"Fetched: {full_url}")
            return response.text

        except requests.RequestException as e:
            logging.error(f"Failed to fetch {full_url}: {e}")
            self.stats['errors'] += 1
            raise

    def _extract_products_from_json_ld(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract products from JSON-LD structured data.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of product dictionaries
        """
        products = []

        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)

                # Look for ProductCollection type
                if data.get('@type') == 'ProductCollection':
                    items = data.get('itemListElement', [])
                    for item in items:
                        if item.get('@type') == 'Product':
                            products.append(item)

            except (json.JSONDecodeError, AttributeError) as e:
                logging.debug(f"Failed to parse JSON-LD: {e}")
                continue

        return products

    def _extract_products_from_next_data(self, soup: BeautifulSoup) -> List[Dict]:
        """
        Extract products from __NEXT_DATA__ embedded JSON.

        Args:
            soup: BeautifulSoup object

        Returns:
            List of product dictionaries
        """
        products = []

        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if not next_data_script:
            logging.debug("No __NEXT_DATA__ script tag found")
            return products

        try:
            data = json.loads(next_data_script.string)
            logging.debug("Successfully parsed __NEXT_DATA__ JSON")

            # Navigate nested structure to find products
            page_props = data.get('props', {}).get('pageProps', {})
            logging.debug(f"pageProps keys: {list(page_props.keys())}")

            # Try multiple possible paths for product data
            # Path 1: New structure (as of Dec 2025)
            # props.pageProps.initialSearchData.layout.sections.mainContentCollection.components[i].data.productTiles
            search_data = page_props.get('initialSearchData', {})
            if search_data:
                logging.debug(f"Found initialSearchData, keys: {list(search_data.keys())}")

                layout = search_data.get('layout', {})
                if layout:
                    logging.debug(f"Found layout, keys: {list(layout.keys())}")

                    sections = layout.get('sections', {})
                    if isinstance(sections, dict) and 'mainContentCollection' in sections:
                        logging.debug("Found sections.mainContentCollection")
                        main_content = sections['mainContentCollection']

                        components = main_content.get('components', [])
                        logging.debug(f"Found {len(components)} components in mainContentCollection")

                        # Iterate through components to find productTiles
                        for i, component in enumerate(components):
                            component_data = component.get('data', {})
                            if 'productTiles' in component_data:
                                product_tiles = component_data['productTiles']
                                logging.info(f"Found {len(product_tiles)} products in component {i}")
                                products.extend(product_tiles)

            # Path 2: Legacy structure (direct products key)
            if not products:
                if 'products' in search_data:
                    products = search_data['products']
                    logging.debug(f"Found products via legacy path (initialSearchData.products): {len(products)}")

            # Path 3: Category data (for category pages)
            if not products:
                category_data = page_props.get('initialCategoryData', {})
                if category_data:
                    logging.debug(f"Checking initialCategoryData, keys: {list(category_data.keys())}")

                    # Try new structure for category pages
                    layout = category_data.get('layout', {})
                    if layout:
                        sections = layout.get('sections', {})
                        if isinstance(sections, dict) and 'mainContentCollection' in sections:
                            main_content = sections['mainContentCollection']
                            components = main_content.get('components', [])

                            for i, component in enumerate(components):
                                component_data = component.get('data', {})
                                if 'productTiles' in component_data:
                                    product_tiles = component_data['productTiles']
                                    logging.info(f"Found {len(product_tiles)} products in category component {i}")
                                    products.extend(product_tiles)

                    # Try legacy structure
                    if not products and 'products' in category_data:
                        products = category_data['products']
                        logging.debug(f"Found products via legacy path (initialCategoryData.products): {len(products)}")

            if not products:
                logging.warning("No products found in any known __NEXT_DATA__ path")
                logging.debug("Searched paths: initialSearchData.layout.sections.mainContentCollection, initialCategoryData.layout.sections.mainContentCollection, and legacy paths")

        except (json.JSONDecodeError, AttributeError, KeyError) as e:
            logging.error(f"Failed to parse __NEXT_DATA__: {e}", exc_info=True)

        return products

    def _get_pagination_info(self, soup: BeautifulSoup) -> Optional[Dict]:
        """
        Extract pagination information from __NEXT_DATA__.

        Args:
            soup: BeautifulSoup object

        Returns:
            Pagination dict with hasMore, pageNumber, totalPages
        """
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if not next_data_script:
            return None

        try:
            data = json.loads(next_data_script.string)
            page_props = data.get('props', {}).get('pageProps', {})

            # Try to find pagination in new structure first
            # New path: layout.sections.mainContentCollection.components[i].data.pagination
            search_data = page_props.get('initialSearchData', {})
            if not search_data:
                search_data = page_props.get('initialCategoryData', {})

            if search_data:
                layout = search_data.get('layout', {})
                if layout:
                    sections = layout.get('sections', {})
                    if isinstance(sections, dict) and 'mainContentCollection' in sections:
                        main_content = sections['mainContentCollection']
                        components = main_content.get('components', [])

                        # Look for pagination in component data
                        for component in components:
                            component_data = component.get('data', {})
                            if 'pagination' in component_data:
                                return component_data['pagination']

            # Fallback to legacy direct pagination key
            pagination = search_data.get('pagination', {})
            if pagination:
                return pagination

        except (json.JSONDecodeError, AttributeError, KeyError) as e:
            logging.debug(f"Failed to extract pagination: {e}")

        return None

    def _normalize_product_from_json_ld(self, raw_data: Dict, source_url: str) -> ProductRecord:
        """
        Convert JSON-LD product to standard ProductRecord.

        Args:
            raw_data: Raw JSON-LD product data
            source_url: Source page URL

        Returns:
            ProductRecord
        """
        offers = raw_data.get('offers', {})
        brand = raw_data.get('brand', {})

        return ProductRecord(
            store=self.store_name,
            site_slug=self.site_slug,
            source_url=source_url,
            scrape_ts=get_iso_timestamp(),
            external_id=raw_data.get('sku'),
            name=raw_data.get('name', ''),
            brand=brand.get('name') if isinstance(brand, dict) else str(brand) if brand else None,
            size_text=raw_data.get('description'),
            price=float(offers.get('price', 0)) if offers.get('price') else None,
            currency=offers.get('priceCurrency', 'CAD'),
            unit_price=None,  # Not available in JSON-LD
            unit_price_uom=None,
            image_url=raw_data.get('image'),
            category_path=None,  # Not available in JSON-LD
            availability=self._parse_availability(offers.get('availability', '')),
            query_category=self.current_query,
            raw_source={'type': 'json-ld', 'data': raw_data}
        )

    def _normalize_product_from_next_data(self, raw_data: Dict, source_url: str) -> ProductRecord:
        """
        Convert __NEXT_DATA__ product to standard ProductRecord.

        Args:
            raw_data: Raw __NEXT_DATA__ product data
            source_url: Source page URL

        Returns:
            ProductRecord
        """
        # New structure (Dec 2025) uses different field names
        # Old: code, name, packageSize, pricing.price, pricing.unitPrice, inventory.indicator
        # New: productId, title, packageSizing, pricing.price, inventoryIndicator

        pricing = raw_data.get('pricing', {})
        inventory_indicator = raw_data.get('inventoryIndicator')

        # Handle both old and new image structures
        product_image = raw_data.get('productImage', [])
        image_url = None
        if product_image and len(product_image) > 0:
            img = product_image[0]
            image_url = img.get('largeUrl') or img.get('mediumUrl') or img.get('imageUrl')
        else:
            # Fallback to old structure
            image_assets = raw_data.get('imageAssets', {})
            image_url = image_assets.get('largeUrl') or image_assets.get('mediumUrl')

        # Extract category from breadcrumbs if available
        category_path = None
        breadcrumbs = raw_data.get('breadcrumbs', [])
        if breadcrumbs:
            category_path = ' > '.join([b.get('name', '') for b in breadcrumbs])

        # Parse price - handle both string and float
        price_value = pricing.get('price')
        if isinstance(price_value, str):
            try:
                price_value = float(price_value.replace('$', '').replace(',', ''))
            except (ValueError, AttributeError):
                price_value = None

        # Extract unit price from packageSizing if available
        # Format: "1 l, $0.43/100ml" or similar
        unit_price = pricing.get('unitPrice')
        unit_price_uom = pricing.get('unit')

        package_sizing = raw_data.get('packageSizing', '')
        if not unit_price and package_sizing and '/' in package_sizing:
            # Try to extract unit price from packageSizing text
            try:
                # Example: "1 l, $0.43/100ml" -> extract "$0.43/100ml"
                if '$' in package_sizing:
                    unit_part = package_sizing.split('$')[-1].strip()
                    if '/' in unit_part:
                        price_part, uom_part = unit_part.split('/', 1)
                        unit_price = float(price_part)
                        unit_price_uom = uom_part
            except (ValueError, IndexError):
                pass

        return ProductRecord(
            store=self.store_name,
            site_slug=self.site_slug,
            source_url=source_url,
            scrape_ts=get_iso_timestamp(),
            external_id=raw_data.get('productId') or raw_data.get('code'),
            name=raw_data.get('title') or raw_data.get('name', ''),
            brand=raw_data.get('brand'),
            size_text=raw_data.get('packageSizing') or raw_data.get('packageSize'),
            price=price_value,
            currency='CAD',
            unit_price=unit_price,
            unit_price_uom=unit_price_uom,
            image_url=image_url,
            category_path=category_path,
            availability=self._parse_inventory_indicator_new(inventory_indicator),
            query_category=self.current_query,
            raw_source={'type': 'next-data', 'data': raw_data}
        )

    def _parse_inventory_indicator_new(self, indicator: Optional[str]) -> str:
        """Parse new format inventory indicator to standard format."""
        if not indicator:
            return 'in_stock'  # Default to in stock if no indicator

        indicator_upper = str(indicator).upper()
        if 'OUT' in indicator_upper:
            return 'out_of_stock'
        elif 'LOW' in indicator_upper:
            return 'in_stock'  # Low stock but still available
        return 'in_stock'

    def _parse_availability(self, availability_str: str) -> str:
        """Parse schema.org availability string to standard format."""
        availability_lower = availability_str.lower()
        if 'instock' in availability_lower:
            return 'in_stock'
        elif 'outofstock' in availability_lower:
            return 'out_of_stock'
        return 'unknown'

    def _parse_inventory_indicator(self, indicator: str) -> str:
        """Parse Loblaw inventory indicator to standard format."""
        indicator_upper = indicator.upper()
        if indicator_upper == 'IN_STOCK':
            return 'in_stock'
        elif indicator_upper == 'OUT_OF_STOCK':
            return 'out_of_stock'
        return 'unknown'

    def scrape_category(self, category_url: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape all products from a category page with pagination.

        Args:
            category_url: Category page URL or path
            max_pages: Maximum number of pages to scrape

        Returns:
            Total number of products scraped
        """
        total_scraped = 0
        page = 1

        while True:
            if max_pages and page > max_pages:
                logging.info(f"Reached max_pages limit: {max_pages}")
                break

            # Build paginated URL
            separator = '&' if '?' in category_url else '?'
            paginated_url = f"{category_url}{separator}page={page}"

            logging.info(f"Scraping category page {page}: {paginated_url}")

            try:
                html = self._fetch_page(paginated_url)
                if not html:
                    break

                soup = BeautifulSoup(html, 'lxml')

                # Try __NEXT_DATA__ first (preferred)
                products = self._extract_products_from_next_data(soup)

                # Fallback to JSON-LD
                if not products:
                    products = self._extract_products_from_json_ld(soup)

                if not products:
                    logging.warning(f"No products found on page {page}")
                    break

                # Convert to ProductRecords and save
                records = []
                for product_data in products:
                    try:
                        # Determine which format based on fields present
                        # __NEXT_DATA__ format has 'productId' or 'code', JSON-LD has '@type'
                        if 'productId' in product_data or 'code' in product_data:
                            record = self._normalize_product_from_next_data(product_data, paginated_url)
                        elif '@type' in product_data:
                            record = self._normalize_product_from_json_ld(product_data, paginated_url)
                        else:
                            logging.warning(f"Unknown product format, keys: {list(product_data.keys())}")
                            continue

                        records.append(record)
                    except Exception as e:
                        logging.warning(f"Failed to normalize product: {e}", exc_info=True)

                # Save batch
                saved = self.save_records_batch(records)
                total_scraped += saved
                logging.info(f"Page {page}: Saved {saved}/{len(products)} products")

                # Increment pages_processed counter BEFORE pagination check
                self.stats['pages_processed'] += 1

                # Check pagination - determine if there are more pages
                pagination_info = self._get_pagination_info(soup)

                if pagination_info:
                    logging.debug(f"Pagination info found: {pagination_info}")
                    has_more = pagination_info.get('hasMore', None)
                    total_pages = pagination_info.get('totalPages', None)
                    current_page = pagination_info.get('pageNumber', page)

                    # Check if we've reached the end based on pagination data
                    if has_more is False:
                        logging.info("No more pages available (hasMore=False)")
                        break
                    elif total_pages and current_page >= total_pages:
                        logging.info(f"Reached last page ({current_page}/{total_pages})")
                        break
                else:
                    # No pagination info found - use product count as fallback
                    # If we got fewer products than expected, likely no more pages
                    logging.debug("No pagination info found, using product count heuristic")
                    # Continue to next page - products found means there might be more
                    # Only stop if we got 0 products (handled above)

                page += 1

            except Exception as e:
                logging.error(f"Error scraping page {page}: {e}")
                self.stats['errors'] += 1
                break

        return total_scraped

    def scrape_search(self, query: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from search results.

        Args:
            query: Search query string
            max_pages: Maximum number of pages to scrape

        Returns:
            Total number of products scraped
        """
        self.current_query = query  # Track query for ProductRecord
        search_url = self.config.get('search_url_pattern', '/search')
        search_url = f"{search_url}?search-bar={query}"

        logging.info(f"Searching for: {query}")
        return self.scrape_category(search_url, max_pages=max_pages)

    def scrape_product_page(self, product_url: str) -> Optional[ProductRecord]:
        """
        Scrape a single product detail page.

        Args:
            product_url: Product page URL or path

        Returns:
            ProductRecord if successful, None otherwise
        """
        try:
            html = self._fetch_page(product_url)
            if not html:
                return None

            soup = BeautifulSoup(html, 'lxml')

            # Try __NEXT_DATA__ first
            products = self._extract_products_from_next_data(soup)
            if products:
                return self._normalize_product_from_next_data(products[0], product_url)

            # Try JSON-LD
            products = self._extract_products_from_json_ld(soup)
            if products:
                return self._normalize_product_from_json_ld(products[0], product_url)

            logging.warning(f"No product data found on {product_url}")
            return None

        except Exception as e:
            logging.error(f"Failed to scrape product page {product_url}: {e}")
            return None
