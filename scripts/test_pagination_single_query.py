#!/usr/bin/env python3
"""
Test updated pagination with a single query to verify it works before full test
"""

import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scrapers.sites.sobeys_api import SobeysAPIScraper
from scrapers.common import setup_logging_with_rotation

def main():
    """Test pagination with a single query"""
    setup_logging_with_rotation('sobeys_pagination_test', level=logging.INFO)

    logging.info("=" * 70)
    logging.info("TESTING UPDATED PAGINATION LOGIC")
    logging.info("=" * 70)
    logging.info("Query: 'eggs'")
    logging.info("Max pages: None (auto, capped at 100)")
    logging.info("=" * 70)

    config_path = project_root / "configs" / "sobeys.json"
    scraper = SobeysAPIScraper(config_path, project_root)

    # Test with eggs query
    products = scraper.search_products("eggs", max_pages=None, query_category="eggs")

    logging.info(f"\n{'='*70}")
    logging.info("TEST RESULTS")
    logging.info(f"{'='*70}")
    logging.info(f"Total products found: {len(products)}")
    logging.info(f"Expected: 100-2000+ products (if pagination working)")
    logging.info(f"Previous behavior: ~48 products (only 2 pages)")

    if len(products) < 100:
        logging.error("⚠ WARNING: Low product count suggests pagination may not be working")
    elif len(products) < 500:
        logging.info("✓ Pagination working, but could fetch more pages")
    else:
        logging.info("✓ Pagination working well!")

    # Show sample products
    logging.info(f"\nSample products (first 5):")
    for i, product in enumerate(products[:5], 1):
        logging.info(f"\n{i}. {product.name}")
        logging.info(f"   Price: ${product.price}")
        logging.info(f"   UPC: {product.external_id}")

    logging.info(f"\n{'='*70}")

if __name__ == "__main__":
    main()
