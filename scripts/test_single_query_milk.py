#!/usr/bin/env python3
"""
Test single milk query with fixed store-level deduplication
Expected: 30-50 unique products (UPC + store_id combinations)
Previous broken behavior: ~2 products (UPC only dedup)
"""

import logging
import sys
import json
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scrapers.sites.sobeys_api import SobeysAPIScraper
from scrapers.common import setup_logging_with_rotation

def main():
    """Test single milk query to verify store-level deduplication fix"""
    setup_logging_with_rotation('sobeys_milk_test', level=logging.INFO)

    logging.info("=" * 70)
    logging.info("TESTING STORE-LEVEL DEDUPLICATION FIX")
    logging.info("=" * 70)
    logging.info("Query: 'milk'")
    logging.info("Max pages: 10 (optimized)")
    logging.info("Expected: 30-50 unique (UPC, store_id) combinations")
    logging.info("Previous bug: ~2 products (UPC-only dedup)")
    logging.info("=" * 70)

    config_path = project_root / "configs" / "sobeys.json"
    scraper = SobeysAPIScraper(config_path, project_root)

    # Test with milk query
    products = scraper.search_products("milk", max_pages=10, query_category="milk")

    logging.info(f"\n{'='*70}")
    logging.info("TEST RESULTS")
    logging.info(f"{'='*70}")
    logging.info(f"Total unique products: {len(products)}")
    logging.info(f"Expected range: 30-50 products")

    if len(products) < 10:
        logging.error("❌ FAIL: Store-level deduplication may not be working")
    elif len(products) < 30:
        logging.warning("⚠ WARNING: Lower than expected, but improved")
    else:
        logging.info("✓ PASS: Store-level deduplication working correctly!")

    # Analyze store distribution
    store_counts = {}
    for product in products:
        store_id = product.raw_source.get('storeId', 'unknown')
        store_counts[store_id] = store_counts.get(store_id, 0) + 1

    logging.info(f"\nStore distribution:")
    for store_id, count in sorted(store_counts.items()):
        logging.info(f"  Store {store_id}: {count} products")

    # Show sample products
    logging.info(f"\nSample products (first 5):")
    for i, product in enumerate(products[:5], 1):
        store_id = product.raw_source.get('storeId', 'unknown')
        logging.info(f"\n{i}. {product.name}")
        logging.info(f"   Store: {store_id}")
        logging.info(f"   Price: ${product.price}")
        logging.info(f"   UPC: {product.external_id}")

    # Save to FRESH output file (delete old one first)
    output_dir = Path("data/raw/sobeys")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "sobeys_products.jsonl"

    # Delete existing file to start fresh
    if output_file.exists():
        output_file.unlink()
        logging.info(f"\n✓ Cleared existing output file")

    # Write new data
    with open(output_file, 'w', encoding='utf-8') as f:
        for product in products:
            f.write(json.dumps(product.to_dict()) + '\n')

    logging.info(f"\n{'='*70}")
    logging.info("TEST COMPLETE")
    logging.info(f"{'='*70}")
    logging.info(f"Saved {len(products)} products to: {output_file}")
    logging.info(f"File size: {output_file.stat().st_size / 1024:.2f} KB")
    logging.info(f"{'='*70}")

if __name__ == "__main__":
    main()
