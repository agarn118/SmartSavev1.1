#!/usr/bin/env python3
"""
Full Sobeys scraper test with comprehensive query list
"""

import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scrapers.sites.sobeys_api import SobeysAPIScraper
from scrapers.common import setup_logging_with_rotation, backup_data_file

# Comprehensive search queries
SEARCH_QUERIES = [
    "milk", "skim milk", "2% milk", "whole milk", "almond milk",
    "soy milk", "oat milk", "butter", "margarine", "yogurt",
    "greek yogurt", "cheese", "cheddar cheese", "mozzarella", "cream cheese",
    "eggs", "bread", "white bread", "whole wheat bread", "bagels",
    "english muffins", "tortillas", "pasta", "spaghetti", "macaroni",
    "penne", "rice", "white rice", "brown rice", "flour",
    "sugar", "brown sugar", "baking soda", "baking powder", "salt",
    "pepper", "olive oil", "vegetable oil", "canola oil", "cereal",
    "corn flakes", "oatmeal", "granola", "peanut butter", "jam",
    "honey", "maple syrup", "ketchup", "mustard", "mayonnaise",
    "salad dressing", "ranch dressing", "soy sauce", "hot sauce", "vinegar",
    "canned soup", "tomato sauce", "pasta sauce", "canned beans", "canned tomatoes",
    "canned corn", "chicken broth", "beef broth", "coffee", "tea",
    "sugar substitute", "juice", "orange juice", "apple juice", "soda",
    "water", "sparkling water", "chips", "crackers", "cookies",
    "ice cream", "frozen pizza", "frozen vegetables", "frozen fruit", "chicken breast",
    "ground beef", "bacon", "sausage", "ham", "deli meat",
    "hot dogs", "salmon", "tuna", "shrimp", "apples",
    "bananas", "oranges", "grapes", "strawberries", "blueberries",
    "tomatoes", "lettuce", "spinach", "carrots", "potatoes",
    "onions", "garlic", "peppers", "cucumbers", "broccoli",
    "cauliflower", "corn", "peas", "green beans", "paper towels",
    "toilet paper", "dish soap", "laundry detergent", "trash bags"
]


def main():
    """Run comprehensive scraping test with all queries"""
    # Setup logging with automatic rotation
    setup_logging_with_rotation('sobeys', level=logging.INFO)

    logging.info("=" * 70)
    logging.info("SOBEYS COMPREHENSIVE SCRAPER TEST")
    logging.info("=" * 70)
    logging.info(f"Total queries: {len(SEARCH_QUERIES)}")
    logging.info(f"Pages per query: Auto (up to 10 pages per query, optimized for duplication)")
    logging.info(f"Expected API calls: ~{len(SEARCH_QUERIES) * 10} (estimated ~1,140 API calls)")
    logging.info(f"Expected runtime: ~2-3 hours (with 6-second delays)")
    logging.info(f"Expected products: ~1,500-2,000 unique products after deduplication")
    logging.info("=" * 70)

    # Setup paths
    config_path = project_root / "configs" / "sobeys.json"

    scraper = SobeysAPIScraper(config_path, project_root)

    # Collect products for all search terms
    all_products = []
    successful_queries = 0
    failed_queries = []

    for i, query in enumerate(SEARCH_QUERIES, 1):
        logging.info(f"\n{'='*70}")
        logging.info(f"Query {i}/{len(SEARCH_QUERIES)}: '{query}'")
        logging.info(f"{'='*70}")

        try:
            # Search with auto pagination (up to 100 pages per query, ~2400 products max per query)
            # This will automatically fetch all available pages up to the limit
            products = scraper.search_products(query, max_pages=None, query_category=query)

            logging.info(f"Found {len(products)} unique products for '{query}'")
            all_products.extend(products)
            successful_queries += 1

        except Exception as e:
            logging.error(f"Failed to scrape query '{query}': {e}")
            failed_queries.append(query)
            continue

    # Deduplicate across all queries using UPC
    unique_products = {}
    for product in all_products:
        # Use UPC (external_id) as the deduplication key
        key = product.external_id
        if key not in unique_products:
            unique_products[key] = product
        else:
            # Detect price changes
            existing = unique_products[key]
            if existing.price != product.price:
                logging.warning(
                    f"Price change detected for {product.name} (UPC: {key}): "
                    f"${existing.price} -> ${product.price}"
                )
            # Keep the most recent one
            if product.scrape_ts > existing.scrape_ts:
                unique_products[key] = product

    final_products = list(unique_products.values())

    logging.info(f"\n{'='*70}")
    logging.info("FINAL RESULTS")
    logging.info(f"{'='*70}")
    logging.info(f"Successful queries: {successful_queries}/{len(SEARCH_QUERIES)}")
    logging.info(f"Failed queries: {len(failed_queries)}")
    if failed_queries:
        logging.warning(f"Failed query list: {', '.join(failed_queries)}")
    logging.info(f"Total products retrieved: {len(all_products)}")
    logging.info(f"Unique products (after deduplication): {len(final_products)}")
    logging.info(f"Deduplication rate: {(1 - len(final_products)/len(all_products))*100:.1f}%")
    logging.info(f"{'='*70}")

    # Show query category distribution
    query_distribution = {}
    for product in final_products:
        category = product.query_category or "unknown"
        query_distribution[category] = query_distribution.get(category, 0) + 1

    logging.info(f"\nQuery Category Distribution (Top 20):")
    for category, count in sorted(query_distribution.items(), key=lambda x: x[1], reverse=True)[:20]:
        logging.info(f"  {category}: {count} products")

    # Show sample with all fields
    if final_products:
        logging.info(f"\nSample products (first 5):")
        for product in final_products[:5]:
            logging.info(f"\n  Name: {product.name}")
            logging.info(f"  Brand: {product.brand}")
            logging.info(f"  Price: ${product.price} {product.currency}")
            logging.info(f"  Size: {product.size_text}")
            logging.info(f"  Unit Price: {product.unit_price} {product.unit_price_uom}")
            logging.info(f"  Category: {product.category_path}")
            logging.info(f"  Query Category: {product.query_category}")
            logging.info(f"  Availability: {product.availability}")
            logging.info(f"  UPC: {product.external_id}")

    # Save to proper output file
    output_dir = Path("data/raw/sobeys")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "sobeys_products.jsonl"

    # Backup existing data before overwriting
    backup_data_file(output_file)

    import json
    with open(output_file, 'w', encoding='utf-8') as f:
        for product in final_products:
            f.write(json.dumps(product.to_dict()) + '\n')

    logging.info(f"\n{'='*70}")
    logging.info("SCRAPING COMPLETE")
    logging.info(f"{'='*70}")
    logging.info(f"Saved {len(final_products)} products to: {output_file}")
    logging.info(f"File size: {output_file.stat().st_size / 1024:.2f} KB")
    logging.info(f"{'='*70}")


if __name__ == "__main__":
    main()
