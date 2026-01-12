#!/usr/bin/env python3
"""
Investigate the 13 queries that returned 0 results
Tests each query against Sobeys API and reports findings
"""

import logging
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scrapers.sites.sobeys_api import SobeysAPIScraper
from scrapers.common import setup_logging_with_rotation

# The 13 queries that returned 0 results
MISSING_QUERIES = [
    "cereal",
    "chicken broth",
    "corn",
    "crackers",
    "ham",
    "macaroni",
    "pasta",
    "salad dressing",
    "salt",
    "skim milk",
    "spaghetti",
    "sugar",
    "yogurt"
]

# Alternative query suggestions to try
ALTERNATIVE_QUERIES = {
    "skim milk": ["fat free milk", "0% milk", "nonfat milk"],
    "yogurt": ["plain yogurt", "vanilla yogurt", "strawberry yogurt"],
    "sugar": ["white sugar", "granulated sugar", "cane sugar"],
    "pasta": ["linguine", "fettuccine", "rotini"],
    "macaroni": ["elbow macaroni", "mac and cheese", "kraft dinner"],
    "spaghetti": ["spaghetti pasta", "thin spaghetti", "spaghetti noodles"],
    "cereal": ["breakfast cereal", "cheerios", "frosted flakes"],
    "salt": ["table salt", "sea salt", "kosher salt"],
    "crackers": ["saltine crackers", "ritz crackers", "graham crackers"],
    "salad dressing": ["italian dressing", "caesar dressing", "vinaigrette"],
    "chicken broth": ["chicken stock", "chicken bouillon"],
    "corn": ["sweet corn", "corn on the cob", "canned corn"],
    "ham": ["sliced ham", "deli ham", "black forest ham"]
}


def main():
    """Investigate missing queries and suggest alternatives"""
    setup_logging_with_rotation('sobeys_investigation', level=logging.INFO)

    logging.info("=" * 70)
    logging.info("INVESTIGATING MISSING QUERIES")
    logging.info("=" * 70)
    logging.info(f"Testing {len(MISSING_QUERIES)} queries that returned 0 results")
    logging.info("=" * 70)

    config_path = project_root / "configs" / "sobeys.json"
    scraper = SobeysAPIScraper(config_path, project_root)

    results = {
        "confirmed_zero": [],  # Queries that genuinely return 0 results
        "found_with_original": [],  # Queries that now return results
        "found_with_alternative": []  # Queries that work with alternative terms
    }

    for query in MISSING_QUERIES:
        logging.info(f"\n{'='*70}")
        logging.info(f"Testing: '{query}'")
        logging.info(f"{'='*70}")

        try:
            # Test original query
            products = scraper.search_products(query, max_pages=1, query_category=query)

            if products:
                logging.info(f"✓ FOUND {len(products)} products with original query '{query}'")
                results["found_with_original"].append({
                    "query": query,
                    "count": len(products),
                    "sample": products[0].name if products else None
                })
            else:
                logging.warning(f"✗ No results for '{query}'")

                # Try alternatives if available
                if query in ALTERNATIVE_QUERIES:
                    logging.info(f"Trying alternative queries...")
                    alternatives_tested = []

                    for alt_query in ALTERNATIVE_QUERIES[query]:
                        logging.info(f"  Testing: '{alt_query}'")
                        alt_products = scraper.search_products(alt_query, max_pages=1, query_category=alt_query)

                        if alt_products:
                            logging.info(f"  ✓ FOUND {len(alt_products)} products with '{alt_query}'")
                            alternatives_tested.append({
                                "alternative": alt_query,
                                "count": len(alt_products),
                                "sample": alt_products[0].name if alt_products else None
                            })
                        else:
                            logging.info(f"  ✗ No results for '{alt_query}'")

                    if alternatives_tested:
                        results["found_with_alternative"].append({
                            "original_query": query,
                            "alternatives": alternatives_tested
                        })
                    else:
                        results["confirmed_zero"].append(query)
                else:
                    results["confirmed_zero"].append(query)

        except Exception as e:
            logging.error(f"Error testing query '{query}': {e}")
            continue

    # Summary Report
    logging.info(f"\n{'='*70}")
    logging.info("INVESTIGATION SUMMARY")
    logging.info(f"{'='*70}")

    logging.info(f"\nOriginal Queries That Now Return Results: {len(results['found_with_original'])}")
    if results['found_with_original']:
        for item in results['found_with_original']:
            logging.info(f"  - '{item['query']}': {item['count']} products")
            logging.info(f"    Sample: {item['sample']}")

    logging.info(f"\nQueries Needing Alternative Terms: {len(results['found_with_alternative'])}")
    if results['found_with_alternative']:
        for item in results['found_with_alternative']:
            logging.info(f"  Original: '{item['original_query']}' (0 results)")
            for alt in item['alternatives']:
                logging.info(f"    → Try '{alt['alternative']}': {alt['count']} products")
                logging.info(f"      Sample: {alt['sample']}")

    logging.info(f"\nConfirmed Zero Results: {len(results['confirmed_zero'])}")
    if results['confirmed_zero']:
        logging.info("  These queries genuinely return no results on Sobeys:")
        for query in results['confirmed_zero']:
            logging.info(f"  - '{query}'")

    # Recommendations
    logging.info(f"\n{'='*70}")
    logging.info("RECOMMENDATIONS")
    logging.info(f"{'='*70}")

    if results['found_with_original']:
        logging.info(f"\n1. RE-RUN ORIGINAL QUERIES ({len(results['found_with_original'])})")
        logging.info("   These queries now return results - may have been API issues")
        for item in results['found_with_original']:
            logging.info(f"   - {item['query']}")

    if results['found_with_alternative']:
        logging.info(f"\n2. UPDATE QUERY TERMS ({len(results['found_with_alternative'])})")
        logging.info("   Replace generic terms with specific alternatives:")
        for item in results['found_with_alternative']:
            best_alt = max(item['alternatives'], key=lambda x: x['count'])
            logging.info(f"   - Replace '{item['original_query']}' → '{best_alt['alternative']}'")

    if results['confirmed_zero']:
        logging.info(f"\n3. ACCEPT LIMITATIONS ({len(results['confirmed_zero'])})")
        logging.info("   Document these queries as having no results:")
        for query in results['confirmed_zero']:
            logging.info(f"   - {query}")

    # Calculate new coverage estimate
    total_queries = 114
    current_coverage = 101  # From previous test
    potentially_fixed = len(results['found_with_original']) + len(results['found_with_alternative'])
    new_coverage = current_coverage + potentially_fixed
    new_percentage = (new_coverage / total_queries) * 100

    logging.info(f"\n{'='*70}")
    logging.info("COVERAGE PROJECTION")
    logging.info(f"{'='*70}")
    logging.info(f"Current coverage: {current_coverage}/{total_queries} ({current_coverage/total_queries*100:.1f}%)")
    logging.info(f"Potentially fixable: {potentially_fixed} queries")
    logging.info(f"Projected coverage: {new_coverage}/{total_queries} ({new_percentage:.1f}%)")

    if new_percentage >= 95:
        logging.info("✓ PROJECTED TO MEET 95% THRESHOLD")
    else:
        logging.info(f"⚠ Still below 95% threshold by {95 - new_percentage:.1f}%")

    logging.info(f"\n{'='*70}")


if __name__ == "__main__":
    main()
