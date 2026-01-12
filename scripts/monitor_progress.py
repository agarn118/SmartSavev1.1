#!/usr/bin/env python3
"""
Monitor progress of the full Sobeys scraper test
"""

from pathlib import Path
import re

log_file = Path("logs/sobeys_2025_december_25.log")

if not log_file.exists():
    print(f"Log file not found: {log_file}")
    exit(1)

with open(log_file, 'r', encoding='utf-8') as f:
    content = f.read()

# Count query progress
query_matches = re.findall(r'Query (\d+)/(\d+):', content)
if query_matches:
    current, total = query_matches[-1]
    print(f"Progress: Query {current}/{total} ({int(current)/int(total)*100:.1f}%)")
else:
    print("No query progress found yet")

# Count products found
product_matches = re.findall(r'Found (\d+) unique products for', content)
if product_matches:
    total_products = sum(int(m) for m in product_matches)
    print(f"Products found so far: {total_products}")

# Check for completion
if "SCRAPING COMPLETE" in content:
    print("\n✓ SCRAPING COMPLETE!")
    final_match = re.search(r'Saved (\d+) products', content)
    if final_match:
        print(f"Final product count: {final_match.group(1)} products")
elif "Failed to scrape query" in content:
    failed_count = len(re.findall(r'Failed to scrape query', content))
    print(f"\n⚠ Warning: {failed_count} queries failed")
