#!/usr/bin/env python3
"""Verify that all 114 queries were executed and analyze deduplication"""

import re
from pathlib import Path

log_file = Path("logs/sobeys_2025_december_25.log")

with open(log_file, 'r', encoding='utf-8') as f:
    log_content = f.read()

# Count queries executed
queries_run = len(re.findall(r'Query \d+/114:', log_content))
print(f"Queries Executed: {queries_run}/114")

# Find products found per query
found_matches = re.findall(r'Found (\d+) unique products for \'([^\']+)\'', log_content)

print(f"\nProducts Found Per Query:")
print(f"Total query results: {len(found_matches)}")

# Count how many returned 0 products
zero_results = [query for count, query in found_matches if int(count) == 0]
non_zero_results = [(query, int(count)) for count, query in found_matches if int(count) > 0]

print(f"\nQueries with 0 products: {len(zero_results)}")
if zero_results:
    print("  Queries:")
    for query in zero_results:
        print(f"    - {query}")

print(f"\nQueries with products: {len(non_zero_results)}")
total_before_dedup = sum(count for _, count in non_zero_results)
print(f"Total products before global dedup: {total_before_dedup}")

# Show last 20 results
print(f"\nLast 20 query results:")
for query, count in found_matches[-20:]:
    print(f"  {query}: {count} products")

# Get final stats
final_stats = re.search(r'Total products retrieved: (\d+)\s+.*?Unique products.*?: (\d+)', log_content, re.DOTALL)
if final_stats:
    print(f"\nFinal Statistics:")
    print(f"  Total retrieved: {final_stats.group(1)}")
    print(f"  After deduplication: {final_stats.group(2)}")
    print(f"  Deduplication removed: {int(final_stats.group(1)) - int(final_stats.group(2))} duplicates")
