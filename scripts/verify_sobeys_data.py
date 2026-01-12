#!/usr/bin/env python3
"""Verify Sobeys scraped data quality"""

import json
from pathlib import Path

data_file = Path("data/raw/sobeys/sobeys_products.jsonl")

print(f"Reading: {data_file}")
print("="*70)

products = []
with open(data_file, 'r', encoding='utf-8') as f:
    for line in f:
        products.append(json.loads(line))

print(f"\nTotal products: {len(products)}")
print(f"File size: {data_file.stat().st_size / 1024:.2f} KB")

# Field completeness check
fields_to_check = ['name', 'price', 'brand', 'size_text', 'category_path', 'external_id', 'availability']

print("\n" + "="*70)
print("FIELD COMPLETENESS")
print("="*70)

for field in fields_to_check:
    filled = sum(1 for p in products if p.get(field))
    pct = (filled / len(products)) * 100
    print(f"{field:20s}: {filled:4d}/{len(products)} ({pct:5.1f}%)")

# Show diverse samples
print("\n" + "="*70)
print("SAMPLE PRODUCTS (from different sections)")
print("="*70)

sample_indices = [0, 50, 100, 150, 200, 250, 300, 359]

for idx in sample_indices:
    if idx < len(products):
        p = products[idx]
        print(f"\n[{idx+1}] {p['name']}")
        print(f"    Price: ${p['price']} {p['currency']}")
        print(f"    Brand: {p.get('brand', 'N/A')}")
        print(f"    Size: {p.get('size_text', 'N/A')}")
        print(f"    Category: {p.get('category_path', 'N/A')[:60]}")
        print(f"    UPC: {p.get('external_id', 'N/A')}")
        print(f"    Stock: {p['availability']}")

# Category breakdown
print("\n" + "="*70)
print("CATEGORY BREAKDOWN (Top 10)")
print("="*70)

from collections import Counter

categories = [p.get('category_path', 'Unknown').split(' > ')[0] for p in products if p.get('category_path')]
top_cats = Counter(categories).most_common(10)

for cat, count in top_cats:
    print(f"{cat:30s}: {count:3d} products")

# Price range
print("\n" + "="*70)
print("PRICE STATISTICS")
print("="*70)

prices = [p['price'] for p in products if p.get('price') is not None]
if prices:
    print(f"Min price: ${min(prices):.2f}")
    print(f"Max price: ${max(prices):.2f}")
    print(f"Avg price: ${sum(prices)/len(prices):.2f}")

print("\n" + "="*70)
print("✅ DATA VERIFICATION COMPLETE")
print("="*70)
