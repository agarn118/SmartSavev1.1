#!/usr/bin/env python3
"""
Analyze Sobeys scraper output to verify fixes
"""
import json
from pathlib import Path

# Read the data
file_path = Path('data/raw/sobeys/sobeys_products.jsonl')
products = []
with open(file_path, 'r', encoding='utf-8') as f:
    for line in f:
        products.append(json.loads(line))

# Analysis
total = len(products)
unique_upcs = set(p['external_id'] for p in products)
with_unit_price = sum(1 for p in products if p['unit_price'] is not None)
with_price = sum(1 for p in products if p['price'] is not None)

print("=" * 60)
print("SOBEYS SCRAPER DATA QUALITY ANALYSIS")
print("=" * 60)
print(f'Total Products: {total}')
print(f'Unique UPCs: {len(unique_upcs)}')
print(f'Deduplication Rate: {(1 - len(unique_upcs)/total)*100:.1f}%')
print(f'Products with Price: {with_price} ({with_price/total*100:.1f}%)')
print(f'Products with Unit Price: {with_unit_price} ({with_unit_price/total*100:.1f}%)')
print()

# Check for duplicates
print("DUPLICATION CHECK:")
if len(unique_upcs) == total:
    print("[PASS] NO DUPLICATES - All UPCs are unique!")
else:
    print(f"[FAIL] DUPLICATES FOUND - {total - len(unique_upcs)} duplicate records")

print()
print("SAMPLE PRODUCTS:")
print("-" * 60)
for i, p in enumerate(products[:5], 1):
    print(f"{i}. {p['name'][:55]}")
    print(f"   Price: ${p.get('price', 'N/A')} | Unit: ${p.get('unit_price', 'N/A')}/{p.get('unit_price_uom', 'N/A')}")
    print(f"   UPC: {p['external_id']} | Category: {p.get('category_path', 'N/A')}")
    print()

print("=" * 60)
print("QUALITY SCORE ASSESSMENT")
print("=" * 60)

# Calculate quality metrics
dedup_score = (len(unique_upcs) / total) * 100 if total > 0 else 0
price_score = (with_price / total) * 100 if total > 0 else 0
unit_price_score = (with_unit_price / total) * 100 if total > 0 else 0

# Overall score (weighted)
overall_score = (dedup_score * 0.4) + (price_score * 0.3) + (unit_price_score * 0.3)

print(f"Deduplication: {dedup_score:.1f}% (weight: 40%)")
print(f"Price Coverage: {price_score:.1f}% (weight: 30%)")
print(f"Unit Price Coverage: {unit_price_score:.1f}% (weight: 30%)")
print()
print(f"OVERALL QUALITY SCORE: {overall_score:.1f}/100")

if overall_score >= 95:
    print("Grade: A+ (Excellent)")
elif overall_score >= 85:
    print("Grade: A (Very Good)")
elif overall_score >= 75:
    print("Grade: B (Good)")
elif overall_score >= 65:
    print("Grade: C (Acceptable)")
else:
    print("Grade: F (Needs Work)")
