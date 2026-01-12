# QA Auditor Criteria for Sobeys Scraper

## Critical Success Criteria

### 1. Minimum Product Volume ⚠️ **NEW REQUIREMENT**
**Target:** Minimum 1,000+ unique products for comprehensive test
**Why:** 114 queries across a full grocery store should yield thousands of products, not dozens

**Evaluation:**
- Count total unique products in final dataset
- If < 1,000: **FAIL** - Insufficient data collection
- If 1,000-5,000: **ACCEPTABLE** - Good coverage
- If > 5,000: **EXCELLENT** - Comprehensive coverage

**Common Issues:**
- max_pages set too low (should be 50-100+ per query)
- Pagination stopping prematurely
- Only scraping first page of results

### 2. Zero Duplicate UPCs
**Target:** 0 duplicate UPCs
**Why:** UPC (external_id) is unique per product

**Evaluation:**
- Count unique UPCs in dataset
- If duplicates found: **FAIL**

### 3. Unit Price Coverage
**Target:** >95% of products have unit_price and unit_price_uom
**Why:** Essential for price comparison across package sizes

**Evaluation:**
- Calculate: (products with unit_price) / (total products) * 100%
- If < 95%: **FAIL**
- If ≥ 95%: **PASS**

### 4. Query Categorization Coverage
**Target:** 100% of products have query_category field populated
**Why:** Enables frontend organization by search category

**Evaluation:**
- Check that all products have non-null query_category
- If any missing: **FAIL**

### 5. Data Validation Errors
**Target:** 0 validation errors
**Why:** All required fields must be present and valid

**Evaluation:**
- Check required fields: name, price, external_id (UPC), source_url, store
- Check data types: price is numeric, external_id is string, etc.
- If any errors: **FAIL**

### 6. Query Execution Success
**Target:** 100% of queries execute successfully (114/114)
**Why:** All search terms should be attempted

**Evaluation:**
- Verify in logs that all 114 queries were executed
- Count failed queries
- If any failed: Document reason and determine if acceptable

## Secondary Quality Metrics

### 7. Product Distribution Balance
**Evaluation:**
- Check products per query category
- Warn if many queries return 0 products (may indicate query coverage issue)
- Document categories with highest/lowest product counts

### 8. Price Distribution
**Evaluation:**
- Check min, max, mean, median prices
- Flag if prices are all $0 or unreasonably high
- Ensure price range is realistic for grocery products ($0.50 - $100 typical)

### 9. Field Completeness (Optional)
**Evaluation:**
- Brand coverage: Acceptable if >70% (some products lack brands)
- Image coverage: Excellent if >90%, acceptable if >75%
- Size text: Should be 100%
- Category path: Should be 100%

## Quality Score Calculation

**Formula:**
```
Score = (
    (min_products_met ? 30 : 0) +  # NEW: 30 points for meeting minimum volume
    (zero_duplicates ? 25 : 0) +   # Reduced from 40
    (unit_price_coverage >= 0.95 ? 25 : unit_price_coverage * 25) +  # Reduced from 30
    (query_cat_coverage >= 0.95 ? 10 : query_cat_coverage * 10) +    # Reduced from 30
    (zero_validation_errors ? 10 : 0)  # Added validation errors check
)
```

**Grade Thresholds:**
- 95-100: A+ (Excellent) - **APPROVE FOR DEPLOYMENT**
- 85-94: A (Good) - **APPROVE WITH MINOR NOTES**
- 75-84: B (Acceptable) - **REQUEST IMPROVEMENTS**
- 65-74: C (Needs Work) - **REQUIRES FIXES**
- < 65: F (Fail) - **REJECT, MAJOR FIXES NEEDED**

## Product Volume Benchmarks

Based on typical grocery store inventory:

**Expected Product Counts per Query (before deduplication):**
- Generic queries (milk, bread, eggs): 500-2,000 products each
- Specific queries (skim milk, 2% milk): 100-500 products each
- Niche queries (oat milk, almond milk): 50-200 products each

**Expected Total After Deduplication:**
- Minimum acceptable: 1,000 unique products
- Good coverage: 2,000-5,000 unique products
- Excellent coverage: 5,000-10,000 unique products

**If < 1,000 products:**
1. Check max_pages setting (should be 50-100 or None for auto)
2. Check logs for premature pagination stopping
3. Verify API is returning products (check debug snapshots)
4. Consider increasing max_pages limit

## Report Template

### Executive Summary
- Total products: X
- Product volume: [PASS/FAIL] (target: 1,000+)
- UPC uniqueness: [PASS/FAIL]
- Unit price coverage: X% [PASS/FAIL]
- Query categorization: X% [PASS/FAIL]
- Validation errors: X [PASS/FAIL]
- Query execution: X/114 [PASS/FAIL]

### Overall Quality Score: X/100
**Deployment Decision:** [APPROVED / REQUIRES FIXES / REJECTED]

### Critical Issues (if any):
1. Issue description
2. Issue description

### Recommendations:
1. Recommendation
2. Recommendation

## Changelog

**2025-12-25:** Added minimum product volume requirement (1,000+ products)
- Updated quality score formula to include product volume (30 points)
- Added benchmarks for expected product counts
- Added troubleshooting steps for low product counts
