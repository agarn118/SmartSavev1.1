# Grocery Scrapers - Complete Guide

**Version:** 1.0
**Last Updated:** 2025-12-19

## Table of Contents

1. [Overview](#overview)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Usage Guide](#usage-guide)
5. [Supported Sites](#supported-sites)
6. [Output Format](#output-format)
7. [Checkpoints & Resume](#checkpoints--resume)
8. [Adding New Sites](#adding-new-sites)
9. [Troubleshooting](#troubleshooting)
10. [Architecture](#architecture)

---

## Overview

This framework provides a robust, ethical web scraping system for Canadian grocery websites. It extracts product data (prices, names, descriptions, availability) and writes standardized JSONL output.

**Key Features:**
- **Consistent schema** across all sites
- **Automatic deduplication** by product ID or normalized name
- **Checkpoint/resume** capability for long-running scrapes
- **Rate limiting** with jitter to avoid detection
- **Retry logic** with exponential backoff
- **Dual output** formats (JSONL + optional CSV)
- **Structured logging** for debugging

**Sites Currently Supported:**
- Real Canadian Superstore (Loblaw network) - **EASIEST**
- No Frills (Loblaw network) - **EASIEST**
- Safeway (Sobeys network) - Requires Playwright
- Sobeys (Sobeys network) - Requires Playwright

**Sites NOT Supported** (per ethical/technical constraints):
- Walmart.ca - PerimeterX CAPTCHA blocking
- Save-on-Foods - 403 WAF blocking
- Co-op - 403 WAF blocking

---

## Installation

### Pre-Flight Check (IMPORTANT - Start Here!)

Before using the scrapers, verify all dependencies are installed:

```bash
# Run the dependency checker
python check_dependencies.py
```

The checker will validate:
- Core packages (requests, beautifulsoup4, lxml, numpy)
- Playwright package (for Safeway/Sobeys)
- Chromium browser installation

If any dependencies are missing, you'll see clear instructions for installation.

### Prerequisites

- Python 3.8+
- pip

### Automated Installation (Recommended)

```powershell
# Run the automated installer (Windows PowerShell)
.\install_dependencies.ps1
```

This script will:
1. Install all Python packages from requirements.txt
2. Install Playwright Chromium browser
3. Verify installation was successful

### Manual Installation

```bash
# Install required packages
pip install -r requirements.txt

# For Safeway/Sobeys (Playwright required):
playwright install chromium
```

### Verify Installation

```bash
# Check if all dependencies are installed
python check_dependencies.py

# Check if framework is working
python scrapers/run.py --help
```

### Important Notes

**Scraper Requirements:**
- **realcanadiansuperstore & nofrills**: Only need core packages (requests, beautifulsoup4, lxml)
- **safeway & sobeys**: REQUIRE Playwright + Chromium browser

If you only need Superstore/No Frills, you can skip Playwright installation.

---

## Quick Start

### Example 1: Scrape Real Canadian Superstore

```bash
# Search for "milk" products (max 3 pages)
python scrapers/run.py \
  --site realcanadiansuperstore \
  --query "milk" \
  --max-pages 3 \
  --output-format both
```

**Output:**
- `data/raw/realcanadiansuperstore/realcanadiansuperstore_products.jsonl`
- `data/raw/realcanadiansuperstore/realcanadiansuperstore_products.csv`
- `data/logs/realcanadiansuperstore.log`

### Example 2: Scrape Specific Category

```bash
# Scrape dairy category from No Frills
python scrapers/run.py \
  --site nofrills \
  --category-url "/en/food/dairy-and-eggs/milk-and-cream/milk/c/28000" \
  --max-pages 5
```

### Example 3: Resume Interrupted Scrape

```bash
# Resume from checkpoint
python scrapers/run.py \
  --site safeway \
  --query "bread" \
  --resume
```

---

## Usage Guide

### Command-Line Interface

```bash
python scrapers/run.py --site <site_slug> [options]
```

**Required:**
- `--site` - Site identifier (realcanadiansuperstore, nofrills, safeway, sobeys)

**Scraping Mode** (pick one):
- `--query "search term"` - Search for products
- `--category-url "/path"` - Scrape category page
- `--product-url "/path"` - Scrape single product (testing)

**Options:**
- `--max-pages N` - Limit number of pages (default: all)
- `--headless` - Run browser in headless mode (default: True)
- `--headful` - Run browser visibly (for debugging)
- `--output-format {jsonl,csv,both}` - Output format (default: jsonl)
- `--resume` - Resume from checkpoint
- `--clear-checkpoint` - Clear checkpoint before starting
- `--log-level {DEBUG,INFO,WARNING,ERROR}` - Logging verbosity (default: INFO)

### Examples

**Search Mode:**
```bash
python scrapers/run.py --site realcanadiansuperstore --query "organic milk"
```

**Category Mode:**
```bash
python scrapers/run.py --site nofrills --category-url "/en/food/bakery/c/27990"
```

**Single Product (Testing):**
```bash
python scrapers/run.py --site safeway --product-url "/products/product/milk/p/20188875001"
```

**Visible Browser (Debug):**
```bash
python scrapers/run.py --site sobeys --query "cheese" --headful
```

**Export to CSV:**
```bash
python scrapers/run.py --site realcanadiansuperstore --query "bread" --output-format both
```

---

## Supported Sites

### Real Canadian Superstore

**Difficulty:** ⭐ Easy
**Method:** Embedded JSON extraction (requests)
**JavaScript Required:** No

**Example URLs:**
- Category: `/en/food/dairy-and-eggs/milk-and-cream/milk/c/28000`
- Search: `/search?search-bar=milk`
- Product: `/en/2-milk/p/20188873_EA`

**Notes:**
- Fastest scraping (no browser needed)
- All product data in `__NEXT_DATA__` JSON
- Permissive robots.txt

### No Frills

**Difficulty:** ⭐ Easy
**Method:** Same as Superstore (Loblaw network)
**JavaScript Required:** No

**Notes:**
- Identical platform to Real Canadian Superstore
- Same product IDs, different prices
- No browser required

### Safeway

**Difficulty:** ⭐⭐⭐ Moderate
**Method:** Playwright (JavaScript rendering)
**JavaScript Required:** Yes

**Example URLs:**
- Category: `/shop/categories/dairy-eggs.24.html`
- Search: `/shop/search-results.html?q=milk`

**Notes:**
- Requires Playwright browser
- Store selection affects pricing (optional)
- Slower due to JS rendering

### Sobeys

**Difficulty:** ⭐⭐⭐ Moderate
**Method:** Same as Safeway (Sobeys network)
**JavaScript Required:** Yes

**Notes:**
- Identical platform to Safeway
- Requires Playwright
- Store context recommended for accurate prices

---

## Output Format

### Standard Product Schema

Every product record includes these fields:

```json
{
  "store": "Real Canadian Superstore",
  "site_slug": "realcanadiansuperstore",
  "source_url": "https://www.realcanadiansuperstore.ca/en/2-milk/p/20188873_EA",
  "scrape_ts": "2025-12-19T10:30:00Z",
  "external_id": "20188873_EA",
  "name": "2% Milk",
  "brand": "Neilson",
  "size_text": "4 L",
  "price": 6.25,
  "currency": "CAD",
  "unit_price": 1.56,
  "unit_price_uom": "per L",
  "image_url": "https://assets.shop.loblaws.ca/products/20188873/b1/en/front/20188873_front_a01_@2.png",
  "category_path": "Food > Dairy & Eggs > Milk & Cream",
  "availability": "in_stock",
  "raw_source": {
    "type": "next-data",
    "data": {...}
  }
}
```

**Field Descriptions:**
- `store` - Human-readable store name
- `site_slug` - Site identifier (matches config)
- `source_url` - URL where product was found
- `scrape_ts` - ISO 8601 timestamp of scrape
- `external_id` - Site's product ID (null if unavailable)
- `name` - Product name (required)
- `brand` - Brand name (nullable)
- `size_text` - Package size/description (nullable)
- `price` - Price in specified currency (nullable)
- `currency` - ISO currency code (default: CAD)
- `unit_price` - Price per unit (e.g., per kg) (nullable)
- `unit_price_uom` - Unit of measure for unit price (nullable)
- `image_url` - Product image URL (nullable)
- `category_path` - Category breadcrumb (nullable)
- `availability` - Stock status: `in_stock`, `out_of_stock`, `unknown`
- `raw_source` - Original data snippet for debugging

### Output Files

All outputs are saved under `data/` directory:

```
PROJECT_ROOT/
  data/
    raw/
      realcanadiansuperstore/
        realcanadiansuperstore_products.jsonl
        realcanadiansuperstore_products.csv
      nofrills/
        nofrills_products.jsonl
        nofrills_products.csv
      safeway/
        safeway_products.jsonl
      sobeys/
        sobeys_products.jsonl
    logs/
      realcanadiansuperstore.log
      nofrills.log
      safeway.log
      sobeys.log
    checkpoints/
      realcanadiansuperstore_checkpoint.json
      nofrills_checkpoint.json
      safeway_checkpoint.json
      sobeys_checkpoint.json
```

**JSONL Format:**
- One JSON object per line
- Append-safe for long-running scrapes
- Easy to stream/process incrementally

**CSV Format:**
- Generated from JSONL on request
- Nested objects (like `raw_source`) are JSON-stringified
- Useful for non-technical stakeholders

---

## Checkpoints & Resume

### How Checkpoints Work

Checkpoints save scraper state to allow resuming interrupted scrapes:

- **Seen keys** - List of already-scraped product IDs for deduplication
- **Statistics** - Counts of scraped/duplicates/errors
- **Custom data** - Site-specific state (e.g., last page number)

**Checkpoint Location:**
```
data/checkpoints/<site_slug>_checkpoint.json
```

### Resume from Checkpoint

```bash
# Start scraping
python scrapers/run.py --site nofrills --query "milk" --max-pages 10

# [Interrupted by Ctrl+C or error]

# Resume where you left off
python scrapers/run.py --site nofrills --query "milk" --max-pages 10 --resume
```

The scraper will:
1. Load previously seen product keys
2. Skip duplicates automatically
3. Continue from where it stopped

### Clear Checkpoint

```bash
# Start fresh (ignore previous scrape)
python scrapers/run.py --site safeway --query "bread" --clear-checkpoint
```

---

## Adding New Sites

### Template Checklist

To add a new grocery site:

1. **Create config file:** `configs/<site_slug>.json`
2. **Create scraper module:** `scrapers/sites/<site_slug>.py`
3. **Implement required methods**
4. **Test with single product first**
5. **Update this README**

### Step-by-Step Guide

#### 1. Create Config File

`configs/newsite.json`:
```json
{
  "site_slug": "newsite",
  "store_name": "New Site Grocery",
  "base_url": "https://www.newsite.com",
  "search_url_pattern": "/search",
  "min_delay_seconds": 3.0,
  "max_delay_seconds": 5.0,
  "max_requests_per_minute": 15,
  "max_retries": 3,
  "headers": {
    "User-Agent": "Mozilla/5.0 ..."
  },
  "example_urls": {
    "category": "/category/dairy",
    "search": "/search?q=milk",
    "product": "/product/12345"
  }
}
```

#### 2. Create Scraper Module

`scrapers/sites/newsite.py`:
```python
#!/usr/bin/env python3
"""Newsite scraper."""

from scrapers.base import BaseScraper, ProductRecord
from scrapers.common import get_iso_timestamp
import requests
from bs4 import BeautifulSoup

class NewsiteScraper(BaseScraper):
    """Scraper for New Site Grocery."""

    def __init__(self, config_path, project_root, headless=True):
        super().__init__(config_path, project_root)
        self.base_url = self.config['base_url']
        # Initialize HTTP session, browser, etc.

    def scrape_category(self, category_url: str, max_pages=None) -> int:
        """Scrape category page. Return number of products scraped."""
        # TODO: Implement
        pass

    def scrape_search(self, query: str, max_pages=None) -> int:
        """Scrape search results. Return number of products scraped."""
        # TODO: Implement
        pass

    def scrape_product_page(self, product_url: str) -> ProductRecord:
        """Scrape single product. Return ProductRecord or None."""
        # TODO: Implement
        pass
```

#### 3. Choose Scraping Approach

**Option A: Simple (Embedded JSON/HTML)**
- Inherit from `BaseScraper`
- Use `requests` + `BeautifulSoup`
- Extract from `<script>` tags or HTML structure
- See: `scrapers/sites/realcanadiansuperstore.py`

**Option B: JavaScript Rendering (React/Vue/Angular)**
- Inherit from `BaseScraper`
- Use `playwright` for browser automation
- Extract from rendered DOM or `window.__NEXT_DATA__`
- See: `scrapers/sites/safeway.py`

#### 4. Test Implementation

```bash
# Test single product first
python scrapers/run.py --site newsite --product-url "/product/12345"

# Test search (limited)
python scrapers/run.py --site newsite --query "milk" --max-pages 1

# Full scrape
python scrapers/run.py --site newsite --category-url "/category/dairy"
```

#### 5. Key Implementation Points

**Required Methods:**
- `scrape_category()` - Handle pagination, extract products from category
- `scrape_search()` - Search by query, extract results
- `scrape_product_page()` - Extract single product details

**Use Helper Methods:**
- `self.rate_limiter.wait()` - Before each request
- `self.save_record(record)` - Save single product (validates + dedupes)
- `self.save_records_batch(records)` - Efficient batch save
- `self.stats['errors'] += 1` - Track errors
- `logging.info()`, `logging.error()` - Log progress

**Return ProductRecord:**
```python
from scrapers.base import ProductRecord
from scrapers.common import get_iso_timestamp

record = ProductRecord(
    store=self.store_name,
    site_slug=self.site_slug,
    source_url=url,
    scrape_ts=get_iso_timestamp(),
    external_id=product_id,
    name="Product Name",
    brand="Brand Name",
    size_text="500g",
    price=4.99,
    currency="CAD",
    unit_price=None,
    unit_price_uom=None,
    image_url="https://...",
    category_path="Food > Dairy",
    availability="in_stock",
    raw_source={"type": "json", "data": {...}}
)
```

---

## Troubleshooting

### Common Issues

#### 1. "Config file not found"

**Problem:** Missing config file for site.

**Solution:**
```bash
# Check config exists
ls configs/<site_slug>.json

# Create from template if missing
```

#### 2. "Failed to load scraper"

**Problem:** Scraper module not found or has syntax errors.

**Solution:**
```bash
# Check scraper exists
ls scrapers/sites/<site_slug>.py

# Check for Python syntax errors
python -m py_compile scrapers/sites/<site_slug>.py
```

#### 3. "No products found"

**Problem:** Extraction logic not finding data.

**Solution:**
- Use `--headful` to watch browser (for Playwright scrapers)
- Use `--log-level DEBUG` to see detailed logs
- Check if site structure changed
- Inspect page source manually to verify selectors

#### 4. "Rate limit exceeded / 429 errors"

**Problem:** Scraping too fast.

**Solution:**
- Increase delays in config: `min_delay_seconds`, `max_delay_seconds`
- Decrease `max_requests_per_minute`
- Add longer pauses between pages

#### 5. "CAPTCHA detected / Bot blocking"

**Problem:** Site has anti-bot protection.

**Solution:**
- For Walmart/Save-on-Foods/Co-op: **Do not scrape** (see site_designs.md)
- For other sites: Increase delays, use more realistic headers
- Consider requesting official API access

#### 6. Playwright "Browser not found"

**Problem:** Chromium not installed.

**Solution:**
```bash
playwright install chromium
```

---

## Architecture

### Project Structure

```
PROJECT_ROOT/
├── scrapers/
│   ├── __init__.py
│   ├── common.py          # Utilities (rate limiting, file ops, logging)
│   ├── base.py            # BaseScraper abstract class
│   ├── run.py             # CLI entrypoint
│   └── sites/
│       ├── __init__.py
│       ├── realcanadiansuperstore.py
│       ├── nofrills.py
│       ├── safeway.py
│       └── sobeys.py
├── configs/
│   ├── realcanadiansuperstore.json
│   ├── nofrills.json
│   ├── safeway.json
│   └── sobeys.json
├── data/
│   ├── raw/
│   │   └── <site_slug>/
│   │       ├── <site_slug>_products.jsonl
│   │       └── <site_slug>_products.csv
│   ├── logs/
│   │   └── <site_slug>.log
│   └── checkpoints/
│       └── <site_slug>_checkpoint.json
├── SCRAPERS_README.md
├── requirements.txt
└── site_designs.md        # Technical site analysis
```

### Component Roles

**`scrapers/common.py`**
- `RateLimiter` - Jittered delays + request tracking
- `retry_on_exception()` - Decorator for automatic retries
- File operations (JSONL append, CSV export, JSON load/save)
- Logging setup
- Utility functions (price parsing, normalization)

**`scrapers/base.py`**
- `ProductRecord` - Standard data model with validation
- `CheckpointManager` - Save/load checkpoint state
- `BaseScraper` - Abstract base with common functionality
  - Config loading
  - Path management (all outputs under `data/`)
  - Deduplication by product ID or normalized name
  - Validation before saving
  - Statistics tracking

**`scrapers/run.py`**
- CLI argument parsing
- Dynamic scraper import
- Logging initialization
- Exception handling (graceful shutdown)
- Checkpoint save on interrupt (Ctrl+C)

**`scrapers/sites/<site_slug>.py`**
- Site-specific extraction logic
- Implements abstract methods from `BaseScraper`
- Handles site structure, pagination, selectors

### Data Flow

```
User Input (CLI)
    ↓
run.py (load config, init scraper)
    ↓
Site-Specific Scraper (scrape_search / scrape_category)
    ↓
Extract Products → Normalize to ProductRecord
    ↓
Validate & Deduplicate (base.py)
    ↓
Save to JSONL (append-safe)
    ↓
[Optional] Export to CSV
    ↓
Save Checkpoint
```

### Rate Limiting Strategy

1. **Jittered delays** - Random delays between min/max avoid patterns
2. **Requests-per-minute tracking** - Enforces max request rate
3. **Adaptive backoff** - Increases delay on errors
4. **Per-site configuration** - Customize for each site's tolerance

### Deduplication Logic

Products are deduplicated using a "dedupe key":

1. **If `external_id` exists:** Use `site_slug:external_id`
2. **Otherwise:** Use `site_slug:normalized_name:normalized_size:store`

Seen keys are tracked in memory and saved to checkpoint for resume capability.

---

## Legal & Ethical Considerations

**⚠️ Important:**

1. **Respect robots.txt** - Only scrape allowed paths
2. **Rate limiting** - Never overwhelm site servers
3. **Terms of Service** - Review each site's ToS before scraping
4. **Official APIs** - Prefer official data access when available
5. **Attribution** - Credit data sources appropriately
6. **Do Not Scrape:**
   - Sites with aggressive bot protection (Walmart, Save-on-Foods, Co-op)
   - Sites explicitly prohibiting scraping in ToS
   - Sites behind authentication/paywalls

See `site_designs.md` for detailed legal assessment of each site.

---

## Support & Contributions

### Reporting Issues

When reporting problems, include:
- Full command used
- Error message / stack trace
- Log file (`data/logs/<site_slug>.log`)
- Site + URL being scraped

### Contributing New Sites

1. Follow "Adding New Sites" guide above
2. Test thoroughly with `--max-pages 1` first
3. Ensure rate limiting is respectful (3-5s minimum delay)
4. Document any site-specific quirks in config `notes`

---

## Changelog

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-12-19 | Initial framework release with 4 sites |

---

**END OF DOCUMENTATION**
