# Grocery Scrapers - Quick Reference Card

## Setup & Validation

```bash
# Install dependencies
pip install -r requirements.txt
playwright install chromium

# Validate setup
python validate_setup.py

# Test all scrapers
.\test_scrapers.ps1
```

---

## Basic Commands

### Search for Products
```bash
python -m scrapers.run --site SITE_NAME --query "SEARCH_TERM" --max-pages N
```

### Scrape Category
```bash
python -m scrapers.run --site SITE_NAME --category-url "/path/to/category" --max-pages N
```

### Single Product (Testing)
```bash
python -m scrapers.run --site SITE_NAME --product-url "/path/to/product"
```

---

## Site Names

- `realcanadiansuperstore` - Real Canadian Superstore (fast, no browser)
- `nofrills` - No Frills (fast, no browser)
- `safeway` - Safeway (slower, requires Playwright)
- `sobeys` - Sobeys (slower, requires Playwright)

---

## Common Options

| Option | Description | Example |
|--------|-------------|---------|
| `--max-pages N` | Limit pages to scrape | `--max-pages 5` |
| `--output-format csv` | Export to CSV | `--output-format csv` |
| `--output-format both` | JSONL + CSV | `--output-format both` |
| `--headful` | Show browser (debug) | `--headful` |
| `--log-level DEBUG` | Verbose logging | `--log-level DEBUG` |
| `--resume` | Continue from checkpoint | `--resume` |
| `--clear-checkpoint` | Start fresh | `--clear-checkpoint` |

---

## Example Commands

### Test Each Scraper (1 page)
```bash
# Superstore
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1

# No Frills
python -m scrapers.run --site nofrills --query "bread" --max-pages 1

# Safeway
python -m scrapers.run --site safeway --query "eggs" --max-pages 1

# Sobeys
python -m scrapers.run --site sobeys --query "cheese" --max-pages 1
```

### Real-World Usage
```bash
# Get all dairy products from Superstore (with CSV export)
python -m scrapers.run --site realcanadiansuperstore --category-url "/en/food/dairy-and-eggs/c/28000" --output-format both

# Search for organic products at Safeway (5 pages max)
python -m scrapers.run --site safeway --query "organic" --max-pages 5 --output-format csv

# Debug a scraping issue (visible browser, verbose logs)
python -m scrapers.run --site sobeys --query "test" --max-pages 1 --headful --log-level DEBUG

# Resume interrupted scrape
python -m scrapers.run --site nofrills --query "snacks" --max-pages 20 --resume
```

---

## Output Files

### Data
```
data/raw/SITE_NAME/SITE_NAME_products.jsonl  (raw data)
data/raw/SITE_NAME/SITE_NAME_products.csv    (spreadsheet, if requested)
```

### Logs
```
data/logs/SITE_NAME.log  (detailed scraping log)
```

### Checkpoints
```
data/checkpoints/SITE_NAME_checkpoint.json  (progress tracking)
```

---

## Troubleshooting

### Import Error
```bash
# Verify setup
python validate_setup.py

# Check directory
pwd  # Should be: .../test_claude

# Use either invocation method
python -m scrapers.run --help       # Recommended
python scrapers/run.py --help        # Also works
```

### No Products Found
```bash
# Try search instead of category
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1

# Use debug mode
python -m scrapers.run --site SITE --query "test" --max-pages 1 --headful --log-level DEBUG

# Check the log file
cat data/logs/SITE_NAME.log
```

### Playwright Not Found
```bash
playwright install chromium
```

---

## File Structure

```
test_claude/
├── scrapers/
│   ├── run.py              # Main entry point
│   ├── base.py             # Base scraper class
│   ├── common.py           # Shared utilities
│   └── sites/              # Site-specific scrapers
│       ├── realcanadiansuperstore.py
│       ├── nofrills.py
│       ├── safeway.py
│       └── sobeys.py
├── configs/                # Site configurations
├── data/
│   ├── raw/                # Output data
│   ├── logs/               # Scraping logs
│   └── checkpoints/        # Progress tracking
├── validate_setup.py       # Environment validation
├── test_scrapers.ps1       # Automated test script
└── instructions.md         # Full documentation
```

---

## Quick Tips

1. **Always start with `--max-pages 1`** when testing
2. **Use `--output-format csv`** for easy viewing in Excel
3. **Check logs** if something goes wrong: `data/logs/SITE_NAME.log`
4. **Use `--resume`** for long scrapes that might get interrupted
5. **Scrape during off-peak hours** (late night/early morning)
6. **Be patient** - Safeway/Sobeys are intentionally slow to avoid detection

---

## Getting Help

```bash
# Show all options
python -m scrapers.run --help

# Validate your setup
python validate_setup.py

# Test all scrapers
.\test_scrapers.ps1

# Read full documentation
# See: instructions.md
```

---

## One-Liners for Copy-Paste

```bash
# Quick test - Superstore
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1

# Quick test - Safeway
python -m scrapers.run --site safeway --query "bread" --max-pages 1

# Full scrape with CSV export
python -m scrapers.run --site nofrills --category-url "/en/food/dairy/c/28000" --max-pages 10 --output-format csv

# Debug mode (see what's happening)
python -m scrapers.run --site sobeys --query "test" --max-pages 1 --headful --log-level DEBUG
```

---

**Remember:** Use `python -m scrapers.run` (recommended) or `python scrapers/run.py` (also works)
