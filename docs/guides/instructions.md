# How to Run the Grocery Scrapers - Step-by-Step Guide

This guide will walk you through everything you need to know to run the grocery scrapers, from installation to interpreting results. No prior experience required!

---

## Table of Contents

1. [What This Tool Does](#what-this-tool-does)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Quick Start - Your First Scrape](#quick-start---your-first-scrape)
5. [Running Each Scraper](#running-each-scraper)
6. [Understanding Command-Line Options](#understanding-command-line-options)
7. [Common Use Cases with Examples](#common-use-cases-with-examples)
8. [Understanding Output Files](#understanding-output-files)
9. [Troubleshooting](#troubleshooting)
10. [Tips and Best Practices](#tips-and-best-practices)

---

## What This Tool Does

This tool automatically collects product information (names, prices, brands, sizes) from Canadian grocery websites and saves them to files on your computer. Think of it as a robot that visits grocery websites and copies down product details for you.

**Supported Stores:**
- Real Canadian Superstore
- No Frills
- Safeway
- Sobeys

**What You Get:**
- Product names, prices, and descriptions
- Brand information
- Package sizes
- Stock availability
- Product images
- All saved in easy-to-read files (JSONL and optionally CSV)

---

## Prerequisites

Before you start, you need:

### 1. Python Installed

**Check if you have Python:**
Open Command Prompt (Windows) or Terminal (Mac/Linux) and type:
```bash
python --version
```

You should see something like `Python 3.8.10` or higher. If you see an error, download Python from [python.org](https://www.python.org/downloads/).

### 2. Basic Command Line Knowledge

You'll need to know how to:
- Open Command Prompt (Windows) or Terminal (Mac/Linux)
- Navigate to a folder using `cd` command
- Copy and paste commands

Don't worry - we'll provide all the exact commands you need!

### 3. Windows Users: PowerShell vs Command Prompt

If you're on Windows, you can use either:
- **Command Prompt (CMD)** - Classic Windows terminal
- **PowerShell** - Modern Windows terminal (recommended)

Both work the same for these commands. If you see "bash" in code examples, those commands work in PowerShell, CMD, and bash terminals.

---

## Installation

### Step 1: Navigate to the Project Folder

Open Command Prompt or PowerShell and navigate to the project directory:

```bash
cd C:\Users\ashto\Desktop\First_claude\test_claude
```

**IMPORTANT:** Make sure you navigate to the `test_claude` directory, NOT `First_claude`. The scraper won't work unless you're in the correct directory.

**Windows Quick Tip:** You can also navigate to this folder in File Explorer, then:
- Type `cmd` in the address bar and press Enter (opens Command Prompt)
- Or type `powershell` in the address bar and press Enter (opens PowerShell)

### Step 2: Install Required Software

Copy and paste this command to install the basic dependencies:

```bash
pip install -r requirements.txt
```

This will install:
- `requests` - for downloading web pages
- `beautifulsoup4` - for reading HTML
- `lxml` - for parsing data
- `playwright` - for sites that need a browser

### Step 3: Install Playwright Browser (For Safeway and Sobeys Only)

Safeway and Sobeys require a browser to work. Run these two commands:

```bash
pip install playwright
playwright install chromium
```

This downloads a lightweight browser that the scraper controls automatically.

### Step 4: Verify Installation

Check that everything is working:

```bash
python -m scrapers.run --help
```

Or if that doesn't work (especially on Windows):

```bash
python -m scrapers.run --help
```

If you see a help message with usage instructions, you're ready to go! If you see an error, see the [Troubleshooting](#troubleshooting) section.

**Note:** Both commands do the same thing. Use whichever works on your system.

---

## Quick Start - Your First Scrape

Let's do a simple test to make sure everything works. We'll search for "milk" at Real Canadian Superstore and get the first page of results.

**Copy and paste this command:**

```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

**What's happening:**
1. The scraper connects to realcanadiansuperstore.ca
2. Searches for "milk"
3. Collects product information from the first page
4. Saves results to a file

**Where to find your results:**

```
C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\realcanadiansuperstore\realcanadiansuperstore_products.jsonl
```

You should see product data saved! We'll explain how to read this file in the [Understanding Output Files](#understanding-output-files) section.

---

## Running Each Scraper

Each grocery store scraper works the same way, but here's specific information for each one.

### Real Canadian Superstore

**Difficulty:** Easy (Fastest, no browser needed)

**Example 1: Search for products**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "bread" --max-pages 3
```

**Example 2: Scrape a specific category**
```bash
python -m scrapers.run --site realcanadiansuperstore --category-url "/en/food/dairy-and-eggs/milk-and-cream/milk/c/28000" --max-pages 5
```

**Example 3: Get a single product (for testing)**
```bash
python -m scrapers.run --site realcanadiansuperstore --product-url "/en/2-milk/p/20188873_EA"
```

**Output location:**
```
C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\realcanadiansuperstore\
```

---

### No Frills

**Difficulty:** Easy (Same as Superstore, no browser needed)

**Example 1: Search for products**
```bash
python -m scrapers.run --site nofrills --query "organic milk" --max-pages 2
```

**Example 2: Scrape a category**
```bash
python -m scrapers.run --site nofrills --category-url "/en/food/bakery/c/27990" --max-pages 5
```

**Output location:**
```
C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\nofrills\
```

**Note:** No Frills uses the same website system as Real Canadian Superstore, so products will have the same IDs but different prices.

---

### Safeway

**Difficulty:** Moderate (Requires browser, slower but still reliable)

**Important:** You must have run `playwright install chromium` first (see [Installation](#installation)).

**Example 1: Search for products**
```bash
python -m scrapers.run --site safeway --query "cheese" --max-pages 2
```

**Example 2: Scrape a category**
```bash
python -m scrapers.run --site safeway --category-url "/shop/categories/dairy-eggs.24.html" --max-pages 3
```

**Example 3: Watch the browser while scraping (for debugging)**
```bash
python -m scrapers.run --site safeway --query "bread" --max-pages 1 --headful
```

**Output location:**
```
C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\safeway\
```

**Note:** Safeway scraping is slower because it needs to render JavaScript. Be patient!

---

### Sobeys

**Difficulty:** Moderate (Same as Safeway)

**Important:** You must have run `playwright install chromium` first.

**Example 1: Search for products**
```bash
python -m scrapers.run --site sobeys --query "yogurt" --max-pages 3
```

**Example 2: Scrape a category**
```bash
python -m scrapers.run --site sobeys --category-url "/products/dairy-eggs" --max-pages 5
```

**Output location:**
```
C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\sobeys\
```

---

## Understanding Command-Line Options

Every command follows this pattern:

```bash
python -m scrapers.run --site <STORE_NAME> [MODE] [OPTIONS]
```

### Required Arguments

**`--site <store_name>`** (REQUIRED)
- Which grocery store to scrape
- Options: `realcanadiansuperstore`, `nofrills`, `safeway`, `sobeys`
- Example: `--site realcanadiansuperstore`

### Scraping Modes (Choose ONE)

**`--query "search term"`**
- Search for products by keyword
- Example: `--query "organic milk"`
- Example: `--query "gluten free bread"`

**`--category-url "/path/to/category"`**
- Scrape a specific category page
- Get the URL path from the website
- Example: `--category-url "/en/food/dairy-and-eggs/c/28000"`

**`--product-url "/path/to/product"`**
- Scrape just one product (useful for testing)
- Example: `--product-url "/en/2-milk/p/20188873_EA"`

### Optional Settings

**`--max-pages N`**
- Limit how many pages to scrape
- Default: scrapes ALL pages (can take a long time!)
- Example: `--max-pages 5`
- **Recommendation:** Always use this when testing!

**`--output-format {jsonl,csv,both}`**
- Choose output file format
- `jsonl`: Technical format (default, fastest)
- `csv`: Spreadsheet format (easy to open in Excel)
- `both`: Save in both formats
- Example: `--output-format both`

**`--headless` / `--headful`**
- Controls browser visibility (Safeway/Sobeys only)
- `--headless`: Browser runs in background (default, faster)
- `--headful`: See the browser window (useful for debugging)
- Example: `--headful`

**`--resume`**
- Continue from where you left off if scraping was interrupted
- Example: `--resume`

**`--clear-checkpoint`**
- Start fresh, ignore previous progress
- Example: `--clear-checkpoint`

**`--log-level {DEBUG,INFO,WARNING,ERROR}`**
- How much detail to show while running
- `INFO`: Normal (default)
- `DEBUG`: Show everything (useful when troubleshooting)
- `WARNING`: Only show problems
- Example: `--log-level DEBUG`

---

## Common Use Cases with Examples

### Use Case 1: Compare Milk Prices Across Stores

**Goal:** Get milk prices from all stores to compare.

**Commands:**
```bash
# Scrape milk from Real Canadian Superstore
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 3 --output-format csv

# Scrape milk from No Frills
python -m scrapers.run --site nofrills --query "milk" --max-pages 3 --output-format csv

# Scrape milk from Safeway
python -m scrapers.run --site safeway --query "milk" --max-pages 3 --output-format csv

# Scrape milk from Sobeys
python -m scrapers.run --site sobeys --query "milk" --max-pages 3 --output-format csv
```

**Results:** You'll have 4 CSV files (one per store) that you can open in Excel and compare.

---

### Use Case 2: Get All Products in the Dairy Category

**Goal:** Scrape the entire dairy section from Real Canadian Superstore.

**Command:**
```bash
python -m scrapers.run --site realcanadiansuperstore --category-url "/en/food/dairy-and-eggs/c/28000" --output-format both
```

**Note:** No `--max-pages` means it will scrape ALL pages. This could take a while!

**Results:**
- `realcanadiansuperstore_products.jsonl` - Technical format
- `realcanadiansuperstore_products.csv` - Spreadsheet format

---

### Use Case 3: Test a Single Product Before Large Scrape

**Goal:** Make sure the scraper works before running a big job.

**Command:**
```bash
python -m scrapers.run --site safeway --product-url "/products/product/milk/p/20188875001"
```

**Results:** If you see product data saved, the scraper is working correctly!

---

### Use Case 4: Resume an Interrupted Scrape

**Goal:** Your computer crashed or you stopped the scraper. Pick up where you left off.

**Scenario:** You started this command but it was interrupted:
```bash
python -m scrapers.run --site realcanadiansuperstore --query "bread" --max-pages 20
```

**Resume command:**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "bread" --max-pages 20 --resume
```

**What happens:** The scraper remembers which products it already saved and skips duplicates.

---

### Use Case 5: Debug When Things Go Wrong

**Goal:** See detailed information about what's happening.

**Command:**
```bash
python -m scrapers.run --site sobeys --query "cheese" --max-pages 1 --headful --log-level DEBUG
```

**What you'll see:**
- Browser window opening (with `--headful`)
- Detailed logs of every action (with `--log-level DEBUG`)

---

### Use Case 6: Export Existing Data to CSV

**Goal:** You already scraped data in JSONL format, now you want a spreadsheet.

**Command:**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "bread" --max-pages 1 --output-format csv
```

**Note:** This will convert your existing JSONL file to CSV. It won't re-scrape if you use `--resume`.

---

## Understanding Output Files

After running a scraper, you'll find several files created in the `data` folder.

### File Structure

```
C:\Users\ashto\Desktop\First_claude\test_claude\data\
├── raw\
│   ├── realcanadiansuperstore\
│   │   ├── realcanadiansuperstore_products.jsonl
│   │   └── realcanadiansuperstore_products.csv
│   ├── nofrills\
│   │   ├── nofrills_products.jsonl
│   │   └── nofrills_products.csv
│   ├── safeway\
│   │   └── safeway_products.jsonl
│   └── sobeys\
│       └── sobeys_products.jsonl
├── logs\
│   ├── realcanadiansuperstore.log
│   ├── nofrills.log
│   ├── safeway.log
│   └── sobeys.log
└── checkpoints\
    ├── realcanadiansuperstore_checkpoint.json
    ├── nofrills_checkpoint.json
    ├── safeway_checkpoint.json
    └── sobeys_checkpoint.json
```

### Output File Types

#### 1. JSONL Files (`*_products.jsonl`)

**What is it:** Text file with one product per line in JSON format.

**How to view:**
- Open in any text editor (Notepad, VS Code, etc.)
- Each line is a separate product

**Example line:**
```json
{"store": "Real Canadian Superstore", "site_slug": "realcanadiansuperstore", "source_url": "https://www.realcanadiansuperstore.ca/en/2-milk/p/20188873_EA", "scrape_ts": "2025-12-20T10:30:00Z", "external_id": "20188873_EA", "name": "2% Milk", "brand": "Neilson", "size_text": "4 L", "price": 6.25, "currency": "CAD", "unit_price": 1.56, "unit_price_uom": "per L", "image_url": "https://...", "category_path": "Food > Dairy & Eggs", "availability": "in_stock", "raw_source": {...}}
```

**When to use:**
- For technical analysis
- For importing into databases
- Best for large datasets

#### 2. CSV Files (`*_products.csv`)

**What is it:** Spreadsheet file you can open in Excel or Google Sheets.

**How to view:**
- Double-click the file (opens in Excel)
- Or: Right-click > Open With > Excel/Google Sheets

**Columns include:**
- `store` - Store name
- `name` - Product name
- `brand` - Brand name
- `size_text` - Package size (e.g., "4 L")
- `price` - Price in dollars
- `currency` - CAD
- `unit_price` - Price per unit (e.g., per liter)
- `unit_price_uom` - Unit of measure
- `availability` - in_stock, out_of_stock, or unknown
- `source_url` - Product page URL
- `scrape_ts` - When this was scraped
- Plus more...

**When to use:**
- For easy viewing and sorting
- For sharing with non-technical people
- For creating charts and reports

#### 3. Log Files (`*.log`)

**What is it:** Record of everything the scraper did.

**Location:** `data/logs/<site_name>.log`

**How to view:** Open in text editor

**What you'll see:**
```
2025-12-20 10:30:00 - INFO - Starting scraper for: realcanadiansuperstore
2025-12-20 10:30:01 - INFO - Search mode: query='milk', max_pages=3
2025-12-20 10:30:05 - DEBUG - Fetched: https://www.realcanadiansuperstore.ca/search?search-bar=milk
2025-12-20 10:30:06 - INFO - Found 48 products on page 1
2025-12-20 10:30:10 - INFO - Scraped 48 products from search results
```

**When to use:**
- When troubleshooting errors
- To see exactly what happened during scraping

#### 4. Checkpoint Files (`*_checkpoint.json`)

**What is it:** Saves progress so you can resume later.

**Location:** `data/checkpoints/<site_name>_checkpoint.json`

**Contains:**
- List of product IDs already scraped
- Statistics (how many products, errors, etc.)
- Last page number

**When to use:**
- Automatically used with `--resume` flag
- Delete with `--clear-checkpoint` to start fresh

---

## Troubleshooting

### Problem 1: "No module named 'scrapers'"

**Error message:**
```
ModuleNotFoundError: No module named 'scrapers'
```

**This error should not occur anymore** - the script has been updated to handle path issues automatically. However, if you still see it:

**Solution 1:** Make sure you're in the correct directory.

```bash
# Check current directory (Windows PowerShell/CMD)
cd

# Check current directory (Mac/Linux)
pwd

# Should show: C:\Users\ashto\Desktop\First_claude\test_claude
# NOT: C:\Users\ashto\Desktop\First_claude

# If not in test_claude directory, navigate there
cd C:\Users\ashto\Desktop\First_claude\test_claude
```

**Solution 2:** Both invocation methods now work:

```bash
# Method 1: Module syntax (recommended)
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1

# Method 2: Direct script (also works)
python scrapers/run.py --site realcanadiansuperstore --query "milk" --max-pages 1
```

**Solution 3:** Run the validation script to diagnose issues:

```bash
python validate_setup.py
```

---

### Problem 2: "Config file not found"

**Error message:**
```
Config file not found: C:\Users\ashto\Desktop\First_claude\test_claude\configs\<site>.json
```

**Possible causes:**
- Typo in site name
- Missing config file

**Solution:**
```bash
# List available configs
dir configs   # Windows
ls configs    # Mac/Linux

# Use exact site name
python -m scrapers.run --site realcanadiansuperstore --query "milk"
```

**Valid site names:**
- `realcanadiansuperstore` (NOT superstore, NOT real-canadian-superstore)
- `nofrills` (NOT no-frills)
- `safeway`
- `sobeys`

---

### Problem 3: "Browser not found" (Safeway/Sobeys)

**Error message:**
```
Playwright browser not found
```

**Solution:** Install the Playwright browser.

```bash
pip install playwright
playwright install chromium
```

---

### Problem 4: "No products found"

**Error in logs:**
```
WARNING - No products found on page 1
INFO - Scraped 0 products from search results
```

**Possible causes:**
- Site changed their HTML structure (most common for Real Canadian Superstore)
- Wrong URL format
- Category doesn't exist
- Network or rate-limiting issue

**Solution 1: For Real Canadian Superstore - Update Already Applied**

As of December 2025, Real Canadian Superstore changed their website structure. The scraper has been updated to handle the new format. If you're still seeing this issue:

1. Make sure you have the latest version of the scraper code
2. Try running with debug logging to see what's happening:

```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1 --log-level DEBUG
```

3. Look for these log messages:
   - "Successfully parsed __NEXT_DATA__ JSON" - Good, found the data script
   - "Found X products in component Y" - Good, extraction is working
   - "No products found in any known __NEXT_DATA__ path" - Bad, structure changed again

**Solution 2: Try a search instead of category URL**
```bash
# Instead of category URL, try search
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

**Solution 3: Use debug mode (for Safeway/Sobeys)**
```bash
python -m scrapers.run --site safeway --query "bread" --max-pages 1 --headful --log-level DEBUG
```

This shows you exactly what's happening by opening the browser window and showing detailed logs.

**Solution 4: Check the debug script**

We include a debug script to examine the website structure:

```bash
python debug_next_data.py
```

This will show you the current JSON structure from Real Canadian Superstore and help diagnose extraction issues.

---

### Problem 5: Scraper is Very Slow

**Explanation:** This is normal! The scrapers intentionally wait 3-5 seconds between requests to avoid overwhelming the websites.

**Speed by site:**
- Real Canadian Superstore: FAST (20-40 products per minute)
- No Frills: FAST (20-40 products per minute)
- Safeway: SLOW (10-20 products per minute)
- Sobeys: SLOW (10-20 products per minute)

**Tips:**
- Use `--max-pages` to limit scraping
- Be patient - large scrapes can take hours
- Use `--resume` if you need to stop and restart

---

### Problem 6: "Rate limit exceeded" or "429 Error"

**Error message:**
```
Failed to fetch: 429 Too Many Requests
```

**Explanation:** You're scraping too fast and the website blocked you temporarily.

**Solution:** The scraper should handle this automatically. If it keeps happening:

1. Check the config file to increase delays
2. Wait 10-15 minutes before trying again
3. Use `--max-pages 1` to test first

---

### Problem 7: Permission Denied on Output Files

**Error message:**
```
Permission denied: C:\Users\ashto\Desktop\First_claude\test_claude\data\raw\...
```

**Cause:** File is open in Excel or another program.

**Solution:** Close Excel and any programs viewing the output files, then try again.

---

## Tips and Best Practices

### Tip 1: Always Start Small

Before doing a big scrape, test with a small number of pages:

```bash
# Good: Test with 1 page first
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1

# Then expand
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 10
```

### Tip 2: Use CSV Format for Easy Viewing

If you're not technical, always use `--output-format csv`:

```bash
python -m scrapers.run --site nofrills --query "bread" --max-pages 5 --output-format csv
```

Then you can open the CSV in Excel and sort/filter easily.

### Tip 3: Monitor the Log File

Open the log file while the scraper runs to see progress:

**Location:** `C:\Users\ashto\Desktop\First_claude\test_claude\data\logs\<site_name>.log`

You'll see real-time updates of what's being scraped.

### Tip 4: Use Resume for Large Scrapes

For scrapes that take hours, use checkpoints:

```bash
# Start scraping
python -m scrapers.run --site realcanadiansuperstore --category-url "/en/food/c/27985" --resume

# If interrupted (Ctrl+C or crash), just run the same command again
# The --resume flag makes it continue where it stopped
```

### Tip 5: Scrape During Off-Peak Hours

To be respectful to the websites:
- Scrape late at night or early morning
- Avoid 9am-9pm on weekdays
- Never scrape during Black Friday or major sales

### Tip 6: Compare Prices Across Stores

Save each store's data with different commands, then compare the CSV files in Excel:

1. Scrape all stores for the same product
2. Open all CSV files in Excel
3. Use Excel's comparison features to find best prices

### Tip 7: Set Realistic Max Pages

**Estimation:**
- 1 page = ~48 products
- 10 pages = ~480 products
- 100 pages = ~4,800 products

Start with 5-10 pages and increase from there.

### Tip 8: Keep Your Output Organized

The scraper automatically organizes files by site, but you can manually back them up:

```bash
# Example: Backup your data
# Create a folder like "data_backup_2025_12_20"
# Copy the entire data/raw folder there
```

---

## What Each Field Means

When you look at the CSV or JSONL files, here's what each column means:

| Field | Description | Example |
|-------|-------------|---------|
| `store` | Full store name | "Real Canadian Superstore" |
| `site_slug` | Internal site identifier | "realcanadiansuperstore" |
| `source_url` | Product page URL | "https://www.realcanadiansuperstore.ca/en/2-milk/p/20188873_EA" |
| `scrape_ts` | When this was scraped | "2025-12-20T10:30:00Z" |
| `external_id` | Store's product ID | "20188873_EA" |
| `name` | Product name | "2% Milk" |
| `brand` | Brand name | "Neilson" |
| `size_text` | Package size/description | "4 L" |
| `price` | Price (in dollars) | 6.25 |
| `currency` | Currency code | "CAD" |
| `unit_price` | Price per unit | 1.56 |
| `unit_price_uom` | Unit of measure | "per L" |
| `image_url` | Product image URL | "https://..." |
| `category_path` | Category breadcrumb | "Food > Dairy & Eggs" |
| `availability` | Stock status | "in_stock", "out_of_stock", "unknown" |
| `raw_source` | Technical data (ignore this) | {...} |

---

## Summary: Most Common Commands

Here are the commands you'll use 90% of the time:

**1. Search for a product (with CSV output)**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 5 --output-format csv
```

**2. Scrape a category**
```bash
python -m scrapers.run --site nofrills --category-url "/en/food/dairy-and-eggs/c/28000" --max-pages 10
```

**3. Resume interrupted scrape**
```bash
python -m scrapers.run --site safeway --query "bread" --max-pages 20 --resume
```

**4. Debug when something goes wrong**
```bash
python -m scrapers.run --site sobeys --query "cheese" --max-pages 1 --headful --log-level DEBUG
```

**5. Start completely fresh**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "yogurt" --max-pages 5 --clear-checkpoint
```

---

## Need More Help?

### Check the Logs

Every scrape creates a log file with detailed information:
```
C:\Users\ashto\Desktop\First_claude\test_claude\data\logs\<site_name>.log
```

Open this file in a text editor to see what happened.

### Read the Technical Documentation

For advanced usage, see:
- `SCRAPERS_README.md` - Complete technical guide
- `site_designs.md` - How each site works
- `configs/` folder - Site-specific settings

### Remember

- Start with small scrapes (`--max-pages 1`)
- Use CSV format for easy viewing (`--output-format csv`)
- Check logs when things go wrong
- Be patient - scraping takes time!

---

**Happy scraping!**
