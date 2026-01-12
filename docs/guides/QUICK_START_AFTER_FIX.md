# Quick Start Guide - After Bug Fixes

All test issues have been resolved! Follow these steps to get started.

---

## What Was Fixed

1. **UnboundLocalError bug** in `scrapers/run.py` - Fixed
2. **Missing Playwright dependency** - Installation tools created
3. **Missing documentation** - Pre-flight check added to README

---

## Step-by-Step Setup

### Step 1: Check Dependencies

Run the dependency checker to see what's missing:

```bash
python check_dependencies.py
```

You'll see a detailed report of installed vs. missing packages.

---

### Step 2: Install Missing Dependencies

**Option A: Automated (Recommended)**

```powershell
.\install_dependencies.ps1
```

This will:
- Install all Python packages
- Install Playwright Chromium browser
- Verify everything is working

**Option B: Manual**

```bash
# Install Python packages
pip install -r requirements.txt

# Install Playwright browser (for Safeway/Sobeys)
playwright install chromium
```

---

### Step 3: Verify Installation

```bash
python check_dependencies.py
```

You should see all green checkmarks (OK).

---

### Step 4: Run Your First Scraper

**Test with Real Canadian Superstore (easiest, no browser needed):**

```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

**Test with Safeway (requires Playwright):**

```bash
python -m scrapers.run --site safeway --query "bread" --max-pages 1
```

---

## Files Created

1. **`check_dependencies.py`** - Validates all required packages
2. **`install_dependencies.ps1`** - Automated installer for Windows
3. **`TEST_RESULTS.md`** - Detailed test report with bug analysis
4. **`SCRAPERS_README.md`** - Updated with pre-flight check section

---

## Files Modified

1. **`scrapers/run.py`** - Fixed UnboundLocalError (line 32)

---

## Scraper Status

| Scraper | Status | Requirements |
|---------|--------|--------------|
| realcanadiansuperstore | ✅ Working | Core packages only |
| nofrills | ✅ Working | Core packages only |
| safeway | ✅ Fixed (install Playwright) | Core + Playwright |
| sobeys | ✅ Fixed (install Playwright) | Core + Playwright |

---

## Common Commands

**Check dependencies:**
```bash
python check_dependencies.py
```

**Install everything:**
```powershell
.\install_dependencies.ps1
```

**Run a scraper:**
```bash
python -m scrapers.run --site <site_name> --query "search_term" --max-pages 3
```

**Available sites:**
- `realcanadiansuperstore`
- `nofrills`
- `safeway`
- `sobeys`

---

## Next Steps

1. Run the dependency checker
2. Install missing dependencies (if any)
3. Re-run your test script - all 4 scrapers should work now!

---

For detailed information, see:
- **TEST_RESULTS.md** - Full test report
- **SCRAPERS_README.md** - Complete usage guide
