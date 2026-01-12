# Quick Fix Reference - Real Canadian Superstore Scraper

**Problem:** Scraper was getting 0 products
**Solution:** Updated to handle new website structure (December 2025 changes)
**Status:** FIXED - Now extracting 54+ products per page

---

## What Changed

Real Canadian Superstore reorganized their website's internal data structure. The product information moved from:

```
OLD: props.pageProps.initialSearchData.products
NEW: props.pageProps.initialSearchData.layout.sections.mainContentCollection.components[i].data.productTiles
```

---

## Quick Test

**Verify the fix is working:**

```bash
cd C:\Users\ashto\Desktop\First_claude\test_claude
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

**Expected output:**
```
INFO - Found 15 products in component 0
INFO - Found 39 products in component 2
INFO - Batch saved 54 records
INFO - Scraped 54 products from search results
```

---

## Windows PowerShell Users

If you see "No module named 'scrapers'", make sure you're in the right directory:

```powershell
# Check where you are
cd

# Should show: C:\Users\ashto\Desktop\First_claude\test_claude
# NOT: C:\Users\ashto\Desktop\First_claude

# Navigate to correct directory
cd C:\Users\ashto\Desktop\First_claude\test_claude

# Run scraper using module syntax
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

---

## Troubleshooting

### Still Getting 0 Products?

1. **Enable debug logging:**
   ```bash
   python -m scrapers.run --site realcanadiansuperstore --query "test" --max-pages 1 --log-level DEBUG
   ```

2. **Look for these messages:**
   - "Successfully parsed __NEXT_DATA__ JSON" = Good
   - "Found X products in component Y" = Good
   - "No products found in any known __NEXT_DATA__ path" = Site changed again

3. **Run the diagnostic script:**
   ```bash
   python debug_next_data.py
   ```
   This shows you the current website structure.

### Site Changed Again?

If Real Canadian Superstore changes their structure again:

1. Run `python debug_next_data.py` to see new structure
2. Update the JSON path in `scrapers/sites/realcanadiansuperstore.py`
3. Update field mappings if product field names changed
4. Test with debug logging

---

## Files Modified

| File | What Changed |
|------|--------------|
| `scrapers/sites/realcanadiansuperstore.py` | Updated extraction logic for new JSON structure, added debug logging |
| `instructions.md` | Added Windows PowerShell section and enhanced troubleshooting |
| `debug_next_data.py` | NEW - Diagnostic tool for examining website structure |
| `FIX_SUMMARY.md` | Complete technical documentation of the fix |

---

## Useful Commands

**Test scraper:**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1
```

**Debug mode (see detailed logs):**
```bash
python -m scrapers.run --site realcanadiansuperstore --query "milk" --max-pages 1 --log-level DEBUG
```

**Check website structure:**
```bash
python debug_next_data.py
```

**View scraped data:**
```bash
# Windows PowerShell
Get-Content data\raw\realcanadiansuperstore\realcanadiansuperstore_products.jsonl | Select-Object -First 1

# Windows CMD or Mac/Linux
head -1 data/raw/realcanadiansuperstore/realcanadiansuperstore_products.jsonl
```

---

## Sample Output

The scraper now correctly extracts:

```json
{
  "name": "2% Milk",
  "brand": "Neilson",
  "price": 6.25,
  "unit_price": 0.16,
  "unit_price_uom": "100ml",
  "size_text": "4 l, $0.16/100ml",
  "availability": "in_stock",
  "image_url": "https://digital.loblaws.ca/...",
  "external_id": "20188873_EA"
}
```

All fields are populated correctly including prices, unit prices, brands, and images.

---

**Last Updated:** December 20, 2025
**Next Check:** If you see "No products found" error again, site likely changed. Run debug script.
