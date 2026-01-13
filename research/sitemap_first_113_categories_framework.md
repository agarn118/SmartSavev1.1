# Sitemap-first method to build and assign **113 labelled categories** (no search bar)

> **Goal:** Build a stable, ethical category labelling system for grocery items (exactly 113 canonical categories) **without** using site search (e.g., searching “milk”, “cheese”, etc.).  
> **Approach:** Use **sitemaps** for discovery + **breadcrumbs/structured data** for labels, then map store-specific paths into your fixed 113-category taxonomy.

---

## 1) The core idea

1. Use **category sitemaps** (if published) to discover category pages.  
2. Use **product sitemaps** to discover product pages.  
3. For each product, extract a **category path** (breadcrumbs / structured data).  
4. Map that store-specific path into your **fixed 113-category taxonomy**.

This avoids search entirely, stays robots-friendly, and is typically more stable over time.

---

## 2) Step-by-step framework

### Step 1 — Create your canonical “113 categories” list first
Make a table for your *fixed* taxonomy:

- `canon_id` (1–113)
- `canon_name` (e.g., “Dairy & Eggs”)
- `canon_parent` (optional hierarchy)
- `keywords_or_slugs` (optional helper field)
- `notes`

**Rule:** Never auto-create new canonical categories during crawling. You only map store categories into the 113 bins you defined.

---

### Step 2 — Discover category pages from sitemaps

#### Best case: the site publishes category sitemaps
Some sites (e.g., Walmart.ca) publish category sitemaps directly in robots.txt (e.g., `sitemap-categories.xml`).  
Process:
1. Fetch category sitemap(s)
2. Parse XML → extract `<loc>` URLs
3. Add to a `category_urls` inventory

**Output:** store category URL list.

#### If the store publishes a single “mixed” sitemap
Many retailers publish one sitemap containing product pages + category pages + content pages.

Process:
1. Fetch sitemap (or sitemap-index)
2. Parse URLs
3. Classify each URL into:
   - **Category-like** pages (aisles/departments/categories)
   - **Product** pages (product detail pages)
   - **Non-catalog** pages (help/blog/account/etc.) → ignore

**Output:** two URL inventories:
- category URLs
- product URLs

> You do **not** need to crawl category pages deeply. You just need them to define/validate your label set.

---

### Step 3 — Convert category URLs into clean labels
For each category URL (polite crawling):
Extract:
- `category_name` (page title or H1)
- `breadcrumb_path` (parent → child → subchild)
- canonical URL if present

Store:
- `store`
- `category_url`
- `category_name`
- `breadcrumb_path` (e.g., `Grocery > Dairy & Eggs > Milk`)
- `category_depth`

Normalize strings:
- lowercase (for matching)
- strip punctuation
- standardize `&` vs `and`
- trim whitespace

---

### Step 4 — Deduplicate into exactly **113 bins**
Stores often have **many more** than 113 categories (micro-subcategories, seasonal, etc.).  
To produce exactly 113 labelled categories, you need a mapping layer.

1. Decide the “category depth” your canonical taxonomy represents  
   - Example: map store categories to a consistent level (often level 2–3), not every micro-subcategory.

2. Build and maintain a mapping table:
- `store_category_path` → `canon_id`

This mapping table is the **heart** of “113 categories”.

**Rule:** Every store category path must map to one of the 113 canonical IDs (or be flagged as unmapped for review).

---

### Step 5 — Label products using product sitemaps (not search)
Now label items via product URLs found in product sitemaps.

For each product page:
1. Extract the product’s **breadcrumb/category path** (best signal)
2. If available, extract structured metadata (JSON-LD / embedded JSON)
3. Assign label:
   - lookup `breadcrumb_path` in your mapping table → `canon_id`

If you encounter a breadcrumb path that isn’t mapped:
- set `canon_id = NULL`
- record it as `unmapped_category`
- add it to a review queue
- update the mapping table to map it into one of the 113 bins

---

### Step 6 — QA checks (so it’s ethical **and** successful)

#### Coverage
- `% products labelled` (target high; unmapped should trend down)
- `# products per canon_id` (watch for categories with 0 items)

#### Stability
- same product consistently maps to the same `canon_id` across runs
- diffs in category paths are logged for review

#### Ethics
- you never request disallowed routes (search/cart/login/account/rewards)
- you rate limit and cache
- if blocked, you back off and stop rather than escalating tactics

---

## 3) Practical example (no search)
1. Parse sitemap → discover product URLs
2. For a product page, breadcrumb is: `Grocery > Dairy & Eggs > Milk`
3. Mapping table maps `Dairy & Eggs > Milk` → canonical category #12 (example)
4. Store record includes:
   - product id, name, price, url
   - `canon_id = 12`

---

## 4) If a store doesn’t expose category pages cleanly
You can still build categories **from product breadcrumbs alone**:

- Product sitemap → product pages → extract breadcrumbs → aggregate unique paths
- Map those unique paths into your 113 bins

This can be more robust than relying on category page URLs.

---

## 5) Recommended data tables (minimal)

### A) canonical_categories (your 113 taxonomy)
- `canon_id`
- `canon_name`
- `canon_parent` (optional)

### B) store_category_paths (discovered)
- `store`
- `breadcrumb_path_raw`
- `breadcrumb_path_norm`
- `example_url`

### C) category_mapping (manual/curated)
- `store`
- `breadcrumb_path_norm`
- `canon_id`
- `confidence` (optional)

### D) products (scraped)
- `store`
- `store_product_id`
- `name`
- `price`
- `breadcrumb_path_norm`
- `canon_id`
- `url`
- `scraped_at`

---

## 6) Operational checklist
- [ ] Refresh sitemap inventory
- [ ] Extract product URLs (and category URLs if available)
- [ ] Crawl product pages politely (low concurrency + delays + caching)
- [ ] Extract breadcrumbs / structured data
- [ ] Assign `canon_id` via mapping table
- [ ] Log and review unmapped paths
- [ ] Monitor label coverage and stability

---
