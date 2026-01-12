# Canadian Grocery Website Structure Analysis

**Document Version:** 1.0
**Date:** 2025-12-19
**Purpose:** Technical documentation for understanding the structure and data access patterns of major Canadian grocery e-commerce websites.

---

## LEGAL & ETHICAL CONSIDERATIONS

**CRITICAL NOTICE:** Before implementing any web scraping solution based on this research:

1. **Robots.txt Compliance:** Review current robots.txt files for each site before scraping
2. **Terms of Service:** Examine ToS for explicit prohibitions on automated access
3. **Rate Limiting:** Implement respectful delays (2-5 seconds minimum between requests)
4. **Official APIs:** Check if vendor provides official data access methods
5. **Legal Risk:** Consult legal counsel regarding CFAA, GDPR, PIPEDA compliance
6. **Attribution:** Ensure compliance with any data usage attribution requirements

**Sites with Aggressive Anti-Bot Measures:**
- Walmart.ca (PerimeterX bot detection)
- Save-on-Foods.com (403 blocking on automated requests)
- Co-op online services (403 blocking on automated requests)

---

## 1. WALMART.CA

### Legal & Access Considerations
**Robots.txt Summary:**
- **Disallows:** `/cart`, `/panier`, `/account/*`, search queries with `+` characters, faceted browsing (`?f=*`, `?facet=*`)
- **Allows:** Product detail pages (`/en/ip/*/*`, `/fr/ip/*/*`), categories, brands, collections
- **Crawl Delay:** 1 second for Bingbot
- **Sitemap:** Available at robots.txt

**Anti-Bot Measures:**
- **PerimeterX Bot Detection:** Implements challenge-response CAPTCHA system
- **UUID Verification:** Redirects suspected bots to verification pages with `?uuid=[UUID]` parameters
- **JavaScript Required:** Product data requires JavaScript execution to access

**Risk Assessment:** HIGH - Active bot detection makes scraping difficult without specialized tools (Playwright, Selenium) and risks account/IP blocking.

### Technical Stack
- **Framework:** Vanilla JavaScript with Adobe DTM analytics
- **Data Loading:** Client-side rendering with verification barriers
- **Bot Detection:** PerimeterX (PXnp9B16Cq)
- **Analytics:** Adobe Analytics via `window.walmart.analytics.dataLayer`

### URL Patterns
```
Product Pages (Allowed):
- /en/ip/[product-slug]/[product-id]
- /fr/ip/[product-slug]/[product-id]

Search (Disallowed for bots):
- /search?q=[query]

Categories (Allowed):
- /en/browse/[category]
- /en/browse/[category]/[subcategory]
```

### Data Extraction Notes
- **Challenge:** Bot verification page prevents direct access to product listings
- **Product URLs:** May be obtainable from sitemap.xml if publicly accessible
- **Alternative:** Consider requesting official API access from Walmart Canada

### Recommended Approach
**DO NOT SCRAPE** - Request official API access or use Walmart's affiliate program if available. The aggressive bot protection makes ethical scraping impractical.

---

## 2. SAFEWAY.CA (Sobeys Network)

### Legal & Access Considerations
**Robots.txt Summary:**
- **Disallows:** `/private/`, `/_not-found/`, `/change-password/`, `/maintenance/`, `/my-profile/`, `/verified-prices/`, `/verify-card/`, `/VerifyAccount/`, `/recipes/my-recipe-lists/`, `/shopping-list/`
- **Allows:** Root directory `/`, `/shopping-list` (exact match), `/recipes/my-recipe-lists` (exact match)
- **Crawl Delay:** None specified
- **Sitemap:** https://www.safeway.ca/sitemap.xml

**Anti-Bot Measures:**
- **Store Selection Required:** Pricing and availability require selecting a local store
- **Session Management:** User sessions track store preferences
- **JavaScript Rendering:** Next.js React app requires JS execution

**Risk Assessment:** MEDIUM - No explicit scraping prohibition, but requires careful implementation with store context.

### Technical Stack
- **Framework:** Next.js (React) with Server-Side Rendering
- **Data Loading:** SSR with React Server Components + client hydration
- **Monitoring:** New Relic (Account ID: 708702)
- **Analytics:** Google Analytics 4 (_ga_J8J2BCGMN6), Adobe Experience Manager integration

### URL Patterns
```
Homepage:
- https://www.safeway.ca/

Categories:
- /shop/categories/[category-name].[category-id].html
- Example: /shop/categories/dairy-eggs.24.html

Products:
- /products/product/[product-slug]/p/[product-id]
- Example: /products/product/1-milk-1-l/p/20188875001

Search:
- /shop/search-results.html?q=[query]
- Example: /shop/search-results.html?q=milk
```

### HTML Structure & Selectors

**Product Tiles (Search/Category):**
```css
/* Note: Many selectors are in compiled Next.js bundles */
Product Container: [data-component-type="product-tile"] (if available)
Product Image: Look for Next.js Image component (<img> with srcset)
Product Name: Text content in product link
Price: Look for price-related CSS classes or data-price attributes
```

**Data Extraction Strategy:**
- **Server-Side JSON:** Product data embedded in `self.__next_f` push arrays
- **React Props:** Data serialized in React component props
- **Structured Data:** Check for JSON-LD schema.org markup in <script type="application/ld+json">

### API Endpoints
- **No Public REST/GraphQL Endpoints Identified**
- **Data Delivery:** Server-rendered with embedded JSON payloads
- **Content Source:** Adobe Experience Manager (AEM) backend

### Product Data Schema (Expected)
```json
{
  "product_id": "20188875001",
  "name": "1% Milk 1 L",
  "brand": "Neilson",
  "price": {
    "amount": 6.49,
    "currency": "CAD"
  },
  "unit": "per 1 L",
  "in_stock": true,
  "category": "Dairy & Eggs > Milk & Cream",
  "image_url": "https://...",
  "url": "/products/product/1-milk-1-l/p/20188875001"
}
```

### Scraping Considerations
1. **JavaScript Required:** Use Playwright/Selenium to render React components
2. **Store Selection:** Must set store context (cookie/session) to get accurate pricing
3. **Rate Limiting:** Implement 3-5 second delays between requests
4. **Session Rotation:** Rotate sessions to avoid rate limiting
5. **Monitoring:** New Relic may track unusual traffic patterns

### Recommended Approach
**MODERATE RISK** - Can scrape with proper JavaScript rendering and respectful rate limiting. Must handle store selection. Consider contacting Sobeys for official data partnership.

---

## 3. REAL CANADIAN SUPERSTORE (Loblaws Network)

### Legal & Access Considerations
**Robots.txt Summary:**
- **Disallows:** `/cart/`, `/checkout/`, `/account/`, `/collections-id/`
- **Blocks User Agents:** CazoodleBot, MJ12bot, dotbot/1.0, Gigabot
- **Allows:** Homepage, product pages, general content
- **Crawl Delay:** None specified
- **Sitemap:** https://www.realcanadiansuperstore.ca/sitemap.xml

**Anti-Bot Measures:**
- **Kameleoon A/B Testing:** May present different content to different users
- **Google Tag Manager:** Tracks user interactions
- **Sentry Error Monitoring:** Monitors application errors

**Risk Assessment:** LOW-MEDIUM - Permissive robots.txt, but sophisticated monitoring may detect scraping patterns.

### Technical Stack
- **Framework:** Next.js (React) with "Bronx" proprietary framework (Loblaw's internal platform)
- **Version:** 3.82.10+ed6b1958
- **Data Loading:** Hybrid SSR + CSR with BFF (Backend for Frontend) API
- **CDN:** https://assets.loblaws.ca/pcx_bronx_fe_prod/builds/
- **Analytics:** Google Tag Manager (GTM-NPWHZ7F, GTM-WSZ6X6NM), Kameleoon (tptncdvd7b), Snowplow Analytics

### URL Patterns
```
Homepage:
- https://www.realcanadiansuperstore.ca/

Categories:
- /en/[category]/c/[category-id]
- /en/[category]/[subcategory]/c/[category-id]
- Example: /en/food/dairy-and-eggs/milk-and-cream/milk/c/28000

Products:
- /en/[product-slug]/p/[product-id]_[UOM]
- Example: /en/2-milk/p/20188873_EA
- UOM codes: EA (each), KG (kilogram), LB (pound), etc.

Search:
- /search?search-bar=[query]
- Example: /search?search-bar=milk
```

### API Endpoints
```
BFF (Backend for Frontend):
- GET /pcx-bff/api/v1/slug/homepage-refresh
- Response time: ~850ms
- Returns: Structured JSON with product collections, deals, recommendations

Product Facade:
- V3ProductFacadeFlyerSearch (feature flag enabled)
- EnableRecommendationsV2Endpoint (feature flag enabled)
```

### HTML Structure & Selectors

**Product Tiles (Category/Search Pages):**
```css
Product Grid Container: .css-c5f5bu
Product Tile Container: .css-yxqevf (relative positioning, min-height: 18.1rem)
Product Image Wrapper: .css-179zpez (flex column, centered)
Product Image: .css-1qfh40k (10rem × 10rem, object-fit: cover)
Product Info Section: .css-8t32m0 (flex column)
Product Name: .css-1wmnia2 (font-weight: 600, max 3 lines with line-clamp)
Product Brand: .css-wp84x7 (font-size: 0.75rem, overflow: ellipsis)
Current Price: .css-s9i4ca (font-weight: 600, font-size: 0.875rem)
Unit Price: .css-pfkbv2
Stock Status Badge: .css-1ybw7by (background: #FFEFD1 for out of stock)
Out of Stock Indicator: .css-1wazsvo
```

**Pagination:**
```css
Pagination Container: .css-12c5bzw
Page Numbers: Individual button elements
Pattern: [Previous] 1 2 3 4 5 6 ... 14 15 [Next]
Query Parameter: ?page=[number]
```

**Data Attributes (Look for):**
```html
data-product-id="[product-id]"
data-track="[analytics-tracking]"
data-code="[product-code]"
```

### Embedded Product Data

**JSON-LD Structured Data:**
```json
{
  "@context": "https://schema.org/",
  "@type": "ProductCollection",
  "name": "Search Results",
  "itemListElement": [
    {
      "@type": "Product",
      "name": "2% Milk",
      "url": "https://www.realcanadiansuperstore.ca/en/2-milk/p/20188873_EA",
      "image": "https://...",
      "sku": "20188873",
      "description": "...",
      "brand": {
        "@type": "Brand",
        "name": "Neilson"
      },
      "offers": {
        "@type": "Offer",
        "price": "6.25",
        "priceCurrency": "CAD",
        "availability": "https://schema.org/InStock"
      }
    }
  ]
}
```

**Embedded Initial Data (in <script> tags):**
```javascript
window.__NEXT_DATA__ = {
  "props": {
    "pageProps": {
      "initialSearchData": {
        // Complete product catalog
        "products": [...],
        "pagination": {
          "pageNumber": 1,
          "pageSize": 48,
          "hasMore": true,
          "totalResults": 534
        }
      }
    }
  }
}
```

### Product Data Schema
```json
{
  "code": "20188873_EA",
  "productId": "20188873",
  "brand": "Neilson",
  "name": "2% Milk",
  "description": "Partly Skimmed Milk",
  "imageAssets": {
    "smallUrl": "https://assets.shop.loblaws.ca/products/20188873/b1/en/front/20188873_front_a01_@2.png",
    "mediumUrl": "...",
    "largeUrl": "..."
  },
  "pricing": {
    "price": 6.25,
    "wasPrice": null,
    "displayPrice": "$6.25",
    "unit": "EA"
  },
  "packageSize": "4 L",
  "inventory": {
    "indicator": "IN_STOCK",
    "badge": null
  },
  "badges": ["SALE", "PC_OPTIMUM"],
  "loyaltyOffers": {
    "points": 1000
  },
  "dealType": "MULTI",
  "promotionCode": "..."
}
```

### Category Hierarchy
```
Food (c/27985)
├── Fruits & Vegetables (c/28000)
│   ├── Fresh Fruits (c/28194)
│   ├── Fresh Vegetables
│   └── Packaged Salad & Dressing
├── Dairy & Eggs (c/28000)
│   ├── Milk & Cream
│   ├── Cheese
│   └── Yogurt
├── Meat & Seafood
└── Bakery
```

### Scraping Strategy

**Approach 1: Sitemap-Based Discovery**
1. Fetch sitemap.xml
2. Extract product URLs
3. Visit product detail pages with respectful delays
4. Parse JSON-LD or embedded __NEXT_DATA__

**Approach 2: Category Traversal**
1. Build category tree from navigation
2. Visit each category page
3. Parse embedded `initialSearchData` JSON
4. Extract all products from embedded data (no need to render individual product pages)
5. Handle pagination with ?page=[N] parameter

**Approach 3: Search-Based**
1. Use search endpoint with common terms
2. Parse search results embedded data
3. Extract product information from JSON

**Recommended:** Approach 2 (Category Traversal) - Most efficient as all product data is embedded in category page JSON.

### Implementation Considerations

**Rate Limiting:**
```python
import time
DELAY_SECONDS = 3  # 3-5 seconds between requests
MAX_REQUESTS_PER_MINUTE = 15

# Implement exponential backoff on errors
def make_request_with_backoff(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            if response.status_code == 429:  # Rate limited
                wait_time = (2 ** attempt) * 5
                time.sleep(wait_time)
                continue
            return response
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(DELAY_SECONDS * (attempt + 1))
```

**Headers:**
```python
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://www.realcanadiansuperstore.ca/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}
```

**Session Management:**
```python
session = requests.Session()
session.headers.update(headers)
# Let site set cookies naturally
response = session.get('https://www.realcanadiansuperstore.ca/')
```

**Data Extraction from Embedded JSON:**
```python
import json
import re
from bs4 import BeautifulSoup

def extract_product_data(html):
    soup = BeautifulSoup(html, 'lxml')

    # Method 1: JSON-LD structured data
    json_ld_scripts = soup.find_all('script', type='application/ld+json')
    for script in json_ld_scripts:
        try:
            data = json.loads(script.string)
            if data.get('@type') == 'ProductCollection':
                return data['itemListElement']
        except:
            continue

    # Method 2: Next.js data
    next_data_script = soup.find('script', id='__NEXT_DATA__')
    if next_data_script:
        data = json.loads(next_data_script.string)
        products = data.get('props', {}).get('pageProps', {}).get('initialSearchData', {}).get('products', [])
        return products

    return []
```

### Recommended Approach
**MEDIUM RISK** - Can scrape effectively with proper rate limiting and respectful behavior. All data is embedded in page source, so no complex JavaScript rendering needed for category pages. Consider reaching out to Loblaws for partnership opportunities.

---

## 4. NO FRILLS (Loblaws Network)

### Legal & Access Considerations
**Robots.txt Summary:**
- **Identical to Real Canadian Superstore** (same Loblaw network)
- **Disallows:** `/cart/`, `/checkout/`, `/account/`, `/collections-id/`
- **Blocks User Agents:** CazoodleBot, MJ12bot, dotbot/1.0, Gigabot
- **Sitemap:** https://www.nofrills.ca/sitemap.xml

**Technical Stack:**
- **Identical to Real Canadian Superstore** - Same "Bronx" framework (v3.82.10)
- **Shared Infrastructure:** Same CDN, analytics, and backend systems

### URL Patterns
```
Same as Real Canadian Superstore, with nofrills.ca domain:

Products:
- /en/[product-slug]/p/[product-id]_[UOM]
- Example: /en/2-milk/p/20188873_EA

Categories:
- /en/[category]/[subcategory]/c/[category-id]
- Example: /food/dairy-and-eggs/milk-and-cream/milk/c/28000

Search:
- /search?search-bar=[query]
```

### HTML Structure & Selectors
**Identical to Real Canadian Superstore** - See section 3 for complete CSS selector reference.

### Product Data Schema
**Identical to Real Canadian Superstore** - Products share same IDs and data structure across Loblaw network.

### Scraping Strategy
**Same as Real Canadian Superstore** - Use category traversal with embedded JSON extraction.

**Cross-Brand Note:** Products with same product IDs (e.g., 20188873_EA) appear across all Loblaw banners (No Frills, Real Canadian Superstore, Loblaws, etc.) with potentially different pricing.

### Recommended Approach
**MEDIUM RISK** - Identical technical implementation to Real Canadian Superstore. Can use same scraping codebase with different domain.

---

## 5. SOBEYS.COM

### Legal & Access Considerations
**Robots.txt Summary:**
- **Identical to Safeway.ca** (same Sobeys network)
- **Disallows:** `/private/`, `/my-profile/`, `/verified-prices/`, `/shopping-list/`, etc.
- **Sitemap:** https://www.sobeys.com/sitemap.xml

**Technical Stack:**
- **Same as Safeway.ca** - Next.js React with Adobe Experience Manager
- **Shared Network:** Scene+ loyalty integration, same backend systems

### URL Patterns
```
Products:
- /products/[category]/[subcategory]/[product]?sort=[param]&page=[N]
- Example: /products/dairy-eggs/milk-cream/milk?sort=relevance&page=0

Categories:
- /en/products/[category]/[subcategory]
```

### Scraping Strategy
**Same as Safeway.ca** - Requires JavaScript rendering, store context, respectful rate limiting.

### Recommended Approach
**MEDIUM RISK** - Identical to Safeway.ca. Consider unified Sobeys network partnership request.

---

## 6. SAVE-ON-FOODS

### Legal & Access Considerations
**Robots.txt:** Not accessible (403 Forbidden)

**Anti-Bot Measures:**
- **Aggressive 403 Blocking:** Immediate rejection of automated requests
- **Cloudflare or Similar WAF:** Strong web application firewall
- **No robots.txt Access:** Even robots.txt returns 403

**Risk Assessment:** VERY HIGH - Site actively blocks all automated access attempts.

### Technical Analysis
**Unable to analyze** due to 403 blocking on all requests.

### Recommended Approach
**DO NOT SCRAPE** - Request official API access or data partnership from Save-on-Foods / Pattison Food Group. Attempting to bypass 403 protections violates ethical scraping principles and likely violates terms of service.

---

## 7. CO-OP (Various Regional Sites)

### Legal & Access Considerations
**Robots.txt:** Not accessible (403 Forbidden)

**Anti-Bot Measures:**
- **Aggressive 403 Blocking:** Similar to Save-on-Foods
- **Regional Variations:** Different Co-op regions may have different systems

**Risk Assessment:** VERY HIGH - Active blocking of automated access.

### Recommended Approach
**DO NOT SCRAPE** - Contact regional Co-op federations for official data access. Each region (Calgary Co-op, Saskatoon Co-op, etc.) may have different data access policies.

---

## CROSS-SITE COMPARISON SUMMARY

| Site | Risk Level | Bot Detection | JS Required | Robots.txt | Best Method |
|------|-----------|---------------|-------------|------------|-------------|
| Walmart.ca | HIGH | PerimeterX CAPTCHA | Yes | Restrictive | Request API access |
| Safeway.ca | MEDIUM | Store required | Yes (React) | Moderate | JS rendering + store context |
| Superstore.ca | LOW-MED | Analytics tracking | No (data embedded) | Permissive | Category traversal |
| No Frills | LOW-MED | Analytics tracking | No (data embedded) | Permissive | Category traversal |
| Sobeys.com | MEDIUM | Store required | Yes (React) | Moderate | JS rendering + store context |
| Save-on-Foods | VERY HIGH | 403 WAF | Unknown | Inaccessible | Request partnership |
| Co-op | VERY HIGH | 403 WAF | Unknown | Inaccessible | Request partnership |

---

## GENERAL SCRAPING ARCHITECTURE

### Technology Stack Recommendations

**For Sites with Embedded Data (Loblaw Network):**
```python
import requests
from bs4 import BeautifulSoup
import json
import time
from typing import List, Dict
import logging

# Simple requests-based scraper
```

**For Sites Requiring JavaScript (Sobeys Network):**
```python
from playwright.sync_api import sync_playwright
import json
import time

# Playwright-based scraper
```

### Standard Product Data Schema (JSONL Output)

```jsonl
{"source":"realcanadiansuperstore","scraped_at":"2025-12-19T10:30:00Z","product_id":"20188873_EA","name":"2% Milk","brand":"Neilson","price":{"amount":6.25,"currency":"CAD"},"unit":"4 L","category":"Food > Dairy & Eggs > Milk & Cream","url":"https://www.realcanadiansuperstore.ca/en/2-milk/p/20188873_EA","in_stock":true,"image_url":"https://assets.shop.loblaws.ca/products/20188873/b1/en/front/20188873_front_a01_@2.png","package_size":"4 L","badges":["SALE","PC_OPTIMUM"],"metadata":{"loyalty_points":1000,"promotion_code":"XYZ123","deal_type":"MULTI"}}
{"source":"nofrills","scraped_at":"2025-12-19T10:31:00Z","product_id":"20188873_EA","name":"2% Milk","brand":"Neilson","price":{"amount":5.99,"currency":"CAD"},"unit":"4 L","category":"Food > Dairy & Eggs > Milk & Cream","url":"https://www.nofrills.ca/en/2-milk/p/20188873_EA","in_stock":true,"image_url":"https://assets.shop.loblaws.ca/products/20188873/b1/en/front/20188873_front_a01_@2.png","package_size":"4 L","badges":["PC_OPTIMUM"],"metadata":{"loyalty_points":800}}
```

### Rate Limiting Configuration

```yaml
# scraper_config.yaml
rate_limiting:
  default_delay_seconds: 3
  max_requests_per_minute: 15
  max_concurrent_requests: 1
  exponential_backoff_base: 2
  max_retries: 3

headers:
  user_agent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  accept: "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
  accept_language: "en-US,en;q=0.9"

sites:
  realcanadiansuperstore:
    base_url: "https://www.realcanadiansuperstore.ca"
    delay_seconds: 3
    max_requests_per_minute: 15
    enabled: true

  nofrills:
    base_url: "https://www.nofrills.ca"
    delay_seconds: 3
    max_requests_per_minute: 15
    enabled: true

  safeway:
    base_url: "https://www.safeway.ca"
    delay_seconds: 5
    max_requests_per_minute: 10
    requires_store_selection: true
    enabled: false  # Requires JS rendering

  walmart:
    base_url: "https://www.walmart.ca"
    enabled: false  # Do not scrape - bot protection

  saveonfoods:
    enabled: false  # Do not scrape - 403 blocking

  coop:
    enabled: false  # Do not scrape - 403 blocking
```

### Error Handling Strategy

```python
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Optional

class ScraperErrorType(Enum):
    RATE_LIMIT = "rate_limit"
    BOT_DETECTION = "bot_detection"
    NETWORK_ERROR = "network_error"
    PARSE_ERROR = "parse_error"
    CAPTCHA_REQUIRED = "captcha_required"
    STORE_REQUIRED = "store_required"

@dataclass
class ScraperError:
    error_type: ScraperErrorType
    url: str
    status_code: Optional[int]
    message: str
    timestamp: str

    def should_retry(self) -> bool:
        return self.error_type in [
            ScraperErrorType.NETWORK_ERROR,
            ScraperErrorType.RATE_LIMIT
        ]

    def should_abort(self) -> bool:
        return self.error_type in [
            ScraperErrorType.BOT_DETECTION,
            ScraperErrorType.CAPTCHA_REQUIRED
        ]

# Log all errors with structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
```

### Validation & Data Quality

```python
from pydantic import BaseModel, HttpUrl, Field, validator
from decimal import Decimal
from datetime import datetime
from typing import Optional, List

class Price(BaseModel):
    amount: Decimal = Field(..., gt=0, description="Price amount must be positive")
    currency: str = Field(default="CAD", regex="^[A-Z]{3}$")

class ProductRecord(BaseModel):
    source: str = Field(..., min_length=1)
    scraped_at: datetime
    product_id: str = Field(..., min_length=1)
    name: str = Field(..., min_length=1, max_length=500)
    brand: Optional[str] = Field(None, max_length=200)
    price: Price
    unit: str = Field(..., min_length=1, max_length=50)
    category: str
    url: HttpUrl
    in_stock: bool
    image_url: Optional[HttpUrl] = None
    package_size: Optional[str] = None
    badges: List[str] = Field(default_factory=list)
    metadata: dict = Field(default_factory=dict)

    @validator('price')
    def validate_price_range(cls, v):
        if v.amount > 10000:  # Sanity check
            raise ValueError('Price seems unreasonably high')
        return v

    @validator('category')
    def validate_category_format(cls, v):
        if '>' not in v:
            raise ValueError('Category must include hierarchy with >')
        return v

# Usage
try:
    product = ProductRecord(**scraped_data)
    # Write to JSONL
    with open('products.jsonl', 'a') as f:
        f.write(product.json() + '\n')
except ValidationError as e:
    logger.error(f"Invalid product data: {e}")
```

---

## SITE-SPECIFIC SCRAPER PSEUDOCODE

### Loblaw Network (Superstore, No Frills) - Category Traversal

```python
import requests
from bs4 import BeautifulSoup
import json
import time
from typing import List, Dict
import logging

class LoblawScraper:
    """Scraper for Loblaw network sites (Superstore, No Frills)"""

    def __init__(self, base_url: str, delay_seconds: int = 3):
        self.base_url = base_url
        self.delay_seconds = delay_seconds
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        self.logger = logging.getLogger(__name__)

    def get_category_tree(self) -> List[Dict]:
        """Extract category tree from homepage or sitemap"""
        # Implementation would parse navigation or sitemap
        return [
            {'id': '28000', 'name': 'Dairy & Eggs', 'url': '/food/dairy-and-eggs/c/28000'},
            # ... more categories
        ]

    def scrape_category(self, category_url: str) -> List[Dict]:
        """Scrape all products from a category page"""
        all_products = []
        page = 1

        while True:
            url = f"{self.base_url}{category_url}?page={page}"
            self.logger.info(f"Scraping {url}")

            try:
                response = self.session.get(url)
                response.raise_for_status()

                products = self._extract_products_from_html(response.text)

                if not products:
                    break  # No more products

                all_products.extend(products)

                # Check if there are more pages
                has_more = self._check_pagination(response.text, page)
                if not has_more:
                    break

                page += 1
                time.sleep(self.delay_seconds)

            except Exception as e:
                self.logger.error(f"Error scraping {url}: {e}")
                break

        return all_products

    def _extract_products_from_html(self, html: str) -> List[Dict]:
        """Extract product data from embedded JSON"""
        soup = BeautifulSoup(html, 'lxml')
        products = []

        # Method 1: Extract from JSON-LD
        json_ld_scripts = soup.find_all('script', type='application/ld+json')
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string)
                if data.get('@type') == 'ProductCollection':
                    for item in data.get('itemListElement', []):
                        products.append(self._normalize_product(item, source='json-ld'))
                    return products
            except Exception as e:
                self.logger.debug(f"Failed to parse JSON-LD: {e}")

        # Method 2: Extract from __NEXT_DATA__
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if next_data_script:
            try:
                data = json.loads(next_data_script.string)
                product_list = data.get('props', {}).get('pageProps', {}).get('initialSearchData', {}).get('products', [])
                for product in product_list:
                    products.append(self._normalize_product(product, source='next-data'))
                return products
            except Exception as e:
                self.logger.error(f"Failed to parse __NEXT_DATA__: {e}")

        return products

    def _normalize_product(self, raw_data: Dict, source: str) -> Dict:
        """Normalize product data to standard schema"""
        if source == 'json-ld':
            return {
                'product_id': raw_data.get('sku'),
                'name': raw_data.get('name'),
                'brand': raw_data.get('brand', {}).get('name'),
                'price': {
                    'amount': float(raw_data.get('offers', {}).get('price', 0)),
                    'currency': raw_data.get('offers', {}).get('priceCurrency', 'CAD')
                },
                'url': raw_data.get('url'),
                'image_url': raw_data.get('image'),
                'in_stock': 'InStock' in raw_data.get('offers', {}).get('availability', ''),
                'description': raw_data.get('description', '')
            }
        else:  # next-data
            return {
                'product_id': raw_data.get('code'),
                'name': raw_data.get('name'),
                'brand': raw_data.get('brand'),
                'price': {
                    'amount': raw_data.get('pricing', {}).get('price'),
                    'currency': 'CAD'
                },
                'url': f"/en/{raw_data.get('link', '')}",
                'image_url': raw_data.get('imageAssets', {}).get('largeUrl'),
                'in_stock': raw_data.get('inventory', {}).get('indicator') == 'IN_STOCK',
                'package_size': raw_data.get('packageSize'),
                'badges': raw_data.get('badges', []),
                'metadata': {
                    'loyalty_points': raw_data.get('loyaltyOffers', {}).get('points'),
                    'deal_type': raw_data.get('dealType')
                }
            }

    def _check_pagination(self, html: str, current_page: int) -> bool:
        """Check if more pages exist"""
        # Parse pagination info from embedded data
        soup = BeautifulSoup(html, 'lxml')
        next_data_script = soup.find('script', id='__NEXT_DATA__')
        if next_data_script:
            try:
                data = json.loads(next_data_script.string)
                pagination = data.get('props', {}).get('pageProps', {}).get('initialSearchData', {}).get('pagination', {})
                return pagination.get('hasMore', False)
            except:
                pass
        return False

# Usage
scraper = LoblawScraper("https://www.realcanadiansuperstore.ca")
categories = scraper.get_category_tree()
for category in categories:
    products = scraper.scrape_category(category['url'])
    # Save to JSONL
```

### Sobeys Network (Safeway, Sobeys) - Playwright-Based

```python
from playwright.sync_api import sync_playwright, Page
import json
import time
from typing import List, Dict
import logging

class SobeysNetworkScraper:
    """Scraper for Sobeys network requiring JavaScript rendering"""

    def __init__(self, base_url: str, store_id: str = None):
        self.base_url = base_url
        self.store_id = store_id
        self.logger = logging.getLogger(__name__)

    def scrape_with_playwright(self, category_url: str) -> List[Dict]:
        """Scrape category using Playwright for JS rendering"""

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1920, 'height': 1080}
            )
            page = context.new_page()

            try:
                # Set store if required
                if self.store_id:
                    self._set_store_context(page, self.store_id)

                # Navigate to category
                url = f"{self.base_url}{category_url}"
                self.logger.info(f"Loading {url}")
                page.goto(url, wait_until='networkidle')

                # Wait for product grid to load
                page.wait_for_selector('[data-component="product-tile"]', timeout=10000)

                # Scroll to load all products if infinite scroll
                self._scroll_to_bottom(page)

                # Extract product data
                products = self._extract_products_from_page(page)

                return products

            except Exception as e:
                self.logger.error(f"Error scraping with Playwright: {e}")
                return []
            finally:
                browser.close()

    def _set_store_context(self, page: Page, store_id: str):
        """Set store selection in cookies/storage"""
        # Set cookies or localStorage for store selection
        page.evaluate(f"localStorage.setItem('selectedStore', '{store_id}')")
        self.logger.info(f"Set store context to {store_id}")

    def _scroll_to_bottom(self, page: Page):
        """Scroll page to trigger infinite scroll loading"""
        previous_height = 0
        while True:
            current_height = page.evaluate("document.body.scrollHeight")
            if current_height == previous_height:
                break
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(2)
            previous_height = current_height

    def _extract_products_from_page(self, page: Page) -> List[Dict]:
        """Extract products from rendered page"""
        products = []

        # Try to extract from embedded JSON first
        try:
            next_data = page.evaluate("JSON.stringify(window.__NEXT_DATA__)")
            if next_data:
                data = json.loads(next_data)
                # Parse products from Next.js data
                # ... implementation
        except:
            pass

        # Fallback to DOM scraping
        product_elements = page.query_selector_all('[data-component="product-tile"]')
        for element in product_elements:
            try:
                product = {
                    'name': element.query_selector('.product-name').inner_text(),
                    'price': self._parse_price(element.query_selector('.price').inner_text()),
                    'url': element.query_selector('a').get_attribute('href'),
                    # ... more fields
                }
                products.append(product)
            except Exception as e:
                self.logger.warning(f"Failed to parse product element: {e}")

        return products
```

---

## ALTERNATIVE APPROACHES

### 1. Official API Requests
Before scraping, contact each retailer's business development or data partnerships team:

**Loblaw Network (Superstore, No Frills):**
- Website: https://www.loblaw.ca/en/contact-us
- Potential Programs: PC Express API, B2B data partnerships

**Sobeys Network (Safeway, Sobeys):**
- Website: https://www.sobeys.com/en/contact-us/
- Voilà partnership opportunities

**Walmart Canada:**
- Website: https://developer.walmart.com/ (US - check for Canadian equivalent)
- Affiliate program may offer product data feeds

**Save-on-Foods / Pattison Food Group:**
- Website: https://www.saveonfoods.com/contact-us/
- Request B2B partnership

### 2. Public Datasets & Third-Party Aggregators
- **OpenFood Facts:** https://world.openfoodfacts.org/ (crowdsourced product database)
- **Flyer APIs:** Some companies provide flyer data via APIs
- **Price Comparison Sites:** May offer data partnerships (e.g., Flipp)

### 3. Manual Data Exports
Some retailers offer CSV exports for business accounts or loyalty programs.

---

## MONITORING & MAINTENANCE

### Scraper Health Checks
```python
import time
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ScraperHealth:
    site: str
    last_successful_run: datetime
    total_products_scraped: int
    error_count: int
    average_response_time: float

    def is_healthy(self) -> bool:
        """Check if scraper is functioning properly"""
        # No successful run in last 24 hours
        if (datetime.now() - self.last_successful_run).days > 1:
            return False

        # High error rate
        if self.error_count > 100:
            return False

        # No products scraped
        if self.total_products_scraped == 0:
            return False

        return True

# Alert on failures
def check_scraper_health():
    health = ScraperHealth(
        site="realcanadiansuperstore",
        last_successful_run=datetime.now(),
        total_products_scraped=1234,
        error_count=5,
        average_response_time=1.2
    )

    if not health.is_healthy():
        send_alert(f"Scraper health check failed for {health.site}")
```

### Structure Change Detection
```python
def detect_structure_changes(html: str, expected_selectors: List[str]) -> bool:
    """Detect if website structure has changed"""
    soup = BeautifulSoup(html, 'lxml')

    missing_selectors = []
    for selector in expected_selectors:
        if not soup.select(selector):
            missing_selectors.append(selector)

    if missing_selectors:
        logger.warning(f"Missing expected selectors: {missing_selectors}")
        return True

    return False

# Run periodically
expected_selectors = ['.css-1wmnia2', '.css-s9i4ca', '.css-yxqevf']
if detect_structure_changes(html, expected_selectors):
    send_alert("Website structure may have changed - scraper needs review")
```

---

## LEGAL DISCLAIMER

This document is provided for educational and research purposes only. The information contained herein:

1. Does not constitute legal advice
2. May become outdated as websites change their structures and policies
3. Should not be used to violate any website's Terms of Service
4. Requires independent verification of current robots.txt and ToS before implementation

Users of this information are responsible for:
- Obtaining necessary legal counsel before scraping
- Complying with all applicable laws (CFAA, GDPR, PIPEDA, etc.)
- Respecting website terms of service
- Implementing ethical scraping practices (rate limiting, attribution, etc.)
- Seeking official data partnerships when possible

The author assumes no liability for misuse of this information.

---

## REVISION HISTORY

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | 2025-12-19 | Initial documentation based on research conducted 2025-12-19 |

---

## APPENDIX A: CSS SELECTOR REFERENCE

### Loblaw Network (Superstore, No Frills)

```css
/* Product Grid */
.css-c5f5bu                 /* Grid container */
.css-yxqevf                 /* Product tile container */

/* Product Elements */
.css-1wmnia2                /* Product name (3-line clamp) */
.css-wp84x7                 /* Brand name */
.css-s9i4ca                 /* Current price */
.css-pfkbv2                 /* Unit price */
.css-1qfh40k                /* Product image (10rem x 10rem) */
.css-179zpez                /* Image wrapper */
.css-8t32m0                 /* Product info section */

/* Status Indicators */
.css-1ybw7by                /* Badge (e.g., out of stock) */
.css-1wazsvo                /* Stock status text */

/* Pagination */
.css-12c5bzw                /* Pagination container */
```

### Sobeys Network (Safeway, Sobeys)

```css
/* Note: Selectors are dynamically generated by Next.js/Tailwind */
/* Use data attributes when available */

[data-component="product-tile"]
[data-product-id]
.product-name
.price
.brand
```

---

## APPENDIX B: PRODUCT ID FORMATS

| Retailer | Format | Example | Notes |
|----------|--------|---------|-------|
| Real Canadian Superstore | [digits]_[UOM] | 20188873_EA | EA=Each, KG=Kilogram, LB=Pound |
| No Frills | [digits]_[UOM] | 20188873_EA | Same as Superstore (shared network) |
| Safeway | [digits] | 20188875001 | Numeric only |
| Sobeys | [digits] | 20188875001 | Same as Safeway (shared network) |
| Walmart | Varies | Unknown | Not accessible due to bot protection |

**UOM Codes (Loblaw Network):**
- EA: Each (individual item)
- KG: Kilogram
- LB: Pound
- L: Liter
- ML: Milliliter
- G: Gram

---

## APPENDIX C: CONTACT INFORMATION FOR DATA PARTNERSHIPS

| Retailer | Parent Company | Contact URL |
|----------|----------------|-------------|
| Real Canadian Superstore | Loblaw Companies | https://www.loblaw.ca/en/contact-us |
| No Frills | Loblaw Companies | https://www.loblaw.ca/en/contact-us |
| Safeway | Sobeys Inc. | https://www.sobeys.com/en/contact-us/ |
| Sobeys | Sobeys Inc. | https://www.sobeys.com/en/contact-us/ |
| Walmart Canada | Walmart | https://corporate.walmart.com/contact-us |
| Save-on-Foods | Pattison Food Group | https://www.saveonfoods.com/contact-us/ |
| Co-op (Regional) | Various federations | Contact local/regional Co-op directly |

---

**END OF DOCUMENT**
