# SCRAPING_FRAMEWORK_PLAYBOOK.md

## Executive Summary: The Rooney Method
The core philosophy of John Watson Rooney’s approach to web scraping is **efficiency through reverse engineering**. Instead of brute-forcing a website with browser automation (which is slow and resource-heavy), the "Rooney Method" prioritizes finding the "Backdoor"—the hidden APIs and JSON data sources that power modern front-ends.

**Core Tenets:**
1.  **Backend Over Frontend:** Always check the Network tab for JSON endpoints before parsing HTML.
2.  **Lightweight Performance:** Prefer `httpx` and `selectolax` over `requests` and `BeautifulSoup` for speed.
3.  **Browser as a Last Resort:** Use Playwright only when JavaScript execution is strictly required and no API exists.
4.  **Scaling with Frameworks:** Transition from scripts to `Scrapy` when handling >10k items or complex pipelines.
5.  **Polite Persistence:** Use headers, proxies, and session management to mimic real users and avoid blocks.

---

## Video Index (Research Log)

| Video Title | Publish Date | Main Topic | Key Takeaways | Link |
| :--- | :--- | :--- | :--- | :--- |
| **This is How I Scrape 99% of Sites** | 2024-07-09 | Hidden APIs | - Focus on Network Tab (XHR/Fetch)<br>- Mimic the API request directly<br>- Data comes back as clean JSON | [Watch](https://www.youtube.com/watch?v=DqmlH_P9jD0) |
| **BeautifulSoup is NOT the king** | 2022-11-07 | Parsing Libraries | - `Selectolax` is significantly faster than BS4<br>- Uses C-based Modest engine<br>- Ideal for large-scale HTML parsing | [Watch](https://www.youtube.com/watch?v=U97vX_kOq6Y) |
| **My System for 150k Items** | 2024-07-02 | Scalability | - Use Scrapy for high-volume jobs<br>- Custom middleware for proxies<br>- Save to SQLite/Postgres for durability | [Watch](https://www.youtube.com/watch?v=FmS34VjZio4) |
| **The Biggest Mistake Beginners Make** | 2022-05-04 | Logic | - Scraping the "Front-end" vs the "Back-end"<br>- Understanding SPA (Single Page Apps)<br>- Don't fight the JS; find the source | [Watch](https://www.youtube.com/watch?v=1pE6W_L9K0E) |
| **Stop Using Selenium or Playwright** | 2024-10-27 | Browser Automation | - Introduction to "No-driver" solutions<br>- Using Chrome DevTools Protocol (CDP)<br>- Bypassing detection by avoiding standard drivers | [Watch](https://www.youtube.com/watch?v=fXpD-vH-XbM) |
| **Scrapy in 30 Minutes** | 2023-11-16 | Frameworks | - Scrapy Shell for rapid selector testing<br>- Spiders, Items, and Pipelines workflow<br>- Handling pagination automatically | [Watch](https://www.youtube.com/watch?v=mBoX_JCKZTE) |

---

## Framework: End-to-End Scraper Architecture

### Folder Structure
```text
project_root/
├── scrapers/           # Individual spider scripts (e.g., amazon_spider.py)
├── utils/              # Reusable logic (parsers, cleaning, proxy rotators)
├── configs/            # .env files, headers.json, selector_maps.yaml
├── data/
│   ├── raw/            # Initial JSONL/CSV dumps
│   └── processed/      # Cleaned, validated data
├── logs/               # Rotating log files (scraper_date.log)
├── checkpoints/        # State files (last_page_scanned.txt)
└── requirements.txt
```

### Data Schema Design
*   **JSONL First:** Always save results as JSON Lines (`.jsonl`). It is appendable, memory-efficient for large crawls, and survives crashes better than standard JSON or CSV.
*   **Optional CSV:** Convert to CSV only at the final export stage for clients.

### Strategy & Workflow
*   **Logging:** Use `loguru` for beautiful, informative logs. Log status codes, item counts per page, and retry attempts.
*   **Checkpoints:** For long scrapes, save the "current page" or "last ID" to a local file. On restart, the scraper reads this to resume instead of starting from page 1.
*   **Deduplication:** Use a `Set` of URLs or IDs to avoid scraping the same item twice within a session.

---

## Extraction Techniques

### Decision Tree: How to Get the Data
1.  **Check for API:** Open DevTools > Network > Fetch/XHR. Refresh. Do you see JSON with the data? **(USE THIS)**.
2.  **Check for Script Tags:** View Page Source. Search for `JSON-LD` or `__NEXT_DATA__`. Is the data in a `<script>` tag? **(PARSE THIS)**.
3.  **Check for Static HTML:** If neither of the above, is the data in the raw HTML? **(USE SELECTOLAX)**.
4.  **Browser Automation:** If the site is a complex React/Vue app with no obvious API and high bot detection. **(USE PLAYWRIGHT)**.

### Selector Stability Tactics
*   **Avoid generated classes:** Don't use selectors like `.css-123xyz`.
*   **Prefer Data Attributes:** Use `[data-testid="price"]` or `[itemprop="name"]`.
*   **Text Anchors:** In `Selectolax`, find elements containing specific text if classes are unstable.

---

## Polite Anti-Block Practices

> **Do:** Use `User-Agent` rotation and realistic headers (`Accept-Language`, `Referer`).
> **Don't:** Scrape without a delay between requests on smaller sites.

### Key Strategies
*   **TLS Fingerprinting:** Use `curl-cffi` or `tls-client` to mimic a real browser's TLS handshake. Many firewalls (Cloudflare) block standard Python `requests` based on this fingerprint alone.
*   **Proxies:** Use Residential Proxies for high-value targets. Stick to one IP per session (Sticky Sessions) to look like a single user.
*   **CAPTCHA Guidance:** 
    *   Avoid triggering them by using `undetected-chromedriver` or Playwright with stealth plugins.
    *   If blocked, use a 3rd party solver service (2Captcha/Anti-Captcha) via their API, but focus on *avoidance* first.

---

## Reusable Templates

### 1. New Site Checklist
- [ ] View Page Source: Is the data there?
- [ ] Network Tab: Is there a hidden API?
- [ ] Robots.txt: What are the crawl rules?
- [ ] Pagination: Is it URL-based (`?page=2`), Infinite Scroll (XHR), or "Load More" button?

### 2. Generic Scraper (HTTPX + Selectolax)
```python
import httpx
from selectolax.parser import HTMLParser

def get_html(url):
    headers = {"User-Agent": "Mozilla/5.0 ..."}
    with httpx.Client(headers=headers, follow_redirects=True) as client:
        resp = client.get(url)
        return HTMLParser(resp.text)

def parse_item(html):
    products = html.css("div.product-card")
    for item in products:
        yield {
            "name": item.css_first("h2").text().strip(),
            "price": item.css_first(".price").text()
        }

# Main execution logic goes here...
```

---

## Common Failure Modes + Fix Patterns

| Failure | Detection | Fix Pattern |
| :--- | :--- | :--- |
| **403 Forbidden** | Status Code 403 | Update User-Agent, add Cookies, or use a Residential Proxy. |
| **429 Too Many Requests** | Status Code 429 | Implement `time.sleep()` with jitter; reduce concurrency. |
| **Empty Results** | `len(items) == 0` | Check if selectors changed or if content is rendered via JS. |
| **Timeout** | `ConnectTimeout` | Increase timeout in client; check proxy health. |

---

## Practice Section

1.  **Level 1:** Scrape a static quotes site using `httpx` and `selectolax`.
2.  **Level 2:** Handle pagination on a site that uses `?p=1`, `?p=2`.
3.  **Level 3:** Find a hidden API on a retail site and extract 5 pages of JSON data.
4.  **Level 4 (Debug):** A scraper returns `None` for prices. *Hint: Check if prices are in a `<script type="application/ld+json">` tag instead of the HTML tags.*
5.  **Level 5:** Build a Playwright script that scrolls to the bottom of a page to trigger infinite loading.
6.  **Level 6:** Create a Scrapy spider with a `CrawlSpider` rule to follow all product links on a category page.
7.  **Level 7 (Debug):** Scraper works on your machine but gets 403 on the server. *Hint: Compare TLS fingerprints or check for Geoblocking.*
8.  **Level 8:** Implement a retry decorator that uses exponential backoff.

---

## Recommended Learning Path

1.  **The Foundation:** [Web Scraping with Python - Start HERE](https://www.youtube.com/watch?v=mBoX_JCKZTE)
2.  **The "Rooney" Secret:** [This is How I Scrape 99% of Sites](https://www.youtube.com/watch?v=DqmlH_P9jD0)
3.  **Parsing Speed:** [BeautifulSoup is NOT the king](https://www.youtube.com/watch?v=U97vX_kOq6Y)
4.  **Scaling Up:** [Scrapy in 30 Minutes](https://www.youtube.com/watch?v=mBoX_JCKZTE)
5.  **Advanced Bypassing:** [Stop Using Selenium or Playwright](https://www.youtube.com/watch?v=fXpD-vH-XbM)