"""
Enhanced API Discovery for Sobeys
Waits for full page load and captures all product-related data
"""

import json
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

def discover_sobeys_api():
    """Comprehensive API discovery with longer wait times"""

    debug_dir = Path("data/debug/sobeys")
    debug_dir.mkdir(parents=True, exist_ok=True)

    # Track all API calls
    api_calls = []
    product_apis = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )

        # Intercept all network traffic
        def log_request(request):
            if request.resource_type in ['xhr', 'fetch']:
                api_calls.append({
                    'url': request.url,
                    'method': request.method,
                    'timestamp': time.time()
                })
                print(f"[XHR] {request.method} {request.url[:100]}")

        def log_response(response):
            if response.request.resource_type in ['xhr', 'fetch']:
                try:
                    if 'json' in response.headers.get('content-type', ''):
                        body = response.json()

                        # Check if this looks like product data
                        body_str = json.dumps(body).lower()
                        if any(keyword in body_str for keyword in ['product', 'price', 'item', 'sku']):
                            print(f"⭐ PRODUCT API: {response.status} {response.url[:80]}")
                            product_apis.append({
                                'url': response.url,
                                'status': response.status,
                                'body': body
                            })

                            # Save individual response
                            timestamp = int(time.time())
                            filename = f"product_api_{timestamp}_{len(product_apis)}.json"
                            (debug_dir / filename).write_text(json.dumps(body, indent=2), encoding='utf-8')
                            print(f"   Saved to: {filename}")
                except Exception as e:
                    pass

        context.on('request', log_request)
        context.on('response', log_response)

        page = context.new_page()

        print("\n" + "="*60)
        print("SOBEYS API DISCOVERY")
        print("="*60 + "\n")

        # Navigate to Sobeys search
        print("[1/5] Navigating to Sobeys...")
        page.goto("https://www.sobeys.com", timeout=60000)
        time.sleep(5)

        # Search for milk
        print("[2/5] Searching for 'milk'...")
        try:
            search_input = page.locator('input[placeholder*="Search" i]').first
            search_input.fill("milk")
            search_input.press("Enter")
            print("   Search submitted!")
        except Exception as e:
            print(f"   Error searching: {e}")

        # Wait for page to fully load
        print("[3/5] Waiting for results to load (30 seconds)...")
        time.sleep(10)

        # Check for __NEXT_DATA__
        print("[4/5] Checking for embedded data...")
        try:
            next_data = page.evaluate("window.__NEXT_DATA__")
            if next_data:
                print("   ✓ Found __NEXT_DATA__!")
                (debug_dir / "next_data.json").write_text(json.dumps(next_data, indent=2), encoding='utf-8')
            else:
                print("   ✗ No __NEXT_DATA__ found")
        except:
            print("   ✗ Error accessing __NEXT_DATA__")

        # Check for Algolia window objects
        try:
            algolia_data = page.evaluate("""
                () => {
                    const data = {};
                    for (let key in window) {
                        if (key.toLowerCase().includes('algolia') || key.toLowerCase().includes('search')) {
                            try {
                                data[key] = window[key];
                            } catch(e) {}
                        }
                    }
                    return data;
                }
            """)
            if algolia_data:
                print(f"   ✓ Found {len(algolia_data)} search-related window objects")
                (debug_dir / "window_search_objects.json").write_text(json.dumps(algolia_data, indent=2, default=str), encoding='utf-8')
        except Exception as e:
            print(f"   Error checking window objects: {e}")

        # Try to scroll and trigger lazy loading
        print("[5/5] Scrolling to trigger lazy-loaded content...")
        for i in range(5):
            page.evaluate(f"window.scrollBy(0, {500 * (i+1)})")
            time.sleep(2)

        # Save page HTML and screenshot
        print("\n[SAVING] Capturing page state...")
        (debug_dir / "search_page.html").write_text(page.content(), encoding='utf-8')
        page.screenshot(path=str(debug_dir / "search_page.png"), full_page=True)
        print(f"   ✓ Saved HTML and screenshot")

        # Keep browser open for manual inspection
        print("\n" + "="*60)
        print("BROWSER READY FOR INSPECTION")
        print("="*60)
        print("Open DevTools (F12) → Network tab → Filter: Fetch/XHR")
        print(f"Found {len(product_apis)} potential product APIs so far")
        print("\nClick 'Load More', scroll, or interact with the page")
        print("Press Enter when done to save full report...")
        input()

        browser.close()

    # Generate comprehensive report
    print("\n[GENERATING REPORT]...")
    report_path = debug_dir / "api_discovery_report.md"

    with open(report_path, 'w', encoding='utf-8') as f:
        f.write("# Sobeys API Discovery Report\n\n")
        f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        f.write("## Executive Summary\n\n")
        f.write(f"- Total XHR/Fetch requests: {len(api_calls)}\n")
        f.write(f"- Product-related APIs: {len(product_apis)}\n")
        f.write(f"- __NEXT_DATA__ available: {'Yes' if (debug_dir / 'next_data.json').exists() else 'No'}\n\n")

        if product_apis:
            f.write("## ⭐ Product APIs Found\n\n")
            for i, api in enumerate(product_apis, 1):
                f.write(f"### API #{i}\n\n")
                f.write(f"**URL:** `{api['url']}`\n\n")
                f.write(f"**Status:** {api['status']}\n\n")

                # Show sample of response
                body_str = json.dumps(api['body'], indent=2)
                f.write("**Sample Response:**\n```json\n")
                f.write(body_str[:1000])
                if len(body_str) > 1000:
                    f.write("\n...(truncated)")
                f.write("\n```\n\n")

                # Try to identify endpoint type
                url_lower = api['url'].lower()
                if 'algolia' in url_lower:
                    f.write("**Type:** Algolia Search API\n\n")
                elif 'graphql' in url_lower:
                    f.write("**Type:** GraphQL API\n\n")
                elif '/api/product' in url_lower:
                    f.write("**Type:** REST Product API\n\n")

        f.write("## All XHR/Fetch Requests\n\n")
        for call in api_calls:
            f.write(f"- {call['method']} `{call['url']}`\n")

        f.write("\n## Files Generated\n\n")
        f.write("- `search_page.html` - Full page HTML\n")
        f.write("- `search_page.png` - Screenshot\n")
        if (debug_dir / 'next_data.json').exists():
            f.write("- `next_data.json` - Next.js embedded data\n")
        if (debug_dir / 'window_search_objects.json').exists():
            f.write("- `window_search_objects.json` - Search-related JS objects\n")
        for i in range(len(product_apis)):
            f.write(f"- `product_api_{i+1}.json` - Product API response\n")

        f.write("\n## Recommendation\n\n")
        if product_apis:
            f.write("✅ **PROCEED WITH API SCRAPING**\n\n")
            f.write("Hidden JSON APIs were discovered. Implement lightweight HTTP scraper instead of browser automation.\n")
        else:
            f.write("⚠️ **PROCEED WITH DOM SCRAPING**\n\n")
            f.write("No product APIs found. Use DOM scraping with correct selectors from saved HTML.\n")

    print(f"✅ Report saved to: {report_path}")
    print(f"✅ All files saved to: {debug_dir}")
    print("\n" + "="*60)

if __name__ == "__main__":
    discover_sobeys_api()
