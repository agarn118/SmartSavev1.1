#!/usr/bin/env python3
"""
API Discovery Script for Sobeys
Manually inspect the page to find hidden APIs
"""

import logging
import json
from pathlib import Path
from playwright.sync_api import sync_playwright
import time

logging.basicConfig(level=logging.INFO)

def discover_apis():
    """Run browser in headful mode to discover APIs"""

    debug_dir = Path("C:/Users/ashto/Desktop/First_claude/test_claude/data/debug/sobeys")
    debug_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()

        # Track network requests
        api_calls = []

        def handle_request(request):
            if any(keyword in request.url.lower() for keyword in ['api', 'graphql', 'search', 'product']):
                api_calls.append({
                    'url': request.url,
                    'method': request.method,
                    'headers': dict(request.headers),
                    'post_data': request.post_data
                })
                logging.info(f"API Call: {request.method} {request.url}")

        def handle_response(response):
            if any(keyword in response.url.lower() for keyword in ['api', 'graphql', 'search', 'product']):
                logging.info(f"API Response: {response.status} {response.url}")
                try:
                    if 'json' in response.headers.get('content-type', ''):
                        body = response.json()
                        # Save sample response
                        filename = response.url.split('/')[-1].split('?')[0] or 'response'
                        response_path = debug_dir / f"api_response_{filename}_{int(time.time())}.json"
                        with open(response_path, 'w', encoding='utf-8') as f:
                            json.dump(body, f, indent=2)
                        logging.info(f"Saved API response to {response_path}")
                except Exception as e:
                    logging.debug(f"Could not parse response: {e}")

        page.on('request', handle_request)
        page.on('response', handle_response)

        # Navigate to search page
        logging.info("Navigating to Sobeys search page...")
        page.goto("https://www.sobeys.com/?query=milk&tab=products", wait_until='networkidle')

        # Wait for user to inspect DevTools
        logging.info("=" * 60)
        logging.info("MANUAL INSPECTION TIME:")
        logging.info("1. Open DevTools (F12)")
        logging.info("2. Go to Network tab")
        logging.info("3. Filter by Fetch/XHR")
        logging.info("4. Scroll the page or click 'Load More'")
        logging.info("5. Look for API calls with product data")
        logging.info("=" * 60)
        logging.info("Waiting 120 seconds for manual inspection...")
        logging.info("The script will automatically save HTML and close after 120s")

        # Save current HTML
        html_path = debug_dir / "search_page.html"
        html_path.write_text(page.content(), encoding='utf-8')
        logging.info(f"Saved HTML to {html_path}")

        # Save screenshot
        screenshot_path = debug_dir / "search_page_screenshot.png"
        page.screenshot(path=str(screenshot_path), full_page=True)
        logging.info(f"Saved screenshot to {screenshot_path}")

        # Check for __NEXT_DATA__
        next_data = page.evaluate("() => window.__NEXT_DATA__")
        if next_data:
            next_data_path = debug_dir / "next_data.json"
            with open(next_data_path, 'w', encoding='utf-8') as f:
                json.dump(next_data, f, indent=2)
            logging.info(f"Saved __NEXT_DATA__ to {next_data_path}")
        else:
            logging.warning("No __NEXT_DATA__ found on page")

        # Wait for manual inspection
        time.sleep(120)

        # Save API calls log
        if api_calls:
            api_log_path = debug_dir / "api_calls_log.json"
            with open(api_log_path, 'w', encoding='utf-8') as f:
                json.dump(api_calls, f, indent=2)
            logging.info(f"Saved {len(api_calls)} API calls to {api_log_path}")

        browser.close()

        # Generate report
        report_path = debug_dir / "api_discovery_report.md"
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# Sobeys API Discovery Report\n\n")
            f.write(f"**Date:** {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("## Summary\n\n")
            f.write(f"- Total API calls detected: {len(api_calls)}\n")
            f.write(f"- __NEXT_DATA__ available: {'Yes' if next_data else 'No'}\n\n")

            f.write("## API Calls Detected\n\n")
            if api_calls:
                for i, call in enumerate(api_calls, 1):
                    f.write(f"### Call {i}\n")
                    f.write(f"- **URL:** `{call['url']}`\n")
                    f.write(f"- **Method:** {call['method']}\n")
                    f.write(f"- **Headers:** See api_calls_log.json\n\n")
            else:
                f.write("No API calls detected during automated capture.\n")
                f.write("Manual inspection via DevTools Network tab is recommended.\n\n")

            f.write("## Files Generated\n\n")
            f.write("- `search_page.html` - Full HTML of search page\n")
            f.write("- `search_page_screenshot.png` - Screenshot of page\n")
            if next_data:
                f.write("- `next_data.json` - __NEXT_DATA__ window object\n")
            if api_calls:
                f.write("- `api_calls_log.json` - All API calls detected\n")
                f.write("- `api_response_*.json` - Sample API responses\n")
            f.write("\n## Recommendation\n\n")
            f.write("Based on automated analysis:\n\n")
            if api_calls:
                f.write("- API endpoints detected - investigate for direct API scraping\n")
            else:
                f.write("- No obvious API detected - proceed with DOM scraping approach\n")

        logging.info(f"Generated report: {report_path}")

if __name__ == "__main__":
    discover_apis()
