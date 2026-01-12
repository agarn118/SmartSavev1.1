#!/usr/bin/env python3
"""
Grocery Scraper Launcher
Runs multiple scrapers in parallel, each in its own terminal window.

Usage:
    python run_scrapers.py                      # Default: all sites, query="milk", fresh start
    python run_scrapers.py --query bread        # Custom query for all sites
    python run_scrapers.py --sites safeway,sobeys  # Specific sites only
    python run_scrapers.py --sequential         # Run one at a time (not parallel)
    python run_scrapers.py --no-fresh           # Keep existing data (append mode)
"""

import subprocess
import sys
import argparse
import time
from pathlib import Path

# Configuration
VENV_ACTIVATE = r"C:\Users\ashto\Desktop\First_claude\.venv\Scripts\activate.bat"
PROJECT_ROOT = Path(__file__).parent

SITES = [
    "safeway",
    "sobeys",
    "nofrills",
    "realcanadiansuperstore",
]

# Comprehensive grocery queries (113 queries)
COMMON_QUERIES = [
    # Dairy & Eggs (15)
    "milk", "skim milk", "2% milk", "whole milk", "almond milk",
    "soy milk", "oat milk", "butter", "margarine", "yogurt",
    "greek yogurt", "cheese", "cheddar cheese", "mozzarella", "cream cheese",
    # Eggs & Bread (10)
    "eggs", "bread", "white bread", "whole wheat bread", "bagels",
    "english muffins", "tortillas", "croissants", "buns", "pita bread",
    # Pasta & Grains (10)
    "pasta", "spaghetti", "macaroni", "penne", "rice",
    "white rice", "brown rice", "flour", "quinoa", "oatmeal",
    # Baking & Sweeteners (10)
    "sugar", "brown sugar", "baking soda", "baking powder", "salt",
    "pepper", "vanilla extract", "honey", "maple syrup", "jam",
    # Oils & Condiments (12)
    "olive oil", "vegetable oil", "canola oil", "ketchup", "mustard",
    "mayonnaise", "salad dressing", "ranch dressing", "soy sauce", "hot sauce",
    "vinegar", "bbq sauce",
    # Canned & Jarred (10)
    "canned soup", "tomato sauce", "pasta sauce", "canned beans", "canned tomatoes",
    "canned corn", "chicken broth", "beef broth", "pickles", "olives",
    # Breakfast & Snacks (10)
    "cereal", "corn flakes", "granola", "peanut butter", "nutella",
    "chips", "crackers", "cookies", "popcorn", "pretzels",
    # Beverages (10)
    "coffee", "tea", "juice", "orange juice", "apple juice",
    "soda", "water", "sparkling water", "energy drinks", "sports drinks",
    # Frozen Foods (8)
    "ice cream", "frozen pizza", "frozen vegetables", "frozen fruit",
    "frozen waffles", "frozen meals", "frozen fries", "frozen berries",
    # Meat & Seafood (10)
    "chicken breast", "ground beef", "bacon", "sausage", "ham",
    "deli meat", "hot dogs", "salmon", "tuna", "shrimp",
    # Fresh Produce (15)
    "apples", "bananas", "oranges", "grapes", "strawberries",
    "blueberries", "tomatoes", "lettuce", "spinach", "carrots",
    "potatoes", "onions", "garlic", "peppers", "cucumbers",
    # Household (8)
    "paper towels", "toilet paper", "dish soap", "laundry detergent",
    "trash bags", "aluminum foil", "plastic wrap", "napkins",
]


def launch_scraper_window(site: str, query: str, max_pages: int = 10, fresh: bool = True):
    """Launch a scraper in a new terminal window."""
    fresh_flag = "--fresh" if fresh else ""
    cmd = f'cd /d "{PROJECT_ROOT}" && call "{VENV_ACTIVATE}" && python -m scrapers.run --site {site} --query "{query}" --max-pages {max_pages} {fresh_flag} && echo. && echo Done! Press any key... && pause >nul'

    subprocess.Popen(
        f'start "{site.title()} Scraper" cmd /k "{cmd}"',
        shell=True
    )


def launch_all_parallel(sites: list, query: str, max_pages: int = 10, delay: float = 1.0, fresh: bool = True):
    """Launch all scrapers in parallel (separate windows)."""
    print("=" * 50)
    print("  Grocery Scraper Launcher - Parallel Mode")
    print("=" * 50)
    print(f"Query: {query}")
    print(f"Sites: {', '.join(sites)}")
    print(f"Max Pages: {max_pages}")
    print(f"Fresh Start: {'Yes' if fresh else 'No'}")
    print()

    for site in sites:
        print(f"Launching {site}...")
        launch_scraper_window(site, query, max_pages, fresh)
        time.sleep(delay)

    print()
    print("All scrapers launched!")
    print("Check each terminal window for progress.")


def run_sequential(sites: list, query: str, max_pages: int = 10, fresh: bool = True):
    """Run scrapers one at a time in the current terminal."""
    print("=" * 50)
    print("  Grocery Scraper - Sequential Mode")
    print("=" * 50)
    print(f"Query: {query}")
    print(f"Sites: {', '.join(sites)}")
    print(f"Fresh Start: {'Yes' if fresh else 'No'}")
    print()

    for site in sites:
        print(f"\n{'='*50}")
        print(f"Running {site} scraper...")
        print("=" * 50)

        cmd = [sys.executable, "-m", "scrapers.run", "--site", site, "--query", query, "--max-pages", str(max_pages)]
        if fresh:
            cmd.append("--fresh")

        result = subprocess.run(cmd, cwd=PROJECT_ROOT)

        if result.returncode != 0:
            print(f"Warning: {site} scraper exited with code {result.returncode}")

    print("\n" + "=" * 50)
    print("All scrapers completed!")
    print("=" * 50)


def run_multi_query(sites: list, queries: list, max_pages: int = 5, fresh: bool = True):
    """Run scrapers for multiple queries (useful for comprehensive data collection)."""
    print("=" * 50)
    print("  Grocery Scraper - Multi-Query Mode")
    print("=" * 50)
    print(f"Queries: {', '.join(queries)}")
    print(f"Sites: {', '.join(sites)}")
    print(f"Fresh Start: {'Yes (first query only)' if fresh else 'No'}")
    print()

    total_runs = len(sites) * len(queries)
    current = 0
    first_run = True

    for query in queries:
        for site in sites:
            current += 1
            print(f"\n[{current}/{total_runs}] {site} - '{query}'")
            print("-" * 40)

            cmd = [sys.executable, "-m", "scrapers.run", "--site", site, "--query", query, "--max-pages", str(max_pages)]
            # Only use --fresh on first run for each site to avoid clearing data between queries
            if fresh and first_run:
                cmd.append("--fresh")

            result = subprocess.run(cmd, cwd=PROJECT_ROOT)

            # Small delay between runs
            time.sleep(2)

        first_run = False  # Only fresh start on first query

    print("\n" + "=" * 50)
    print(f"Completed {total_runs} scraper runs!")
    print("=" * 50)


def main():
    parser = argparse.ArgumentParser(description="Launch grocery scrapers")
    parser.add_argument("--query", "-q", default="milk", help="Search query (default: milk)")
    parser.add_argument("--sites", "-s", default=",".join(SITES), help="Comma-separated list of sites")
    parser.add_argument("--max-pages", "-m", type=int, default=10, help="Max pages per scraper (default: 10)")
    parser.add_argument("--sequential", action="store_true", help="Run scrapers one at a time")
    parser.add_argument("--multi-query", action="store_true", help="Run common grocery queries")
    parser.add_argument("--no-fresh", action="store_true", help="Keep existing data (append mode instead of fresh start)")

    args = parser.parse_args()

    sites = [s.strip() for s in args.sites.split(",")]
    fresh = not args.no_fresh  # Default is fresh=True

    if args.multi_query:
        run_multi_query(sites, COMMON_QUERIES, args.max_pages, fresh)
    elif args.sequential:
        run_sequential(sites, args.query, args.max_pages, fresh)
    else:
        launch_all_parallel(sites, args.query, args.max_pages, fresh=fresh)


if __name__ == "__main__":
    main()
