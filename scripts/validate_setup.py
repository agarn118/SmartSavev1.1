#!/usr/bin/env python3
"""
Setup Validation Script for Grocery Scrapers

This script validates that your environment is correctly configured to run the scrapers.
It checks:
- Python version
- All required dependencies
- Scrapers module can be imported
- Playwright browsers installed
- Current working directory
- File structure

Run this before attempting to use the scrapers.
"""

import sys
import os
from pathlib import Path
import importlib.util


class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(message):
    """Print a formatted header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}  {message}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 70}{Colors.ENDC}\n")


def print_success(message):
    """Print a success message."""
    print(f"{Colors.OKGREEN}[OK] {message}{Colors.ENDC}")


def print_fail(message):
    """Print a failure message."""
    print(f"{Colors.FAIL}[FAIL] {message}{Colors.ENDC}")


def print_warning(message):
    """Print a warning message."""
    print(f"{Colors.WARNING}[WARNING] {message}{Colors.ENDC}")


def print_info(message):
    """Print an info message."""
    print(f"{Colors.OKCYAN}[INFO] {message}{Colors.ENDC}")


def check_python_version():
    """Check if Python version meets requirements."""
    print_header("Checking Python Version")

    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"

    if version.major >= 3 and version.minor >= 8:
        print_success(f"Python {version_str} (requires 3.8+)")
        return True
    else:
        print_fail(f"Python {version_str} is too old (requires 3.8+)")
        print_info("Please upgrade Python: https://www.python.org/downloads/")
        return False


def check_directory():
    """Check if current directory is project root."""
    print_header("Checking Working Directory")

    cwd = Path.cwd()
    required_files = ['scrapers', 'configs', 'requirements.txt']

    missing = [f for f in required_files if not (cwd / f).exists()]

    if not missing:
        print_success(f"Current directory is correct: {cwd}")
        return True
    else:
        print_fail("You are not in the project root directory")
        print_info(f"Current directory: {cwd}")
        print_info(f"Missing: {', '.join(missing)}")
        print_info("\nNavigate to the project root:")
        print_info("  cd C:\\Users\\ashto\\Desktop\\First_claude\\test_claude")
        return False


def check_module_import(module_name):
    """Check if a module can be imported."""
    try:
        __import__(module_name)
        return True
    except ImportError:
        return False


def check_dependencies():
    """Check if all required Python packages are installed."""
    print_header("Checking Python Dependencies")

    required_packages = {
        'requests': 'requests',
        'beautifulsoup4': 'bs4',
        'lxml': 'lxml',
        'playwright': 'playwright',
    }

    all_installed = True

    for package_name, import_name in required_packages.items():
        if check_module_import(import_name):
            print_success(f"{package_name} is installed")
        else:
            print_fail(f"{package_name} is NOT installed")
            all_installed = False

    if not all_installed:
        print_info("\nInstall missing packages:")
        print_info("  pip install -r requirements.txt")

    return all_installed


def check_scrapers_module():
    """Check if scrapers module can be imported."""
    print_header("Checking Scrapers Module")

    try:
        import scrapers
        from scrapers.common import setup_logging
        from scrapers.base import BaseScraper

        print_success("scrapers module can be imported")
        print_success("scrapers.common accessible")
        print_success("scrapers.base accessible")
        return True

    except ModuleNotFoundError as e:
        print_fail(f"Cannot import scrapers module: {e}")
        print_info("\nThis usually means:")
        print_info("  1. You're not in the project root directory")
        print_info("  2. The 'scrapers' folder is missing or corrupted")
        print_info("\nTo fix:")
        print_info("  1. Navigate to project root:")
        print_info("     cd C:\\Users\\ashto\\Desktop\\First_claude\\test_claude")
        print_info("  2. Verify 'scrapers' folder exists")
        print_info("  3. Run scrapers using: python -m scrapers.run")
        return False


def check_scrapers_installed():
    """Check if individual scraper modules exist."""
    print_header("Checking Scraper Implementations")

    scrapers_dir = Path('scrapers/sites')
    if not scrapers_dir.exists():
        print_fail("scrapers/sites directory not found")
        return False

    expected_scrapers = [
        'realcanadiansuperstore',
        'nofrills',
        'safeway',
        'sobeys'
    ]

    all_exist = True
    for scraper in expected_scrapers:
        scraper_file = scrapers_dir / f"{scraper}.py"
        if scraper_file.exists():
            print_success(f"{scraper}.py exists")
        else:
            print_fail(f"{scraper}.py is missing")
            all_exist = False

    return all_exist


def check_playwright_browsers():
    """Check if Playwright browsers are installed."""
    print_header("Checking Playwright Browsers")

    try:
        from playwright.sync_api import sync_playwright

        # Try to launch a browser to verify installation
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()

            print_success("Playwright Chromium browser is installed and working")
            return True

        except Exception as e:
            print_fail("Playwright Chromium browser is NOT installed")
            print_info(f"Error: {e}")
            print_info("\nInstall Playwright browsers:")
            print_info("  playwright install chromium")
            return False

    except ImportError:
        print_warning("Playwright package not installed (skipping browser check)")
        print_info("Playwright is only needed for Safeway and Sobeys scrapers")
        return None  # Not a critical failure


def check_configs():
    """Check if configuration files exist."""
    print_header("Checking Configuration Files")

    configs_dir = Path('configs')
    if not configs_dir.exists():
        print_fail("configs directory not found")
        return False

    expected_configs = [
        'realcanadiansuperstore.json',
        'nofrills.json',
        'safeway.json',
        'sobeys.json'
    ]

    all_exist = True
    for config in expected_configs:
        config_file = configs_dir / config
        if config_file.exists():
            print_success(f"{config} exists")
        else:
            print_fail(f"{config} is missing")
            all_exist = False

    return all_exist


def print_usage_instructions(all_passed):
    """Print usage instructions based on validation results."""
    print_header("Setup Validation Complete")

    if all_passed:
        print_success("All checks passed! You're ready to run the scrapers.\n")
        print(f"{Colors.BOLD}How to run a scraper:{Colors.ENDC}\n")
        print(f"{Colors.OKGREEN}  python -m scrapers.run --site realcanadiansuperstore --query 'milk' --max-pages 1{Colors.ENDC}\n")
        print(f"{Colors.BOLD}Available sites:{Colors.ENDC}")
        print("  - realcanadiansuperstore")
        print("  - nofrills")
        print("  - safeway")
        print("  - sobeys")
        print(f"\n{Colors.BOLD}Common commands:{Colors.ENDC}")
        print("  Search: --query 'search term' --max-pages N")
        print("  Category: --category-url '/path/to/category' --max-pages N")
        print("  CSV output: --output-format csv")
        print(f"\nFor help: {Colors.OKCYAN}python -m scrapers.run --help{Colors.ENDC}\n")
    else:
        print_fail("Some checks failed. Please fix the issues above before running scrapers.\n")
        print(f"{Colors.WARNING}Common fixes:{Colors.ENDC}")
        print("  1. Install dependencies: pip install -r requirements.txt")
        print("  2. Install Playwright browsers: playwright install chromium")
        print("  3. Navigate to project root: cd C:\\Users\\ashto\\Desktop\\First_claude\\test_claude")
        print()


def main():
    """Run all validation checks."""
    print(f"\n{Colors.BOLD}Grocery Scraper Setup Validation{Colors.ENDC}")
    print(f"Validating environment configuration...\n")

    checks = [
        check_python_version(),
        check_directory(),
        check_dependencies(),
        check_scrapers_module(),
        check_scrapers_installed(),
        check_configs(),
    ]

    # Playwright is optional (only for Safeway/Sobeys)
    playwright_ok = check_playwright_browsers()
    if playwright_ok is not None:
        checks.append(playwright_ok)

    all_passed = all(checks)

    print_usage_instructions(all_passed)

    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())
