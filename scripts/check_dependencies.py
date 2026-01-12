#!/usr/bin/env python3
"""
Dependency Checker for Grocery Scraper Project
Validates that all required Python packages and browser dependencies are installed.

Usage: python check_dependencies.py
"""

import sys
import importlib.util
from pathlib import Path


class DependencyChecker:
    """Checks for required Python packages and browser installations."""

    def __init__(self):
        self.missing_packages = []
        self.warnings = []
        self.has_errors = False

    def check_package(self, package_name: str, import_name: str = None) -> bool:
        """
        Check if a Python package is installed.

        Args:
            package_name: Name of the package (e.g., 'beautifulsoup4')
            import_name: Name to use for import (e.g., 'bs4'), defaults to package_name

        Returns:
            True if package is installed, False otherwise
        """
        if import_name is None:
            import_name = package_name

        spec = importlib.util.find_spec(import_name)
        if spec is None:
            self.missing_packages.append(package_name)
            return False
        return True

    def check_playwright_browsers(self) -> bool:
        """
        Check if Playwright browsers (specifically Chromium) are installed.

        Returns:
            True if browsers are installed, False otherwise
        """
        try:
            from playwright.sync_api import sync_playwright

            # Try to get browser path - this will fail if browsers not installed
            with sync_playwright() as p:
                # Attempt to get chromium executable path
                try:
                    browser_path = p.chromium.executable_path
                    return True
                except Exception:
                    return False

        except Exception:
            return False

    def print_header(self, text: str):
        """Print a formatted header."""
        print("\n" + "=" * 70)
        print(f"  {text}")
        print("=" * 70)

    def print_status(self, item: str, status: str, details: str = ""):
        """Print a status line with color."""
        status_symbols = {
            "OK": "[OK]",
            "MISSING": "[X]",
            "WARNING": "[!]"
        }
        symbol = status_symbols.get(status, "[?]")

        if details:
            print(f"  {symbol} {item}: {details}")
        else:
            print(f"  {symbol} {item}")

    def run_checks(self):
        """Run all dependency checks and print results."""
        self.print_header("GROCERY SCRAPER - DEPENDENCY CHECK")

        # Core dependencies (required for all scrapers)
        print("\n1. CORE DEPENDENCIES (Required for all scrapers):")
        print("-" * 70)

        core_packages = [
            ("requests", "requests"),
            ("beautifulsoup4", "bs4"),
            ("lxml", "lxml"),
            ("numpy", "numpy"),
        ]

        for package_name, import_name in core_packages:
            if self.check_package(package_name, import_name):
                self.print_status(package_name, "OK", "installed")
            else:
                self.print_status(package_name, "MISSING", "NOT INSTALLED")
                self.has_errors = True

        # Playwright dependency (required for Safeway/Sobeys)
        print("\n2. PLAYWRIGHT DEPENDENCY (Required for Safeway/Sobeys):")
        print("-" * 70)

        playwright_installed = self.check_package("playwright", "playwright")
        if playwright_installed:
            self.print_status("playwright", "OK", "package installed")

            # Check if browsers are installed
            print("\n   Checking Playwright browsers...")
            if self.check_playwright_browsers():
                self.print_status("Chromium browser", "OK", "installed")
            else:
                self.print_status("Chromium browser", "MISSING", "NOT INSTALLED")
                self.warnings.append("playwright_browsers")
                self.has_errors = True
        else:
            self.print_status("playwright", "MISSING", "NOT INSTALLED")
            self.warnings.append("playwright_package")
            self.has_errors = True

        # Print summary
        self.print_results()

    def print_results(self):
        """Print final results and installation instructions."""
        self.print_header("RESULTS")

        if not self.has_errors:
            print("\n  All dependencies are installed!")
            print("  You're ready to run all scrapers.")
            print("\n  To run a scraper:")
            print("    python scrapers/run.py --site <site_name> --query \"search_term\"")
            return

        # Has errors - print installation instructions
        print("\n  Some dependencies are missing. Follow the instructions below:")
        print()

        if self.missing_packages:
            print("  MISSING PYTHON PACKAGES:")
            for package in self.missing_packages:
                print(f"    - {package}")
            print()

        print("  INSTALLATION OPTIONS:")
        print()
        print("  OPTION 1 - Automated Installation (Recommended):")
        print("  ----------------------------------------------------")
        print("    Run the automated setup script:")
        print("      .\\install_dependencies.ps1")
        print()
        print("  OPTION 2 - Manual Installation:")
        print("  ----------------------------------------------------")

        if self.missing_packages:
            print("    Step 1: Install Python packages:")
            print("      pip install -r requirements.txt")
            print()

        if "playwright_package" in self.warnings or "playwright_browsers" in self.warnings:
            if "playwright_package" in self.warnings:
                print("    Step 2: Install Playwright package:")
                print("      pip install playwright")
                print()
                print("    Step 3: Install Playwright browsers:")
                print("      playwright install chromium")
            else:
                print("    Step 2: Install Playwright browsers:")
                print("      playwright install chromium")
            print()

        print("  SCRAPER COMPATIBILITY:")
        print("  ----------------------------------------------------")
        if not self.missing_packages or all(pkg != "requests" and pkg != "beautifulsoup4" and pkg != "lxml" for pkg in self.missing_packages):
            print("    Without Playwright:")
            print("      - realcanadiansuperstore (WILL WORK)")
            print("      - nofrills (WILL WORK)")
            print("      - safeway (WILL NOT WORK - requires Playwright)")
            print("      - sobeys (WILL NOT WORK - requires Playwright)")
        else:
            print("    Core packages missing - no scrapers will work until installed.")

        print()
        print("  After installation, run this check again:")
        print("    python check_dependencies.py")
        print()


def main():
    """Main entry point."""
    checker = DependencyChecker()
    checker.run_checks()

    # Exit with error code if dependencies are missing
    if checker.has_errors:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
