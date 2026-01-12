#!/usr/bin/env python3
"""
No Frills scraper (Loblaw Network).
Inherits from Real Canadian Superstore scraper as they share the same platform.
"""

from scrapers.sites.realcanadiansuperstore import RealcanadiansuperstoreScraper


class NofrillsScraper(RealcanadiansuperstoreScraper):
    """
    Scraper for No Frills.
    Uses same implementation as Real Canadian Superstore (both Loblaw network).
    Only difference is base URL and store name (configured in config file).
    """
    pass
