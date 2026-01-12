#!/usr/bin/env python3
"""
Base scraper class providing common functionality for all site-specific scrapers.
Handles configuration, checkpointing, deduplication, validation, and output management.
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime

from scrapers.common import (
    load_json_file,
    save_json_file,
    append_jsonl,
    append_jsonl_batch,
    read_jsonl,
    jsonl_to_csv,
    normalize_product_name,
    parse_price,
    get_iso_timestamp,
    ensure_data_directories,
    RateLimiter,
    get_dated_log_path,
    backup_data_file,
    list_backups,
    restore_backup,
    purge_debug_files,
    cleanup_workspace
)


# =============================================================================
# DATA MODELS
# =============================================================================

@dataclass
class ProductRecord:
    """Standard product record schema (consistent across all sites)."""
    store: str
    site_slug: str
    source_url: str
    scrape_ts: str
    external_id: Optional[str]
    name: str
    brand: Optional[str]
    size_text: Optional[str]
    price: Optional[float]
    currency: str
    unit_price: Optional[float]
    unit_price_uom: Optional[str]
    image_url: Optional[str]
    category_path: Optional[str]
    availability: str  # "in_stock" | "out_of_stock" | "unknown"
    query_category: Optional[str]  # Search query that found this product
    raw_source: Optional[Dict]  # Minimal structured snippet

    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def dedupe_key(self) -> str:
        """
        Generate deduplication key.
        Uses external_id if available, otherwise normalized name+size+store.
        """
        if self.external_id:
            return f"{self.site_slug}:{self.external_id}"

        # Fallback: normalize name+size+store
        normalized_name = normalize_product_name(self.name or "")
        normalized_size = normalize_product_name(self.size_text or "")
        return f"{self.site_slug}:{normalized_name}:{normalized_size}:{self.store}"

    def validate(self) -> bool:
        """
        Validate required fields and data types.
        Returns True if valid, False otherwise.
        """
        try:
            # Required string fields
            if not self.store or not isinstance(self.store, str):
                logging.warning("Invalid store field")
                return False

            if not self.site_slug or not isinstance(self.site_slug, str):
                logging.warning("Invalid site_slug field")
                return False

            if not self.name or not isinstance(self.name, str):
                logging.warning("Invalid name field")
                return False

            # Validate price if present
            if self.price is not None:
                if not isinstance(self.price, (int, float)) or self.price < 0:
                    logging.warning(f"Invalid price: {self.price}")
                    return False

            # Validate unit_price if present
            if self.unit_price is not None:
                if not isinstance(self.unit_price, (int, float)) or self.unit_price < 0:
                    logging.warning(f"Invalid unit_price: {self.unit_price}")
                    return False

            # Validate availability enum
            valid_availability = ["in_stock", "out_of_stock", "unknown"]
            if self.availability not in valid_availability:
                logging.warning(f"Invalid availability: {self.availability}")
                return False

            return True

        except Exception as e:
            logging.error(f"Validation error: {e}")
            return False


# =============================================================================
# CHECKPOINT MANAGER
# =============================================================================

class CheckpointManager:
    """Manages scraping checkpoints for resumability."""

    def __init__(self, checkpoint_path: Path):
        self.checkpoint_path = checkpoint_path

    def save(self, data: Dict):
        """Save checkpoint data."""
        data['last_updated'] = get_iso_timestamp()
        save_json_file(self.checkpoint_path, data)
        logging.debug(f"Checkpoint saved: {self.checkpoint_path}")

    def load(self) -> Dict:
        """Load checkpoint data. Returns empty dict if not found."""
        if not self.checkpoint_path.exists():
            logging.info("No checkpoint found. Starting fresh.")
            return {}

        try:
            checkpoint = load_json_file(self.checkpoint_path)
            logging.info(f"Loaded checkpoint from {self.checkpoint_path}")
            return checkpoint
        except Exception as e:
            logging.warning(f"Failed to load checkpoint: {e}. Starting fresh.")
            return {}

    def clear(self):
        """Clear checkpoint file."""
        if self.checkpoint_path.exists():
            self.checkpoint_path.unlink()
            logging.info(f"Checkpoint cleared: {self.checkpoint_path}")


# =============================================================================
# BASE SCRAPER
# =============================================================================

class BaseScraper(ABC):
    """
    Abstract base class for all site-specific scrapers.
    Provides common functionality: config, paths, checkpoints, dedupe, validation.
    """

    def __init__(self, config_path: Path, project_root: Path, fresh_start: bool = False):
        """
        Initialize base scraper.

        Args:
            config_path: Path to site-specific config JSON
            project_root: Project root directory
            fresh_start: If True, archive and clear data files for a completely fresh scrape
        """
        self.project_root = project_root
        self.config_path = config_path
        self.config = self._load_config()
        self.fresh_start = fresh_start

        # Site metadata
        self.site_slug = self.config['site_slug']
        self.store_name = self.config.get('store_name', self.site_slug)

        # Setup paths
        ensure_data_directories(self.project_root)
        self._setup_paths()

        # Backup existing data before starting (and optionally clear for fresh start)
        self._backup_existing_data()

        # Initialize components
        self.rate_limiter = RateLimiter(
            min_delay=self.config.get('min_delay_seconds', 2.0),
            max_delay=self.config.get('max_delay_seconds', 5.0),
            requests_per_minute=self.config.get('max_requests_per_minute', 15)
        )

        self.checkpoint_manager = CheckpointManager(self.checkpoint_path)

        # Deduplication tracking
        self.seen_keys: Set[str] = set()

        # Statistics
        self.stats = {
            'total_scraped': 0,
            'duplicates_skipped': 0,
            'invalid_records': 0,
            'pages_processed': 0,
            'errors': 0
        }

    def _load_config(self) -> Dict:
        """Load site-specific configuration."""
        try:
            config = load_json_file(self.config_path)
            logging.info(f"Loaded config from {self.config_path}")
            return config
        except Exception as e:
            logging.error(f"Failed to load config: {e}")
            raise

    def _setup_paths(self):
        """Setup output paths under data/ directory."""
        data_root = self.project_root / 'data'

        # Raw data directory for this site
        self.raw_data_dir = data_root / 'raw' / self.site_slug
        self.raw_data_dir.mkdir(parents=True, exist_ok=True)

        # Output files
        self.jsonl_path = self.raw_data_dir / f"{self.site_slug}_products.jsonl"
        self.csv_path = self.raw_data_dir / f"{self.site_slug}_products.csv"

        # Log file (dated for automatic rotation)
        logs_dir = self.project_root / 'logs'
        self.log_path = get_dated_log_path(self.site_slug, logs_dir)

        # Checkpoint file
        checkpoints_dir = self.project_root / 'checkpoints'
        self.checkpoint_path = checkpoints_dir / f"{self.site_slug}_checkpoint.json"

    def _backup_existing_data(self):
        """
        Create timestamped backup of existing data file before starting new scrape run.
        If fresh_start is True, also clears the data files after backup.
        """
        # Get backup settings from config (with sensible defaults)
        max_backups = self.config.get('max_backups', 5)
        compress_backups = self.config.get('compress_backups', False)

        backup_data_file(self.jsonl_path, max_backups=max_backups, compress=compress_backups)
        # Also backup CSV if it exists
        backup_data_file(self.csv_path, max_backups=max_backups, compress=compress_backups)

        # For fresh start, clear the data files after backup
        if self.fresh_start:
            if self.jsonl_path.exists():
                self.jsonl_path.unlink()
                logging.info(f"Cleared data file for fresh start: {self.jsonl_path.name}")
            if self.csv_path.exists():
                self.csv_path.unlink()
                logging.info(f"Cleared CSV file for fresh start: {self.csv_path.name}")

    def load_checkpoint(self) -> Dict:
        """Load checkpoint data."""
        checkpoint = self.checkpoint_manager.load()

        # Load seen keys for deduplication
        if 'seen_keys' in checkpoint:
            self.seen_keys = set(checkpoint['seen_keys'])
            logging.info(f"Loaded {len(self.seen_keys)} seen keys from checkpoint")

        return checkpoint

    def save_checkpoint(self, additional_data: Optional[Dict] = None):
        """
        Save checkpoint with current state.

        Args:
            additional_data: Optional additional data to save
        """
        checkpoint_data = {
            'seen_keys': list(self.seen_keys),
            'stats': self.stats
        }

        if additional_data:
            checkpoint_data.update(additional_data)

        self.checkpoint_manager.save(checkpoint_data)

    def is_duplicate(self, record: ProductRecord) -> bool:
        """
        Check if record is a duplicate based on dedupe key.

        Args:
            record: Product record to check

        Returns:
            True if duplicate, False otherwise
        """
        dedupe_key = record.dedupe_key()

        if dedupe_key in self.seen_keys:
            return True

        self.seen_keys.add(dedupe_key)
        return False

    def save_record(self, record: ProductRecord) -> bool:
        """
        Validate, deduplicate, and save a single record.

        Args:
            record: Product record to save

        Returns:
            True if saved, False if skipped (duplicate/invalid)
        """
        # Validate
        if not record.validate():
            self.stats['invalid_records'] += 1
            logging.warning(f"Invalid record skipped: {record.name}")
            return False

        # Check duplicate
        if self.is_duplicate(record):
            self.stats['duplicates_skipped'] += 1
            logging.debug(f"Duplicate skipped: {record.name}")
            return False

        # Save to JSONL
        try:
            append_jsonl(self.jsonl_path, record.to_dict())
            self.stats['total_scraped'] += 1
            logging.debug(f"Saved: {record.name}")
            return True
        except Exception as e:
            logging.error(f"Failed to save record: {e}")
            self.stats['errors'] += 1
            return False

    def save_records_batch(self, records: List[ProductRecord]) -> int:
        """
        Save multiple records efficiently.

        Args:
            records: List of product records

        Returns:
            Number of records successfully saved
        """
        saved_count = 0

        # Filter valid and non-duplicate records
        valid_records = []
        for record in records:
            if not record.validate():
                self.stats['invalid_records'] += 1
                continue

            if self.is_duplicate(record):
                self.stats['duplicates_skipped'] += 1
                continue

            valid_records.append(record)

        # Batch write
        if valid_records:
            try:
                records_dict = [r.to_dict() for r in valid_records]
                append_jsonl_batch(self.jsonl_path, records_dict)
                saved_count = len(valid_records)
                self.stats['total_scraped'] += saved_count
                logging.info(f"Batch saved {saved_count} records")
            except Exception as e:
                logging.error(f"Failed to save batch: {e}")
                self.stats['errors'] += 1

        return saved_count

    def export_to_csv(self):
        """Export JSONL to CSV format."""
        try:
            jsonl_to_csv(self.jsonl_path, self.csv_path)
            logging.info(f"Exported to CSV: {self.csv_path}")
        except Exception as e:
            logging.error(f"Failed to export CSV: {e}")

    def print_stats(self):
        """Print scraping statistics."""
        logging.info("=" * 60)
        logging.info("SCRAPING STATISTICS")
        logging.info("=" * 60)
        for key, value in self.stats.items():
            logging.info(f"{key}: {value}")
        logging.info("=" * 60)

    # =========================================================================
    # ABSTRACT METHODS (must be implemented by subclasses)
    # =========================================================================

    @abstractmethod
    def scrape_category(self, category_url: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from a category page.

        Args:
            category_url: Category page URL or path
            max_pages: Maximum number of pages to scrape (None = all)

        Returns:
            Number of products scraped
        """
        pass

    @abstractmethod
    def scrape_search(self, query: str, max_pages: Optional[int] = None) -> int:
        """
        Scrape products from search results.

        Args:
            query: Search query string
            max_pages: Maximum number of pages to scrape (None = all)

        Returns:
            Number of products scraped
        """
        pass

    @abstractmethod
    def scrape_product_page(self, product_url: str) -> Optional[ProductRecord]:
        """
        Scrape a single product detail page.

        Args:
            product_url: Product page URL

        Returns:
            ProductRecord if successful, None otherwise
        """
        pass
