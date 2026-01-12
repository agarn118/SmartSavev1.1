#!/usr/bin/env python3
"""
Common utilities for all scrapers.
Provides rate limiting, retries, file operations, and logging setup.
"""

import time
import random
import json
import csv
import logging
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Callable, Any
from functools import wraps
from datetime import datetime


# =============================================================================
# RATE LIMITING
# =============================================================================

class RateLimiter:
    """
    Rate limiter with jitter to avoid detection patterns.
    Implements adaptive delays and request tracking.
    """

    def __init__(self, min_delay: float = 2.0, max_delay: float = 5.0,
                 requests_per_minute: int = 15):
        """
        Initialize rate limiter.

        Args:
            min_delay: Minimum delay between requests (seconds)
            max_delay: Maximum delay between requests (seconds)
            requests_per_minute: Maximum requests per minute threshold
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.requests_per_minute = requests_per_minute
        self.request_times: List[float] = []

    def wait(self):
        """Wait with jittered delay and enforce rate limits."""
        # Add jitter to avoid pattern detection
        delay = random.uniform(self.min_delay, self.max_delay)

        # Check if we're exceeding rate limit
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 60]

        if len(self.request_times) >= self.requests_per_minute:
            # Wait until oldest request falls outside 1-minute window
            oldest = self.request_times[0]
            wait_time = 60 - (now - oldest)
            if wait_time > 0:
                logging.warning(f"Rate limit reached. Waiting {wait_time:.1f}s")
                time.sleep(wait_time + delay)
        else:
            time.sleep(delay)

        self.request_times.append(time.time())

    def adaptive_wait(self, error_count: int = 0):
        """
        Adaptive wait that increases delay based on errors.

        Args:
            error_count: Number of consecutive errors encountered
        """
        if error_count > 0:
            # Exponential backoff for errors
            backoff_delay = min(self.max_delay * (2 ** error_count), 300)  # Max 5 min
            logging.warning(f"Adaptive backoff: {backoff_delay:.1f}s (errors: {error_count})")
            time.sleep(backoff_delay)
        else:
            self.wait()


# =============================================================================
# RETRY LOGIC
# =============================================================================

def retry_on_exception(max_retries: int = 3,
                       backoff_base: float = 2.0,
                       exceptions: tuple = (Exception,)):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_base: Base for exponential backoff calculation
        exceptions: Tuple of exception types to catch and retry
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt < max_retries:
                        wait_time = backoff_base ** attempt
                        logging.warning(
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. "
                            f"Retrying in {wait_time:.1f}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(
                            f"{func.__name__} failed after {max_retries + 1} attempts: {e}"
                        )

            raise last_exception

        return wrapper
    return decorator


# =============================================================================
# FILE OPERATIONS
# =============================================================================

def append_jsonl(file_path: Path, record: Dict):
    """
    Atomically append a single JSONL record to file.
    Creates parent directories if needed.

    Args:
        file_path: Path to JSONL file
        record: Dictionary to append as JSON line
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'a', encoding='utf-8') as f:
        json.dump(record, f, ensure_ascii=False)
        f.write('\n')


def append_jsonl_batch(file_path: Path, records: List[Dict]):
    """
    Atomically append multiple JSONL records.

    Args:
        file_path: Path to JSONL file
        records: List of dictionaries to append
    """
    if not records:
        return

    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'a', encoding='utf-8') as f:
        for record in records:
            json.dump(record, f, ensure_ascii=False)
            f.write('\n')


def read_jsonl(file_path: Path) -> List[Dict]:
    """
    Read all records from a JSONL file.

    Args:
        file_path: Path to JSONL file

    Returns:
        List of dictionaries
    """
    if not file_path.exists():
        return []

    records = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logging.warning(f"Invalid JSON line in {file_path}: {e}")

    return records


def jsonl_to_csv(jsonl_path: Path, csv_path: Path,
                 fieldnames: Optional[List[str]] = None):
    """
    Convert JSONL file to CSV format.

    Args:
        jsonl_path: Source JSONL file path
        csv_path: Destination CSV file path
        fieldnames: List of field names (auto-detected if None)
    """
    records = read_jsonl(jsonl_path)

    if not records:
        logging.warning(f"No records to convert from {jsonl_path}")
        return

    # Auto-detect fieldnames from first record if not provided
    if fieldnames is None:
        fieldnames = list(records[0].keys())

    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for record in records:
            # Flatten nested objects for CSV
            flat_record = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    flat_record[key] = json.dumps(value, ensure_ascii=False)
                else:
                    flat_record[key] = value

            writer.writerow(flat_record)

    logging.info(f"Converted {len(records)} records to {csv_path}")


def load_json_file(file_path: Path) -> Dict:
    """
    Load JSON file with error handling.

    Args:
        file_path: Path to JSON file

    Returns:
        Parsed JSON as dictionary
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        raise
    except json.JSONDecodeError as e:
        logging.error(f"Invalid JSON in {file_path}: {e}")
        raise


def save_json_file(file_path: Path, data: Dict, indent: int = 2):
    """
    Save dictionary to JSON file with pretty printing.

    Args:
        file_path: Path to JSON file
        data: Dictionary to save
        indent: JSON indentation level
    """
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def backup_data_file(file_path: Path, max_backups: int = 5, compress: bool = False) -> Optional[Path]:
    """
    Create a timestamped backup of an existing data file with retention policy.
    Backups are named with timestamp (e.g., sobeys_products_2025-12-30_143022.jsonl).
    Old backups beyond max_backups are automatically cleaned up.

    Args:
        file_path: Path to the data file to backup
        max_backups: Maximum number of backups to retain (default: 5)
        compress: Whether to gzip compress the backup (default: False)

    Returns:
        Path to created backup, or None if source file doesn't exist
    """
    if not file_path.exists():
        logging.debug(f"No existing file to backup: {file_path}")
        return None

    # Create timestamped backup filename
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    backup_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"

    # Create backups subdirectory
    backup_dir = file_path.parent / 'backups'
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / backup_name

    try:
        if compress:
            import gzip
            backup_path = backup_path.with_suffix(backup_path.suffix + '.gz')
            with open(file_path, 'rb') as f_in:
                with gzip.open(backup_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(file_path, backup_path)

        logging.info(f"Created backup: {backup_path}")

        # Clean up old backups beyond retention limit
        cleanup_old_backups(file_path, max_backups)

        return backup_path
    except Exception as e:
        logging.error(f"Failed to create backup of {file_path}: {e}")
        return None


def cleanup_old_backups(file_path: Path, max_backups: int = 5) -> int:
    """
    Remove old backups beyond the retention limit, keeping the most recent ones.

    Args:
        file_path: Path to the original data file (backups are in sibling 'backups' dir)
        max_backups: Maximum number of backups to retain

    Returns:
        Number of backups removed
    """
    backup_dir = file_path.parent / 'backups'
    if not backup_dir.exists():
        return 0

    # Find all backups for this file (with or without .gz extension)
    pattern = f"{file_path.stem}_*{file_path.suffix}*"
    backups = sorted(backup_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

    removed_count = 0
    for old_backup in backups[max_backups:]:
        try:
            old_backup.unlink()
            logging.debug(f"Removed old backup: {old_backup.name}")
            removed_count += 1
        except Exception as e:
            logging.warning(f"Failed to remove old backup {old_backup}: {e}")

    if removed_count > 0:
        logging.info(f"Cleaned up {removed_count} old backup(s)")

    return removed_count


def list_backups(file_path: Path) -> List[Dict]:
    """
    List all available backups for a data file.

    Args:
        file_path: Path to the original data file

    Returns:
        List of dicts with backup info (path, size, modified time)
    """
    backup_dir = file_path.parent / 'backups'
    if not backup_dir.exists():
        return []

    pattern = f"{file_path.stem}_*{file_path.suffix}*"
    backups = sorted(backup_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)

    result = []
    for backup in backups:
        stat = backup.stat()
        result.append({
            'path': backup,
            'name': backup.name,
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'compressed': backup.suffix == '.gz'
        })

    return result


def restore_backup(backup_path: Path, target_path: Path = None) -> bool:
    """
    Restore a data file from a backup.

    Args:
        backup_path: Path to the backup file
        target_path: Where to restore (defaults to original location without timestamp)

    Returns:
        True if restored successfully, False otherwise
    """
    if not backup_path.exists():
        logging.error(f"Backup file not found: {backup_path}")
        return False

    # Determine target path if not provided
    if target_path is None:
        # Extract original filename by removing timestamp
        # e.g., sobeys_products_2025-12-30_143022.jsonl -> sobeys_products.jsonl
        name = backup_path.name
        if backup_path.suffix == '.gz':
            name = backup_path.stem  # Remove .gz

        # Remove timestamp portion (_YYYY-MM-DD_HHMMSS)
        import re
        original_name = re.sub(r'_\d{4}-\d{2}-\d{2}_\d{6}', '', name)
        target_path = backup_path.parent.parent / original_name

    try:
        if backup_path.suffix == '.gz':
            import gzip
            with gzip.open(backup_path, 'rb') as f_in:
                with open(target_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:
            shutil.copy2(backup_path, target_path)

        logging.info(f"Restored backup to: {target_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to restore backup: {e}")
        return False


def purge_debug_files(debug_dir: Path, older_than_days: int = 0) -> int:
    """
    Purge debug/temporary files from a directory.

    Args:
        debug_dir: Directory containing debug files to purge
        older_than_days: Only purge files older than this many days (0 = all files)

    Returns:
        Number of files purged
    """
    if not debug_dir.exists():
        logging.debug(f"Debug directory does not exist: {debug_dir}")
        return 0

    cutoff_time = None
    if older_than_days > 0:
        cutoff_time = datetime.now().timestamp() - (older_than_days * 86400)

    purged_count = 0
    for file_path in debug_dir.rglob('*'):
        if file_path.is_file():
            if cutoff_time is None or file_path.stat().st_mtime < cutoff_time:
                try:
                    file_path.unlink()
                    purged_count += 1
                except Exception as e:
                    logging.warning(f"Failed to purge {file_path}: {e}")

    # Remove empty directories
    for dir_path in sorted(debug_dir.rglob('*'), reverse=True):
        if dir_path.is_dir():
            try:
                dir_path.rmdir()  # Only removes if empty
            except OSError:
                pass  # Directory not empty

    # Try to remove the debug_dir itself if empty
    try:
        debug_dir.rmdir()
    except OSError:
        pass

    if purged_count > 0:
        logging.info(f"Purged {purged_count} debug file(s) from {debug_dir}")

    return purged_count


def cleanup_workspace(project_root: Path, purge_debug: bool = True,
                      max_backup_age_days: int = 30) -> Dict:
    """
    Comprehensive workspace cleanup: purge debug files and old backups.

    Args:
        project_root: Project root directory
        purge_debug: Whether to purge debug files (default: True)
        max_backup_age_days: Remove backups older than this (default: 30 days)

    Returns:
        Dict with cleanup statistics
    """
    stats = {
        'debug_files_purged': 0,
        'old_backups_removed': 0,
        'old_logs_archived': 0
    }

    # Purge debug files
    if purge_debug:
        debug_dir = project_root / 'data' / 'debug'
        stats['debug_files_purged'] = purge_debug_files(debug_dir)

    # Clean up old backups across all raw data directories
    raw_data_dir = project_root / 'data' / 'raw'
    if raw_data_dir.exists():
        for site_dir in raw_data_dir.iterdir():
            if site_dir.is_dir():
                backup_dir = site_dir / 'backups'
                if backup_dir.exists():
                    cutoff_time = datetime.now().timestamp() - (max_backup_age_days * 86400)
                    for backup in backup_dir.glob('*'):
                        if backup.is_file() and backup.stat().st_mtime < cutoff_time:
                            try:
                                backup.unlink()
                                stats['old_backups_removed'] += 1
                            except Exception as e:
                                logging.warning(f"Failed to remove old backup {backup}: {e}")

    logging.info(f"Workspace cleanup complete: {stats}")
    return stats


# =============================================================================
# LOGGING SETUP
# =============================================================================

def get_dated_log_path(store_name: str, logs_dir: Path = None) -> Path:
    """
    Generate dated log file path for a store scraper.
    Format: {store_name}_YYYY_monthname_DD.log (e.g., sobeys_2025_december_24.log)

    Args:
        store_name: Name of the store (e.g., 'sobeys', 'safeway')
        logs_dir: Directory for log files (defaults to project_root/logs)

    Returns:
        Path to dated log file
    """
    if logs_dir is None:
        logs_dir = Path("logs")

    # Get current date components
    now = datetime.now()
    month_name = now.strftime('%B').lower()  # Full month name in lowercase
    year = now.year
    day = now.day

    # Generate dated filename
    log_filename = f"{store_name}_{year}_{month_name}_{day:02d}.log"
    return logs_dir / log_filename


def rotate_old_logs(store_name: str, logs_dir: Path = None, backup_logs_dir: Path = None):
    """
    Move old log files (from previous dates) to backup_logs directory.
    Only keeps the current day's log in logs/.

    Args:
        store_name: Name of the store (e.g., 'sobeys', 'safeway')
        logs_dir: Directory containing current logs (defaults to project_root/logs)
        backup_logs_dir: Directory for archived logs (defaults to project_root/backup_logs)
    """
    if logs_dir is None:
        logs_dir = Path("logs")
    if backup_logs_dir is None:
        backup_logs_dir = Path("backup_logs")

    # Ensure backup directory exists
    backup_logs_dir.mkdir(parents=True, exist_ok=True)

    # Get current dated log path
    current_log = get_dated_log_path(store_name, logs_dir)

    # Find all log files for this store
    pattern = f"{store_name}_*.log"
    for log_file in logs_dir.glob(pattern):
        # Skip the current day's log
        if log_file == current_log:
            continue

        # Move old log to backup directory
        try:
            backup_path = backup_logs_dir / log_file.name
            shutil.move(str(log_file), str(backup_path))
            logging.info(f"Archived old log: {log_file.name} -> backup_logs/")
        except Exception as e:
            logging.warning(f"Failed to archive log {log_file}: {e}")


def setup_logging(log_file: Path, level: int = logging.INFO):
    """
    Configure structured logging with file and console handlers.

    Args:
        log_file: Path to log file
        level: Logging level (default: INFO)
    """
    log_file.parent.mkdir(parents=True, exist_ok=True)

    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # File handler
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    root_logger.handlers.clear()

    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.info(f"Logging initialized. Writing to {log_file}")


def setup_logging_with_rotation(store_name: str, level: int = logging.INFO,
                                  logs_dir: Path = None, backup_logs_dir: Path = None):
    """
    Configure logging with automatic date-based rotation.
    Creates dated log files and moves old logs to backup directory.

    Args:
        store_name: Name of the store (e.g., 'sobeys', 'safeway')
        level: Logging level (default: INFO)
        logs_dir: Directory for current logs (defaults to project_root/logs)
        backup_logs_dir: Directory for archived logs (defaults to project_root/backup_logs)
    """
    # Rotate old logs before setting up new logging
    rotate_old_logs(store_name, logs_dir, backup_logs_dir)

    # Get current dated log path
    log_file = get_dated_log_path(store_name, logs_dir)

    # Setup logging with the dated log file
    setup_logging(log_file, level)


# =============================================================================
# UTILITIES
# =============================================================================

def normalize_product_name(name: str) -> str:
    """
    Normalize product name for deduplication.
    Lowercase, strip whitespace, remove extra spaces.

    Args:
        name: Product name

    Returns:
        Normalized name
    """
    if not name:
        return ""

    return ' '.join(name.lower().strip().split())


def parse_price(price_str: str) -> Optional[float]:
    """
    Parse price string to float, handling various formats.

    Args:
        price_str: Price string (e.g., "$4.99", "4,99 €")

    Returns:
        Parsed price as float, or None if parsing fails
    """
    if not price_str:
        return None

    try:
        # Remove currency symbols and common formatting
        cleaned = price_str.replace('$', '').replace('€', '').replace('£', '')
        cleaned = cleaned.replace(',', '.').strip()

        return float(cleaned)
    except (ValueError, AttributeError):
        logging.warning(f"Failed to parse price: {price_str}")
        return None


def get_iso_timestamp() -> str:
    """
    Get current timestamp in ISO 8601 format.

    Returns:
        ISO formatted timestamp string
    """
    return datetime.utcnow().isoformat() + 'Z'


def ensure_data_directories(project_root: Path):
    """
    Create required data directory structure.

    Args:
        project_root: Root directory of the project
    """
    dirs = [
        project_root / 'data' / 'raw',
        project_root / 'data' / 'logs',
        project_root / 'data' / 'checkpoints',
    ]

    for dir_path in dirs:
        dir_path.mkdir(parents=True, exist_ok=True)

    logging.debug(f"Ensured data directories exist under {project_root / 'data'}")
