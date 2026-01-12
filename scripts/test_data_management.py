#!/usr/bin/env python3
"""
Test script to verify the new data management system.
Tests backup creation, log rotation, and dated log files.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scrapers.common import (
    backup_data_file,
    get_dated_log_path,
    rotate_old_logs,
    setup_logging_with_rotation
)
import logging


def test_dated_log_path():
    """Test dated log path generation"""
    print("\n=== Testing dated log path ===")
    log_path = get_dated_log_path('test_store', Path('logs'))
    print(f"[OK] Dated log path: {log_path}")
    # Should be in format: logs/test_store_YYYY_monthname_DD.log
    assert 'test_store' in str(log_path)
    assert '.log' in str(log_path)
    print("[OK] Path format is correct")


def test_backup_data_file():
    """Test data file backup"""
    print("\n=== Testing data file backup ===")

    # Create a test data file
    test_dir = project_root / 'data' / 'raw' / 'test_store'
    test_dir.mkdir(parents=True, exist_ok=True)
    test_file = test_dir / 'test_products.jsonl'

    # Write some test data
    test_file.write_text('{"name": "Test Product 1"}\n{"name": "Test Product 2"}\n')
    print(f"[OK] Created test file: {test_file}")

    # Create backup
    backup_created = backup_data_file(test_file)
    print(f"[OK] Backup created: {backup_created}")

    # Verify backup exists
    backup_file = test_dir / 'test_products_BACKUP.jsonl'
    assert backup_file.exists(), "Backup file should exist"
    print(f"[OK] Backup file exists: {backup_file}")

    # Verify backup content matches original
    original_content = test_file.read_text()
    backup_content = backup_file.read_text()
    assert original_content == backup_content, "Backup content should match original"
    print("[OK] Backup content matches original")

    # Test overwriting backup
    test_file.write_text('{"name": "Test Product 3"}\n')
    backup_data_file(test_file)
    new_backup_content = backup_file.read_text()
    assert new_backup_content != backup_content, "Backup should be updated"
    print("[OK] Backup can be overwritten")

    # Cleanup
    test_file.unlink()
    backup_file.unlink()
    print("[OK] Cleanup complete")


def test_log_rotation():
    """Test log rotation"""
    print("\n=== Testing log rotation ===")

    # Setup logging with rotation
    setup_logging_with_rotation('test_store', level=logging.INFO)
    print("[OK] Logging setup with rotation")

    # Log some messages
    logging.info("Test log message 1")
    logging.info("Test log message 2")
    print("[OK] Logged test messages")

    # Verify log file exists
    log_path = get_dated_log_path('test_store', project_root / 'logs')
    assert log_path.exists(), f"Log file should exist at {log_path}"
    print(f"[OK] Log file exists: {log_path}")

    # Check log content
    log_content = log_path.read_text()
    assert "Test log message 1" in log_content
    assert "Test log message 2" in log_content
    print("[OK] Log messages were written correctly")


def main():
    """Run all tests"""
    print("=" * 60)
    print("Data Management System Tests")
    print("=" * 60)

    try:
        test_dated_log_path()
        test_backup_data_file()
        test_log_rotation()

        print("\n" + "=" * 60)
        print("[SUCCESS] All tests passed!")
        print("=" * 60)

    except AssertionError as e:
        print(f"\n[FAIL] Test failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Error during testing: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
