#!/usr/bin/env python3
"""
CLI entrypoint for running grocery scrapers.

Usage:
  Recommended: python -m scrapers.run --site <site_slug> [options]
  Alternative: python scrapers/run.py --site <site_slug> [options]
"""

import sys
from pathlib import Path

# Add project root to Python path for absolute imports
# This allows the script to work when run directly (python scrapers/run.py)
# or as a module (python -m scrapers.run)
project_root = Path(__file__).parent.parent.resolve()
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import argparse
import logging

from scrapers.common import setup_logging_with_rotation, rotate_old_logs


def get_project_root() -> Path:
    """Get the project root directory."""
    # Assuming run.py is in PROJECT_ROOT/scrapers/
    return Path(__file__).parent.parent.resolve()


def get_scraper_class(site_slug: str):
    """
    Dynamically import and return scraper class for given site.

    Args:
        site_slug: Site identifier

    Returns:
        Scraper class
    """
    # Calculate expected class name before try block to avoid UnboundLocalError
    class_name = ''.join(word.capitalize() for word in site_slug.split('_')) + 'Scraper'

    try:
        # Import site-specific scraper module
        module_name = f"scrapers.sites.{site_slug}"
        module = __import__(module_name, fromlist=[''])

        # Get scraper class (convention: PascalCase from site_slug)
        scraper_class = getattr(module, class_name)

        return scraper_class

    except (ImportError, AttributeError) as e:
        logging.error(f"Failed to load scraper for site '{site_slug}': {e}")
        logging.error(f"Make sure scrapers/sites/{site_slug}.py exists and contains {class_name}")
        sys.exit(1)


def main():
    """Main CLI entrypoint."""
    parser = argparse.ArgumentParser(
        description='Grocery website scraper',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Scrape category from Real Canadian Superstore
  python -m scrapers.run --site realcanadiansuperstore --category-url "/food/dairy-and-eggs/c/28000" --max-pages 5

  # Search for products at No Frills
  python -m scrapers.run --site nofrills --query "milk" --max-pages 3

  # Export to CSV after scraping
  python -m scrapers.run --site safeway --query "bread" --output-format both

  # Resume from checkpoint
  python -m scrapers.run --site sobeys --category-url "/products/dairy-eggs" --resume
        """
    )

    # Required arguments
    parser.add_argument(
        '--site',
        required=True,
        help='Site slug (e.g., realcanadiansuperstore, nofrills, safeway, sobeys)'
    )

    # Scraping mode (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        '--query',
        help='Search query string'
    )
    mode_group.add_argument(
        '--category-url',
        help='Category page URL or path'
    )
    mode_group.add_argument(
        '--product-url',
        help='Single product page URL (for testing)'
    )

    # Scraping options
    parser.add_argument(
        '--max-pages',
        type=int,
        default=None,
        help='Maximum number of pages to scrape (default: all)'
    )

    parser.add_argument(
        '--headless',
        action='store_true',
        default=True,
        help='Run browser in headless mode (default: True)'
    )

    parser.add_argument(
        '--headful',
        dest='headless',
        action='store_false',
        help='Run browser in visible mode (for debugging)'
    )

    # Output options
    parser.add_argument(
        '--output-format',
        choices=['jsonl', 'csv', 'both'],
        default='jsonl',
        help='Output format (default: jsonl)'
    )

    # Checkpoint options
    parser.add_argument(
        '--resume',
        action='store_true',
        help='Resume from checkpoint if available'
    )

    parser.add_argument(
        '--clear-checkpoint',
        action='store_true',
        help='Clear checkpoint before starting'
    )

    parser.add_argument(
        '--fresh',
        action='store_true',
        help='Fresh start: clear checkpoint AND archive+clear data files (recommended for new scrape runs)'
    )

    # Logging options
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Get project root and setup paths
    project_root = get_project_root()
    config_path = project_root / 'configs' / f"{args.site}.json"

    # Validate config exists
    if not config_path.exists():
        logging.error(f"Config file not found: {config_path}")
        sys.exit(1)

    # Initialize scraper
    scraper_class = get_scraper_class(args.site)
    scraper = scraper_class(
        config_path=config_path,
        project_root=project_root,
        headless=args.headless,
        fresh_start=args.fresh
    )

    # Setup logging with automatic rotation
    log_level = getattr(logging, args.log_level)
    logs_dir = project_root / 'logs'
    backup_logs_dir = project_root / 'backup_logs'
    setup_logging_with_rotation(args.site, level=log_level, logs_dir=logs_dir, backup_logs_dir=backup_logs_dir)

    logging.info("=" * 60)
    logging.info(f"Starting scraper for: {args.site}")
    logging.info("=" * 60)

    # Handle checkpoint
    if args.fresh or args.clear_checkpoint:
        scraper.checkpoint_manager.clear()
        logging.info("Checkpoint cleared")

    if args.fresh:
        logging.info("Fresh start mode: data files archived and cleared")

    if args.resume:
        checkpoint = scraper.load_checkpoint()
        logging.info(f"Resuming from checkpoint: {checkpoint}")

    # Execute scraping based on mode
    try:
        if args.query:
            logging.info(f"Search mode: query='{args.query}', max_pages={args.max_pages}")
            count = scraper.scrape_search(args.query, max_pages=args.max_pages)
            logging.info(f"Scraped {count} products from search results")

        elif args.category_url:
            logging.info(f"Category mode: url='{args.category_url}', max_pages={args.max_pages}")
            count = scraper.scrape_category(args.category_url, max_pages=args.max_pages)
            logging.info(f"Scraped {count} products from category")

        elif args.product_url:
            logging.info(f"Product page mode: url='{args.product_url}'")
            record = scraper.scrape_product_page(args.product_url)
            if record:
                scraper.save_record(record)
                logging.info(f"Scraped product: {record.name}")
            else:
                logging.warning("Failed to scrape product page")

        else:
            logging.error("Must specify one of: --query, --category-url, or --product-url")
            parser.print_help()
            sys.exit(1)

        # Export to CSV if requested
        if args.output_format in ['csv', 'both']:
            logging.info("Exporting to CSV...")
            scraper.export_to_csv()

        # Print statistics
        scraper.print_stats()

        # Save final checkpoint
        scraper.save_checkpoint()

        logging.info("=" * 60)
        logging.info("Scraping completed successfully")
        logging.info(f"Output: {scraper.jsonl_path}")
        if args.output_format in ['csv', 'both']:
            logging.info(f"CSV Export: {scraper.csv_path}")
        logging.info("=" * 60)

    except KeyboardInterrupt:
        logging.warning("Scraping interrupted by user")
        scraper.save_checkpoint()
        scraper.print_stats()
        sys.exit(0)

    except Exception as e:
        logging.error(f"Scraping failed with error: {e}", exc_info=True)
        scraper.save_checkpoint()
        scraper.print_stats()
        sys.exit(1)


if __name__ == '__main__':
    main()
