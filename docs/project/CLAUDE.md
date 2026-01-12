# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains a Python-based web scraper designed to evade anti-bot systems (specifically PerimeterX) when scraping walmart.ca. The scraper implements advanced anti-detection techniques including browser fingerprint rotation, human-like behavior simulation, and adaptive rate limiting.

## Architecture

### Core Components

**scraper.py** - Single-file implementation containing all scraper functionality organized into logical sections:

1. **Configuration (Config class)** - Centralized settings for delays, session behavior, and feature toggles
2. **Browser Fingerprints** - Multiple predefined browser profiles (Windows/macOS, different viewports) to rotate between sessions
3. **Human-like Behavior Functions** - Utilities that simulate realistic user interactions:
   - `human_delay()` - Gaussian-distributed timing delays
   - `scroll_slowly()` - Progressive page scrolling with random backtracking
   - `random_mouse_movement()` - Cursor movement simulation
   - `human_type()` - Character-by-character typing with occasional typos
4. **AdaptiveRateLimiter class** - Dynamically adjusts request delays based on CAPTCHA encounter rate
5. **Browser Initialization** - Creates undetected Chrome driver with stealth patches and fingerprint application
6. **Session Warmup** - Pre-scraping routine that visits non-target pages to build trust score
7. **CAPTCHA Detection** - Identifies PerimeterX and other CAPTCHA systems in page source

### Key Design Patterns

- **Randomization Throughout**: All delays, mouse movements, and behaviors use random distributions to avoid pattern detection
- **Adaptive Response**: Rate limiter automatically slows down when CAPTCHA rate exceeds thresholds
- **Layered Evasion**: Combines multiple techniques (fingerprints, stealth patches, behavior simulation) for defense-in-depth

## Commands

### Running the scraper

```bash
python scraper.py
```

This executes the example workflow in `main()` which demonstrates:
- Session warmup (visits homepage and random category pages)
- Search functionality with human-like typing
- CAPTCHA detection and adaptive rate limiting

### Dependencies

Install required packages:
```bash
pip install selenium undetected-chromedriver numpy selenium-stealth
```

Note: `selenium-stealth` is optional but recommended for enhanced evasion capabilities.

## Development Considerations

### Modifying Anti-Detection Behavior

- **Delays**: Adjust `Config.DELAY_*` tuples to change timing ranges. Values are `(min, max)` in seconds.
- **Fingerprints**: Add/modify entries in `FINGERPRINTS` list to change browser profiles. Each fingerprint should include user_agent, viewport, platform, timezone, and language.
- **Warmup Strategy**: Modify `warmup_session()` to visit different pages or change visit patterns. Update `casual_pages` list to add new warm-up targets.
- **Rate Limiting**: Adjust `Config.MAX_CAPTCHA_RATE` threshold or modify `AdaptiveRateLimiter.wait()` logic to change response to CAPTCHA encounters.

### Adding New Scraping Logic

The example in `main()` shows the typical workflow pattern:
1. Initialize rate limiter and driver
2. Run warmup session
3. Navigate to target pages with `rate_limiter.wait()` between requests
4. Check for CAPTCHAs with `is_captcha_present()` after each navigation
5. Use human-like interaction functions (`human_type`, `human_click`, `scroll_slowly`) instead of direct Selenium calls

### Chrome Driver Path

The script uses `undetected_chromedriver` which automatically manages ChromeDriver installation. If you need to specify a custom Chrome binary location, modify the `uc.Chrome()` call in `create_stealth_driver()` to include `driver_executable_path` parameter.

## Important Notes

This scraper is designed for authorized security testing and research purposes. Ensure you have permission before scraping any website and comply with terms of service and robots.txt directives.
