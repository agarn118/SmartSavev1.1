#!/usr/bin/env python3
"""
Walmart Scraper V3.0 - PerimeterX Evasion Edition
Implements advanced anti-detection techniques:
- Session warmup
- Fingerprint rotation
- Human-like behavior
- Adaptive rate limiting
- Mouse/scroll simulation
"""

import time
import random
import logging
from typing import Tuple, Dict
import numpy as np
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Install with: pip install selenium-stealth
try:
    from selenium_stealth import stealth
    STEALTH_AVAILABLE = True
except ImportError:
    STEALTH_AVAILABLE = False
    logging.warning("selenium-stealth not installed. Install with: pip install selenium-stealth")


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Scraper configuration"""
    
    # Delays (base values, will be randomized)
    DELAY_PAGE_LOAD = (20, 35)      # Increased from (15, 25)
    DELAY_SCROLL = (2, 5)
    DELAY_TYPING = (0.05, 0.15)
    DELAY_MOUSE_MOVE = (0.1, 0.3)
    
    # Session settings
    WARMUP_ENABLED = True
    WARMUP_PAGES = 2                 # Visit 2 pages before scraping
    FINGERPRINT_ROTATION = True
    
    # Rate limiting
    ADAPTIVE_RATE_LIMITING = True
    MAX_CAPTCHA_RATE = 0.3          # If >30% CAPTCHAs, slow down
    
    # Behavior simulation
    SIMULATE_MOUSE = True
    SIMULATE_SCROLL = True
    SIMULATE_READING = True


# =============================================================================
# BROWSER FINGERPRINTS
# =============================================================================

FINGERPRINTS = [
    {
        'name': 'Windows 10 Desktop',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'viewport': (1920, 1080),
        'platform': 'Win32',
        'timezone': 'America/Toronto',
        'language': 'en-CA,en-US;q=0.9,en;q=0.8',
    },
    {
        'name': 'Windows 11 Desktop',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'viewport': (2560, 1440),
        'platform': 'Win32',
        'timezone': 'America/Vancouver',
        'language': 'en-CA;q=0.9,fr-CA;q=0.8,en-US;q=0.7',
    },
    {
        'name': 'macOS Laptop',
        'user_agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'viewport': (1440, 900),
        'platform': 'MacIntel',
        'timezone': 'America/Edmonton',
        'language': 'en-CA,en;q=0.9',
    },
    {
        'name': 'Windows Laptop',
        'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'viewport': (1366, 768),
        'platform': 'Win32',
        'timezone': 'America/Winnipeg',
        'language': 'en-CA,fr-CA;q=0.9,en;q=0.8',
    },
]


def get_random_fingerprint() -> Dict:
    """Select random browser fingerprint"""
    return random.choice(FINGERPRINTS)


# =============================================================================
# HUMAN-LIKE BEHAVIOR
# =============================================================================

def human_delay(mean: float, std: float = None, min_val: float = None, max_val: float = None) -> float:
    """
    Generate human-like delay using Gaussian distribution
    
    Args:
        mean: Average delay
        std: Standard deviation (default: mean * 0.3)
        min_val: Minimum delay (default: mean * 0.5)
        max_val: Maximum delay (default: mean * 1.5)
    
    Returns:
        Delay in seconds
    """
    if std is None:
        std = mean * 0.3
    if min_val is None:
        min_val = mean * 0.5
    if max_val is None:
        max_val = mean * 1.5
    
    delay = np.random.normal(mean, std)
    delay = max(min_val, min(max_val, delay))
    return delay


def scroll_slowly(driver, scroll_pause_time: float = 0.5):
    """
    Scroll page slowly like human reading
    
    Args:
        driver: Selenium WebDriver
        scroll_pause_time: Pause between scrolls
    """
    if not Config.SIMULATE_SCROLL:
        return
    
    screen_height = driver.execute_script("return window.innerHeight")
    scroll_height = driver.execute_script("return document.body.scrollHeight")
    
    i = 1
    while True:
        # Scroll down
        scroll_to = screen_height * i
        driver.execute_script(f"window.scrollTo(0, {scroll_to});")
        time.sleep(scroll_pause_time + random.uniform(-0.2, 0.3))
        
        i += 1
        
        # Check if at bottom
        scroll_pos = driver.execute_script("return window.pageYOffset + window.innerHeight")
        if scroll_pos >= scroll_height:
            break
    
    # Scroll back up a bit (humans do this)
    if random.random() < 0.3:  # 30% chance
        scroll_up = random.randint(0, scroll_height // 3)
        driver.execute_script(f"window.scrollTo(0, {scroll_up});")
        time.sleep(random.uniform(0.5, 1.5))


def random_mouse_movement(driver, element=None):
    """
    Simulate random mouse movements
    
    Args:
        driver: Selenium WebDriver
        element: Optional element to move towards
    """
    if not Config.SIMULATE_MOUSE:
        return
    
    actions = ActionChains(driver)
    
    if element:
        # Move to element with random offset
        size = element.size
        width, height = size['width'], size['height']
        offset_x = random.randint(-width//3, width//3)
        offset_y = random.randint(-height//3, height//3)
        actions.move_to_element_with_offset(element, offset_x, offset_y)
    else:
        # Random movement
        viewport_width = driver.execute_script("return window.innerWidth")
        viewport_height = driver.execute_script("return window.innerHeight")
        x = random.randint(0, viewport_width)
        y = random.randint(0, viewport_height)
        actions.move_by_offset(x, y)
    
    actions.pause(random.uniform(*Config.DELAY_MOUSE_MOVE))
    actions.perform()


def human_click(driver, element):
    """
    Click element like human
    
    Args:
        driver: Selenium WebDriver
        element: Element to click
    """
    # Move mouse to element
    random_mouse_movement(driver, element)
    
    # Pause before clicking
    time.sleep(random.uniform(0.1, 0.3))
    
    # Click
    element.click()


def human_type(element, text: str):
    """
    Type text like human (with realistic speed)
    
    Args:
        element: Input element
        text: Text to type
    """
    for char in text:
        element.send_keys(char)
        # Vary typing speed
        time.sleep(random.uniform(*Config.DELAY_TYPING))
        
        # Random typo (2% chance)
        if random.random() < 0.02:
            # Press wrong key
            wrong_char = random.choice('qwertyuiop')
            element.send_keys(wrong_char)
            time.sleep(random.uniform(0.1, 0.2))
            # Backspace to correct
            element.send_keys(Keys.BACKSPACE)
            time.sleep(random.uniform(0.05, 0.1))


def simulate_reading_delay():
    """
    Pause as if reading content
    """
    if Config.SIMULATE_READING:
        time.sleep(human_delay(4, 2, 2, 8))


# =============================================================================
# ADAPTIVE RATE LIMITER
# =============================================================================

class AdaptiveRateLimiter:
    """
    Adjusts delays based on CAPTCHA frequency
    If CAPTCHAs increase → slow down
    """
    
    def __init__(self, logger):
        self.logger = logger
        self.base_delay = (Config.DELAY_PAGE_LOAD[0] + Config.DELAY_PAGE_LOAD[1]) / 2
        self.captcha_count = 0
        self.request_count = 0
        
    def wait(self):
        """Wait with adaptive delay"""
        if not Config.ADAPTIVE_RATE_LIMITING:
            time.sleep(human_delay(self.base_delay))
            return
        
        # Calculate current CAPTCHA rate
        captcha_rate = self.captcha_count / max(self.request_count, 1)
        
        # Adjust delay based on CAPTCHA rate
        if captcha_rate > 0.5:  # >50% CAPTCHAs
            delay_mean = self.base_delay * 2.5
            self.logger.warning(f"High CAPTCHA rate ({captcha_rate:.1%}). Slowing down significantly.")
        elif captcha_rate > Config.MAX_CAPTCHA_RATE:  # >30% CAPTCHAs
            delay_mean = self.base_delay * 1.5
            self.logger.warning(f"Elevated CAPTCHA rate ({captcha_rate:.1%}). Slowing down.")
        else:
            delay_mean = self.base_delay
        
        # Add randomness
        delay = human_delay(delay_mean, delay_mean * 0.3)
        
        self.logger.debug(f"Waiting {delay:.1f}s (CAPTCHA rate: {captcha_rate:.1%})")
        time.sleep(delay)
        
        self.request_count += 1
    
    def report_captcha(self):
        """Report CAPTCHA encountered"""
        self.captcha_count += 1
        captcha_rate = self.captcha_count / max(self.request_count, 1)
        self.logger.warning(f"CAPTCHA #{self.captcha_count} (rate: {captcha_rate:.1%})")


# =============================================================================
# BROWSER INITIALIZATION
# =============================================================================

def create_stealth_driver(fingerprint: Dict = None, logger = None) -> uc.Chrome:
    """
    Create undetected Chrome driver with stealth patches
    
    Args:
        fingerprint: Browser fingerprint to use
        logger: Logger instance
    
    Returns:
        Configured Chrome driver
    """
    if fingerprint is None:
        fingerprint = get_random_fingerprint()
    
    if logger:
        logger.info(f"Creating driver with fingerprint: {fingerprint['name']}")
    
    # Chrome options
    options = uc.ChromeOptions()
    
    # Set viewport
    width, height = fingerprint['viewport']
    options.add_argument(f'--window-size={width},{height}')
    
    # Set user agent
    options.add_argument(f'--user-agent={fingerprint["user_agent"]}')
    
    # Additional options
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--no-sandbox')
    
    # Set preferences
    prefs = {
        'profile.default_content_setting_values.notifications': 2,
        'profile.default_content_setting_values.geolocation': 2,
        'intl.accept_languages': fingerprint['language'],
    }
    options.add_experimental_option('prefs', prefs)
    
    # Exclude automation switches
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    
    # Create driver
    driver = uc.Chrome(options=options)
    
    # Apply selenium-stealth (if available)
    if STEALTH_AVAILABLE:
        stealth(
            driver,
            languages=fingerprint['language'].split(','),
            vendor="Google Inc.",
            platform=fingerprint['platform'],
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
        )
        if logger:
            logger.info("Applied selenium-stealth patches")
    else:
        if logger:
            logger.warning("selenium-stealth not available. Install for better stealth.")
    
    # Set timezone (requires CDP)
    try:
        driver.execute_cdp_cmd('Emulation.setTimezoneOverride', {
            'timezoneId': fingerprint['timezone']
        })
        if logger:
            logger.info(f"Set timezone: {fingerprint['timezone']}")
    except Exception as e:
        if logger:
            logger.warning(f"Could not set timezone: {e}")
    
    return driver


# =============================================================================
# SESSION WARMUP
# =============================================================================

def warmup_session(driver, logger):
    """
    Build trust score before scraping
    Visits non-target pages to appear human
    
    Args:
        driver: Selenium WebDriver
        logger: Logger instance
    """
    if not Config.WARMUP_ENABLED:
        return
    
    logger.info("🔥 Warming up session (building trust score)...")
    
    casual_pages = [
        ('/', 'Homepage'),
        ('/cp/grocery/6000200775', 'Grocery category'),
        ('/cp/food/6000206361', 'Food category'),
    ]
    
    # Visit homepage first (always)
    try:
        logger.info("Visiting homepage...")
        driver.get('https://www.walmart.ca/')
        time.sleep(human_delay(6, 2))
        scroll_slowly(driver)
        
        # Random mouse movements
        for _ in range(random.randint(2, 4)):
            random_mouse_movement(driver)
        
        # Visit 1-2 additional pages
        num_pages = min(Config.WARMUP_PAGES - 1, len(casual_pages) - 1)
        pages_to_visit = random.sample(casual_pages[1:], num_pages)
        
        for url, name in pages_to_visit:
            logger.info(f"Visiting {name}...")
            driver.get(f'https://www.walmart.ca{url}')
            time.sleep(human_delay(4, 2))
            scroll_slowly(driver)
            
            # Simulate reading
            simulate_reading_delay()
        
        logger.info("✅ Session warmed up. Trust score improved.")
        
    except Exception as e:
        logger.error(f"Error during warmup: {e}")


# =============================================================================
# CAPTCHA DETECTION
# =============================================================================

def is_captcha_present(driver) -> bool:
    """
    Check if CAPTCHA page is shown
    
    Args:
        driver: Selenium WebDriver
    
    Returns:
        True if CAPTCHA detected
    """
    try:
        # PerimeterX CAPTCHA indicators
        indicators = [
            "Press & Hold",
            "distil_r_captcha",
            "_Incapsula_Resource",
            "perimeterx",
            "px-captcha",
        ]
        
        page_source = driver.page_source.lower()
        
        for indicator in indicators:
            if indicator.lower() in page_source:
                return True
        
        # Check for specific CAPTCHA elements
        captcha_selectors = [
            "iframe[src*='captcha']",
            "div[class*='captcha']",
            "div[id*='captcha']",
        ]
        
        for selector in captcha_selectors:
            if driver.find_elements(By.CSS_SELECTOR, selector):
                return True
        
        return False
        
    except Exception:
        return False


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

def main():
    """Example scraping workflow with PerimeterX evasion"""
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("="*80)
    logger.info("Walmart Scraper V3.0 - PerimeterX Evasion Edition")
    logger.info("="*80)
    
    # Initialize rate limiter
    rate_limiter = AdaptiveRateLimiter(logger)
    
    # Create driver with random fingerprint
    fingerprint = get_random_fingerprint()
    driver = create_stealth_driver(fingerprint, logger)
    
    try:
        # Warmup session
        warmup_session(driver, logger)
        
        # Now ready to scrape
        logger.info("\n🎯 Starting scraping...")
        
        # Example: Search for products
        logger.info("Navigating to search...")
        driver.get('https://www.walmart.ca')
        time.sleep(human_delay(3))
        
        # Check for CAPTCHA
        if is_captcha_present(driver):
            logger.error("❌ CAPTCHA detected after warmup!")
            rate_limiter.report_captcha()
        else:
            logger.info("✅ No CAPTCHA! Trust score is good.")
            
            # Find search box
            try:
                search_box = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "input[aria-label='Search']"))
                )
                
                # Type search query
                logger.info("Typing search query...")
                human_type(search_box, "milk")
                time.sleep(human_delay(0.5))
                
                # Submit search
                search_box.send_keys(Keys.RETURN)
                
                # Wait for results
                rate_limiter.wait()
                
                # Check for CAPTCHA again
                if is_captcha_present(driver):
                    logger.error("❌ CAPTCHA detected after search!")
                    rate_limiter.report_captcha()
                else:
                    logger.info("✅ Search successful! No CAPTCHA.")
                    
                    # Scroll results
                    scroll_slowly(driver)
                    
                    # Continue scraping...
                    logger.info("Ready to extract product data...")
                    
            except Exception as e:
                logger.error(f"Error during search: {e}")
        
    finally:
        logger.info("\nClosing browser...")
        time.sleep(random.uniform(2, 5))  # Don't close immediately
        driver.quit()
        
    logger.info("\n✅ Done!")


if __name__ == "__main__":
    main()
