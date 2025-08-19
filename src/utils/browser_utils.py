"""
Browser utilities for web scraping operations.
"""

import time
from selenium.webdriver.chrome.options import Options


def setup_chrome_options() -> Options:
    """
    Setup optimized Chrome options for web scraping.
    
    Returns:
        Configured Chrome options
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-java")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--window-size=1366,768")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Speed optimizations via preferences
    prefs = {
        "profile.default_content_setting_values": {
            "images": 2,  # Block images
            "plugins": 2,  # Block plugins
            "popups": 2,  # Block popups
            "geolocation": 2,  # Block location sharing
            "notifications": 2,  # Block notifications
            "media_stream": 2,  # Block media stream
        },
        "profile.managed_default_content_settings": {
            "images": 2
        }
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    return chrome_options


def smart_cache_bust(driver, force_aggressive: bool = False):
    """
    Smart cache busting for handling SPA caching issues.
    
    Args:
        driver: Selenium WebDriver instance
        force_aggressive: If True, perform aggressive cache bust for track switches
    """
    try:
        if force_aggressive:
            print("    Performing AGGRESSIVE cache bust (track switch)...")
            
            # Full aggressive cache bust for track switches
            driver.delete_all_cookies()
            try:
                driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
                driver.execute_script("window.location.reload(true);")  # Hard reload
            except:
                pass
            time.sleep(8)  # Longer wait for track switches
            
            print("    Aggressive cache bust completed")
        else:
            print("    Performing light cache bust (same track)...")
            
            # Light cache bust for same track races
            try:
                driver.execute_script("window.sessionStorage.clear();")
                # Clear only specific cache entries that might affect race content
                driver.execute_script("""
                    if (window.caches) {
                        caches.keys().then(function(names) {
                            names.forEach(function(name) {
                                if (name.includes('race') || name.includes('card')) {
                                    caches.delete(name);
                                }
                            });
                        });
                    }
                """)
            except:
                pass
            time.sleep(2)  # Much shorter wait for same track
            
            print("    Light cache bust completed")
        
    except Exception as e:
        print(f"    Cache bust error: {e}")


def aggressive_cache_bust(driver):
    """
    Extremely aggressive cache busting for problematic cases.
    
    Args:
        driver: Selenium WebDriver instance
    """
    try:
        print("    Performing aggressive cache bust...")
        
        # Clear all cookies
        driver.delete_all_cookies()
        
        # Clear local storage and session storage via JavaScript
        try:
            driver.execute_script("window.localStorage.clear();")
            driver.execute_script("window.sessionStorage.clear();")
            driver.execute_script("window.indexedDB.deleteDatabase('greyhoundbet');")
        except:
            pass
        
        # Clear cache via CDP (Chrome DevTools Protocol)
        try:
            driver.execute_cdp_cmd('Network.clearBrowserCache', {})
            driver.execute_cdp_cmd('Network.clearBrowserCookies', {})
        except:
            pass
        
        # Navigate to blank page first
        driver.get("about:blank")
        time.sleep(1)
        
        # Navigate to main page with cache-busting parameters
        main_url = f"https://greyhoundbet.racingpost.com/?nocache={int(time.time())}&refresh=true"
        driver.get(main_url)
        time.sleep(3)
        
        # Force a hard refresh
        driver.refresh()
        time.sleep(2)
        
        print("    Cache bust completed")
        
    except Exception as e:
        print(f"    Cache bust error: {e}")
