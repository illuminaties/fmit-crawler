#!/usr/bin/env python3
"""
Test script to check if we can extract the dictionary-items element from fmit.vn
"""
import os
import sys
import logging
from crawler import create_driver, get_chrome_version

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_URL = "https://fmit.vn/en/glossary"

def test_element_extraction():
    """Test if we can find the dictionary-items element"""
    logging.info("=" * 60)
    logging.info("Testing element extraction from fmit.vn")
    logging.info("=" * 60)
    
    try:
        # Create driver - handle local execution
        logging.info("Creating Chrome driver...")
        
        # Try to find Chrome on macOS if not in PATH
        import platform
        if platform.system() == "Darwin":  # macOS
            chrome_paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            ]
            for chrome_path in chrome_paths:
                if os.path.exists(chrome_path):
                    os.environ["CHROME_BIN"] = chrome_path
                    logging.info(f"Found Chrome at: {chrome_path}")
                    break
        
        driver = create_driver()
        
        # Navigate to the page
        logging.info(f"Navigating to: {BASE_URL}")
        driver.get(BASE_URL)
        
        # Wait a bit for page to load
        import time
        time.sleep(5)
        
        # Check page title
        page_title = driver.title
        logging.info(f"Page title: {page_title}")
        
        # Check current URL (might be redirected)
        current_url = driver.current_url
        logging.info(f"Current URL: {current_url}")
        
        # Get page source length
        page_source = driver.page_source
        logging.info(f"Page source length: {len(page_source)} characters")
        
        # Try to find the element using different methods
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        from selenium.common.exceptions import TimeoutException, NoSuchElementException
        
        logging.info("\n" + "=" * 60)
        logging.info("Testing element selectors...")
        logging.info("=" * 60)
        
        # Test 1: Try to find .dictionary-items
        logging.info("\n1. Looking for '.dictionary-items' (CLASS_NAME)...")
        try:
            items = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CLASS_NAME, "dictionary-items"))
            )
            logging.info(f"✅ FOUND: .dictionary-items")
            logging.info(f"   Element text preview: {items.text[:200]}...")
            
            # Try to find links inside
            links = items.find_elements(By.XPATH, './/li[@class="item"]/a[@href]')
            logging.info(f"   Found {len(links)} links with selector './/li[@class=\"item\"]/a[@href]'")
            
            if links:
                logging.info("   Sample links:")
                for i, link in enumerate(links[:5]):
                    href = link.get_attribute("href")
                    text = link.text[:50] if link.text else "no text"
                    logging.info(f"      {i+1}. {href} - {text}")
            
        except TimeoutException:
            logging.warning("❌ NOT FOUND: .dictionary-items (timeout after 15s)")
        except Exception as e:
            logging.error(f"❌ ERROR finding .dictionary-items: {e}")
        
        # Test 2: Try CSS selector
        logging.info("\n2. Looking for '.dictionary-items' (CSS_SELECTOR)...")
        try:
            items = driver.find_element(By.CSS_SELECTOR, ".dictionary-items")
            logging.info(f"✅ FOUND: .dictionary-items (CSS selector)")
        except NoSuchElementException:
            logging.warning("❌ NOT FOUND: .dictionary-items (CSS selector)")
        
        # Test 3: Look for any element with "dictionary" in class
        logging.info("\n3. Looking for any element with 'dictionary' in class...")
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, "[class*='dictionary']")
            logging.info(f"   Found {len(elements)} elements with 'dictionary' in class")
            for i, elem in enumerate(elements[:5]):
                class_name = elem.get_attribute("class")
                tag_name = elem.tag_name
                logging.info(f"      {i+1}. <{tag_name}> class='{class_name}'")
        except Exception as e:
            logging.error(f"   Error: {e}")
        
        # Test 4: Look for any element with "item" in class
        logging.info("\n4. Looking for any element with 'item' in class...")
        try:
            elements = driver.find_elements(By.CSS_SELECTOR, "[class*='item']")
            logging.info(f"   Found {len(elements)} elements with 'item' in class")
            for i, elem in enumerate(elements[:5]):
                class_name = elem.get_attribute("class")
                tag_name = elem.tag_name
                logging.info(f"      {i+1}. <{tag_name}> class='{class_name}'")
        except Exception as e:
            logging.error(f"   Error: {e}")
        
        # Test 5: Look for list items (li)
        logging.info("\n5. Looking for <li> elements...")
        try:
            li_elements = driver.find_elements(By.TAG_NAME, "li")
            logging.info(f"   Found {len(li_elements)} <li> elements")
            if li_elements:
                logging.info("   Sample <li> elements:")
                for i, li in enumerate(li_elements[:5]):
                    class_name = li.get_attribute("class")
                    text = li.text[:50] if li.text else "no text"
                    logging.info(f"      {i+1}. class='{class_name}' - {text}")
        except Exception as e:
            logging.error(f"   Error: {e}")
        
        # Test 6: Check for links (a tags)
        logging.info("\n6. Looking for links (a tags)...")
        try:
            links = driver.find_elements(By.TAG_NAME, "a")
            logging.info(f"   Found {len(links)} links total")
            # Filter for glossary links
            glossary_links = [link for link in links 
                            if link.get_attribute("href") and 
                            ("/glossary/" in link.get_attribute("href") or 
                             "/tu-dien-quan-ly/" in link.get_attribute("href"))]
            logging.info(f"   Found {len(glossary_links)} glossary-related links")
            if glossary_links:
                logging.info("   Sample glossary links:")
                for i, link in enumerate(glossary_links[:5]):
                    href = link.get_attribute("href")
                    text = link.text[:50] if link.text else "no text"
                    logging.info(f"      {i+1}. {href} - {text}")
        except Exception as e:
            logging.error(f"   Error: {e}")
        
        # Test 7: Check page source for dictionary-items
        logging.info("\n7. Checking page source for 'dictionary-items' string...")
        if "dictionary-items" in page_source:
            logging.info("   ✅ 'dictionary-items' string found in page source")
            # Find the context
            idx = page_source.find("dictionary-items")
            context = page_source[max(0, idx-100):idx+200]
            logging.info(f"   Context around 'dictionary-items':")
            logging.info(f"   {context}")
        else:
            logging.warning("   ❌ 'dictionary-items' string NOT found in page source")
        
        # Test 8: Check if page might be blocked
        logging.info("\n8. Checking for common bot detection indicators...")
        bot_indicators = [
            "captcha",
            "cloudflare",
            "access denied",
            "blocked",
            "forbidden",
            "robot",
            "bot"
        ]
        page_source_lower = page_source.lower()
        found_indicators = [indicator for indicator in bot_indicators if indicator in page_source_lower]
        if found_indicators:
            logging.warning(f"   ⚠️  Found potential bot detection: {found_indicators}")
        else:
            logging.info("   ✅ No obvious bot detection indicators found")
        
        # Save screenshot for debugging
        try:
            screenshot_path = "test_screenshot.png"
            driver.save_screenshot(screenshot_path)
            logging.info(f"\n✅ Screenshot saved to: {screenshot_path}")
        except Exception as e:
            logging.warning(f"   Could not save screenshot: {e}")
        
        # Save page source snippet
        try:
            with open("test_page_source.html", "w", encoding="utf-8") as f:
                f.write(page_source)
            logging.info(f"✅ Page source saved to: test_page_source.html")
        except Exception as e:
            logging.warning(f"   Could not save page source: {e}")
        
        logging.info("\n" + "=" * 60)
        logging.info("Test completed!")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"Fatal error during test: {e}", exc_info=True)
        sys.exit(1)
    finally:
        try:
            driver.quit()
        except:
            pass

if __name__ == "__main__":
    test_element_extraction()

