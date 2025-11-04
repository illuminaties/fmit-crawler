import os
import sys
import time
import json
import logging
from typing import Tuple, List, Dict, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

PARQUET_FILE = os.path.join(DATA_DIR, "fmit_data.parquet")
PAGE_CHECKPOINT = os.path.join(DATA_DIR, "page_checkpoint.json")

BASE_URL = "https://fmit.vn/en/glossary"
MAX_PAGES = 6729

# Timeout settings (5.5 hours = 19800 seconds) - leave buffer for GitHub Actions 6-hour limit
MAX_RUNTIME_SECONDS = int(os.getenv("MAX_RUNTIME_SECONDS", "19800"))  # 5.5 hours default
MAX_URLS_PER_RUN = int(os.getenv("MAX_URLS_PER_RUN", "500"))  # Reduced from 1500


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_chrome_version(chrome_bin: str) -> str:
    """Get Chrome version from the binary."""
    try:
        import subprocess
        result = subprocess.run([chrome_bin, "--version"], capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            version_str = result.stdout.strip()
            logging.info(f"Chrome version output: {version_str}")
            # Extract version number (e.g., "Chromium 144.0.7508.0" -> "144.0.7508.0")
            import re
            # Try full version first (e.g., 144.0.7508.0)
            match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_str)
            if match:
                full_version = match.group(1)
                logging.info(f"Detected full Chrome version: {full_version}")
                return full_version
            # Try major.minor (e.g., 144.0)
            match = re.search(r'(\d+\.\d+)', version_str)
            if match:
                major_minor = match.group(1)
                logging.info(f"Detected Chrome version (major.minor): {major_minor}")
                return major_minor
        logging.warning(f"Could not parse Chrome version from: {result.stdout}")
    except Exception as e:
        logging.warning(f"Failed to get Chrome version: {e}")
    return None


def create_driver() -> webdriver.Chrome:
    try:
        logging.info("Creating Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--blink-settings=imagesEnabled=false")
        
        # Use CHROME_BIN if provided (e.g., in GitHub Actions)
        chrome_bin = os.getenv("CHROME_BIN")
        if chrome_bin:
            chrome_options.binary_location = chrome_bin
            logging.info(f"Using Chrome binary: {chrome_bin}")
            
            # Get Chrome version to match ChromeDriver
            chrome_version = get_chrome_version(chrome_bin)
            if chrome_version:
                logging.info(f"Detected Chrome version: {chrome_version}")
        else:
            logging.info("CHROME_BIN not set, using default Chrome")
            chrome_version = None
        
        # Install ChromeDriver - try to match Chrome version
        logging.info("Installing ChromeDriver...")
        driver_path = None
        try:
            if chrome_version:
                # Extract major version number for ChromeDriverManager
                major_version = chrome_version.split('.')[0] if '.' in chrome_version else chrome_version
                logging.info(f"Attempting to get ChromeDriver for Chrome major version {major_version}...")
                try:
                    # Try with major version
                    driver_path = ChromeDriverManager(version=major_version).install()
                    logging.info(f"ChromeDriver installed at: {driver_path}")
                except Exception as e1:
                    logging.warning(f"ChromeDriverManager with major version {major_version} failed: {e1}")
                    # Try with full version
                    try:
                        driver_path = ChromeDriverManager(version=chrome_version).install()
                        logging.info(f"ChromeDriver installed at: {driver_path}")
                    except Exception as e2:
                        logging.warning(f"ChromeDriverManager with full version {chrome_version} failed: {e2}")
                        raise e1  # Re-raise original error
            
            if not driver_path:
                # Fallback: Let ChromeDriverManager auto-detect (should work with latest)
                logging.info("Using ChromeDriverManager auto-detection...")
                driver_path = ChromeDriverManager().install()
                logging.info(f"ChromeDriver installed at: {driver_path}")
                
        except Exception as e:
            logging.error(f"ChromeDriverManager failed: {e}")
            logging.info("Trying to use system chromedriver if available...")
            driver_path = None
        
        service = Service(driver_path) if driver_path else Service()
        logging.info("Starting Chrome browser...")
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.info("Chrome driver created successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome driver: {e}", exc_info=True)
        raise


def save_page_checkpoint(page: int) -> None:
    with open(PAGE_CHECKPOINT, "w", encoding="utf-8") as f:
        json.dump({"last_page": page}, f, ensure_ascii=False)


def load_page_checkpoint() -> int:
    if not os.path.exists(PAGE_CHECKPOINT):
        return 0
    try:
        with open(PAGE_CHECKPOINT, "r", encoding="utf-8") as f:
            return int(json.load(f).get("last_page", 0))
    except Exception:
        return 0


def read_parquet_df() -> pd.DataFrame:
    if not os.path.exists(PARQUET_FILE):
        return pd.DataFrame(columns=["url", "h1", "h2", "content"])
    try:
        return pd.read_parquet(PARQUET_FILE)
    except Exception:
        return pd.DataFrame(columns=["url", "h1", "h2", "content"])


def write_parquet_df(df: pd.DataFrame) -> None:
    for col in ["url", "h1", "h2", "content"]:
        if col not in df.columns:
            df[col] = ""
    df = df[["url", "h1", "h2", "content"]]
    df.to_parquet(PARQUET_FILE, index=False)


def load_processed_urls() -> Set[str]:
    df = read_parquet_df()
    if "url" in df.columns:
        return set(df["url"].dropna().astype(str))
    return set()


def append_to_parquet(rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    new_df = pd.DataFrame(rows)
    for col in ["url", "h1", "h2", "content"]:
        if col not in new_df.columns:
            new_df[col] = ""
    old_df = read_parquet_df()
    if not old_df.empty and "url" in old_df.columns and "url" in new_df.columns:
        new_df = new_df[~new_df["url"].isin(old_df["url"])]
        if new_df.empty:
            return
        df = pd.concat([old_df, new_df], ignore_index=True)
    else:
        df = new_df
    write_parquet_df(df)


def extract_page_links(driver: webdriver.Chrome, url: str, max_retries: int = 3) -> Tuple[List[str], webdriver.Chrome]:
    for _ in range(max_retries):
        try:
            driver.get(url)
            time.sleep(2)  # Reduced from 3
            items = driver.find_element(By.CLASS_NAME, "dictionary-items")
            links = items.find_elements(By.XPATH, './/li[@class="item"]/a[@href]')
            hrefs: List[str] = []
            for link in links:
                href = link.get_attribute("href")
                if href and "fmit.vn" in href and ("/glossary/" in href or "/tu-dien-quan-ly/" in href):
                    hrefs.append(href)
            return list(set(hrefs)), driver
        except Exception as e:
            logging.warning(f"Page error {url}: {e}. Retry in 5s...")
            time.sleep(5)  # Reduced from 10
    return [], driver


def extract_url_data(driver: webdriver.Chrome, url: str, max_retries: int = 3) -> Tuple[Dict[str, str], webdriver.Chrome]:
    for _ in range(max_retries):
        try:
            driver.get(url)
            time.sleep(1.5)  # Reduced from 2
            h1 = h2 = content = ""
            try:
                h1_el = driver.find_element(By.CSS_SELECTOR, "h1.dictionary-detail-title")
                h1 = h1_el.text.strip()
            except NoSuchElementException:
                pass
            try:
                h2_el = driver.find_element(By.CSS_SELECTOR, "h2.dictionary-detail-title")
                h2 = h2_el.text.strip()
            except NoSuchElementException:
                pass
            try:
                content_el = driver.find_element(By.CSS_SELECTOR, "div.dictionary-details")
                content = content_el.text.strip()
            except NoSuchElementException:
                pass
            return {"url": url, "h1": h1, "h2": h2, "content": content}, driver
        except Exception as e:
            logging.warning(f"URL error {url}: {e}. Retry in 5s...")
            time.sleep(5)  # Reduced from 10
    return {"url": url, "h1": "", "h2": "", "content": ""}, driver


def run_once() -> None:
    setup_logging()
    start_time = time.time()
    driver = None
    try:
        logging.info("=" * 60)
        logging.info("START CRAWL (GitHub Actions, Parquet)")
        logging.info("=" * 60)
        logging.info(f"Python version: {sys.version}")
        logging.info(f"Max runtime: {MAX_RUNTIME_SECONDS}s ({MAX_RUNTIME_SECONDS/3600:.1f}h)")
        logging.info(f"Max URLs per run: {MAX_URLS_PER_RUN}")
        logging.info(f"DATA_DIR: {DATA_DIR}")
        logging.info(f"CHROME_BIN: {os.getenv('CHROME_BIN', 'Not set')}")
        
        logging.info("Loading processed URLs...")
        processed = load_processed_urls()
        logging.info(f"Found {len(processed)} already processed URLs")
        
        logging.info("Creating Chrome driver...")
        driver = create_driver()

        def check_timeout() -> bool:
            elapsed = time.time() - start_time
            if elapsed >= MAX_RUNTIME_SECONDS:
                logging.warning(f"Timeout reached ({elapsed:.0f}s). Stopping gracefully.")
                return True
            return False

        start_page = load_page_checkpoint() + 1
        current_page = max(start_page, 1)
        all_urls: Set[str] = set()

        logging.info(f"Collect URLs from page {current_page} to {MAX_PAGES}")
        while current_page <= MAX_PAGES:
            if check_timeout():
                break
            
            url = BASE_URL if current_page == 1 else f"{BASE_URL}?page={current_page}"
            hrefs, driver = extract_page_links(driver, url)
            new_hrefs = [h for h in hrefs if h not in processed and h not in all_urls]
            all_urls.update(new_hrefs)
            if new_hrefs or hrefs:
                save_page_checkpoint(current_page)
            
            elapsed = time.time() - start_time
            logging.info(f"Page {current_page}: +{len(new_hrefs)} new links (total: {len(all_urls)}), elapsed: {elapsed/60:.1f}m")
            
            current_page += 1
            time.sleep(1.5)  # Reduced from 2
            if len(all_urls) >= MAX_URLS_PER_RUN:
                logging.info(f"Reached max URLs limit ({MAX_URLS_PER_RUN})")
                break

        logging.info(f"Total new URLs this run: {len(all_urls)}")

        if all_urls and not check_timeout():
            batch: List[Dict[str, str]] = []
            for idx, url in enumerate(all_urls, 1):
                if check_timeout():
                    logging.warning(f"Timeout during URL processing. Processed {idx-1}/{len(all_urls)} URLs.")
                    break
                
                data, driver = extract_url_data(driver, url)
                batch.append(data)
                
                if len(batch) >= 50:
                    append_to_parquet(batch)
                    batch = []
                    elapsed = time.time() - start_time
                    remaining = len(all_urls) - idx
                    eta_minutes = (elapsed / idx) * remaining / 60 if idx > 0 else 0
                    logging.info(f"Saved {idx}/{len(all_urls)} URLs, elapsed: {elapsed/60:.1f}m, ETA: {eta_minutes:.1f}m")
                
                time.sleep(1)  # Reduced from 1.5

            if batch:
                append_to_parquet(batch)

        total_time = time.time() - start_time
        logging.info(f"DONE. Data saved in Parquet. Total time: {total_time/60:.1f}m ({total_time/3600:.2f}h). Can resume next run.")
    finally:
        if driver:
            try:
                driver.quit()
                logging.info("Chrome driver closed")
            except Exception as e:
                logging.warning(f"Error closing driver: {e}")


if __name__ == "__main__":
    import sys
    try:
        run_once()
        sys.exit(0)
    except KeyboardInterrupt:
        logging.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


