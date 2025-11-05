import os
import time
import json
import logging
import subprocess
import re
import glob
import requests
import zipfile
from pathlib import Path
from typing import Tuple, List, Dict, Set

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

DATA_DIR = os.getenv("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

PARQUET_FILE = os.path.join(DATA_DIR, "fmit_data.parquet")
PAGE_CHECKPOINT = os.path.join(DATA_DIR, "page_checkpoint.json")

BASE_URL = "https://fmit.vn/en/glossary"
MAX_PAGES = 6729


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def get_chrome_version() -> str:
    """Get Chrome version from binary."""
    chrome_bin = os.getenv("CHROME_BIN")
    if not chrome_bin:
        chrome_bin = "google-chrome"
        if os.path.exists("/opt/hostedtoolcache/setup-chrome/chromium"):
            # GitHub Actions Chrome
            chrome_bin_pattern = "/opt/hostedtoolcache/setup-chrome/chromium/*/x64/chrome"
            matches = glob.glob(chrome_bin_pattern)
            if matches:
                chrome_bin = matches[0]
    
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        version_output = result.stdout.strip()
        logging.info(f"Chrome version output: {version_output}")
        
        # Extract version number (e.g., "Chromium 144.0.7508.0" -> "144.0.7508.0")
        match = re.search(r'(\d+\.\d+\.\d+\.\d+)', version_output)
        if match:
            full_version = match.group(1)
            logging.info(f"Detected full Chrome version: {full_version}")
            # Get major version (e.g., "144.0.7508.0" -> "144")
            major_version = full_version.split('.')[0]
            logging.info(f"Detected Chrome version: {major_version}")
            return major_version
        return None
    except Exception as e:
        logging.warning(f"Could not detect Chrome version: {e}")
        return None


def download_chromedriver_for_version(chrome_version: str) -> str:
    """Download ChromeDriver for a specific Chrome version from Chrome for Testing."""
    import platform
    
    try:
        # Detect platform
        system = platform.system().lower()
        if system == "darwin":
            # macOS
            if platform.machine().lower() in ["arm64", "aarch64"]:
                platform_name = "mac-arm64"
            else:
                platform_name = "mac-x64"
        elif system == "linux":
            platform_name = "linux64"
        elif system == "windows":
            platform_name = "win64"
        else:
            # Default to linux64 for GitHub Actions
            platform_name = "linux64"
        
        logging.info(f"Detected platform: {platform_name}")
        
        # Get available ChromeDriver versions
        versions_url = "https://googlechromelabs.github.io/chrome-for-testing/known-good-versions-with-downloads.json"
        response = requests.get(versions_url, timeout=30)
        response.raise_for_status()
        versions_data = response.json()
        
        # Find matching ChromeDriver version
        target_version = None
        for version_info in reversed(versions_data["versions"]):
            version_str = version_info["version"]
            if version_str.startswith(f"{chrome_version}."):
                target_version = version_str
                break
        
        if not target_version:
            logging.warning(f"No ChromeDriver found for Chrome {chrome_version}, trying latest")
            # Try to get the latest version for this major version
            for version_info in reversed(versions_data["versions"]):
                version_str = version_info["version"]
                if version_str.split('.')[0] == chrome_version:
                    target_version = version_str
                    break
        
        if not target_version:
            raise Exception(f"No ChromeDriver found for Chrome version {chrome_version}")
        
        logging.info(f"Found ChromeDriver version: {target_version}")
        
        # Get download URL for the detected platform
        download_url = None
        for version_info in versions_data["versions"]:
            if version_info["version"] == target_version:
                downloads = version_info.get("downloads", {})
                chromedriver = downloads.get("chromedriver", [])
                for item in chromedriver:
                    if item["platform"] == platform_name:
                        download_url = item["url"]
                        break
                break
        
        if not download_url:
            raise Exception(f"No {platform_name} ChromeDriver download found for version {target_version}")
        
        # Download and extract
        logging.info(f"Downloading ChromeDriver from {download_url}")
        cache_dir = Path.home() / ".wdm" / "drivers" / "chromedriver" / platform_name / target_version
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        # Determine zip filename based on platform
        zip_filename = {
            "linux64": "chromedriver-linux64.zip",
            "mac-x64": "chromedriver-mac-x64.zip",
            "mac-arm64": "chromedriver-mac-arm64.zip",
            "win64": "chromedriver-win64.zip"
        }.get(platform_name, "chromedriver.zip")
        
        zip_path = cache_dir / zip_filename
        response = requests.get(download_url, timeout=120)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        
        # Extract
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(cache_dir)
        
        # Find chromedriver executable (it might be in a subdirectory)
        chromedriver_path = None
        executable_name = "chromedriver.exe" if system == "windows" else "chromedriver"
        for root, dirs, files in os.walk(cache_dir):
            if executable_name in files:
                chromedriver_path = Path(root) / executable_name
                break
        
        if not chromedriver_path or not chromedriver_path.exists():
            raise Exception(f"ChromeDriver executable not found after extraction in {cache_dir}")
        
        # Make executable (not needed on Windows)
        if system != "windows":
            os.chmod(chromedriver_path, 0o755)
        
        logging.info(f"ChromeDriver installed at: {chromedriver_path}")
        return str(chromedriver_path)
        
    except Exception as e:
        logging.error(f"Failed to download ChromeDriver for version {chrome_version}: {e}")
        raise


def create_driver() -> webdriver.Chrome:
    logging.info("Creating Chrome driver...")
    
    # Get Chrome binary path - prioritize CHROME_BIN env var
    chrome_bin = os.getenv("CHROME_BIN")
    if not chrome_bin:
        # Fallback: try to find Chrome in GitHub Actions location
        if os.path.exists("/opt/hostedtoolcache/setup-chrome/chromium"):
            chrome_bin_pattern = "/opt/hostedtoolcache/setup-chrome/chromium/*/x64/chrome"
            matches = glob.glob(chrome_bin_pattern)
            if matches:
                chrome_bin = matches[0]
        else:
            chrome_bin = "google-chrome"
    
    # Verify Chrome binary exists
    if not os.path.exists(chrome_bin):
        raise FileNotFoundError(f"Chrome binary not found at: {chrome_bin}")
    
    # Verify it's executable
    if not os.access(chrome_bin, os.X_OK):
        raise PermissionError(f"Chrome binary is not executable: {chrome_bin}")
    
    logging.info(f"Using Chrome binary: {chrome_bin}")
    
    # Verify the version of this binary
    try:
        result = subprocess.run(
            [chrome_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=10
        )
        logging.info(f"Chrome binary version check: {result.stdout.strip()}")
    except Exception as e:
        logging.warning(f"Could not verify Chrome binary version: {e}")
    
    # Set Chrome binary location BEFORE getting version (so we use the same binary)
    chrome_options = Options()
    chrome_options.binary_location = chrome_bin
    
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    # Add user agent to avoid bot detection
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")
    
    # Get Chrome version using the same binary we'll use for Selenium
    chromedriver_path = None
    try:
        # Temporarily set CHROME_BIN so get_chrome_version uses the correct binary
        original_chrome_bin = os.getenv("CHROME_BIN")
        os.environ["CHROME_BIN"] = chrome_bin
        
        chrome_version = get_chrome_version()
        if chrome_version:
            logging.info(f"Installing ChromeDriver for Chrome {chrome_version}...")
            chromedriver_path = download_chromedriver_for_version(chrome_version)
        
        # Restore original env var
        if original_chrome_bin:
            os.environ["CHROME_BIN"] = original_chrome_bin
        elif "CHROME_BIN" in os.environ:
            del os.environ["CHROME_BIN"]
    except Exception as e:
        logging.warning(f"Failed to get ChromeDriver for specific version: {e}")
        logging.info("Falling back to webdriver-manager...")
    
    # Fallback to webdriver-manager if Chrome for Testing fails
    if not chromedriver_path:
        try:
            logging.info("Installing ChromeDriver via webdriver-manager...")
            chromedriver_path = ChromeDriverManager().install()
        except Exception as e:
            logging.error(f"Failed to install ChromeDriver: {e}")
            raise
    
    service = Service(chromedriver_path)
    logging.info("Starting Chrome browser...")
    logging.info(f"ChromeDriver path: {chromedriver_path}")
    logging.info(f"Chrome binary path: {chrome_bin}")
    
    try:
        driver = webdriver.Chrome(service=service, options=chrome_options)
        logging.info("Chrome driver created successfully")
        return driver
    except Exception as e:
        logging.error(f"Failed to create Chrome driver: {e}")
        logging.error(f"ChromeDriver path: {chromedriver_path}")
        logging.error(f"Chrome binary path: {chrome_bin}")
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


def extract_page_links(driver: webdriver.Chrome, url: str, max_retries: int = 5) -> Tuple[List[str], webdriver.Chrome]:
    for attempt in range(max_retries):
        try:
            driver.get(url)
            
            # Wait for page to load - wait for body or document ready
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Wait for the dictionary-items element to appear (with longer timeout)
            try:
                items = WebDriverWait(driver, 15).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "dictionary-items"))
                )
            except TimeoutException:
                # If element not found, log page source snippet for debugging
                page_source_preview = driver.page_source[:500] if driver.page_source else "No page source"
                logging.warning(f"Element '.dictionary-items' not found on {url}. Page source preview: {page_source_preview}...")
                # Try to find alternative selectors
                alternative_selectors = [
                    (By.CLASS_NAME, "dictionary"),
                    (By.CSS_SELECTOR, "[class*='dictionary']"),
                    (By.CSS_SELECTOR, "[class*='item']"),
                ]
                for selector_type, selector_value in alternative_selectors:
                    try:
                        elements = driver.find_elements(selector_type, selector_value)
                        if elements:
                            logging.info(f"Found {len(elements)} elements with selector {selector_type}:{selector_value}")
                    except:
                        pass
                raise
            
            links = items.find_elements(By.XPATH, './/li[@class="item"]/a[@href]')
            hrefs: List[str] = []
            for link in links:
                href = link.get_attribute("href")
                if href and "fmit.vn" in href and ("/glossary/" in href or "/tu-dien-quan-ly/" in href):
                    hrefs.append(href)
            
            if hrefs:
                logging.info(f"Found {len(hrefs)} links on {url}")
            else:
                logging.warning(f"No links found in dictionary-items on {url}")
            
            return list(set(hrefs)), driver
        except TimeoutException as e:
            logging.warning(f"Page timeout {url} (attempt {attempt + 1}/{max_retries}): {e}. Retry in 10s...")
            time.sleep(10)
        except Exception as e:
            logging.warning(f"Page error {url} (attempt {attempt + 1}/{max_retries}): {e}. Retry in 10s...")
            time.sleep(10)

    logging.error(f"Failed to extract links from {url} after {max_retries} attempts")
    return [], driver


def extract_url_data(driver: webdriver.Chrome, url: str, max_retries: int = 5) -> Tuple[Dict[str, str], webdriver.Chrome]:
    for _ in range(max_retries):
        try:
            driver.get(url)
            time.sleep(2)
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
            logging.warning(f"URL error {url}: {e}. Retry in 10s...")
            time.sleep(10)
    return {"url": url, "h1": "", "h2": "", "content": ""}, driver


def run_once() -> None:
    setup_logging()
    logging.info("=" * 60)
    logging.info("START CRAWL (GitHub Actions, Parquet)")
    logging.info("=" * 60)
    
    # Set maximum runtime (5.5 hours to stay under 6-hour limit)
    MAX_RUNTIME_SECONDS = 5.5 * 60 * 60  # 5.5 hours
    start_time = time.time()
    
    processed = load_processed_urls()
    logging.info(f"Loaded {len(processed)} already processed URLs")
    
    driver = create_driver()

    all_urls_collected: Set[str] = set()
    total_successful_extractions = 0
    total_failed_extractions = 0
    total_pages_processed = 0
    
    # Continuous loop: process batches until time limit or all pages done
    while True:
        elapsed_time = time.time() - start_time
        remaining_time = MAX_RUNTIME_SECONDS - elapsed_time
        
        if remaining_time < 600:  # Less than 10 minutes left, stop
            logging.warning(f"⏰ Only {remaining_time/60:.1f} minutes remaining. Stopping to avoid timeout.")
            break
        
        # Phase 1: Collect URLs from 10 pages (~600 links)
        start_page = load_page_checkpoint() + 1
        current_page = max(start_page, 1)
        
        if current_page > MAX_PAGES:
            logging.info("✅ All pages processed! Crawling complete.")
            break
        
        batch_urls: Set[str] = set()
        pages_to_collect = 10  # Collect from 10 pages per batch
        
        logging.info(f"⏱️  Runtime: {elapsed_time/3600:.2f}h | Remaining: {remaining_time/3600:.2f}h")
        logging.info(f"Phase 1: Collecting URLs from {pages_to_collect} pages (starting at page {current_page})")
        
        pages_processed = 0
        while current_page <= MAX_PAGES and pages_processed < pages_to_collect:
            # Check time limit
            if time.time() - start_time > MAX_RUNTIME_SECONDS - 600:
                logging.warning("⏰ Approaching time limit, stopping URL collection")
                break
                
            url = BASE_URL if current_page == 1 else f"{BASE_URL}?page={current_page}"
            hrefs, driver = extract_page_links(driver, url)
            new_hrefs = [h for h in hrefs if h not in processed and h not in all_urls_collected and h not in batch_urls]
            batch_urls.update(new_hrefs)
            
            # Always save checkpoint after processing a page
            save_page_checkpoint(current_page)
            pages_processed += 1
            total_pages_processed += 1
            
            logging.info(f"Page {current_page}: Found {len(hrefs)} links, {len(new_hrefs)} new (batch: {len(batch_urls)})")
            
            current_page += 1
            time.sleep(2)
        
        logging.info(f"Phase 1 Complete: Collected {len(batch_urls)} new URLs from {pages_processed} pages")
        all_urls_collected.update(batch_urls)
        
        if not batch_urls:
            logging.info("No new URLs to process in this batch. Continuing to next batch...")
            # Still increment checkpoint even if no links found
            if current_page <= MAX_PAGES:
                save_page_checkpoint(current_page - 1)
            continue
        
        # Phase 2: Extract content from URLs in small batches and save incrementally
        logging.info("=" * 60)
        logging.info(f"Phase 2: Extracting content from {len(batch_urls)} URLs")
        logging.info("=" * 60)
        
        batch: List[Dict[str, str]] = []
        successful_extractions = 0
        failed_extractions = 0
        batch_size = 20  # Save every 20 successful extractions
        
        for idx, url in enumerate(batch_urls, 1):
            # Check time limit
            if time.time() - start_time > MAX_RUNTIME_SECONDS - 300:  # 5 min buffer
                logging.warning("⏰ Approaching time limit, stopping content extraction")
                break
                
            try:
                data, driver = extract_url_data(driver, url)
                
                # Only append if we got actual content (not empty)
                if data.get("h1") or data.get("h2") or data.get("content"):
                    batch.append(data)
                    successful_extractions += 1
                    logging.info(f"[{idx}/{len(batch_urls)}] ✅ Extracted: {url[:80]}...")
                else:
                    failed_extractions += 1
                    logging.warning(f"[{idx}/{len(batch_urls)}] ⚠️  Empty content: {url[:80]}...")
                
                # Save batch incrementally to avoid data loss
                if len(batch) >= batch_size:
                    append_to_parquet(batch)
                    logging.info(f"💾 Saved batch of {len(batch)} URLs to parquet")
                    batch = []
                    
            except Exception as e:
                failed_extractions += 1
                logging.error(f"[{idx}/{len(batch_urls)}] ❌ Failed to extract {url[:80]}...: {e}")
            
            time.sleep(1.5)
        
        # Save remaining batch
        if batch:
            append_to_parquet(batch)
            logging.info(f"💾 Saved final batch of {len(batch)} URLs to parquet")
        
        total_successful_extractions += successful_extractions
        total_failed_extractions += failed_extractions
        
        logging.info(f"Batch Complete: ✅ {successful_extractions} successful | ❌ {failed_extractions} failed")
        
        # Commit after each batch to save progress
        logging.info("💾 Committing progress to git...")
        try:
            import subprocess
            subprocess.run(["git", "config", "user.name", "github-actions[bot]"], check=False)
            subprocess.run(["git", "config", "user.email", "41898282+github-actions[bot]@users.noreply.github.com"], check=False)
            subprocess.run(["git", "add", "-A"], check=False)
            subprocess.run(["git", "commit", "-m", f"data: batch update - pages {start_page}-{current_page-1}, {successful_extractions} URLs"], check=False)
            subprocess.run(["git", "push"], check=False)
            logging.info("✅ Progress committed to git")
        except Exception as e:
            logging.warning(f"⚠️  Could not commit to git: {e}")
        
        # Check if we should continue
        elapsed_time = time.time() - start_time
        if elapsed_time > MAX_RUNTIME_SECONDS - 600:
            logging.warning("⏰ Approaching time limit, stopping crawler")
            break
    
    logging.info("=" * 60)
    logging.info(f"RUN COMPLETE:")
    logging.info(f"  📄 Pages processed: {total_pages_processed}")
    logging.info(f"  🔗 URLs collected: {len(all_urls_collected)}")
    logging.info(f"  ✅ Successful extractions: {total_successful_extractions}")
    logging.info(f"  ❌ Failed extractions: {total_failed_extractions}")
    logging.info(f"  ⏱️  Runtime: {(time.time() - start_time)/3600:.2f} hours")
    logging.info("=" * 60)

    try:
        driver.quit()
    except Exception:
        pass
    
    logging.info("DONE. Data saved in Parquet. Next run will continue from page checkpoint.")


if __name__ == "__main__":
    run_once()


