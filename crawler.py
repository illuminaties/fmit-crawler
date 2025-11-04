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
    try:
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
        
        # Get download URL for Linux
        download_url = None
        for version_info in versions_data["versions"]:
            if version_info["version"] == target_version:
                downloads = version_info.get("downloads", {})
                chromedriver = downloads.get("chromedriver", [])
                for item in chromedriver:
                    if item["platform"] == "linux64":
                        download_url = item["url"]
                        break
                break
        
        if not download_url:
            raise Exception(f"No Linux64 ChromeDriver download found for version {target_version}")
        
        # Download and extract
        logging.info(f"Downloading ChromeDriver from {download_url}")
        cache_dir = Path.home() / ".wdm" / "drivers" / "chromedriver" / "linux64" / target_version
        cache_dir.mkdir(parents=True, exist_ok=True)
        
        zip_path = cache_dir / "chromedriver-linux64.zip"
        response = requests.get(download_url, timeout=120)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        
        # Extract
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(cache_dir)
        
        # Find chromedriver executable (it might be in a subdirectory)
        chromedriver_path = None
        for root, dirs, files in os.walk(cache_dir):
            if "chromedriver" in files:
                chromedriver_path = Path(root) / "chromedriver"
                break
        
        if not chromedriver_path or not chromedriver_path.exists():
            raise Exception(f"ChromeDriver executable not found after extraction in {cache_dir}")
        
        # Make executable
        os.chmod(chromedriver_path, 0o755)
        
        logging.info(f"ChromeDriver installed at: {chromedriver_path}")
        return str(chromedriver_path)
        
    except Exception as e:
        logging.error(f"Failed to download ChromeDriver for version {chrome_version}: {e}")
        raise


def create_driver() -> webdriver.Chrome:
    chrome_options = Options()
    
    # Set Chrome binary if provided
    chrome_bin = os.getenv("CHROME_BIN")
    if chrome_bin:
        chrome_options.binary_location = chrome_bin
        logging.info(f"Using Chrome binary: {chrome_bin}")
    
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    
    # Try to get Chrome version and download matching ChromeDriver
    chromedriver_path = None
    try:
        chrome_version = get_chrome_version()
        if chrome_version:
            logging.info(f"Installing ChromeDriver for Chrome {chrome_version}...")
            chromedriver_path = download_chromedriver_for_version(chrome_version)
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
    driver = webdriver.Chrome(service=service, options=chrome_options)
    logging.info("Chrome driver created successfully")
    return driver


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
    for _ in range(max_retries):
        try:
            driver.get(url)
            time.sleep(3)
            items = driver.find_element(By.CLASS_NAME, "dictionary-items")
            links = items.find_elements(By.XPATH, './/li[@class="item"]/a[@href]')
            hrefs: List[str] = []
            for link in links:
                href = link.get_attribute("href")
                if href and "fmit.vn" in href and ("/glossary/" in href or "/tu-dien-quan-ly/" in href):
                    hrefs.append(href)
            return list(set(hrefs)), driver
        except Exception as e:
            logging.warning(f"Page error {url}: {e}. Retry in 10s...")
            time.sleep(10)
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
    logging.info("START CRAWL (GitHub Actions, Parquet)")
    processed = load_processed_urls()
    driver = create_driver()

    start_page = load_page_checkpoint() + 1
    current_page = max(start_page, 1)
    all_urls: Set[str] = set()

    logging.info(f"Collect URLs from page {current_page} to {MAX_PAGES}")
    while current_page <= MAX_PAGES:
        url = BASE_URL if current_page == 1 else f"{BASE_URL}?page={current_page}"
        hrefs, driver = extract_page_links(driver, url)
        new_hrefs = [h for h in hrefs if h not in processed and h not in all_urls]
        all_urls.update(new_hrefs)
        if new_hrefs or hrefs:
            save_page_checkpoint(current_page)
        logging.info(f"Page {current_page}: +{len(new_hrefs)} new links (total: {len(all_urls)})")
        current_page += 1
        time.sleep(2)
        if len(all_urls) >= 1500:
            break

    logging.info(f"Total new URLs this run: {len(all_urls)}")

    batch: List[Dict[str, str]] = []
    for idx, url in enumerate(all_urls, 1):
        data, driver = extract_url_data(driver, url)
        batch.append(data)
        if len(batch) >= 50:
            append_to_parquet(batch)
            batch = []
            logging.info(f"Saved {idx}/{len(all_urls)} URLs")
        time.sleep(1.5)

    if batch:
        append_to_parquet(batch)

    try:
        driver.quit()
    except Exception:
        pass
    logging.info("DONE. Data saved in Parquet and can resume next run.")


if __name__ == "__main__":
    run_once()


