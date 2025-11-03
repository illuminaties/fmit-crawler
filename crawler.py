import os
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


def setup_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def create_driver() -> webdriver.Chrome:
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--blink-settings=imagesEnabled=false")
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=chrome_options)


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


