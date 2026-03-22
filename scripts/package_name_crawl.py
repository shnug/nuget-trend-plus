"""
NuGet Package Name Crawler
Uses the official NuGet Catalog API to enumerate all package names.
"""

import requests
import time
import json
import logging
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── Configuration ─────────────────────────────────────────────────────────────

OUTPUT_FILE   = "nuget_packages.txt"      # One package ID per line
PROGRESS_FILE = "nuget_progress.json"     # Tracks resume state
MAX_WORKERS   = 5                         # Parallel page fetches
RATE_LIMIT    = 0.2                       # Seconds between requests per worker
LOG_LEVEL     = logging.INFO

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("nuget_crawler.log"),
    ],
)
log = logging.getLogger(__name__)

# ── HTTP Session with Retries ──────────────────────────────────────────────────

def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,          # 1.5s, 3s, 6s, 12s, 24s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({"User-Agent": "nuget-crawler/1.0 (research)"})
    return session

SESSION = make_session()

# ── Progress Persistence ───────────────────────────────────────────────────────

def load_progress() -> set:
    """Return set of already-processed page URLs."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return set(json.load(f).get("done_pages", []))
    return set()

def save_progress(done_pages: set) -> None:
    with open(PROGRESS_FILE, "w") as f:
        json.dump({"done_pages": list(done_pages)}, f)

# ── API Helpers ────────────────────────────────────────────────────────────────

def get_json(url: str) -> dict:
    """GET a URL and return parsed JSON, with basic rate limiting."""
    time.sleep(RATE_LIMIT)
    resp = SESSION.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_catalog_url() -> str:
    log.info("Fetching NuGet service index…")
    index = get_json("https://api.nuget.org/v3/index.json")
    for resource in index["resources"]:
        if resource.get("@type") == "Catalog/3.0.0":
            return resource["@id"]
    raise RuntimeError("Catalog/3.0.0 resource not found in service index.")

def get_catalog_pages(catalog_url: str) -> list[dict]:
    log.info("Fetching catalog root…")
    catalog = get_json(catalog_url)
    pages = catalog.get("items", [])
    log.info(f"Found {len(pages):,} catalog pages.")
    return pages

# ── Page Processing ────────────────────────────────────────────────────────────

def process_page(page_meta: dict) -> tuple[str, list[str]]:
    """
    Fetch one catalog page and return (page_url, [package_ids]).
    Package IDs are deduplicated within the page.
    """
    page_url = page_meta["@id"]
    try:
        page_data = get_json(page_url)
        ids = list({
            item["nuget:id"]
            for item in page_data.get("items", [])
            if "nuget:id" in item
        })
        return page_url, ids
    except Exception as exc:
        log.warning(f"Failed to fetch page {page_url}: {exc}")
        return page_url, []

# ── Main Crawler ───────────────────────────────────────────────────────────────

def crawl() -> None:
    start_time = datetime.now()
    log.info("=== NuGet Crawler Started ===")

    catalog_url = get_catalog_url()
    pages       = get_catalog_pages(catalog_url)
    done_pages  = load_progress()

    pending = [p for p in pages if p["@id"] not in done_pages]
    log.info(f"Pages to process: {len(pending):,}  |  Already done: {len(done_pages):,}")

    total_written = 0
    errors        = 0

    with open(OUTPUT_FILE, "a", buffering=1) as out_file:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(process_page, p): p for p in pending}

            for i, future in enumerate(as_completed(futures), 1):
                page_url, ids = future.result()

                if ids:
                    out_file.write("\n".join(ids) + "\n")
                    total_written += len(ids)
                    done_pages.add(page_url)
                else:
                    errors += 1

                # Log progress every 50 pages
                if i % 50 == 0 or i == len(pending):
                    elapsed = (datetime.now() - start_time).seconds
                    log.info(
                        f"Pages: {i:,}/{len(pending):,} | "
                        f"Packages written: {total_written:,} | "
                        f"Errors: {errors} | "
                        f"Elapsed: {elapsed}s"
                    )

                # Save progress every 100 pages so we can resume on failure
                if i % 100 == 0:
                    save_progress(done_pages)

    save_progress(done_pages)

    elapsed = datetime.now() - start_time
    log.info("=== Crawl Complete ===")
    log.info(f"Total packages written : {total_written:,}")
    log.info(f"Total pages processed  : {len(done_pages):,}")
    log.info(f"Errors                 : {errors}")
    log.info(f"Time elapsed           : {elapsed}")
    log.info(f"Output file            : {OUTPUT_FILE}")

    # Deduplicate output file
    log.info("Deduplicating output file…")
    deduplicate(OUTPUT_FILE)

def deduplicate(filepath: str) -> None:
    """Read the output file, deduplicate IDs, and rewrite it sorted."""
    with open(filepath) as f:
        ids = {line.strip() for line in f if line.strip()}

    with open(filepath, "w") as f:
        f.write("\n".join(sorted(ids)) + "\n")

    log.info(f"Unique package IDs: {len(ids):,}")

# ── Entry Point ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    crawl()