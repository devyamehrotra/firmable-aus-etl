import requests
import csv
import os
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import io
import logging
from typing import Optional
import json
import re

class CommonCrawlExtractor:
    CDX_API = "https://index.commoncrawl.org/CC-MAIN-2025-18-index"
    WARC_BASE = "https://data.commoncrawl.org/"

    def __init__(self, settings: Optional[object] = None):
        self.settings = settings
        self.logger = logging.getLogger("CommonCrawlExtractor")
        logging.basicConfig(level=logging.INFO)

    def query_cdx(self, domain_pattern: str, limit: int = 100, offset: int = 0):
        params = {
            "url": domain_pattern,
            "output": "json",
            "filter": "mime:text/html",
            "limit": str(limit),
            "offset": str(offset)
        }
        self.logger.info(f"Querying CDX API for pattern {domain_pattern} (limit {limit}, offset {offset})...")
        resp = requests.get(self.CDX_API, params=params, timeout=60)
        self.logger.info(resp)
        lines = resp.text.strip().split('\n')
        records = []
        for line in lines:
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    records.append(obj)
            except Exception as e:
                self.logger.warning(f"Skipping invalid line in CDX response: {line[:100]}... ({e})")
        return records

    def fetch_html_from_warc(self, filename: str, offset: str, length: str):
        url = self.WARC_BASE + filename
        headers = {"Range": f"bytes={offset}-{int(offset)+int(length)-1}"}
        self.logger.info(f"Fetching WARC record: {url} [{headers['Range']}] ...")
        resp = requests.get(url, headers=headers, timeout=60)
        if resp.status_code not in (200, 206):
            self.logger.warning(f"Failed to fetch WARC record: {resp.status_code}")
            return None
        stream = io.BytesIO(resp.content)
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                return record.content_stream().read()
        return None

    def extract_company_info(self, html_bytes):
        soup = BeautifulSoup(html_bytes, "html.parser")
        company_name = None

        # 1. Try Open Graph site name
        og_site_name = soup.find("meta", property="og:site_name")
        if og_site_name and og_site_name.get("content"):
            company_name = og_site_name["content"].strip()

        # 2. Try JSON-LD Organization name
        if not company_name:
            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and data.get("@type") == "Organization" and "name" in data:
                        company_name = data["name"].strip()
                        break
                    if isinstance(data, list):
                        for entry in data:
                            if isinstance(entry, dict) and entry.get("@type") == "Organization" and "name" in entry:
                                company_name = entry["name"].strip()
                                break
                except Exception:
                    continue

        # 3. Try schema.org microdata
        if not company_name:
            itemprop_name = soup.find(attrs={"itemprop": "name"})
            if itemprop_name and itemprop_name.text:
                company_name = itemprop_name.text.strip()

        # 4. Try common class names
        if not company_name:
            for class_name in ["company-name", "logo", "brand"]:
                tag = soup.find(class_=class_name)
                if tag and tag.text:
                    company_name = tag.text.strip()
                    break

        # 5. Fallback to <title> (but clean it)
        if not company_name:
            title = soup.title.string.strip() if soup.title and soup.title.string else None
            if title:
                # Remove common suffixes/prefixes
                # e.g., "Microsoft Office Is Getting A Name Change - 123 Buy Online" -> "123 Buy Online"
                # Try to extract after the last dash or pipe
                parts = re.split(r"[-|–—]", title)
                if len(parts) > 1:
                    company_name = parts[-1].strip()
                else:
                    company_name = title

        # 6. Fallback to <h1>
        if not company_name:
            h1 = soup.h1.string.strip() if soup.h1 and soup.h1.string else None
            company_name = h1

        # 7. Industry (unchanged)
        meta_keywords = soup.find("meta", attrs={"name": "keywords"})
        industry = meta_keywords["content"] if meta_keywords and meta_keywords.has_attr("content") else None

        return company_name, industry

    def extract_all_australian_companies(self, domain_pattern: str = "*.au", batch_size: int = 10000, max_batches: Optional[int] = None, csv_path: str = None):
        import os
        if csv_path is None:
            # Always resolve relative to project root
            SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
            PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
            csv_path = os.path.join(PROJECT_ROOT, 'data', 'common_crawl', 'raw', 'companies.csv')
        else:
            # If a path is provided, resolve it relative to project root if not absolute
            if not os.path.isabs(csv_path):
                SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
                PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
                csv_path = os.path.join(PROJECT_ROOT, csv_path)
        offset = 0
        batch_num = 0
        fieldnames = ["website_url", "company_name", "industry"]
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        write_header = not os.path.exists(csv_path)
        with open(csv_path, 'a', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            while True:
                records = self.query_cdx(domain_pattern, limit=batch_size, offset=offset)
                if not records:
                    break
                for rec in records:
                    html_bytes = self.fetch_html_from_warc(rec["filename"], rec["offset"], rec["length"])
                    if not html_bytes:
                        continue
                    company_name, industry = self.extract_company_info(html_bytes)
                    row = {
                        "website_url": rec["url"],
                        "company_name": company_name,
                        "industry": industry
                    }
                    writer.writerow(row)
                    self.logger.info(f"Wrote: {row}")
                    print("Writing to:", os.path.abspath(csv_path))
                offset += batch_size
                batch_num += 1
                if max_batches and batch_num >= max_batches:
                    break
        print("Writing to:", os.path.abspath(csv_path))

if __name__ == "__main__":
    extractor = CommonCrawlExtractor()
    extractor.extract_all_australian_companies(
        domain_pattern="*.au",
        batch_size=10000,         # Adjust as needed for your resources
        max_batches=None,         # Set to None for all, or a number for testing
        csv_path="data/common_crawl/raw/companies.csv"
    ) 