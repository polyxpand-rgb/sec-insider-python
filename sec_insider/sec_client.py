"""Client for fetching SEC EDGAR Form 4 filings."""
from __future__ import annotations

import time
from datetime import date
from typing import List, Dict

import requests
from requests import Response

from .config import load_config

# SEC search endpoint
SEARCH_URL = "https://efts.sec.gov/LATEST/search-index"


class SecClient:
    def __init__(self):
        self.config = load_config()
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self.config.user_agent})
        self.max_retries = 3

    def _sleep(self):
        time.sleep(1 / max(self.config.rate_limit_per_second, 1))

    def _request(self, method: str, url: str, **kwargs) -> Response:
        for attempt in range(1, self.max_retries + 1):
            self._sleep()
            try:
                resp = self.session.request(method, url, timeout=30, **kwargs)
            except requests.RequestException:
                if attempt == self.max_retries:
                    raise
                continue
            if resp.status_code >= 500:
                if attempt == self.max_retries:
                    resp.raise_for_status()
                continue
            if resp.status_code >= 400:
                resp.raise_for_status()
            return resp
        raise RuntimeError("HTTP request failed after retries")

    def fetch_form4_filings_metadata(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Return metadata for Form 4 filings between the given dates.
        Uses the SEC search API with pagination.
        """
        query = f"formType:\"4\" AND filedAt:[{start_date} TO {end_date}]"
        size = 200
        start = 0
        results: List[Dict] = []

        while True:
            payload = {
                "query": query,
                "from": start,
                "size": size,
                "sort": [{"filedAt": {"order": "asc"}}],
            }
            resp = self._request("POST", SEARCH_URL, json=payload)
            data = resp.json()
            hits = data.get("hits", [])
            if not hits:
                break
            for item in hits:
                results.append(
                    {
                        "accession_no": item.get("adsh") or item.get("accessionNo"),
                        "filed_at": item.get("filedAt"),
                        "form_type": item.get("formType"),
                        "company_name": item.get("companyName"),
                        "cik": item.get("cik") or (item.get("ciks") or [None])[0],
                        "primary_document": item.get("primaryDocument"),
                        "link_to_filing": item.get("linkToFilingDetails"),
                    }
                )
            if len(hits) < size:
                break
            start += size
        return results

    def fetch_form4_raw(self, accession_metadata: Dict) -> str:
        """Given metadata, download the raw Form 4 content as text."""
        accession_no = accession_metadata.get("accession_no") or accession_metadata.get("accessionNumber")
        cik = accession_metadata.get("cik")
        primary_doc = accession_metadata.get("primary_document")
        if not accession_no or not cik or not primary_doc:
            raise ValueError("Metadata missing required fields")

        accession_no_nodash = accession_no.replace("-", "")
        cik_padded = str(cik).lstrip("0")
        url = f"https://www.sec.gov/Archives/edgar/data/{cik_padded}/{accession_no_nodash}/{primary_doc}"

        resp = self._request("GET", url)
        return resp.text


def fetch_form4_filings_metadata(start_date: date, end_date: date) -> List[Dict]:
    return SecClient().fetch_form4_filings_metadata(start_date, end_date)


def fetch_form4_raw(accession_metadata: Dict) -> str:
    return SecClient().fetch_form4_raw(accession_metadata)


__all__ = [
    "SecClient",
    "fetch_form4_filings_metadata",
    "fetch_form4_raw",
]