#!/usr/bin/env python3
"""
fetch_data.py — SeeThroughNY South Country CSD / Central Schools Payroll Fetcher

Calls the seethroughny.net API directly to fetch all payroll records
for South Country CSD and South Country Central Schools across all available years, saves to JSON.

Usage:
  python3 fetch_data.py              # fetch all years
  python3 fetch_data.py --year 2025  # fetch specific year
  python3 fetch_data.py --year 2024,2025  # fetch multiple years
"""

import requests
import json
import re
import time
import argparse
import sys
from pathlib import Path

API_URL    = "https://www.seethroughny.net/tools/required/reports/payroll?action=get"
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "south_country_csd.json"
DELAY      = 2.0   # seconds between page requests
AGENCIES   = ["South Country CSD", "South Country Central Schools"]

# SeeThroughNY has payroll data starting from 2008; without explicit years the
# API silently returns only the most recent ~5 years.
ALL_YEARS = list(range(2008, 2026))

HEADERS = {
    "User-Agent":      "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept":          "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "Referer":         "https://www.seethroughny.net/payrolls",
    "Origin":          "https://www.seethroughny.net",
    "Content-Type":    "application/x-www-form-urlencoded; charset=UTF-8",
}


def parse_html_rows(html):
    """
    Parse the HTML returned by the API into a list of record dicts.

    Each record has two <tr> elements:
      - resultRow{id}: summary (name, agency, total_pay, subagency_type)
      - expandRow{id}: details (title, rate_of_pay, pay_year, pay_basis, branch)
    """
    records = []

    # Split into result/expand row pairs using record IDs
    result_rows = re.findall(
        r'<tr id="resultRow(\d+)".*?</tr>',
        html, re.DOTALL
    )
    expand_rows = re.findall(
        r'<tr id="expandRow(\d+)".*?</tr>',
        html, re.DOTALL
    )

    result_map = {}
    expand_map = {}

    for row_html in re.finditer(r'<tr id="resultRow(\d+)"(.*?)</tr>', html, re.DOTALL):
        rid = row_html.group(1)
        content = row_html.group(2)
        tds = re.findall(r'<td[^>]*>(.*?)</td>', content, re.DOTALL)
        if len(tds) >= 4:
            # strip HTML tags and decode entities
            def clean(s):
                s = re.sub(r'<[^>]+>', '', s)
                s = s.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&nbsp;', ' ')
                return s.strip()
            result_map[rid] = {
                "id": rid,
                "name":          clean(tds[1]),
                "agency":        clean(tds[2]),
                "total_pay_str": clean(tds[3]),
                "subagency_type_summary": clean(tds[4]) if len(tds) > 4 else "",
            }

    for row_html in re.finditer(r'<tr id="expandRow(\d+)"(.*?)</tr>', html, re.DOTALL):
        rid = row_html.group(1)
        content = row_html.group(2)
        # Extract label: value pairs from the detail div rows
        pairs = re.findall(
            r'<strong>(.*?)</strong>.*?<div class="col-xs-6">(.*?)</div>',
            content, re.DOTALL
        )
        def clean(s):
            s = re.sub(r'<[^>]+>', '', s)
            s = s.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&nbsp;', ' ')
            return s.strip()

        detail = {}
        for label, val in pairs:
            detail[clean(label)] = clean(val)
        expand_map[rid] = detail

    # Merge result + expand rows
    for rid, rdata in result_map.items():
        detail = expand_map.get(rid, {})

        # Parse total pay as a number
        pay_str = rdata["total_pay_str"].replace("$", "").replace(",", "").strip()
        try:
            total_pay = float(pay_str)
        except ValueError:
            total_pay = None

        # Parse rate of pay
        rate_str = detail.get("Rate of Pay", "NDR").replace("$", "").replace(",", "").strip()
        try:
            rate_of_pay = float(rate_str)
        except ValueError:
            rate_of_pay = None  # NDR or blank

        record = {
            "id":            rid,
            "name":          rdata["name"],
            "agency":        rdata["agency"],
            "total_pay":     total_pay,
            "total_pay_str": rdata["total_pay_str"],
            "subagency_type": detail.get("SubAgency/Type", rdata["subagency_type_summary"]),
            "title":         detail.get("Title", ""),
            "rate_of_pay":   rate_of_pay,
            "rate_of_pay_str": detail.get("Rate of Pay", "NDR"),
            "pay_year":      detail.get("Pay Year", ""),
            "pay_basis":     detail.get("Pay Basis", ""),
            "branch":        detail.get("Branch/Major Category", ""),
        }
        records.append(record)

    return records


def fetch_all_pages(years=None):
    """
    Fetch all pages for the given list of years (or all years if None).
    Returns a list of record dicts.
    """
    all_records = []
    seen_ids = set()

    # The API defaults to ~5 recent years when PayYear[] is omitted, so always
    # send explicit years.
    years_to_fetch = years if years else ALL_YEARS

    post_data = [("AgencyName[]", a) for a in AGENCIES]
    for y in years_to_fetch:
        post_data.append(("PayYear[]", str(y)))
    post_data += [
        ("SortBy",       "YTDPay DESC"),
        ("url",          "/tools/required/reports/payroll?action=get"),
        ("nav_request",  "0"),
    ]

    def post_with_retry(data, page_label, retries=3):
        for attempt in range(1, retries + 1):
            try:
                r = requests.post(API_URL, headers=HEADERS, data=data, timeout=60)
                r.raise_for_status()
                return r.json()
            except (requests.Timeout, requests.ConnectionError) as e:
                if attempt == retries:
                    raise
                wait = attempt * 5
                print(f"  {page_label}: timeout/error ({e}), retrying in {wait}s...")
                time.sleep(wait)

    # ---- Page 1 ----
    print(f"Fetching page 1 for {AGENCIES} (years: {years_to_fetch[0]}–{years_to_fetch[-1]})...")
    resp = post_with_retry(post_data, "Page 1")

    total_records = int(resp.get("total_records", 0))
    total_pages   = int(resp.get("total_pages", 1))
    result_id     = resp.get("result_id", "")
    total_sum     = resp.get("total_sum", "")

    print(f"  Total records: {total_records}, Total pages: {total_pages}")
    print(f"  Total sum: ${float(total_sum or 0):,.2f}" if total_sum else "")

    page_records = parse_html_rows(resp["html"])
    for rec in page_records:
        if rec["id"] not in seen_ids:
            seen_ids.add(rec["id"])
            all_records.append(rec)
    print(f"  Page 1: parsed {len(page_records)} rows (running total: {len(all_records)})")

    # ---- Pages 2..N ----
    current_page = 1  # after first call, JS would set this to 1 (null++)
    for page_num in range(2, total_pages + 1):
        time.sleep(DELAY)

        page_data = list(post_data) + [
            ("current_page", str(current_page)),
            ("result_id",    result_id),
        ]

        print(f"Fetching page {page_num}/{total_pages}...")
        resp = post_with_retry(page_data, f"Page {page_num}")

        page_records = parse_html_rows(resp["html"])
        new_count = 0
        for rec in page_records:
            if rec["id"] not in seen_ids:
                seen_ids.add(rec["id"])
                all_records.append(rec)
                new_count += 1
        print(f"  Page {page_num}: parsed {len(page_records)} rows, {new_count} new (running total: {len(all_records)})")

        # current_page increments by 1 each time
        current_page = resp.get("current_page", current_page)
        if current_page is None:
            current_page = page_num
        else:
            current_page = int(current_page) + 1

    return all_records, total_records, total_sum


def main():
    parser = argparse.ArgumentParser(description="Fetch South Country Central Schools payroll data from SeeThroughNY")
    parser.add_argument("--year", type=str, default=None,
                        help="Comma-separated years to fetch, e.g. 2024,2025 (default: all years)")
    args = parser.parse_args()

    years = None
    if args.year:
        years = [y.strip() for y in args.year.split(",")]

    OUTPUT_DIR.mkdir(exist_ok=True)

    try:
        records, total_records, total_sum = fetch_all_pages(years)
    except requests.RequestException as e:
        print(f"ERROR fetching data: {e}", file=sys.stderr)
        sys.exit(1)

    output = {
        "meta": {
            "agency":         AGENCIES,
            "years_requested": years if years else f"{ALL_YEARS[0]}-{ALL_YEARS[-1]}",
            "total_records":   int(total_records),
            "total_sum":       float(total_sum) if total_sum else None,
            "records_fetched": len(records),
            "source":          "https://www.seethroughny.net/payrolls",
        },
        "records": records,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2)

    print()
    print("=" * 50)
    print(f"Done! Fetched {len(records)} records (expected {total_records})")
    print(f"Saved to: {OUTPUT_FILE}")
    if total_sum:
        print(f"Total payroll sum: ${float(total_sum):,.2f}")

    # Print year breakdown
    from collections import Counter
    year_counts = Counter(r["pay_year"] for r in records)
    print("\nBreakdown by year:")
    for yr, cnt in sorted(year_counts.items()):
        print(f"  {yr}: {cnt} records")


if __name__ == "__main__":
    main()
