#!/usr/bin/env python3
"""
Scrape Oxford AI Centre course data and write courses_cache.json.

This script mirrors the parsing logic from course_browser.html.
It runs in GitHub Actions on a daily cron schedule.
"""

import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

SOURCE_URL = "https://oerc.ox.ac.uk/ai-centre/training-events/workshops-and-webinars"
BASE_URL = "https://oerc.ox.ac.uk"
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "courses_cache.json")
MAX_CONCURRENT = 3
REQUEST_TIMEOUT = 30
USER_AGENT = "Mozilla/5.0 (compatible; AI-Course-Browser-Bot/1.0)"

THEME_MAP = {
    "Getting Started with AI at Oxford": "General AI",
    "AI at Oxford": "General AI",
    "Mapping the Landscape": "General AI",
    "AI for Strategic Thinking and Operations": "Operations & Admin",
    "AI in Education": "Teaching & Education",
    "AI for Coding": "Coding & Development",
    "AI for Researchers": "Research",
    "Other training": "General AI",
}


def log(msg):
    print(f"[SyncCourses] {msg}", file=sys.stderr)


def fetch_with_retry(url, retries=3):
    headers = {"User-Agent": USER_AGENT}
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            log(f"Fetch attempt {attempt + 1}/{retries} failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return None


def normalize_url(href):
    if not href or not href.strip():
        return ""
    href = href.strip()
    if href.startswith("/"):
        return BASE_URL + href
    return href


def get_text(el):
    return el.get_text(strip=True) if el else ""


def get_date_cell_text(td):
    if not td:
        return ""
    text = td.get_text(separator="\n")
    parts = [s.strip() for s in text.split("\n") if s.strip()]
    return "; ".join(parts) if parts else ""


def get_theme(heading):
    for key, val in THEME_MAP.items():
        if key in heading:
            return val
    return "General AI"


def get_section_audience(heading):
    if re.search(r"AI for Researchers", heading, re.IGNORECASE):
        return "Researchers"
    if re.search(r"AI in Education", heading, re.IGNORECASE):
        return "Educators"
    return ""


def parse_table_ai_at_oxford(table, heading):
    courses = []
    theme = get_theme(heading)
    rows = table.find_all("tr")
    for row in rows[1:]:  # skip header
        tds = row.find_all("td")
        if len(tds) < 2:
            continue
        a = tds[0].find("a")
        title = get_text(a) if a else get_text(tds[0])
        href = a.get("href", "") if a else ""
        audience = get_text(tds[1])
        fmt = get_text(tds[2]) if len(tds) >= 3 else ""
        if not title:
            continue
        courses.append({
            "title": title,
            "link": normalize_url(href),
            "theme": theme,
            "date": "Multiple dates available",
            "audience": audience,
            "format": fmt,
            "experience": "",
            "tags": "",
            "training_provider": "",
        })
    return courses


def parse_table_two_cols(table, heading):
    courses = []
    theme = get_theme(heading)
    section_audience = get_section_audience(heading)
    rows = table.find_all("tr")
    for row in rows[1:]:
        tds = row.find_all("td")
        if len(tds) < 1:
            continue
        a = tds[0].find("a")
        title = get_text(a) if a else get_text(tds[0])
        href = a.get("href", "") if a else ""
        date_text = get_date_cell_text(tds[1]) if len(tds) > 1 else ""
        if not title:
            continue
        courses.append({
            "title": title,
            "link": normalize_url(href),
            "theme": theme,
            "date": date_text,
            "audience": section_audience,
            "format": "",
            "experience": "",
            "tags": "",
            "training_provider": "",
        })
    return courses


def parse_table_other_training(table, heading):
    courses = []
    theme = get_theme(heading)
    rows = table.find_all("tr")
    for row in rows[1:]:
        tds = row.find_all("td")
        if len(tds) < 2:
            continue
        a = tds[0].find("a")
        title = get_text(a) if a else get_text(tds[0])
        href = a.get("href", "") if a else ""
        provider = get_text(tds[1])
        date_text = get_date_cell_text(tds[2]) if len(tds) > 2 else ""
        if not title:
            continue
        courses.append({
            "title": title,
            "link": normalize_url(href),
            "theme": theme,
            "date": date_text,
            "audience": "",
            "format": "",
            "experience": "",
            "tags": "",
            "training_provider": provider,
        })
    return courses


def find_next_table(el):
    sibling = el.find_next_sibling()
    while sibling:
        if sibling.name == "table":
            return sibling
        if sibling.name == "h2":
            return None
        sibling = sibling.find_next_sibling()
    return None


def looks_like_date(s):
    if not s:
        return False
    t = s.lower()
    return "multiple" in t or "on demand" in t or "date" in t or bool(re.search(r"\d", s))


def deduplicate_courses(courses):
    by_key = {}
    for c in courses:
        key = f"{c.get('title', '')}|{c.get('link', '')}"
        if key not in by_key:
            by_key[key] = dict(c)
        else:
            ex = by_key[key]
            for k in ["date", "audience", "format", "experience", "tags"]:
                if c.get(k) and (not ex.get(k) or (k == "date" and looks_like_date(c[k]) and not looks_like_date(ex[k]))):
                    ex[k] = c[k]
    return list(by_key.values())


def parse_listing(html):
    soup = BeautifulSoup(html, "html.parser")
    all_courses = []

    for h2 in soup.find_all("h2"):
        heading = get_text(h2)
        tbl = find_next_table(h2)
        if not tbl:
            continue

        if "Other training" in heading:
            all_courses.extend(parse_table_other_training(tbl, heading))
            continue

        if "Getting Started with AI at Oxford" in heading or "AI at Oxford" in heading:
            first_row = tbl.find("tr")
            ncols = len(first_row.find_all(["th", "td"])) if first_row else 0
            if ncols >= 3:
                all_courses.extend(parse_table_ai_at_oxford(tbl, heading))
            else:
                table2 = find_next_table(tbl)
                if table2:
                    r2 = table2.find("tr")
                    if r2 and len(r2.find_all(["th", "td"])) >= 3:
                        all_courses.extend(parse_table_ai_at_oxford(table2, heading))
                    else:
                        all_courses.extend(parse_table_two_cols(tbl, heading))
                else:
                    all_courses.extend(parse_table_two_cols(tbl, heading))
            continue

        all_courses.extend(parse_table_two_cols(tbl, heading))

    deduped = deduplicate_courses(all_courses)

    # Reclassify Mapping the Landscape coding courses
    for c in deduped:
        if c["theme"] == "General AI" and re.search(r"mapping the landscape", c["title"], re.IGNORECASE) and re.search(r"coding|code", c["title"], re.IGNORECASE):
            c["theme"] = "Coding & Development"

    return deduped


def parse_course_detail_page(html):
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text()
    detail = {}

    for h4 in soup.find_all("h4"):
        txt = get_text(h4)
        if re.match(r"^location$", txt, re.IGNORECASE):
            nxt = h4.find_next_sibling()
            if nxt:
                val = get_text(nxt)
                if val:
                    detail["format"] = val
        if re.match(r"^date\s*[&]\s*time$", txt, re.IGNORECASE):
            nxt = h4.find_next_sibling()
            if nxt:
                val = get_text(nxt)
                if val:
                    detail["date"] = val

    exp_match = re.search(r"Audience Exposure Level[:\s]*([^\n]+)", body_text, re.IGNORECASE)
    if exp_match:
        val = exp_match.group(1).strip()
        if val:
            detail["experience"] = val

    return detail


def fetch_course_details(courses):
    internal = [c for c in courses if c.get("link") and "oerc.ox.ac.uk" in c["link"]]
    if not internal:
        return
    log(f"Fetching detail pages for {len(internal)} internal courses...")

    def fetch_one(course):
        try:
            html = fetch_with_retry(course["link"], retries=2)
            if not html:
                return
            detail = parse_course_detail_page(html)
            if detail.get("format"):
                course["format"] = detail["format"]
            if detail.get("experience"):
                course["experience"] = detail["experience"]
            if detail.get("date") and not course.get("date"):
                course["date"] = detail["date"]
            log(f"Detail OK: {course['title']}")
        except Exception as e:
            log(f"Detail FAIL for: {course['title']} ({e})")

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as executor:
        futures = [executor.submit(fetch_one, c) for c in internal]
        for f in as_completed(futures):
            f.result()  # propagate exceptions if any

    log(f"Detail fetching complete ({len(internal)} courses)")


def normalize_experience(exp):
    if not exp:
        return ""
    e = re.sub(r"^[\s\-:]+", "", exp).strip().lower()
    if re.search(r"open to all", e, re.IGNORECASE):
        return "Open to all"
    if re.search(r"never\s*used|level\s*1", e, re.IGNORECASE):
        return "Level 1: Never used AI"
    if re.search(r"sometimes|level\s*2", e, re.IGNORECASE):
        return "Level 2: Sometimes use AI"
    if re.search(r"daily|level\s*3", e, re.IGNORECASE):
        return "Level 3: Use AI on a daily basis"
    return exp.strip()


def assign_experience(course):
    if course.get("experience"):
        return
    t = course.get("title", "").lower()
    if re.search(r"first steps|introductory|intro to|getting started", t, re.IGNORECASE):
        course["experience"] = "Level 1: Never used AI"
        return
    if re.search(r"10 truths|building a relationship", t, re.IGNORECASE):
        course["experience"] = "Open to all"
        return
    if re.search(r"next steps|advanced|building|from zero", t, re.IGNORECASE):
        course["experience"] = "Level 2: Sometimes use AI"
        return
    if re.search(r"open source|local ai models", t, re.IGNORECASE):
        course["experience"] = "Level 3: Use AI on a daily basis"
        return
    course["experience"] = "Level 2: Sometimes use AI"


def normalize_format(fmt):
    if not fmt:
        return ""
    f = fmt.lower().strip()
    has_online = "online" in f
    has_in_person = "in-person" in f or "in person" in f
    if has_online and has_in_person:
        return "Online and in-person"
    if has_in_person:
        return "In-person"
    if has_online:
        return "Online"
    return fmt.strip()


def normalize_audience(aud):
    if not aud:
        return ""
    canonical = {
        "all": "All",
        "staff": "Staff",
        "students": "Students",
        "staff and students": "Staff and students",
        "educators": "Educators",
        "researchers": "Researchers",
    }
    a = aud.lower().strip()
    if a in canonical:
        return canonical[a]
    return aud.strip()[:1].upper() + aud.strip()[1:] if aud.strip() else ""


def apply_defaults(course):
    course["audience"] = normalize_audience(course.get("audience", "")) or "All"
    course["format"] = normalize_format(course.get("format", ""))
    course["experience"] = normalize_experience(course.get("experience", ""))
    course["tags"] = course.get("tags", "")


def main():
    log(f"Fetching listing page: {SOURCE_URL}")
    html = fetch_with_retry(SOURCE_URL)
    if not html:
        log("ERROR: Could not fetch listing page after retries")
        sys.exit(1)

    log("Parsing listing page...")
    courses = parse_listing(html)
    log(f"Parsed {len(courses)} courses from listing")

    if not courses:
        log("ERROR: Zero courses parsed — aborting to preserve existing data")
        sys.exit(1)

    fetch_course_details(courses)

    for c in courses:
        assign_experience(c)
        apply_defaults(c)

    # Remove training_provider from output if empty
    for c in courses:
        if not c.get("training_provider"):
            c.pop("training_provider", None)

    output = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "source_url": SOURCE_URL,
        "courses": courses,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    log(f"Wrote {len(courses)} courses to {OUTPUT_FILE}")
    log("Done.")


if __name__ == "__main__":
    main()
