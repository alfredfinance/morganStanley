import time
import random
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException
from selenium import webdriver
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import datetime
import json
from selenium.webdriver.chrome.options import Options
from rapidfuzz import process, fuzz
from difflib import get_close_matches
from google.cloud import bigquery
from google.oauth2 import service_account
import os


# ─────────────────────────────────────────────
# SELENIUM OPTIONS
# ─────────────────────────────────────────────
options = Options()
# options.add_argument("--headless=new")
options.add_argument("--disable-gpu")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-blink-features=AutomationControlled")
options.add_argument("--remote-debugging-port=9222")
options.add_argument("--window-size=1920,1080")

# ─────────────────────────────────────────────
# PHASE 1 — COLLECT ALL JOB URLS
# ─────────────────────────────────────────────
driver = webdriver.Chrome(options=options)

base_url = "https://morganstanley.eightfold.ai/careers?domain=morganstanley.com&hl=en&start=0&pid=549796840606&sort_by=timestamp&filter_businessarea=wealth+management%2Csales+and+trading%2Clegal+and+compliance%2Cfinance%2Cinvestment+management%2Crisk+management%2Cinvestment+banking%2Cinstitutional+securities%2Cglobal+capital+markets%2Cwealth+management+%26+im+shared+services"
driver.get(base_url)
time.sleep(random.uniform(4, 7))

job_urls = []
seen = set()
MAX_PAGES = 100

for page_count in range(MAX_PAGES):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/careers/job/']"))
        )
    except TimeoutException:
        print("⚠️ Job cards did not load in time — stopping.")
        break

    # Scope to the TOP-LEVEL job card div
    cards = driver.find_elements(By.CSS_SELECTOR, "div.stack-module_gap-s__snYAO")

    for card in cards:
        # Get URL
        try:
            anchor = card.find_element(By.XPATH, ".//ancestor::a[contains(@href,'/careers/job/')]")
            href = anchor.get_attribute("href")
        except NoSuchElementException:
            try:
                anchor = card.find_element(By.XPATH, "./preceding::a[contains(@href,'/careers/job/')][1]")
                href = anchor.get_attribute("href")
            except NoSuchElementException:
                href = None

        # Get division — fieldsContainer with NO span (no icon) = business area
        try:
            division = card.find_element(
                By.XPATH,
                ".//div[contains(@class,'fieldsContainer-3Jtts') and not(.//span)]/div[contains(@class,'fieldValue-3kEar')]"
            ).text.strip()
        except NoSuchElementException:
            division = ""

        if href and href not in seen:
            seen.add(href)
            job_urls.append({"url": href, "division": division})

    print(f"Page {page_count + 1} — {len(job_urls)} URLs collected so far")

    try:
        next_btn = WebDriverWait(driver, 8).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "button.pagination-module_pagination-next__OHCf9"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", next_btn)
        time.sleep(0.5)
        driver.execute_script("arguments[0].click();", next_btn)
        print(f"✅ Clicked 'Next' — going to page {page_count + 2}")
        time.sleep(random.uniform(3, 5))
    except TimeoutException:
        print("ℹ️ No more 'Next' button — all pages visited.")
        break

driver.quit()
print(f"Collected {len(job_urls)} job URL+division pairs")
df_urls = pd.DataFrame(job_urls, columns=["url", "division"])


# ─────────────────────────────────────────────
# BIGQUERY — CHECK FOR EXISTING URLs (dedup)
# ─────────────────────────────────────────────

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

query = """
    SELECT url
    FROM `databasealfred.alfredFinance.morganStanley`
    WHERE url IS NOT NULL
"""
query_job = client.query(query)
existing_urls = {row.url for row in query_job}
print(f"Loaded {len(existing_urls)} URLs from BigQuery")

df_urls = df_urls[~df_urls["url"].isin(existing_urls)].reset_index(drop=True)
print(f"✅ Remaining job URLs to scrape: {len(df_urls)}")

# ─────────────────────────────────────────────
# PHASE 2 — SCRAPE EACH JOB PAGE
# ─────────────────────────────────────────────
options2 = Options()
# options2.add_argument("--headless=new")
options2.add_argument("--disable-gpu")
options2.add_argument("--no-sandbox")
options2.add_argument("--disable-dev-shm-usage")
options2.add_argument("--window-size=1920,1080")

driver = webdriver.Chrome(options=options2)

job_data = []

for _, row in df_urls.iterrows():
    job_url = row["url"]
    division = row["division"]

    try:
        driver.get(job_url)
        time.sleep(random.uniform(3, 6))

        # ── Title ──────────────────────────────────────────────────────────
        try:
            title = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "h2"))
            ).text.strip()
        except TimeoutException:
            title = ""

        # ── Location ───────────────────────────────────────────────────────
        location = ""
        try:
            location = driver.find_element(By.CSS_SELECTOR, "div.position-location-12ZUO").text.strip().split(",")[0].strip()
            
        except NoSuchElementException:
            location = ""

        # ── Experience Level ───────────────────────────────────────────────
        experienceLevel = ""

        # ── Description ────────────────────────────────────────────────────
        description = ""
        try:
            container = WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.container-3Gm1a"))
            )
            html = container.get_attribute("innerHTML")
            soup = BeautifulSoup(html, "html.parser")
            lines = []
            for element in soup.find_all(["p", "li"]):
                text = element.get_text(" ", strip=True)
                if text:
                    if element.name == "li":
                        lines.append(f"- {text}")
                    else:
                        lines.append(text)
            description = "\n".join(lines)
        except TimeoutException:
            description = ""

        # ── Timestamps ─────────────────────────────────────────────────────
        scrappedDateTime    = datetime.datetime.now().isoformat()
        scrappedDate        = datetime.datetime.now().strftime("%Y-%m-%d")
        scrappedHour        = datetime.datetime.now().strftime("%H")
        scrappedMinutes     = datetime.datetime.now().strftime("%M")

        print(f"  → {title} | {location} | {division}")

        job_data.append({
            "title":                title,
            "location":             location,
            "scrappedDateTime":     scrappedDateTime,
            "description":          description,
            "division":             division,
            "experienceLevel":      experienceLevel,
            "url":                  job_url,
            "source":               "Morgan Stanley",
            "scrappedDate":         scrappedDate,
            "scrappedHour":         scrappedHour,
            "scrappedMinutes":      scrappedMinutes,
            "scrappedDateTimeText": scrappedDateTime,
        })

    except Exception as e:
        print(f"⚠️ Error scraping {job_url}: {e}")
        continue

driver.quit()

df_jobs = pd.DataFrame(job_data)
new_data = df_jobs
print(f"\n📦 Scraped {len(new_data)} jobs")

import re
import numpy as np

def extract_experience_level(title):
    if pd.isna(title):
        return ""
    
    title = title.lower()

    patterns = [
        (r'\bsummer\s+analyst\b|\bsummer\s+analyste\b', "Summer Analyst"),
        (r'\bsummer\s+associate\b|\bsummer\s+associé\b', "Summer Associate"),
        (r'\bvice\s+president\b|\bsvp\b|\bvp\b|\bprincipal\b', "Vice President"),
        (r'\bassistant\s+vice\s+president\b|\bsavp\b|\bavp\b', "Assistant Vice President"),
        (r'\bsenior\s+manager\b', "Senior Manager"),
        (r'\bproduct\s+manager\b|\bpm\b|\bmanager\b', "Manager"),
        (r'\bmanager\b', "Manager"),
        (r'\bengineer\b|\bengineering\b', "Engineer"),
        (r'\badministrative\s+assistant\b|\bexecutive\s+assistant\b|\badmin\b', "Assistant"),
        (r'\bassociate\b|\bassocié\b', "Associate"),
        (r'\banalyst\b|\banalyste\b|\banalist\b', "Analyst"),
        (r'\bchief\b|\bhead\b', "C-Level"),
        (r'\bV.I.E\b|\bVIE\b|\bvolontariat international\b|\bV I E\b|', "VIE"),
    ]

    for pattern, label in patterns:
        if re.search(pattern, title):
            return label

    return "" 

# Apply to dataframe
new_data["experienceLevel"] = new_data["title"].apply(extract_experience_level)


# ─────────────────────────────────────────────
# DIVISION MAPPING (fuzzy)
# ─────────────────────────────────────────────
BASE_MAPPING = {
    "investment banking": "Investment Banking (M&A / Advisory)",
    "m&a": "Investment Banking (M&A / Advisory)",
    "mergers and acquisitions": "Investment Banking (M&A / Advisory)",
    "corporate finance": "Investment Banking (M&A / Advisory)",
    "ecm": "Investment Banking (M&A / Advisory)",
    "dcm": "Investment Banking (M&A / Advisory)",
    "capital markets origination": "Investment Banking (M&A / Advisory)",
    "corporate & investment banking": "Investment Banking (M&A / Advisory)",
    "markets": "Markets (Sales & Trading)",
    "sales and trading": "Markets (Sales & Trading)",
    "trading": "Markets (Sales & Trading)",
    "sales": "Markets (Sales & Trading)",
    "structuring": "Markets (Sales & Trading)",
    "derivatives": "Markets (Sales & Trading)",
    "fixed income": "Markets (Sales & Trading)",
    "equities": "Markets (Sales & Trading)",
    "fx": "Markets (Sales & Trading)",
    "global markets": "Markets (Sales & Trading)",
    "relationship management": "Markets (Sales & Trading)",       # Barclays
    "asset management": "Asset & Wealth Management",
    "wealth management": "Asset & Wealth Management",
    "private banking": "Asset & Wealth Management",
    "portfolio management": "Asset & Wealth Management",
    "private equity": "Private Equity & Alternatives",
    "alternatives": "Private Equity & Alternatives",
    "credit": "Credit & Lending",
    "lending": "Credit & Lending",
    "leveraged finance": "Credit & Lending",
    "structured finance": "Credit & Lending",
    "banking operations": "Operations (Back/Middle Office)",       # Barclays
    "research": "Research & Strategy",
    "equity research": "Research & Strategy",
    "risk": "Risk Management",
    "risk management": "Risk Management",
    "market risk": "Risk Management",
    "credit risk": "Risk Management",
    "operational risk": "Risk Management",
    "risk and quantitative analytics": "Risk Management",         # Barclays
    "controls": "Audit & Internal Control",                       # Barclays
    "compliance": "Compliance & Financial Crime",
    "financial crime": "Compliance & Financial Crime",
    "aml": "Compliance & Financial Crime",
    "kyc": "Compliance & Financial Crime",
    "finance": "Finance (Accounting / Controlling / Tax)",
    "accounting": "Finance (Accounting / Controlling / Tax)",
    "controlling": "Finance (Accounting / Controlling / Tax)",
    "tax": "Finance (Accounting / Controlling / Tax)",
    "operations": "Operations (Back/Middle Office)",
    "middle office": "Operations (Back/Middle Office)",
    "back office": "Operations (Back/Middle Office)",
    "trade support": "Operations (Back/Middle Office)",
    "settlement": "Operations (Back/Middle Office)",
    "audit": "Audit & Internal Control",
    "internal audit": "Audit & Internal Control",
    "internal control": "Audit & Internal Control",
    "technology": "Technology (IT / Engineering)",
    "it": "Technology (IT / Engineering)",
    "data": "Technology (IT / Engineering)",
    "engineering": "Technology (IT / Engineering)",
    "software": "Technology (IT / Engineering)",
    "development and engineering": "Technology (IT / Engineering)", # Barclays
    "data & analytics": "Technology (IT / Engineering)",            # Barclays
    "design": "Technology (IT / Engineering)",                      # Barclays
    "human resources": "Corporate Functions",
    "hr": "Corporate Functions",
    "communications": "Corporate Functions",
    "marketing": "Corporate Functions",
    "procurement": "Corporate Functions",                           # Barclays
    "corporate affairs": "Corporate Functions",                     # Barclays
    "business support & administration": "Corporate Functions",     # Barclays
    "customer service": "Corporate Functions",                      # Barclays
    "legal": "Compliance & Financial Crime",                        # Barclays
    "strategy": "Executive / Strategy / Management",
    "management": "Executive / Strategy / Management",
    "business management": "Executive / Strategy / Management",     # Barclays
    "change": "Executive / Strategy / Management",                  # Barclays
    "product development & management": "Executive / Strategy / Management", # Barclays
    "real estate": "Real Estate",
    "real estate & physical security": "Real Estate",               # Barclays
    "other": "Other / Temporary",
    "miscellaneous": "Other / Temporary",
    "early careers": "Other / Temporary",                           # Barclays
    "internships": "Other / Temporary",                             # Barclays
    "third party colleagues": "Other / Temporary",                  # Barclays
    # Morgan Stanley specific divisions
    "sales support": "Markets (Sales & Trading)",
    "sales support staff": "Markets (Sales & Trading)",
    "sales support - optin": "Markets (Sales & Trading)",
    "sales support - liquidity": "Markets (Sales & Trading)",
    "sales support - intermediary": "Markets (Sales & Trading)",
    "sales - international": "Markets (Sales & Trading)",
    "sales - wsg (wireind)_internal": "Markets (Sales & Trading)",
    "sales/relationship mgmt": "Markets (Sales & Trading)",
    "sales trading": "Markets (Sales & Trading)",
    "s&t desk support": "Markets (Sales & Trading)",
    "coverage advisory": "Markets (Sales & Trading)",
    "investor coverage": "Markets (Sales & Trading)",
    "distribution management": "Markets (Sales & Trading)",
    "product sales": "Markets (Sales & Trading)",
    "trading- im": "Markets (Sales & Trading)",
    "strats": "Markets (Sales & Trading)",
    "structuring/origination": "Markets (Sales & Trading)",
    "capital markets": "Investment Banking (M&A / Advisory)",
    "banking": "Investment Banking (M&A / Advisory)",
    "enterprise advisory": "Investment Banking (M&A / Advisory)",
    "acquisition": "Investment Banking (M&A / Advisory)",
    "risk / policy mgmt": "Risk Management",
    "risk / policy mgmt- im": "Risk Management",
    "complex risk officer": "Risk Management",
    "risk, assurance, governance & control": "Risk Management",
    "model development": "Risk Management",
    "quant strategist": "Risk Management",
    "quantitative strategists": "Risk Management",
    "valuation control": "Risk Management",
    "corporate treasury & capital planning": "Risk Management",
    "corporate treasury – market-facing/managing risk": "Risk Management",
    "portfolio analytics": "Risk Management",
    "compliance": "Compliance & Financial Crime",
    "legal": "Compliance & Financial Crime",
    "legal & compliance": "Compliance & Financial Crime",
    "lcd data and analytics": "Compliance & Financial Crime",
    "attorney - litigation": "Compliance & Financial Crime",
    "attorney - product/business advisory": "Compliance & Financial Crime",
    "oversight, monitoring and testing": "Audit & Internal Control",
    "framework / core functions": "Audit & Internal Control",
    "corporate controllers, reporting & accounting": "Finance (Accounting / Controlling / Tax)",
    "product controllers": "Finance (Accounting / Controlling / Tax)",
    "financial planning & analysis": "Finance (Accounting / Controlling / Tax)",
    "tax & advisory": "Finance (Accounting / Controlling / Tax)",
    "accounting & regulatory policy": "Finance (Accounting / Controlling / Tax)",
    "financial reporting": "Finance (Accounting / Controlling / Tax)",
    "fund svcs-accounting": "Finance (Accounting / Controlling / Tax)",
    "finance- im": "Finance (Accounting / Controlling / Tax)",
    "finance business management": "Finance (Accounting / Controlling / Tax)",
    "operations & admin": "Operations (Back/Middle Office)",
    "branch operations": "Operations (Back/Middle Office)",
    "complex business service": "Operations (Back/Middle Office)",
    "fund services": "Operations (Back/Middle Office)",
    "fund svcs-client svcs": "Operations (Back/Middle Office)",
    "investor services": "Operations (Back/Middle Office)",
    "client services": "Operations (Back/Middle Office)",
    "client services- im": "Operations (Back/Middle Office)",
    "documentation": "Operations (Back/Middle Office)",
    "asset management": "Asset & Wealth Management",
    "portfolio management- im": "Asset & Wealth Management",
    "portfolio specialist": "Asset & Wealth Management",
    "private banking": "Asset & Wealth Management",
    "financial advisor": "Asset & Wealth Management",
    "portfolio & change management": "Executive / Strategy / Management",
    "business analysis": "Executive / Strategy / Management",
    "business management": "Executive / Strategy / Management",
    "business service management": "Executive / Strategy / Management",
    "lcd business management": "Executive / Strategy / Management",
    "complex business devel manager": "Executive / Strategy / Management",
    "branch manager": "Executive / Strategy / Management",
    "resident manager": "Executive / Strategy / Management",
    "project management": "Executive / Strategy / Management",
    "business strategy": "Executive / Strategy / Management",
    "business admin": "Executive / Strategy / Management",
    "business mgmt": "Executive / Strategy / Management",
    "data & technology": "Technology (IT / Engineering)",
    "data management": "Technology (IT / Engineering)",
    "ui / ux dev": "Technology (IT / Engineering)",
    "test tooling dev": "Technology (IT / Engineering)",
    "global intelligence": "Technology (IT / Engineering)",
    "product management": "Executive / Strategy / Management",
    "product development": "Executive / Strategy / Management",
    "product specialists": "Executive / Strategy / Management",
    "human resources": "Corporate Functions",
    "admin/support": "Corporate Functions",
    "administrative": "Corporate Functions",
    "administrative support": "Corporate Functions",
    "administration": "Corporate Functions",
    "marketing": "Corporate Functions",
    "specialist": "Corporate Functions",
    "other staff": "Other / Temporary",
    "other advisory and firm processes": "Other / Temporary",
    "mssb other branch": "Other / Temporary",
    "intern - non-program": "Other / Temporary",
    "pre-mba 1": "Other / Temporary",
    "e*trade": "Other / Temporary",
    "analysis": "Research & Strategy",
    "research": "Research & Strategy",
}

KNOWN_DIVISIONS = list(BASE_MAPPING.keys())

def map_division_fuzzy(value: str, threshold: int = 85) -> str:
    if not value:
        return "Other / Temporary"
    v = str(value).strip().lower()
    if v in BASE_MAPPING:
        return BASE_MAPPING[v]
    result = process.extractOne(v, KNOWN_DIVISIONS, scorer=fuzz.token_sort_ratio)
    if result and result[1] >= threshold:
        return BASE_MAPPING[result[0]]
    return "Other / Temporary"

new_data["division"] = new_data["division"].apply(map_division_fuzzy)


# ─────────────────────────────────────────────
# LOCATION MAPPING (fuzzy)
# ─────────────────────────────────────────────
BASE_CITY_MAPPING = {
    "new york": "New York", "new york city": "New York", "jersey city": "Jersey City",
    "london": "London", "glasgow": "Glasgow", "birmingham": "Birmingham",
    "paris": "Paris", "frankfurt": "Frankfurt", "frankfurt am main": "Frankfurt",
    "madrid": "Madrid", "milan": "Milan", "milano": "Milan",
    "zurich": "Zurich", "zürich": "Zurich", "geneva": "Geneva",
    "amsterdam": "Amsterdam", "brussels": "Brussels", "stockholm": "Stockholm",
    "warsaw": "Warsaw", "krakow": "Krakow",
    "dubai": "Dubai", "riyadh": "Riyadh", "doha": "Doha",
    "hong kong": "Hong Kong", "singapore": "Singapore", "tokyo": "Tokyo",
    "sydney": "Sydney", "mumbai": "Mumbai", "bangalore": "Bangalore",
    "chennai": "Chennai", "delhi": "Delhi", "pune": "Pune",
    "new delhi": "Delhi",
}
CITY_CATEGORIES = set(BASE_CITY_MAPPING.values())
BASE_CITY_MAPPING.update({city.lower(): city for city in CITY_CATEGORIES})
KNOWN_LOCATIONS = list(BASE_CITY_MAPPING.keys())

def map_location(value: str, cutoff: float = 0.8) -> str:
    if not value:
        return "Other / Unknown"
    v = str(value).strip().lower()
    if v in BASE_CITY_MAPPING:
        return BASE_CITY_MAPPING[v]
    matches = get_close_matches(v, KNOWN_LOCATIONS, n=1, cutoff=cutoff)
    if matches:
        return BASE_CITY_MAPPING[matches[0]]
    return value

new_data["location"] = new_data["location"].apply(map_location)

#---------UPLOAD TO BIGQUERY-------------------------------------------------------------------------------------------------------------

# Load JSON from GitHub secret
key_json = json.loads(os.environ["BIGQUERY"])

# Create credentials from dict
credentials = service_account.Credentials.from_service_account_info(key_json)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=key_json["project_id"]
)

table_id = "databasealfred.alfredFinance.morganStanley"

# CONFIG WITHOUT PYARROW
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
)

# Convert DataFrame → list of dict rows (JSON compatible)
rows = new_data.to_dict(orient="records")

# Upload
job = client.load_table_from_json(
    rows,
    table_id,
    job_config=job_config
)

job.result()
