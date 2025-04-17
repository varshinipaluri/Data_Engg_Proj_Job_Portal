import asyncio
import os
import re
import pandas as pd
from urllib.parse import urljoin, urlparse, urldefrag
from playwright.async_api import async_playwright

BASE_URL = "https://www.tnprivatejobs.tn.gov.in/"
VISITED = set()
DATA = []
MAX_PAGES = 100  # Set your desired crawl limit

def normalize_url(url):
    url, _ = urldefrag(url)  # Remove URL fragments (#...)
    parsed = urlparse(url)
    return parsed.scheme + "://" + parsed.netloc + parsed.path

def is_valid_link(href):
    return href and not href.startswith(("mailto:", "tel:", "javascript:"))

async def get_internal_links(page, current_url):
    anchors = await page.query_selector_all("a")
    links = set()

    for anchor in anchors:
        href = await anchor.get_attribute("href")
        if is_valid_link(href):
            full_url = urljoin(current_url, href)
            if BASE_URL in full_url:
                links.add(normalize_url(full_url))

    return links

async def extract_text_from_page(page, url):
    print(f"Scraping: {url}")
    try:
        await page.goto(url, timeout=30000)
        await page.wait_for_timeout(2000)
        text = await page.evaluate("() => document.body.innerText")
        DATA.append({"URL": url, "Content": text.strip()})
    except Exception as e:
        print(f"❌ Failed to scrape {url}: {e}")

async def crawl_site():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Change to True to run silently
        page = await browser.new_page()
        await page.goto(BASE_URL)

        to_visit = set([normalize_url(BASE_URL)])

        while to_visit and len(VISITED) < MAX_PAGES:
            current_url = to_visit.pop()
            if current_url in VISITED:
                continue

            VISITED.add(current_url)
            await extract_text_from_page(page, current_url)

            new_links = await get_internal_links(page, current_url)
            to_visit.update(new_links - VISITED)

            print(f"Visited: {len(VISITED)} | To Visit: {len(to_visit)}")

        await browser.close()

    # Save data
    df = pd.DataFrame(DATA)
    df.to_csv("tn_private_jobs_full_website.csv", index=False)
    print("✅ Website content saved to tn_private_jobs_full_website.csv")

# Run the full crawl
asyncio.run(crawl_site())
