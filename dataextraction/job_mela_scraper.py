import asyncio
from playwright.async_api import async_playwright
import csv
from urllib.parse import urljoin
import re

# Constants
BASE_URL = "https://www.tnprivatejobs.tn.gov.in/Home/job_mela"
OUTPUT_FILE = "tn_job_fairs_playwright.csv"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def clean_text(text):
    """Clean and normalize text"""
    if not text:
        return ''
    return ' '.join(str(text).strip().split())

async def get_job_fair_links(page):
    """Get all job fair links from the main page"""
    print("Navigating to job mela page...")
    await page.goto(BASE_URL, timeout=60000)
    await page.wait_for_selector('a[href*="/ca_jobfairlist_single/"]', timeout=10000)
    
    links = []
    elements = await page.query_selector_all('a[href*="/ca_jobfairlist_single/"]')
    for element in elements:
        href = await element.get_attribute('href')
        full_url = urljoin(BASE_URL, href)
        links.append(full_url)
    
    print(f"Found {len(links)} job fair links")
    return links

async def scrape_job_fair(page, url):
    """Scrape all details from a single job fair page"""
    print(f"Processing: {url}")
    await page.goto(url, timeout=60000)
    
    try:
        # Wait for key elements to load
        await page.wait_for_selector('div.job-header', timeout=10000)
        
        # Extract basic job fair info
        job_fair_id = url.split('/')[-1]
        data = {
            'job_fair_url': url,
            'job_fair_id': job_fair_id,
            'title': clean_text(await page.text_content('div.job-header h2')),
            'organization': clean_text(await page.text_content('div.job-header div.ptext:has(i.fa-building)')),
            'location': clean_text(await page.text_content('div.job-header div.ptext:has(i.fa-map-marker)')),
            'date_time': clean_text(await page.text_content('div.job-header div.ptext:has(i.fa-calendar)')),
            'event_url': await page.get_attribute('div.job-header div.ptext:has(i.fa-share-alt) a', 'href'),
            'description': clean_text(await page.text_content('div.contentbox p')),
            'employer_jobs': []
        }

        # Extract contact details
        contact_section = await page.query_selector('h3:has-text("Contact Details")')
        if contact_section:
            contact_text = clean_text(await contact_section.inner_text())
            data.update({
                'contact_person': re.search(r'Contact Person Name\s*(.+)', contact_text).group(1) if re.search(r'Contact Person Name\s*(.+)', contact_text) else 'N/A',
                'mobile': re.search(r'Mobile No\s*(.+)', contact_text).group(1) if re.search(r'Mobile No\s*(.+)', contact_text) else 'N/A',
                'email': re.search(r'Email Id\s*(.+)', contact_text).group(1) if re.search(r'Email Id\s*(.+)', contact_text) else 'N/A',
                'contact_role': re.search(r'Contact Person Role\s*(.+)', contact_text).group(1) if re.search(r'Contact Person Role\s*(.+)', contact_text) else 'N/A'
            })

        # Expand the employer jobs section if collapsed
        try:
            collapse_button = await page.query_selector('a[data-toggle="collapse"][href="#basiccollapse"]')
            if collapse_button and 'collapsed' in await collapse_button.get_attribute('class'):
                await collapse_button.click()
                await page.wait_for_selector('#basiccollapse.show', timeout=5000)
        except:
            pass

        # Extract employer jobs
        await page.wait_for_selector('table#example', timeout=10000)
        rows = await page.query_selector_all('table#example tbody tr')
        
        for row in rows:
            cols = await row.query_selector_all('td')
            if len(cols) >= 6:
                job_data = {
                    's_no': clean_text(await cols[0].inner_text()),
                    'employer_name': clean_text(await cols[1].inner_text()),
                    'job_type': clean_text(await cols[2].inner_text()),
                    'job_location': clean_text(await cols[3].inner_text()),
                    'vacancies': clean_text(await cols[4].inner_text()),
                    'salary': clean_text(await cols[5].inner_text())
                }
                
                # Extract additional data from onclick attribute if available
                job_link = await cols[2].query_selector('a')
                if job_link:
                    onclick = await job_link.get_attribute('onclick')
                    if onclick:
                        params = re.findall(r"'(.*?)'", onclick)
                        if len(params) >= 21:
                            job_data.update({
                                'job_id': params[1],
                                'job_title': params[4],
                                'min_age': params[12],
                                'max_age': params[13],
                                'education': params[14],
                                'specialization': params[15],
                                'experience': params[17]
                            })
                
                data['employer_jobs'].append(job_data)
        
        return data
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return None

async def save_to_csv(data, filename):
    """Save scraped data to CSV file"""
    if not data:
        print("No data to save")
        return
    
    fieldnames = [
        'job_fair_id', 'job_fair_url', 'title', 'organization', 'location',
        'date_time', 'event_url', 'description', 'contact_person', 'mobile',
        'email', 'contact_role', 's_no', 'employer_name', 'job_type',
        'job_location', 'vacancies', 'salary', 'job_id', 'job_title',
        'min_age', 'max_age', 'education', 'specialization', 'experience'
    ]
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for job_fair in data:
            if job_fair and job_fair.get('employer_jobs'):
                for job in job_fair['employer_jobs']:
                    row = {
                        **{k: v for k, v in job_fair.items() if k != 'employer_jobs'},
                        **job
                    }
                    writer.writerow(row)

async def main():
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch(headless=False)  # Set headless=True for production
        context = await browser.new_context()
        page = await context.new_page()
        
        # Get all job fair links
        job_fair_links = list(set(await get_job_fair_links(page)))  # remove duplicates
        print(f"Found {len(job_fair_links)} unique job fair links")

        
        # Scrape each job fair
        job_fair_data = []
        for link in job_fair_links:
            data = await scrape_job_fair(page, link)
            if data:
                job_fair_data.append(data)
            await asyncio.sleep(2)  # Be polite with delays
        
        # Save results
        if job_fair_data:
            await save_to_csv(job_fair_data, OUTPUT_FILE)
            print(f"Successfully saved data to {OUTPUT_FILE}")
        else:
            print("No data was scraped")
        
        # Close browser
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())