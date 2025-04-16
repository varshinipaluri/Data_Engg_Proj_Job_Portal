import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

# Constants
BASE_URL = "https://www.tnprivatejobs.tn.gov.in/Home/jobs/"
NUM_PAGES = 139  # Adjust as needed
OUTPUT_FILE = "tn_jobs_details.csv"
DELAY = 2  # seconds between requests
MAX_RETRIES = 3
NUM_THREADS = 10  # Number of threads to run concurrently

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def get_session():

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=MAX_RETRIES)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_job_links(page_url):
    session = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(page_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            job_links = []
            for a in soup.select('a[href*="/candidate/Home/ca_jobfair_single/"]'):
                full_url = urljoin(page_url, a['href'])
                if full_url not in job_links:  # Avoid duplicates
                    job_links.append(full_url)
            
            return job_links
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {page_url}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return []
            time.sleep(DELAY * (attempt + 1))

def scrape_job_details(job_url):

    session = get_session()
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(job_url, headers=HEADERS, timeout=30)
            response.raise_for_status()
            
            if "job-header" not in response.text:
                print(f"Job page structure not as expected for {job_url}")
                return None
                
            soup = BeautifulSoup(response.text, 'html.parser')
            
            def safe_extract(selector, attribute=None, process=None, default='N/A'):
                try:
                    element = soup.select_one(selector)
                    if not element:
                        return default
                    if attribute:
                        value = element.get(attribute, default)
                    else:
                        value = element.get_text(' ', strip=True)
                    if process and value != default:
                        return process(value)
                    return value
                except Exception:
                    return default

            def extract_after_icon(icon_class):
                try:
                    icon = soup.find('i', class_=icon_class)
                    if icon:
                        next_sibling = icon.next_sibling
                        while next_sibling and not str(next_sibling).strip():
                            next_sibling = next_sibling.next_sibling
                    return 'N/A'
                except:
                    return 'N/A'
            
            def extract_salary():
                try:
                    money_icon = soup.find('i', class_='fa-money')
                    if money_icon:
                        salary_text = ''
                        for sibling in money_icon.next_siblings:
                            if sibling.name == 'i' or str(sibling).strip() == '|':
                                break
                            if isinstance(sibling, str):
                                salary_text += sibling
                        return salary_text
                    return 'N/A'
                except:
                    return 'N/A'

            def extract_details_section():
                details = {}
                try:
                    details_div = None
                    for div in soup.find_all('div', class_='location'):
                        if 'Gender' in div.get_text():
                            details_div = div
                            break
                    
                    if details_div:
                        text = details_div.get_text('|', strip=True)
                        parts = [p.strip() for p in text.split('|') if p.strip()]
                        for part in parts:
                            if 'Gender' in part:
                                details['gender'] = part.split(':')[-1].strip()
                            elif 'Age Limit' in part:
                                details['age_limit'] = part.split('-', 1)[-1].strip()
                            elif 'Openings' in part:
                                details['openings'] = part.split('-')[-1].strip()
                            elif 'Experience' in part:
                                details['experience'] = part.split('-')[-1].strip()
                            elif 'Job Type' in part:
                                details['job_type'] = part.split('-')[-1].strip()
                except Exception as e:
                    print(f"Error extracting details section: {str(e)}")
                return details


            def extract_additional_skills():
                try:
                    skills_header = soup.find('h4', string='Additional Skills')
                    if skills_header:
                        next_span = skills_header.find_next('span')
                        if next_span:
                            return next_span.get_text(strip=True)
                        
                        next_node = skills_header.next_sibling
                        while next_node and (not str(next_node).strip() or next_node.name == 'br'):
                            next_node = next_node.next_sibling
                        if next_node and not next_node.name:
                            return next_node.strip()
                    return 'N/A'
                except:
                    return 'N/A'

            # Extract all job details
            details_section = extract_details_section()
            
            job_details = {
                'url': job_url,
                'title': safe_extract('div.jobinfo h2'),
                'company': safe_extract('div.companyinfo div.title a'),
                'sector': safe_extract('div.jobinfo div.location a'),
                'job_role': extract_after_icon('fa-black-tie'),
                'salary': extract_salary(),
                'qualification': extract_after_icon('fa-graduation-cap'),
                'specialization': safe_extract('div.jobinfo div.location', 
                                             process=lambda x: ' '.join(
                                                 [s.strip() for s in x.split('|')[-1].split('-')[1:] if s.strip()]
                                             ) if '|' in x else 'N/A'),
                'location': extract_after_icon('fa-map-marker'),
                'gender': details_section.get('gender', 'N/A'),
                'age_limit': details_section.get('age_limit', 'N/A'),
                'openings': details_section.get('openings', 'N/A'),
                'experience': details_section.get('experience', 'N/A'),
                'job_type': details_section.get('job_type', 'N/A'),
                'open_until': safe_extract('div.companyinfo div.ptext:contains("Open Until")', 
                                         process=lambda x: x.split(':')[-1].strip()),
                'description': safe_extract('div#ck-content'),
                'skills': ', '.join([li.get_text(strip=True) for li in 
                                    soup.select('h4:contains("Skills") + ul li')]) or 'N/A',
                'additional_skills': extract_additional_skills(),
                'company_jobs_count': safe_extract('div.companyinfo a:has(span:contains("Posted Jobs Count"))',
                                                 process=lambda x: x.split(':')[-1].strip()),
                'company_logo': safe_extract('div.companylogo img', 'src')
            }

            return job_details

        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {job_url}: {str(e)}")
            if attempt == MAX_RETRIES - 1:
                return None
            time.sleep(DELAY * (attempt + 1))

def save_to_csv(job_data, filename):

    if not job_data:
        print("No data to save")
        return False
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            fieldnames = job_data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(job_data)
        return True
    except Exception as e:
        print(f"Error saving to CSV: {str(e)}")
        return False

def main():
    all_job_links = []
    
    print("Starting job link collection...")
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(get_job_links, f"{BASE_URL}{page_num * 10}") for page_num in range(NUM_PAGES)]
        
        for future in as_completed(futures):
            job_links = future.result()
            if job_links:
                all_job_links.extend(job_links)
                print(f"Found {len(job_links)} jobs on a page")
            else:
                print(f"Failed to fetch jobs for a page")

    print(f"\nTotal {len(all_job_links)} job listings found")
    
    job_data = []
    print("\nStarting job details scraping...")
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(scrape_job_details, job_url) for job_url in all_job_links]
        
        for future in as_completed(futures):
            job_details = future.result()
            if job_details:
                job_data.append(job_details)
            else:
                print(f"Failed to scrape job details")

    if save_to_csv(job_data, OUTPUT_FILE):
        print(f"\nSuccessfully saved {len(job_data)} jobs to {OUTPUT_FILE}")
    else:
        print("\nFailed to save data to CSV")

if __name__ == "__main__":
    start_time = time.time()
    main()
    print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
