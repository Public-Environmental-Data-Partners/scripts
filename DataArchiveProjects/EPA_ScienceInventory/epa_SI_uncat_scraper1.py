"""
EPA Science Inventory Complete Database Scraper (All Types Except Journals)
Scrapes all document types in configurable chunks and extracts download URLs

Requirements:
pip install requests beautifulsoup4 pandas
"""

import requests
from bs4 import BeautifulSoup
import time
import csv
from urllib.parse import urljoin, parse_qs, urlparse
from datetime import datetime
import logging
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('epa_scraper_all_types.log'),
        logging.StreamHandler()
    ]
)

class EPAAllTypesScraper:
    def __init__(self, start_page=1, end_page=10):
        self.base_url = "https://cfpub.epa.gov/si/"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Research Bot (respectful scraping for academic purposes)',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        })
        self.delay = 2  # seconds between requests
        self.records = []
        self.start_page = start_page
        self.end_page = end_page
        
        # Document types to scrape (everything except JOURNAL)
        self.document_types = [
            'ASSESSMENT DOCUMENT',
            'BOOK',
            'BOOK CHAPTER',
            'COMMUNICATION PRODUCT',
            'CRITERIA DOCUMENT',
            'DATA/SOFTWARE',
            'EPA PUBLISHED PROCEEDINGS',
            'ETV DOCUMENT',
            'EXTRAMURAL DOCUMENT',
            'IRIS ASSESSMENT',
            'NEWSLETTER',
            'NEWSLETTER ARTICLE',
            'NON-EPA PUBLISHED PROCEEDINGS',
            'PAPER IN EPA PROCEEDINGS',
            'PAPER IN NON-EPA PROCEEDINGS',
            'PRESENTATION',
            'PUBLISHED REPORT',
            'RISK ASSESSMENT GUIDELINES',
            'SCIENCE ACTIVITY',
            'SITE DOCUMENT',
            'SUMMARY',
            'UNCATEGORIZED'  # For records without a type
        ]
        
        # File extensions that indicate downloadable files
        self.download_extensions = [
            '.pdf', '.doc', '.docx', '.xls', '.xlsx',
            '.ppt', '.pptx', '.zip', '.tar', '.gz', '.csv', '.txt'
        ]
        
    def respectful_get(self, url):
        """Make a GET request with respectful delay"""
        time.sleep(self.delay)
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching {url}: {e}")
            return None
    
    def is_download_url(self, url):
        """Check if URL is a direct download link (not a webpage)"""
        url_lower = url.lower()
        
        # EPA's file download system
        if 'si_public_file_download.cfm' in url_lower:
            return True
        
        # Check for file extensions
        for ext in self.download_extensions:
            if url_lower.endswith(ext):
                return True
        
        # Check for file extensions with query parameters
        parsed = urlparse(url_lower)
        if any(ext in parsed.path for ext in self.download_extensions):
            return True
        
        return False
    
    def extract_record_data(self, record_url):
        """Extract download URLs, title, and document type from a record page"""
        logging.info(f"Scraping: {record_url}")
        response = self.respectful_get(record_url)
        
        if not response:
            return [], '', 'UNKNOWN'
        
        soup = BeautifulSoup(response.text, 'html.parser')
        download_urls = []
        title = ''
        doc_type = 'UNCATEGORIZED'
        
        # Extract title from <title> tag
        title_tag = soup.find('title')
        if title_tag:
            title = title_tag.get_text(strip=True)
            # Remove " | US EPA" suffix if present
            title = title.replace(' | US EPA', '').strip()
            logging.info(f"  Title: {title}")
        
        # Extract document type from URL parameters
        parsed = urlparse(record_url)
        params = parse_qs(parsed.query)
        if 'timstype' in params:
            doc_type = params['timstype'][0].upper()
            logging.info(f"  Type: {doc_type}")
        
        # Find all links on the page
        for link in soup.find_all('a', href=True):
            href = link['href']
            full_url = urljoin(self.base_url, href)
            
            # Only keep URLs that are actual downloads
            if self.is_download_url(full_url):
                if full_url not in download_urls:
                    download_urls.append(full_url)
                    logging.info(f"  ✓ Found download: {full_url}")
        
        return download_urls, title, doc_type
    
    def search_all_types(self, start_index=0):
        """Search for all document types except Journal"""
        search_url = f"{self.base_url}si_public_search_results.cfm"
        
        # Parameters for ALL TYPES - no timstype parameter means all types
        # We'll filter out journals during the scraping phase
        params = {
            'simplesearch': '0',
            'showcriteria': '2',
            'sortby': 'pubDate',
            'searchAll': '',
            'datebeginpublishedpresented': '',
            'startIndex': start_index,
            'displayIt': 'Yes'
        }
        
        logging.info(f"Searching all document types starting at index {start_index}...")
        
        try:
            response = self.session.get(search_url, params=params, timeout=30)
            response.raise_for_status()
            time.sleep(self.delay)
            return response
        except Exception as e:
            logging.error(f"Error in search: {e}")
            return None
    
    def parse_search_results(self, html):
        """Parse search results page to extract record links"""
        soup = BeautifulSoup(html, 'html.parser')
        record_links = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'si_public_record_report.cfm' in href:
                full_url = urljoin(self.base_url, href)
                if full_url not in record_links:
                    record_links.append(full_url)
        
        return record_links
    
    def scrape_page_range(self):
        """Scrape records within the specified page range"""
        logging.info("=" * 70)
        logging.info(f"EPA Science Inventory Scraper (ALL TYPES EXCEPT JOURNALS) - Pages {self.start_page} to {self.end_page}")
        logging.info("=" * 70)
        
        results_per_page = 25
        start_index = (self.start_page - 1) * results_per_page
        
        journals_skipped = 0
        
        for page in range(self.start_page, self.end_page + 1):
            logging.info(f"\n{'='*70}")
            logging.info(f"PAGE {page} (startIndex={start_index})")
            logging.info(f"{'='*70}")
            
            # Get search results page
            response = self.search_all_types(start_index)
            
            if not response:
                logging.warning(f"Failed to get search page {page}")
                start_index += results_per_page
                continue
            
            # Parse the search results to get record links
            record_links = self.parse_search_results(response.text)
            
            if not record_links:
                logging.info(f"No records found on page {page} - may have reached end of results")
                break
            
            logging.info(f"Found {len(record_links)} records on page {page}")
            
            # Scrape each record
            for i, record_url in enumerate(record_links, 1):
                logging.info(f"\n--- Record {i}/{len(record_links)} on page {page} ---")
                
                # Extract download URLs, title, and document type
                download_urls, title, doc_type = self.extract_record_data(record_url)
                
                # Skip if it's a JOURNAL
                if doc_type.upper() == 'JOURNAL':
                    logging.info(f"  ⊗ Skipping JOURNAL record")
                    journals_skipped += 1
                    continue
                
                # Create record with original URL and download URLs
                record = {
                    'original_url': record_url,
                    'title': title,
                    'document_type': doc_type,
                    'download_count': len(download_urls)
                }
                
                # Add each download URL in a separate column
                for idx, url in enumerate(download_urls, 1):
                    record[f'download_url_{idx}'] = url
                
                self.records.append(record)
                
                logging.info(f"  Total downloads found: {len(download_urls)}")
            
            # Progress summary for this page
            total_so_far = len(self.records)
            logging.info(f"\n>>> Completed page {page}: {total_so_far} records scraped, {journals_skipped} journals skipped <<<\n")
            
            # Move to next page
            start_index += results_per_page
        
        logging.info("=" * 70)
        logging.info(f"COMPLETED: Scraped {len(self.records)} total records from pages {self.start_page}-{self.end_page}")
        logging.info(f"Skipped {journals_skipped} journal records")
        logging.info("=" * 70)
    
    def save_to_csv(self, filename=None):
        """Save results to CSV with dynamic columns for download URLs"""
        if not self.records:
            logging.warning("No records to save!")
            return
        
        # Auto-generate filename if not provided
        if filename is None:
            filename = f'epa_all_types_pages_{self.start_page}_to_{self.end_page}.csv'
        
        # Find the maximum number of download URLs any record has
        max_downloads = max((r['download_count'] for r in self.records), default=0)
        
        # Create column names
        columns = ['original_url', 'title', 'document_type', 'download_count']
        for i in range(1, max_downloads + 1):
            columns.append(f'download_url_{i}')
        
        # Fill in missing columns with empty strings
        for record in self.records:
            for col in columns:
                if col not in record:
                    record[col] = ''
        
        # Write to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=columns)
            writer.writeheader()
            writer.writerows(self.records)
        
        logging.info(f"Saved {len(self.records)} records to {filename}")
        
        # Print summary statistics
        total_downloads = sum(r['download_count'] for r in self.records)
        records_with_downloads = sum(1 for r in self.records if r['download_count'] > 0)
        records_without_downloads = len(self.records) - records_with_downloads
        avg_downloads = total_downloads / len(self.records) if self.records else 0
        
        # Count by document type
        type_counts = {}
        for record in self.records:
            doc_type = record.get('document_type', 'UNKNOWN')
            type_counts[doc_type] = type_counts.get(doc_type, 0) + 1
        
        print(f"\n{'='*70}")
        print(f"✓ SCRAPING COMPLETE!")
        print(f"{'='*70}")
        print(f"\nResults saved to: {filename}")
        print(f"\nSummary Statistics:")
        print(f"  Pages scraped: {self.start_page} to {self.end_page}")
        print(f"  Total records scraped: {len(self.records):,}")
        print(f"  Records with downloads: {records_with_downloads:,} ({100*records_with_downloads/len(self.records):.1f}%)")
        print(f"  Records without downloads: {records_without_downloads:,} ({100*records_without_downloads/len(self.records):.1f}%)")
        print(f"  Total download URLs found: {total_downloads:,}")
        print(f"  Average downloads per record: {avg_downloads:.2f}")
        print(f"  Maximum downloads in a single record: {max_downloads}")
        
        print(f"\nDocument Types Found:")
        for doc_type in sorted(type_counts.keys()):
            print(f"  {doc_type}: {type_counts[doc_type]:,}")
        
        print(f"\nCSV Format:")
        print(f"  - Column 1: original_url")
        print(f"  - Column 2: title")
        print(f"  - Column 3: document_type")
        print(f"  - Column 4: download_count")
        print(f"  - Columns 5+: download_url_1, download_url_2, ... download_url_{max_downloads}")


def main():
    """Main execution function with interactive mode"""
    print("=" * 70)
    print("  EPA Science Inventory Scraper - ALL TYPES (Except Journals)")
    print("=" * 70)
    
    # Get page range from user
    print("\nEnter the page range you want to scrape:")
    print("(Each page contains ~25 records)")
    print("\nNOTE: This scrapes ALL document types EXCEPT journals")
    print("Estimated total: ~45,000 records (excluding ~23,000 journals)")
    print("\nExamples:")
    print("  - Test run: pages 1-5 (~125 records)")
    print("  - Small chunk: pages 1-100 (~2,500 records)")
    print("  - Medium chunk: pages 1-500 (~12,500 records)")
    print("  - Large chunk: pages 1-1000 (~25,000 records)")
    
    try:
        start_page = int(input("\nStart page (default 1): ").strip() or "1")
        end_page = int(input("End page (default 10): ").strip() or "10")
        
        if start_page < 1 or end_page < start_page:
            print("Invalid page range!")
            return
            
    except ValueError:
        print("Invalid input! Please enter numbers only.")
        return
    
    # Calculate estimates
    estimated_records = (end_page - start_page + 1) * 25
    estimated_time_minutes = (estimated_records * 2) / 60  # 2 seconds per record
    
    print(f"\n{'='*70}")
    print(f"Configuration:")
    print(f"{'='*70}")
    print(f"  Pages to scrape: {start_page} to {end_page}")
    print(f"  Estimated records: ~{estimated_records:,}")
    print(f"  Estimated time: ~{estimated_time_minutes:.1f} minutes")
    print(f"  Delay between requests: 2 seconds")
    print(f"  Output file: epa_all_types_pages_{start_page}_to_{end_page}.csv")
    
    print("\nThis scraper will:")
    print("  ✓ Extract ALL document types EXCEPT journals")
    print("  ✓ Include document type in output")
    print("  ✓ Extract only download URLs (PDFs, DOCs, PPTs, etc.)")
    print("  ✓ Put each download URL in a separate column")
    print("  ✓ Skip regular webpage links")
    print("  ✓ Show detailed progress in console and log file")
    
    response = input(f"\nStart scraping pages {start_page}-{end_page}? (yes/no): ").strip().lower()
    if response not in ['yes', 'y']:
        print("Cancelled.")
        return
    
    print("\nStarting scrape...\n")
    
    # Run the scraper
    scraper = EPAAllTypesScraper(start_page=start_page, end_page=end_page)
    scraper.scrape_page_range()
    scraper.save_to_csv()
    
    print(f"\n{'='*70}")
    print("All done! Check the log file for details: epa_scraper_all_types.log")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()
