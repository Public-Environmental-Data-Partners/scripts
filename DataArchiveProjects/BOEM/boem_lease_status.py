"""
BOEM Combined Leasing Status Report Downloader
Downloads all Combined Leasing Status Reports including archived years
"""

import os
import requests
from datetime import datetime
from pathlib import Path
import time
from bs4 import BeautifulSoup
import re


class BOEMLeaseStatusDownloader:
    def __init__(self, output_dir):
        """
        Initialize the downloader.
        
        Args:
            output_dir (str): Path to save downloaded reports
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        
        self.base_url = "https://www.boem.gov"
        self.report_url = "https://www.boem.gov/oil-gas-energy/leasing/combined-leasing-status-report"
        
        self.downloaded_files = []
        
    def download_file(self, url, filename):
        """Download a file with progress indication."""
        try:
            # Handle relative URLs
            if url.startswith('/'):
                url = self.base_url + url
            
            print(f"Downloading: {filename}")
            print(f"  URL: {url}")
            
            response = self.session.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            output_path = self.output_dir / filename
            total_size = int(response.headers.get('content-length', 0))
            
            with open(output_path, 'wb') as f:
                if total_size == 0:
                    f.write(response.content)
                else:
                    downloaded = 0
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            progress = (downloaded / total_size) * 100
                            print(f"  Progress: {progress:.1f}%", end='\r')
            
            print(f"\n  ✓ Saved to: {output_path}")
            self.downloaded_files.append({
                'filename': filename,
                'url': url,
                'path': output_path,
                'size': os.path.getsize(output_path)
            })
            return True
            
        except Exception as e:
            print(f"\n  ✗ Error downloading {filename}: {str(e)}")
            return False
    
    def scrape_report_page(self):
        """Scrape the Combined Leasing Status Report page for all file links."""
        print("\n=== Scraping Combined Leasing Status Report Page ===\n")
        
        try:
            response = self.session.get(self.report_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find all PDF links
            pdf_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if '.pdf' in href.lower() or 'lease-stats' in href.lower():
                    # Get link text for context
                    link_text = link.get_text(strip=True)
                    pdf_links.append({
                        'url': href,
                        'text': link_text
                    })
            
            # Also find Excel files
            excel_links = []
            for link in soup.find_all('a', href=True):
                href = link['href']
                if any(ext in href.lower() for ext in ['.xlsx', '.xls', '.xlsm']):
                    link_text = link.get_text(strip=True)
                    excel_links.append({
                        'url': href,
                        'text': link_text
                    })
            
            print(f"Found {len(pdf_links)} PDF links")
            print(f"Found {len(excel_links)} Excel links")
            
            return pdf_links + excel_links
            
        except Exception as e:
            print(f"Error scraping page: {str(e)}")
            return []
    
    def generate_historical_urls(self):
        """Generate URLs for historical reports based on known patterns."""
        print("\n=== Generating Historical Report URLs ===\n")
        
        # Known URL patterns for Combined Leasing Status Reports
        historical_urls = []
        
        # Pattern 1: Monthly reports with date format
        # Example: Lease-stats-10-1-19.pdf (October 1, 2019)
        base_path = "/sites/default/files/oil-and-gas-energy-program/Leasing/Combined-Leasing-Status-Report/"
        
        # Generate URLs for recent years (2015-2024)
        for year in range(2015, 2026):
            # Quarterly reports (common pattern)
            for month in [1, 4, 7, 10]:  # Q1, Q2, Q3, Q4
                for day in [1, 15]:
                    # Try both 2-digit and 4-digit year formats
                    year_2digit = str(year)[-2:]
                    filenames = [
                        f"Lease-stats-{month}-{day}-{year_2digit}.pdf",
                        f"Lease-stats-0{month}-01-{year_2digit}.pdf" if month < 10 else f"Lease-stats-{month}-01-{year_2digit}.pdf",
                        f"lease-stats-{month}-{day}-{year_2digit}.pdf",
                        f"LeaseStats-{month}-{day}-{year}.pdf",
                        f"Combined-Lease-Status-{month}-{year}.pdf"
                    ]
                    
                    for filename in filenames:
                        historical_urls.append({
                            'url': base_path + filename,
                            'text': f"Lease Status Report {month}/{year}",
                            'filename': filename
                        })
        
        # Pattern 2: Annual reports
        for year in range(2015, 2026):
            filenames = [
                f"Lease-stats-{year}.pdf",
                f"Combined-Leasing-Status-{year}.pdf",
                f"Annual-Lease-Report-{year}.pdf"
            ]
            
            for filename in filenames:
                historical_urls.append({
                    'url': base_path + filename,
                    'text': f"Annual Lease Status Report {year}",
                    'filename': filename
                })
        
        print(f"Generated {len(historical_urls)} potential historical URLs")
        return historical_urls
    
    def download_all_reports(self):
        """Download all available reports."""
        print(f"\n{'='*60}")
        print("BOEM Combined Leasing Status Report Downloader")
        print(f"{'='*60}\n")
        print(f"Output directory: {self.output_dir}\n")
        
        all_links = []
        
        # Step 1: Scrape current page for links
        scraped_links = self.scrape_report_page()
        all_links.extend(scraped_links)
        
        # Step 2: Add historical URLs
        historical_links = self.generate_historical_urls()
        all_links.extend(historical_links)
        
        # Remove duplicates
        unique_links = {}
        for link in all_links:
            url = link['url']
            if url not in unique_links:
                unique_links[url] = link
        
        print(f"\nTotal unique URLs to try: {len(unique_links)}")
        print("\n" + "="*60)
        print("Starting Downloads")
        print("="*60 + "\n")
        
        # Download each file
        successful = 0
        failed = 0
        
        for url, link_info in unique_links.items():
            # Determine filename
            if 'filename' in link_info:
                filename = link_info['filename']
            else:
                # Extract filename from URL
                filename = url.split('/')[-1]
                if not filename or '?' in filename:
                    # Generate filename from link text
                    safe_text = re.sub(r'[^\w\s-]', '', link_info['text'])
                    safe_text = re.sub(r'[-\s]+', '-', safe_text)
                    filename = f"{safe_text}.pdf"
            
            # Try to download
            if self.download_file(url, filename):
                successful += 1
            else:
                failed += 1
            
            # Be respectful to the server
            time.sleep(1)
        
        # Create summary
        self.create_summary()
        
        print(f"\n{'='*60}")
        print("Download Complete!")
        print(f"{'='*60}")
        print(f"\nResults:")
        print(f"  Successfully downloaded: {successful} files")
        print(f"  Failed downloads: {failed} URLs")
        print(f"  Total data downloaded: {self.format_size(sum(f['size'] for f in self.downloaded_files))}")
        print(f"\nAll files saved to: {self.output_dir}")
        print(f"See download_summary.txt for details")
    
    def format_size(self, bytes):
        """Format bytes into human-readable size."""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if bytes < 1024:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024
        return f"{bytes:.2f} TB"
    
    def create_summary(self):
        """Create a summary file of all downloads."""
        summary_path = self.output_dir / 'download_summary.txt'
        
        with open(summary_path, 'w') as f:
            f.write("BOEM Combined Leasing Status Report Downloads\n")
            f.write("="*60 + "\n")
            f.write(f"Download Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Output Directory: {self.output_dir}\n")
            f.write(f"Total Files: {len(self.downloaded_files)}\n\n")
            
            # Sort by filename
            sorted_files = sorted(self.downloaded_files, key=lambda x: x['filename'])
            
            f.write("Downloaded Files:\n")
            f.write("-"*60 + "\n\n")
            
            for file_info in sorted_files:
                f.write(f"Filename: {file_info['filename']}\n")
                f.write(f"Size: {self.format_size(file_info['size'])}\n")
                f.write(f"URL: {file_info['url']}\n")
                f.write(f"Path: {file_info['path']}\n")
                f.write("\n")
        
        print(f"\n✓ Summary created: {summary_path}")


def main():
    """Main execution function."""
    print("\nBOEM Combined Leasing Status Report Downloader")
    print("="*60)
    print("\nThis script downloads all Combined Leasing Status Reports")
    print("including archived years from the BOEM website.")
    print("="*60)
    
    # Get output directory
    output_dir = input("\nEnter the path to save reports: ").strip()
    
    if not output_dir:
        output_dir = "."
        print(f"Using current directory: {os.path.abspath(output_dir)}")
    
    # Create subdirectory
    output_dir = os.path.join(output_dir, f"BOEM_Lease_Status_Reports_{datetime.now().strftime('%Y%m%d')}")
    
    # Confirm
    print(f"\nReports will be saved to: {output_dir}")
    confirm = input("\nProceed with download? (yes/no): ").strip().lower()
    
    if confirm not in ['yes', 'y']:
        print("Download cancelled.")
        return
    
    # Check for BeautifulSoup
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        print("\nError: beautifulsoup4 is required for web scraping.")
        print("Install it with: pip install beautifulsoup4")
        return
    
    # Run downloader
    downloader = BOEMLeaseStatusDownloader(output_dir)
    downloader.download_all_reports()


if __name__ == "__main__":
    main()
