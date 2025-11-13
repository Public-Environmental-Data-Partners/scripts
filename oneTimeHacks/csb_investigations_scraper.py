"""
CSB Investigations Scraper
Scrapes completed investigations from https://www.csb.gov/investigations/completed-investigations/
and extracts all download links with their titles from each investigation page.

Usage:
    python csb_investigations_scraper.py
    python csb_investigations_scraper.py --output "path/to/output.csv"
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse
import logging
import argparse
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csb_scraper.log'),
        logging.StreamHandler()
    ]
)

class CSBScraper:
    def __init__(self):
        self.base_url = "https://www.csb.gov"
        self.investigations_url = "https://www.csb.gov/investigations/completed-investigations/"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_page(self, url, retries=3):
        """Fetch a page with retry logic"""
        for attempt in range(retries):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logging.error(f"Failed to fetch {url} after {retries} attempts")
                    return None

    def get_incident_urls(self):
        """Scrape all incident URLs from the completed investigations page with pagination"""
        logging.info("Fetching incident URLs from search results...")
        incident_urls = set()  # Use set to avoid duplicates
        page_num = 1
        max_pages = 100  # Safety limit to prevent infinite loops

        while page_num <= max_pages:
            # Construct URL with pagination parameter
            # CSB uses pg= parameter for pagination
            if page_num == 1:
                search_url = f"{self.investigations_url}?PageIndex=1"
            else:
                search_url = f"{self.investigations_url}?PageIndex=1&pg={page_num}"

            logging.info(f"Fetching page {page_num}: {search_url}")

            response = self.get_page(search_url)
            if not response:
                logging.warning(f"Failed to fetch page {page_num}")
                break

            soup = BeautifulSoup(response.content, 'html.parser')

            # Track if we found any new links on this page
            page_links_found = 0

            # Find all investigation links
            # These are links that match the pattern /{investigation-name-slug}/
            # They are NOT under /investigations/ but are root-level slugs
            for link in soup.find_all('a', href=True):
                href = link['href']

                # Investigation detail pages are root-level slugs like /cuisine-solutions-ammonia-release-/
                # They typically:
                # - Start and end with /
                # - Contain hyphens
                # - Are NOT /investigations/completed-investigations/
                # - Are NOT pagination or system links

                # Skip common non-investigation links
                if any(skip in href.lower() for skip in [
                    '/investigations/completed-investigations',
                    '/news/',
                    '/assets/',
                    '/about/',
                    '/contact/',
                    'javascript:',
                    '#',
                    'mailto:',
                    '.gov/search',
                    '/disclaimer',
                    '/privacy'
                ]):
                    continue

                # Look for investigation detail page patterns:
                # - Starts with /
                # - Contains words separated by hyphens
                # - Ends with /
                # - No file extensions
                import re
                if re.match(r'^/[a-z0-9]+-[a-z0-9-]+/$', href.lower()):
                    full_url = urljoin(self.base_url, href)
                    if full_url not in incident_urls:
                        incident_urls.add(full_url)
                        page_links_found += 1

            logging.info(f"Found {page_links_found} new investigation links on page {page_num}")

            # If we found no links on this page, we're likely done
            if page_links_found == 0:
                logging.info(f"No investigation links found on page {page_num}, stopping pagination")
                break

            # Check if there's a next page by looking for pagination controls
            # Look for links with pg= parameter or "next" button
            has_next_page = False
            pagination_links = []

            for link in soup.find_all('a', href=True):
                href = link['href']
                link_text = link.get_text(strip=True)

                # Check if this is a pagination link with pg= parameter
                if 'pg=' in href:
                    # Extract the page number from the link
                    import re
                    match = re.search(r'pg=(\d+)', href)
                    if match:
                        pg_num = int(match.group(1))
                        pagination_links.append(pg_num)
                        if pg_num == page_num + 1:
                            has_next_page = True

                # Also check for "next" buttons
                if link_text.lower() in ['next', 'next >', '»', '›']:
                    has_next_page = True

            # Log pagination info for debugging
            if pagination_links:
                logging.info(f"Found pagination links for pages: {sorted(set(pagination_links))}")

            if not has_next_page:
                logging.info(f"No next page found after page {page_num}, stopping pagination")
                break

            page_num += 1
            time.sleep(1)  # Be polite between page requests

        incident_urls_list = sorted(list(incident_urls))
        logging.info(f"Found {len(incident_urls_list)} total incident URLs across {page_num} pages")
        return incident_urls_list

    def extract_incident_id(self, soup):
        """Extract and format ID from Recommendations section (e.g., 2024-01-I-TN-4 -> 202401ITN4)"""
        incident_id = ""

        # Look for Recommendations heading
        recommendations_heading = None
        for heading in soup.find_all(['h2', 'h3', 'h4']):
            if 'recommendation' in heading.get_text(strip=True).lower():
                recommendations_heading = heading
                break

        if recommendations_heading:
            # Look for the ID pattern in the next siblings or within the recommendations section
            current = recommendations_heading.find_next_sibling()
            while current and current.name not in ['h2', 'h3', 'h4']:
                text = current.get_text()
                # Look for pattern like 2024-01-I-TN-4
                import re
                match = re.search(r'\d{4}-\d{2}-[A-Z]+-[A-Z]+-\d+', text)
                if match:
                    # Remove dashes and special characters
                    incident_id = match.group(0).replace('-', '')
                    break
                current = current.find_next_sibling()

        return incident_id

    def extract_description(self, soup):
        """Extract the description paragraph"""
        description = ""

        # Look for description in common locations
        # Try to find paragraphs near the top of the content
        content_area = soup.find('div', class_=lambda x: x and 'content' in x.lower() if x else False)
        if not content_area:
            content_area = soup.find('main')
        if not content_area:
            content_area = soup.find('article')
        if not content_area:
            content_area = soup

        # Get the first substantial paragraph after the title
        # Skip paragraphs that are just metadata
        paragraphs = content_area.find_all('p')
        for p in paragraphs:
            text = p.get_text(strip=True)

            # Skip if it's too short or contains common metadata indicators
            skip_indicators = [
                'Status:',
                'Location:',
                'Accident Occurred On:',
                'Final Report Released On:',
                'Incident Date:',
                'Report Date:',
                'Download',
                'Page Last Reviewed'
            ]

            # Check if this paragraph should be skipped
            should_skip = False
            for indicator in skip_indicators:
                if text.startswith(indicator):
                    should_skip = True
                    break

            # Look for a paragraph that seems like a description (reasonable length)
            if not should_skip and len(text) > 50:
                description = text
                break

        # If still no description found, try to get text near the h1 title
        if not description:
            h1 = soup.find('h1')
            if h1:
                # Look for the next few siblings that are paragraphs
                next_elem = h1.find_next_sibling()
                while next_elem and not description:
                    if next_elem.name == 'p':
                        text = next_elem.get_text(strip=True)
                        if len(text) > 50:
                            description = text
                            break
                    next_elem = next_elem.find_next_sibling()

        return description

    def extract_field_value(self, soup, field_label):
        """Extract value for fields like Location, Date Incident, etc."""
        value = ""

        # Look for the field label in strong/bold tags or as text
        for tag in soup.find_all(['strong', 'b', 'dt', 'label']):
            if field_label.lower() in tag.get_text(strip=True).lower():
                # Try to get the value from next sibling or parent's next sibling
                if tag.next_sibling:
                    value = tag.next_sibling.get_text(strip=True) if hasattr(tag.next_sibling, 'get_text') else str(tag.next_sibling).strip()
                elif tag.parent and tag.parent.next_sibling:
                    next_elem = tag.parent.next_sibling
                    value = next_elem.get_text(strip=True) if hasattr(next_elem, 'get_text') else str(next_elem).strip()

                # Also check for dd tags (definition lists)
                if tag.name == 'dt':
                    dd = tag.find_next_sibling('dd')
                    if dd:
                        value = dd.get_text(strip=True)

                if value:
                    # Clean up the value
                    value = value.replace(':', '').strip()
                    break

        # Alternative: look in the page text for patterns like "Location: City, State"
        if not value:
            text = soup.get_text()
            import re
            pattern = rf'{field_label}:\s*([^\n]+)'
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()

        return value

    def get_downloads_from_incident(self, incident_url):
        """Extract all download links and titles from an incident page"""
        logging.info(f"Processing incident: {incident_url}")
        response = self.get_page(incident_url)

        if not response:
            return []

        soup = BeautifulSoup(response.content, 'html.parser')
        downloads = []

        # Get incident title
        incident_title = "Unknown Incident"
        title_tag = soup.find('h1')
        if title_tag:
            incident_title = title_tag.get_text(strip=True)

        # Extract additional fields
        incident_id = self.extract_incident_id(soup)
        location = self.extract_field_value(soup, 'Location')

        # Try multiple field names for the incident date
        date_incident = self.extract_field_value(soup, 'Accident Occurred On')
        if not date_incident:
            date_incident = self.extract_field_value(soup, 'Date')
        if not date_incident:
            date_incident = self.extract_field_value(soup, 'Incident Date')

        # Try multiple field names for the final report date
        date_final_report = self.extract_field_value(soup, 'Final Report Released On')
        if not date_final_report:
            date_final_report = self.extract_field_value(soup, 'Final Report')
        if not date_final_report:
            date_final_report = self.extract_field_value(soup, 'Report Date')

        # Extract category (Accident Type)
        category = self.extract_field_value(soup, 'Accident Type')

        description = self.extract_description(soup)

        # Skip this incident if there's no final report date
        if not date_final_report:
            logging.info(f"Skipping incident (no final report date): {incident_title}")
            return []

        # Find all download links
        # Look for common file extensions and download patterns
        file_extensions = ['.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.zip']

        for link in soup.find_all('a', href=True):
            href = link['href']

            # Check if it's a downloadable file
            if any(ext in href.lower() for ext in file_extensions):
                download_url = urljoin(self.base_url, href)

                # Try to get the link text as the title
                link_text = link.get_text(strip=True)

                # If no text, try to find nearby text or use filename
                if not link_text:
                    # Check if there's a title attribute
                    link_text = link.get('title', '')

                    # If still no text, use the filename
                    if not link_text:
                        link_text = href.split('/')[-1]

                downloads.append({
                    'id': incident_id,
                    'source_url': incident_url,
                    'incident_title': incident_title,
                    'location': location,
                    'category': category,
                    'date_incident': date_incident,
                    'date_final_report': date_final_report,
                    'description': description,
                    'download_url': download_url,
                    'download_title': link_text,
                    'file_type': self.get_file_extension(download_url)
                })

        logging.info(f"Found {len(downloads)} downloads for this incident")
        return downloads

    def get_file_extension(self, url):
        """Extract file extension from URL"""
        parsed = urlparse(url)
        path = parsed.path
        if '.' in path:
            return path.split('.')[-1].lower()
        return 'unknown'

    def scrape_all(self, output_file='csb_investigations_downloads.csv'):
        """Main scraping function"""
        import datetime

        start_time = datetime.datetime.now()
        logging.info("Starting CSB investigations scraper...")
        logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Get all incident URLs
        incident_urls = self.get_incident_urls()

        if not incident_urls:
            logging.error("No incident URLs found. Exiting.")
            return

        # Collect all downloads
        all_downloads = []
        incidents_processed = 0
        incidents_skipped = 0

        for i, incident_url in enumerate(incident_urls, 1):
            logging.info(f"Processing incident {i}/{len(incident_urls)}")
            downloads = self.get_downloads_from_incident(incident_url)
            if downloads:
                all_downloads.extend(downloads)
                incidents_processed += 1
            else:
                incidents_skipped += 1

            # Be polite - add delay between requests
            time.sleep(1)

        end_time = datetime.datetime.now()
        duration = end_time - start_time

        # Save to CSV
        if all_downloads:
            self.save_to_csv(all_downloads, output_file)
            logging.info(f"Scraping complete! Found {len(all_downloads)} total downloads from {incidents_processed} incidents")
            logging.info(f"Incidents skipped (no final report): {incidents_skipped}")
            logging.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logging.info(f"Total duration: {duration}")
            logging.info(f"Results saved to: {os.path.abspath(output_file)}")

            # Generate summary log file
            self.save_summary_log(output_file, start_time, end_time, duration,
                                 len(incident_urls), incidents_processed,
                                 incidents_skipped, len(all_downloads))
        else:
            logging.warning("No downloads found")

        return all_downloads

    def save_to_csv(self, downloads, output_file):
        """Save downloads data to CSV file"""
        logging.info(f"Saving results to {output_file}...")

        # Ensure directory exists
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'id',
                'source_url',
                'incident_title',
                'location',
                'category',
                'date_incident',
                'date_final_report',
                'description',
                'download_url',
                'download_title',
                'file_type'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()
            for download in downloads:
                writer.writerow(download)

        logging.info(f"Saved {len(downloads)} downloads to {output_file}")

    def save_summary_log(self, output_file, start_time, end_time, duration,
                        total_urls, processed, skipped, total_downloads):
        """Save a summary log file alongside the CSV output"""
        import datetime

        # Create log filename based on CSV filename
        log_file = output_file.rsplit('.', 1)[0] + '_summary.log'

        logging.info(f"Saving summary log to {log_file}...")

        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("CSB INVESTIGATIONS SCRAPER - SUMMARY REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Scrape Start Time:       {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Scrape End Time:         {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Duration:          {duration}\n\n")

            f.write("-" * 80 + "\n")
            f.write("STATISTICS\n")
            f.write("-" * 80 + "\n\n")

            f.write(f"Total Investigation URLs Found:     {total_urls}\n")
            f.write(f"Investigations Processed:           {processed}\n")
            f.write(f"Investigations Skipped:             {skipped}\n")
            f.write(f"Total Download Links Found:         {total_downloads}\n\n")

            if processed > 0:
                avg_downloads = total_downloads / processed
                f.write(f"Average Downloads per Investigation: {avg_downloads:.2f}\n\n")

            f.write("-" * 80 + "\n")
            f.write("OUTPUT FILES\n")
            f.write("-" * 80 + "\n\n")

            f.write(f"CSV Data File:    {os.path.abspath(output_file)}\n")
            f.write(f"Summary Log:      {os.path.abspath(log_file)}\n")
            f.write(f"Detailed Log:     {os.path.abspath('csb_scraper.log')}\n\n")

            f.write("-" * 80 + "\n")
            f.write("NOTES\n")
            f.write("-" * 80 + "\n\n")

            f.write("- Investigations without a 'Final Report Released On' date were skipped\n")
            f.write("- Only files with common document extensions were captured\n")
            f.write("  (pdf, doc, docx, xls, xlsx, ppt, pptx, zip)\n")
            f.write("- Each row in the CSV represents one downloadable file\n")
            f.write("- Multiple download links may exist for a single investigation\n\n")

            f.write("=" * 80 + "\n")
            f.write(f"Report generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n")

        logging.info(f"Summary log saved to: {os.path.abspath(log_file)}")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape CSB completed investigations and download links'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='csb_investigations_downloads.csv',
        help='Output CSV file path (default: csb_investigations_downloads.csv)'
    )

    args = parser.parse_args()

    scraper = CSBScraper()
    scraper.scrape_all(args.output)


if __name__ == "__main__":
    main()
