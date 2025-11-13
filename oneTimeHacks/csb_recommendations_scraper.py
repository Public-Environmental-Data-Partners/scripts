"""
CSB Recommendations Scraper
Scrapes recommendations from https://www.csb.gov/recommendations/
and extracts all recommendation details and download links.

Usage:
    python csb_recommendations_scraper.py
    python csb_recommendations_scraper.py --output "path/to/output.csv"
"""

import requests
from bs4 import BeautifulSoup
import csv
import time
from urllib.parse import urljoin, urlparse
import logging
import argparse
import os
import re
import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('csb_recommendations_scraper.log'),
        logging.StreamHandler()
    ]
)

class CSBRecommendationsScraper:
    def __init__(self):
        self.base_url = "https://www.csb.gov"
        self.recommendations_url = "https://www.csb.gov/recommendations/?F_All=y"
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

    def get_all_recommendations(self):
        """Scrape all recommendations from the recommendations page (View All mode)"""
        logging.info("Fetching all recommendations from View All page...")
        logging.info(f"URL: {self.recommendations_url}")

        response = self.get_page(self.recommendations_url)
        if not response:
            logging.error("Failed to fetch recommendations page")
            return []

        soup = BeautifulSoup(response.content, 'html.parser')

        # Extract recommendations from this page
        all_recommendations = self.extract_recommendations_from_page(soup)

        logging.info(f"Found {len(all_recommendations)} total recommendations")
        return all_recommendations

    def extract_recommendations_from_page(self, soup):
        """Extract all recommendation details from a page"""
        recommendations = []

        # Debug: Check what we're getting
        logging.info("Analyzing page structure...")

        # Find all investigation headers - they are in <div class="recHd"> tags
        investigation_headers = soup.find_all('div', class_='recHd')
        logging.info(f"Found {len(investigation_headers)} investigation headers")

        for investigation_header in investigation_headers:
            # Extract case name from the header
            # Format: "Aghorn Operating Inc. Waterflood Station Hydrogen Sulfide Release (9 Recommendations)"
            case_text = investigation_header.get_text(strip=True)
            case_match = re.match(r'^(.+?)\s*\(\d+\s+Recommendation', case_text)
            if case_match:
                case = case_match.group(1).strip()
            else:
                case = case_text.split('(')[0].strip() if '(' in case_text else case_text

            logging.info(f"Processing investigation: {case[:60]}...")

            # Find all recipient headers under this investigation
            # They are <a> tags with class="head"
            current_element = investigation_header.find_next_sibling()

            while current_element:
                # Stop if we hit another investigation header (div with class="recHd")
                if current_element.name == 'div' and 'recHd' in current_element.get('class', []):
                    break

                # Look for recipient headers (links with class="head")
                recipient_headers = current_element.find_all('a', class_='head')

                for recipient_header in recipient_headers:
                    # Extract recipient name from the header text
                    # Format: "Aghorn Operating Inc. (7 Recommendations)" or "Occupational Safety and Health Administration (OSHA) (1 Recommendations)"
                    header_text = recipient_header.get_text(strip=True)
                    recipient_match = re.match(r'^(.+?)\s*\(\d+\s+Recommendation', header_text)
                    if recipient_match:
                        recipient = recipient_match.group(1).strip()
                    else:
                        recipient = header_text.split('(')[0].strip() if '(' in header_text else header_text

                    # Find the content div that follows this recipient header
                    # The content is typically in the next sibling div with class="content"
                    content_div = recipient_header.find_next_sibling('div', class_='content')
                    if not content_div:
                        # Try without class restriction
                        content_div = recipient_header.find_next_sibling('div')

                    if not content_div:
                        parent = recipient_header.parent
                        if parent:
                            content_div = parent.find_next_sibling('div')

                    if not content_div:
                        continue

                    # Look for recommendation IDs within this recipient's section
                    recommendation_pattern = re.compile(r'\b\d{4}-\d{2}-I-[A-Z]{2}-\d+\b')

                    # Find all elements containing recommendation IDs
                    for element in content_div.find_all(string=recommendation_pattern):
                        # Try to extract the full recommendation ID
                        rec_id_match = recommendation_pattern.search(element)
                        if not rec_id_match:
                            continue

                        rec_id = rec_id_match.group(0)

                        # Extract root ID and file ID
                        parts = rec_id.split('-')
                        if len(parts) >= 5:
                            root_id = f"{parts[0]}{parts[1]}I{parts[3]}"
                            file_id = f"{root_id}{parts[4]}"
                        else:
                            root_id = rec_id.replace('-', '')
                            file_id = root_id

                        # Find the containing section for this recommendation
                        parent = element.parent
                        section = parent
                        for _ in range(10):
                            if section and section.name in ['div', 'section', 'article', 'li', 'tr', 'td']:
                                break
                            if section and section.parent:
                                section = section.parent
                            else:
                                break

                        # Extract recommendation text and status
                        rec_text = ""
                        status = ""

                        if section:
                            # Extract status from the tooltip link
                            # Format: <a class="tooltip" ...>Closed - Acceptable Action</a>
                            # The tooltip div contains: "Closed - Acceptable Action (C - AA) - ..."
                            status_link = section.find('a', class_='tooltip')
                            if status_link:
                                # Look for the tooltip div that follows
                                tooltip_div = status_link.find_next_sibling('div')
                                if tooltip_div:
                                    tooltip_text = tooltip_div.get_text(strip=True)
                                    # Extract status code in parentheses like (C - AA)
                                    status_pattern = re.compile(r'\(([CO])\s*-\s*([A-Z/]+)\)')
                                    status_match = status_pattern.search(tooltip_text)
                                    if status_match:
                                        status = f"{status_match.group(1)}-{status_match.group(2)}"

                            # Get the recommendation text from the description span
                            # Format: <span id="...lblDesc"><p>recommendation text...</p></span>
                            desc_span = section.find('span', id=re.compile(r'lblDesc$'))
                            if desc_span:
                                rec_text = desc_span.get_text(separator=' ', strip=True)

                            # If we didn't find it via span, try paragraphs
                            if not rec_text:
                                paragraphs = section.find_all('p')
                                for p in paragraphs:
                                    p_text = p.get_text(separator=' ', strip=True)
                                    # Check if this is the recommendation text paragraph
                                    # It's usually the paragraph right after the ID and doesn't contain status info
                                    if rec_id in p_text or (not rec_text and len(p_text) > 20 and 'Status:' not in p_text):
                                        # Extract text after the recommendation ID
                                        if rec_id in p_text:
                                            rec_text = p_text.split(rec_id, 1)[-1].strip()
                                        elif not rec_text:
                                            rec_text = p_text
                                        break

                            # Clean up recommendation text
                            if rec_text:
                                # Remove status info if present
                                if 'Status:' in rec_text:
                                    rec_text = rec_text.split('Status:')[0].strip()

                                # Remove status code if present
                                if status and status in rec_text:
                                    rec_text = rec_text.split(status)[0].strip()

                                # Limit length
                                if len(rec_text) > 500:
                                    sentences = rec_text[:500].split('.')
                                    if len(sentences) > 1:
                                        rec_text = '.'.join(sentences[:-1]) + '.'
                                    else:
                                        rec_text = rec_text[:500] + "..."

                        # Find any PDF links in the section
                        pdf_links = []
                        if section:
                            for link in section.find_all('a', href=True):
                                href = link['href']
                                if '.pdf' in href.lower():
                                    pdf_url = urljoin(self.base_url, href)
                                    pdf_links.append(pdf_url)

                        # Create entry for each PDF link, or one entry if no PDFs
                        if pdf_links:
                            for pdf_link in pdf_links:
                                recommendations.append({
                                    'root_id': root_id,
                                    'file_id': file_id,
                                    'recommendation_id': rec_id,
                                    'case': case,
                                    'recipient': recipient,
                                    'status': status,
                                    'recommendation_text': rec_text,
                                    'download_url': pdf_link
                                })
                        else:
                            recommendations.append({
                                'root_id': root_id,
                                'file_id': file_id,
                                'recommendation_id': rec_id,
                                'case': case,
                                'recipient': recipient,
                                'status': status,
                                'recommendation_text': rec_text,
                                'download_url': ''
                            })

                current_element = current_element.find_next_sibling()

        return recommendations

    def scrape_all(self, output_file='csb_recommendations_downloads.csv'):
        """Main scraping function"""
        start_time = datetime.datetime.now()
        logging.info("Starting CSB recommendations scraper...")
        logging.info(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        # Get all recommendations
        all_recommendations = self.get_all_recommendations()

        if not all_recommendations:
            logging.error("No recommendations found. Exiting.")
            return

        end_time = datetime.datetime.now()
        duration = end_time - start_time

        # Save to CSV
        self.save_to_csv(all_recommendations, output_file)
        logging.info(f"Scraping complete! Found {len(all_recommendations)} total recommendations")
        logging.info(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"Total duration: {duration}")
        logging.info(f"Results saved to: {os.path.abspath(output_file)}")

        # Generate summary log file
        self.save_summary_log(output_file, start_time, end_time, duration, len(all_recommendations))

        return all_recommendations

    def save_to_csv(self, recommendations, output_file):
        """Save recommendations data to CSV file"""
        logging.info(f"Saving results to {output_file}...")

        # Ensure directory exists
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir)

        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'root_id',
                'file_id',
                'recommendation_id',
                'case',
                'recipient',
                'status',
                'recommendation_text',
                'download_url'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)

            writer.writeheader()
            for recommendation in recommendations:
                writer.writerow(recommendation)

        logging.info(f"Saved {len(recommendations)} recommendations to {output_file}")

    def save_summary_log(self, output_file, start_time, end_time, duration, total_recommendations):
        """Save a summary log file alongside the CSV output"""
        log_file = output_file.rsplit('.', 1)[0] + '_summary.log'

        logging.info(f"Saving summary log to {log_file}...")

        with open(log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("CSB RECOMMENDATIONS SCRAPER - SUMMARY REPORT\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Scrape Start Time:       {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Scrape End Time:         {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Total Duration:          {duration}\n\n")

            f.write("-" * 80 + "\n")
            f.write("STATISTICS\n")
            f.write("-" * 80 + "\n\n")

            f.write(f"Total Recommendations Found:        {total_recommendations}\n\n")

            f.write("-" * 80 + "\n")
            f.write("OUTPUT FILES\n")
            f.write("-" * 80 + "\n\n")

            f.write(f"CSV Data File:    {os.path.abspath(output_file)}\n")
            f.write(f"Summary Log:      {os.path.abspath(log_file)}\n")
            f.write(f"Detailed Log:     {os.path.abspath('csb_recommendations_scraper.log')}\n\n")

            f.write("-" * 80 + "\n")
            f.write("NOTES\n")
            f.write("-" * 80 + "\n\n")

            f.write("- Each row represents one recommendation\n")
            f.write("- root_id: Investigation ID (e.g., 202001ITX)\n")
            f.write("- file_id: Recommendation ID (e.g., 202001ITX1)\n")
            f.write("- Recommendation IDs follow format: YYYY-##-I-ST-# (e.g., 2020-01-I-TX-1)\n")
            f.write("- Status codes: C-* = Closed, O-* = Open\n\n")

            f.write("=" * 80 + "\n")
            f.write(f"Report generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n")

        logging.info(f"Summary log saved to: {os.path.abspath(log_file)}")


def main():
    parser = argparse.ArgumentParser(
        description='Scrape CSB recommendations and download links'
    )
    parser.add_argument(
        '--output', '-o',
        type=str,
        default='csb_recommendations_downloads.csv',
        help='Output CSV file path (default: csb_recommendations_downloads.csv)'
    )

    args = parser.parse_args()

    scraper = CSBRecommendationsScraper()
    scraper.scrape_all(args.output)


if __name__ == "__main__":
    main()
