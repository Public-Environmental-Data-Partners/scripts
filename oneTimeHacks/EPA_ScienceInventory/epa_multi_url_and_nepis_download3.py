import csv
import requests
import os
from urllib.parse import urlparse, urljoin
from pathlib import Path
from bs4 import BeautifulSoup
import re
import time
from collections import defaultdict

def get_filename_from_url(url):
    """Extract filename from URL"""
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

def get_nepis_download_url(nepis_url):
    """
    Extract the actual PDF download link from a NEPIS page.
    NEPIS uses a popup page with "Get This Item" -> "PDF" link.
    
    Args:
        nepis_url: NEPIS page URL
        
    Returns:
        tuple: (download_url, filename) or (None, None) if not found
    """
    try:
        # Step 1: Construct the Display=p%7Cf URL to get the popup page
        if 'Display=' in nepis_url:
            popup_url = re.sub(r'Display=[^&]+', 'Display=p%7Cf', nepis_url)
        else:
            separator = '&' if '?' in nepis_url else '?'
            popup_url = f"{nepis_url}{separator}Display=p%7Cf"
        
        print(f"  → Fetching NEPIS popup page...")
        
        # Step 2: Fetch the popup page HTML
        response = requests.get(popup_url, timeout=30)
        response.raise_for_status()
        
        # Step 3: Parse HTML to find the actual PDF link
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Look for "PDF" link (the actual download button)
        for link in soup.find_all('a', href=True):
            link_text = link.get_text().strip()
            
            # Look for the PDF download link
            if link_text.upper() == 'PDF' or 'PDF' in link_text.upper():
                href = link['href']
                print(f"  → Found 'PDF' link with href: {href}")
                
                # Check if it's a JavaScript link or needs modification
                if href == '#' or 'javascript:' in href.lower():
                    print(f"  → PDF link is JavaScript/anchor, skipping...")
                    continue
                
                # Make absolute URL
                if href.startswith('http'):
                    pdf_url = href
                else:
                    pdf_url = urljoin(popup_url, href)
                
                print(f"  → Absolute PDF URL: {pdf_url[:100]}...")
                
                # Check if this URL is different from the popup URL
                if pdf_url == popup_url:
                    print(f"  → PDF URL same as popup URL, skipping...")
                    continue
                
                # Extract filename from URL
                filename = get_filename_from_url(pdf_url)
                if not filename or filename == 'ZyNET.exe':
                    # Extract .TXT filename and convert to .pdf
                    txt_match = re.search(r'/([A-Z0-9]+\.txt)', pdf_url, re.IGNORECASE)
                    if txt_match:
                        filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
                
                return pdf_url, filename
        
        # If not found, look for any link containing "pdf" in href
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'pdf' in href.lower() or '.pdf' in href.lower():
                pdf_url = urljoin(popup_url, href)
                filename = get_filename_from_url(pdf_url)
                print(f"  → Found PDF link (fallback): {pdf_url[:100]}...")
                return pdf_url, filename
        
        # Method: Look for PDF URL in page source/JavaScript
        # NEPIS often embeds the PDF URL directly in the HTML
        # Look for patterns like: pdfURL, pdf_url, or direct ZyNET.exe calls with ZyPDF
        html_text = response.text
        
        # Look for ZyShowPDF JavaScript function to see what URL it constructs
        # The PDF button calls: ZyShowPDF('hardcopy',event)
        zyshowpdf_match = re.search(r'function\s+ZyShowPDF[^{]*\{(.{0,2000})\}', html_text, re.IGNORECASE | re.DOTALL)
        if zyshowpdf_match:
            func_text = zyshowpdf_match.group(0)
            print(f"  → Found ZyShowPDF function, analyzing...")
            
            # Look for URL construction in the function
            # Common patterns: window.open, location.href, etc.
            url_patterns = [
                r'window\.open\(["\']([^"\']+)["\']',
                r'location\.href\s*=\s*["\']([^"\']+)["\']',
                r'["\']([^"\']*ZyPDF[^"\']*)["\']',
                r'/Exe/ZyPDF\.exe[^"\'<>\s]*',
            ]
            
            for pattern in url_patterns:
                match = re.search(pattern, func_text, re.IGNORECASE)
                if match:
                    if match.groups():
                        pdf_path = match.group(1)
                    else:
                        pdf_path = match.group(0)
                    print(f"  → Found URL pattern in function: {pdf_path[:100]}...")
                    
                    # Try to construct full URL
                    if pdf_path.startswith('http'):
                        test_pdf_url = pdf_path
                    elif pdf_path.startswith('/'):
                        test_pdf_url = urljoin(popup_url, pdf_path)
                    else:
                        test_pdf_url = None
                    
                    if test_pdf_url:
                        # Special case: DLwait.htm is just a waiting page, we need to extract the actual PDF URL from it
                        if 'DLwait.htm' in test_pdf_url:
                            print(f"  → DLwait.htm detected, fetching to find actual PDF URL...")
                            try:
                                wait_response = requests.get(test_pdf_url, timeout=30)
                                wait_html = wait_response.text
                                wait_soup = BeautifulSoup(wait_html, 'html.parser')
                                
                                # Look for meta refresh or JavaScript redirect
                                for meta in wait_soup.find_all('meta'):
                                    if meta.get('http-equiv', '').lower() == 'refresh':
                                        content = meta.get('content', '')
                                        url_match = re.search(r'url=([^"\';]+)', content, re.IGNORECASE)
                                        if url_match:
                                            actual_pdf_url = urljoin(test_pdf_url, url_match.group(1))
                                            print(f"  → Found PDF URL in meta refresh: {actual_pdf_url[:100]}...")
                                            txt_match = re.search(r'/([A-Z0-9]+\.(?:txt|pdf))', actual_pdf_url, re.IGNORECASE)
                                            filename = None
                                            if txt_match:
                                                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
                                            return actual_pdf_url, filename
                                
                                # Look for window.location or similar in script tags
                                for script in wait_soup.find_all('script'):
                                    script_text = script.string if script.string else ''
                                    url_match = re.search(r'(?:window\.location|location\.href)\s*=\s*["\']([^"\']+)["\']', script_text, re.IGNORECASE)
                                    if url_match:
                                        actual_pdf_url = urljoin(test_pdf_url, url_match.group(1))
                                        print(f"  → Found PDF URL in JavaScript: {actual_pdf_url[:100]}...")
                                        txt_match = re.search(r'/([A-Z0-9]+\.(?:txt|pdf))', actual_pdf_url, re.IGNORECASE)
                                        filename = None
                                        if txt_match:
                                            filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
                                        return actual_pdf_url, filename
                                
                                print(f"  ✗ Could not find PDF URL in DLwait.htm page")
                            except Exception as e:
                                print(f"  ✗ Error fetching DLwait.htm: {e}")
                        else:
                            print(f"  → Trying PDF URL from JS: {test_pdf_url[:100]}...")
                            # Extract filename
                            txt_match = re.search(r'/([A-Z0-9]+\.(?:txt|pdf))', test_pdf_url, re.IGNORECASE)
                            filename = None
                            if txt_match:
                                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
                            return test_pdf_url, filename
        
        # Method 1: Try to construct ZyPDF URL by changing the action parameter
        # NEPIS likely serves PDFs by changing ZyActionD=ZyDocument to ZyActionD=ZyPDF
        if 'ZyActionD=ZyDocument' in popup_url:
            pdf_url = popup_url.replace('ZyActionD=ZyDocument', 'ZyActionD=ZyPDF')
            # Also change Display parameter
            pdf_url = re.sub(r'Display=[^&]+', 'Display=p%7Cf', pdf_url)
            print(f"  → Constructed ZyPDF URL: {pdf_url[:100]}...")
            
            # Extract filename
            txt_match = re.search(r'/([A-Z0-9]+\.txt)', pdf_url, re.IGNORECASE)
            filename = None
            if txt_match:
                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
            
            return pdf_url, filename
        
        # Method 2: Look for existing ZyPDF URL in page source
        zypdF_url_match = re.search(r'(https?://[^"\'<>\s]*ZyActionD=ZyPDF[^"\'<>\s]*)', html_text, re.IGNORECASE)
        if zypdF_url_match:
            pdf_url = zypdF_url_match.group(1)
            print(f"  → Found ZyPDF URL in source: {pdf_url[:100]}...")
            
            # Extract filename
            txt_match = re.search(r'/([A-Z0-9]+\.txt)', pdf_url, re.IGNORECASE)
            filename = None
            if txt_match:
                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
            
            return pdf_url, filename
        
        # Try to find ZyPDF action in page source
        pdf_pattern = re.search(r'ZyActionD=ZyPDF[^"\'<>\s]*', html_text)
        if pdf_pattern:
            pdf_params = pdf_pattern.group(0)
            # Construct the full URL
            base_url = nepis_url.split('?')[0]  # Get base URL without params
            pdf_url = f"{base_url}?{pdf_params}"
            print(f"  → Found ZyPDF action in page: {pdf_url[:100]}...")
            
            # Extract filename
            txt_match = re.search(r'/([A-Z0-9]+\.txt)', pdf_url, re.IGNORECASE)
            filename = None
            if txt_match:
                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
            
            return pdf_url, filename
        
        # Try to find any ZyNET.exe link with different action
        zynet_pattern = re.search(r'/Exe/ZyNET\.exe/[A-Z0-9]+\.(?:txt|pdf)[^"\'<>\s]*', html_text, re.IGNORECASE)
        if zynet_pattern:
            pdf_path = zynet_pattern.group(0)
            pdf_url = urljoin(popup_url, pdf_path)
            print(f"  → Found ZyNET path in source: {pdf_url[:100]}...")
            
            # Extract filename
            txt_match = re.search(r'/([A-Z0-9]+\.(?:txt|pdf))', pdf_url, re.IGNORECASE)
            filename = None
            if txt_match:
                filename = txt_match.group(1).replace('.txt', '.pdf').replace('.TXT', '.pdf')
            
            return pdf_url, filename
        
        # Debug: Print some of the HTML to see what's there
        print(f"  ✗ Could not find PDF link in popup page")
        print(f"  → Found {len(soup.find_all('a'))} links total")
        print(f"  → Searching HTML source for PDF patterns...")
        
        # Look for any mention of PDF in the HTML
        if 'pdf' in html_text.lower():
            pdf_mentions = re.findall(r'.{0,100}pdf.{0,100}', html_text.lower(), re.IGNORECASE)[:3]
            for i, mention in enumerate(pdf_mentions):
                print(f"     PDF mention {i+1}: ...{mention}...")
        
        return None, None
        
    except Exception as e:
        print(f"  ✗ Error extracting NEPIS PDF URL: {e}")
        import traceback
        traceback.print_exc()
        return None, None

def sanitize_filename(filename):
    """
    Remove invalid characters from filename for Windows/Unix compatibility.
    Invalid characters: < > : " / \ | ? *
    """
    # Replace invalid characters with underscore
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', filename)
    
    # Remove leading/trailing spaces and dots (Windows doesn't allow these)
    sanitized = sanitized.strip('. ')
    
    # Replace multiple underscores with single underscore
    sanitized = re.sub(r'_+', '_', sanitized)
    
    return sanitized

def extract_title_prefix(title):
    """
    Extract first four non-filler words from title and join with underscores.
    Filler words to exclude: to, the, of, and, at, in, for
    """
    if not title:
        return ""
    
    # Filler words to exclude
    filler_words = {'to', 'the', 'of', 'and', 'at', 'in', 'for', 'a', 'an'}
    
    # Split title into words and clean them
    words = re.findall(r'\w+', title.lower())
    
    # Filter out filler words and get first 4 meaningful words
    meaningful_words = [w for w in words if w not in filler_words][:4]
    
    # Join with underscores and sanitize
    prefix = '_'.join(meaningful_words)
    return sanitize_filename(prefix)

def download(url, filename=None, output_dir='downloads'):
    """
    Download file from URL, handling NEPIS redirects.
    Returns tuple: (success: bool, native_filename: str)
    
    Args:
        url: Download URL
        filename: Optional filename to save as (if None, extracts from URL)
        output_dir: Directory to save downloaded files
    """
    try:
        # Create output directory if it doesn't exist
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        # Check if this is a NEPIS URL
        if 'nepis.epa.gov' in url:
            print(f"  → Detected NEPIS URL, extracting download link...")
            actual_url, nepis_filename = get_nepis_download_url(url)
            
            if actual_url:
                url = actual_url
                if nepis_filename and not filename:
                    filename = nepis_filename
                # Small delay to be respectful to the server
                time.sleep(1)
            else:
                print(f"  ✗ Could not extract download link from NEPIS page")
                return False, None
        
        # Download the file - follow redirects to get final URL
        print(f"  → Downloading: {url[:100]}...")
        response = requests.get(url, stream=True, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        # Check if we actually got a PDF (important for NEPIS)
        content_type = response.headers.get('Content-Type', '').lower()
        print(f"  → Content-Type: {content_type}")
        
        # Get native filename from various sources if not provided
        native_filename = filename
        if not native_filename:
            # 1. Try final URL after redirects (BEST - preserves original filename)
            final_url = response.url
            if final_url != url:
                native_filename = get_filename_from_url(final_url)
                if native_filename and native_filename not in ['si_public_file_download.cfm', 'ZyNET.exe', 'DLwait.htm']:
                    print(f"  → Found filename from redirect: {native_filename}")
            
            # 2. Try Content-Disposition header
            if not native_filename or native_filename in ['si_public_file_download.cfm', 'ZyNET.exe', 'DLwait.htm']:
                content_disposition = response.headers.get('Content-Disposition', '')
                if content_disposition:
                    matches = re.findall(r'filename[^;=\n]*=["\']?([^"\';\n]+)', content_disposition)
                    if matches:
                        native_filename = matches[0].strip()
                        print(f"  → Found filename in headers: {native_filename}")
            
            # 3. Try original URL
            if not native_filename or native_filename in ['si_public_file_download.cfm', 'ZyNET.exe', 'DLwait.htm']:
                native_filename = get_filename_from_url(url)
            
            # 4. For NEPIS, ensure we have the right extension based on content
            if not native_filename or native_filename in ['si_public_file_download.cfm', 'ZyNET.exe', 'DLwait.htm']:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
                # Determine extension from Content-Type
                if 'pdf' in content_type:
                    ext = '.pdf'
                elif 'powerpoint' in content_type or 'presentation' in content_type:
                    ext = '.pptx'
                elif 'msword' in content_type:
                    ext = '.docx'
                elif 'excel' in content_type or 'spreadsheet' in content_type:
                    ext = '.xlsx'
                else:
                    ext = '.bin'
                
                native_filename = f"download_{timestamp}{ext}"
                print(f"  → Generated unique filename: {native_filename}")
        
        if not native_filename:
            print(f"  ✗ No filename could be determined for URL: {url}")
            return False, None
        
        output_path = os.path.join(output_dir, native_filename)
        
        # Save the file
        with open(output_path, 'wb') as f:
            # Read first chunk to check if it's actually a PDF
            first_chunk = True
            for chunk in response.iter_content(chunk_size=8192):
                if first_chunk:
                    # Check if this is actually a PDF (should start with %PDF)
                    if chunk[:4] != b'%PDF':
                        print(f"  ⚠ WARNING: File doesn't start with PDF header!")
                        print(f"  → First 200 bytes: {chunk[:200]}")
                        # Check if it's HTML (popup page)
                        if b'<html' in chunk.lower() or b'<!doctype' in chunk.lower():
                            print(f"  ✗ ERROR: Got HTML instead of PDF (probably the popup page)")
                            # Don't save the file
                            f.close()
                            os.remove(output_path)
                            return False, None
                    first_chunk = False
                f.write(chunk)
        
        print(f"  ✓ Saved to: {output_path}")
        return True, native_filename
        
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error downloading {url}: {e}")
        return False, None
    except Exception as e:
        print(f"  ✗ Error processing {url}: {e}")
        return False, None

def process_csv(csv_file, url_columns=None, title_column='title', output_dir='downloads', max_rows=None):
    """
    Process CSV file and download all files from multiple URL columns.
    
    Args:
        csv_file: Path to CSV file
        url_columns: List of column names containing URLs (if None, auto-detects columns starting with 'download_url')
        title_column: Name of column containing title for filename prefix
        output_dir: Directory to save downloaded files
        max_rows: Maximum number of rows to process (for testing)
    """
    successful = 0
    failed = 0
    
    # Track filename usage across all downloads
    filename_counter = defaultdict(int)  # Track how many times each native filename is used
    filename_usage = {}  # Map (native_filename) -> list of (row_num, url_index, title_prefix)
    
    try:
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(csv_file, 'r', encoding=encoding) as f:
                    file_content = f.read()
                print(f"Successfully opened CSV with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            print("Error: Could not decode CSV file with any common encoding")
            return
        
        # First pass: collect all downloads to determine filename conflicts
        from io import StringIO
        f = StringIO(file_content)
        
        all_downloads = []  # List of (row_num, row_data, url_column, url, title_prefix)
        
        with f:
            reader = csv.DictReader(f)
            
            # Auto-detect URL columns if not specified
            if url_columns is None:
                url_columns = [col for col in reader.fieldnames if col.lower().startswith('download_url')]
                if not url_columns:
                    print("Error: No columns starting with 'download_url' found")
                    print(f"Available columns: {reader.fieldnames}")
                    return
                print(f"Auto-detected URL columns: {url_columns}")
            
            # Check if URL columns exist
            missing_cols = [col for col in url_columns if col not in reader.fieldnames]
            if missing_cols:
                print(f"Error: Columns not found in CSV: {missing_cols}")
                print(f"Available columns: {reader.fieldnames}")
                return
            
            # Check if title column exists
            if title_column not in reader.fieldnames:
                print(f"Warning: '{title_column}' column not found. Will not use title prefixes.")
                title_column = None
            
            # First pass: collect all downloads and get native filenames
            print("\n" + "="*60)
            print("PASS 1: Collecting download information...")
            print("="*60)
            
            for row_num, row in enumerate(reader, start=2):
                # Check if we've hit the max_rows limit
                if max_rows and (row_num - 1) >= max_rows:
                    print(f"Reached max_rows limit ({max_rows}). Stopping.")
                    break
                
                # Get title prefix if available
                title = row.get(title_column, '').strip() if title_column else ''
                title_prefix = extract_title_prefix(title)
                
                # Process each URL column
                for url_col in url_columns:
                    url = row[url_col].strip()
                    
                    if url:
                        all_downloads.append((row_num, row, url_col, url, title_prefix))
        
        print(f"\nFound {len(all_downloads)} total downloads to process")
        
        # Second pass: Download files with appropriate naming
        print("\n" + "="*60)
        print("PASS 2: Downloading files...")
        print("="*60)
        
        for download_info in all_downloads:
            row_num, row, url_col, url, title_prefix = download_info
            
            # Count URLs in this row
            urls_in_row = sum(1 for _, r, _, _, _ in all_downloads if r == row)
            multiple_urls = urls_in_row > 1
            
            print(f"\n{'='*60}")
            print(f"Processing row {row_num}, column '{url_col}':")
            print(f"URL: {url[:100]}..." if len(url) > 100 else f"URL: {url}")
            if title_prefix:
                print(f"Title prefix: {title_prefix}")
            
            # Download and get native filename
            success, native_filename = download(url, None, output_dir)
            
            if not success or not native_filename:
                failed += 1
                continue
            
            # Track filename usage
            filename_counter[native_filename] += 1
            if native_filename not in filename_usage:
                filename_usage[native_filename] = []
            filename_usage[native_filename].append((row_num, url_col, title_prefix))
            
            # Determine final filename
            final_filename = native_filename
            
            # Check if we need to rename
            needs_prefix = multiple_urls or filename_counter[native_filename] > 1
            
            if needs_prefix and title_prefix:
                # Split filename into name and extension
                name_parts = os.path.splitext(native_filename)
                base_name = name_parts[0]
                extension = name_parts[1]
                
                # Add letter suffix if this is a duplicate
                letter_suffix = ''
                if filename_counter[native_filename] > 1:
                    # Determine which occurrence this is (a, b, c, etc.)
                    occurrence_index = filename_counter[native_filename] - 1
                    letter_suffix = '_' + chr(ord('a') + occurrence_index)
                
                # Construct new filename
                final_filename = f"{title_prefix}_{base_name}{letter_suffix}{extension}"
            elif filename_counter[native_filename] > 1 and not title_prefix:
                # If no title prefix but still a duplicate, add letter suffix
                name_parts = os.path.splitext(native_filename)
                base_name = name_parts[0]
                extension = name_parts[1]
                occurrence_index = filename_counter[native_filename] - 1
                letter_suffix = chr(ord('a') + occurrence_index)
                final_filename = f"{base_name}_{letter_suffix}{extension}"
            
            # Rename if necessary
            if final_filename != native_filename:
                # Sanitize the final filename to ensure it's valid
                final_filename = sanitize_filename(final_filename)
                
                old_path = os.path.join(output_dir, native_filename)
                new_path = os.path.join(output_dir, final_filename)
                
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    print(f"  → Renamed to: {final_filename}")
            
            successful += 1
            
            # Be respectful to the server
            time.sleep(1)
        
        print(f"\n{'='*60}")
        print(f"Complete! {successful} successful, {failed} failed")
        
        # Print filename conflict report
        duplicates = {k: v for k, v in filename_usage.items() if len(v) > 1}
        if duplicates:
            print(f"\n{'='*60}")
            print("Filename conflict report:")
            print("="*60)
            for native_fn, usages in duplicates.items():
                print(f"\n'{native_fn}' used {len(usages)} times:")
                for row_num, url_col, title_prefix in usages:
                    print(f"  - Row {row_num}, column '{url_col}', prefix: '{title_prefix}'")
        
    except FileNotFoundError:
        print(f"Error: Could not find file '{csv_file}'")
    except Exception as e:
        print(f"Error processing CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    # Example usage:
    # Your CSV should have columns like: url, url1, url2, etc. (up to 31)
    # And a 'title' column for generating filename prefixes
    
    csv_file = r'C:\Python313\temp_csv_epa_dls\epa_all_types_pages_1251_to_2000_dl.csv'  # Change to your CSV filename
    
    # Install required package if not already installed:
    # pip install beautifulsoup4
    
    process_csv(
        csv_file=csv_file,
        url_columns=None,          # Auto-detect columns starting with 'url', or specify list like ['url', 'url1', 'url2']
        title_column='title',      # Column containing record title for filename prefix
        output_dir=r'D:\data_archives_2025\EPA_SI\full5',     # Use raw string (r'') for Windows paths
        max_rows=None              # Process all rows (set to a number for testing)
    )