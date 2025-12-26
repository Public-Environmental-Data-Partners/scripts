import csv
import requests
import os
from urllib.parse import urlparse
from pathlib import Path
import re
import time
from collections import defaultdict

def get_filename_from_url(url):
    """Extract filename from URL"""
    parsed_url = urlparse(url)
    return os.path.basename(parsed_url.path)

def extract_nepis_dockey(url):
    """Extract the Dockey from any NEPIS URL format."""
    # Pattern 1: Dockey parameter
    dockey_match = re.search(r'Dockey=([A-Z0-9]+)(?:\.txt|\.pdf|\.PDF)?', url, re.IGNORECASE)
    if dockey_match:
        return dockey_match.group(1)
    
    # Pattern 2: In path like /P100GNGT.PDF or /P100GNGT.pdf
    path_match = re.search(r'/([A-Z0-9]{8,})\.(?:pdf|txt|PDF|TXT)', url, re.IGNORECASE)
    if path_match:
        return path_match.group(1)
    
    return None

def is_nepis_url(url):
    """Check if URL is from NEPIS"""
    return 'nepis.epa.gov' in url.lower()

def sanitize_filename(filename):
    """Remove invalid characters from filename"""
    invalid_chars = r'[<>:"/\\|?*]'
    sanitized = re.sub(invalid_chars, '_', filename)
    sanitized = sanitized.strip('. ')
    sanitized = re.sub(r'_+', '_', sanitized)
    return sanitized

def extract_title_prefix(title):
    """Extract first four non-filler words from title"""
    if not title:
        return ""
    
    filler_words = {'to', 'the', 'of', 'and', 'at', 'in', 'for', 'a', 'an'}
    words = re.findall(r'\w+', title.lower())
    meaningful_words = [w for w in words if w not in filler_words][:4]
    prefix = '_'.join(meaningful_words)
    return sanitize_filename(prefix)

def download(url, filename=None, output_dir='downloads'):
    """
    Download file from URL (NON-NEPIS only).
    Returns tuple: (success: bool, native_filename: str)
    """
    try:
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        print(f"\n  → Starting download process...")
        print(f"  → URL: {url[:100]}...")
        
        # Download the file
        print(f"  → Downloading...")
        response = requests.get(url, stream=True, timeout=60, allow_redirects=True)
        response.raise_for_status()
        print(f"  → Response received (Status: {response.status_code})")
        
        content_type = response.headers.get('Content-Type', '').lower()
        print(f"  → Content-Type: {content_type}")
        
        # Determine native filename
        native_filename = filename
        if not native_filename:
            # Try final URL after redirects
            final_url = response.url
            if final_url != url:
                native_filename = get_filename_from_url(final_url)
                if native_filename and native_filename not in ['si_public_file_download.cfm']:
                    print(f"  → Found filename from redirect: {native_filename}")
            
            # Try Content-Disposition header
            if not native_filename or native_filename in ['si_public_file_download.cfm']:
                content_disposition = response.headers.get('Content-Disposition', '')
                if content_disposition:
                    matches = re.findall(r'filename[^;=\n]*=["\']?([^"\';\n]+)', content_disposition)
                    if matches:
                        native_filename = matches[0].strip()
                        print(f"  → Found filename in headers: {native_filename}")
            
            # Try original URL
            if not native_filename or native_filename in ['si_public_file_download.cfm']:
                native_filename = get_filename_from_url(url)
            
            # Generate filename from content type
            if not native_filename or native_filename in ['si_public_file_download.cfm']:
                from datetime import datetime
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                
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
            print(f"  ✗ No filename could be determined")
            return False, None
        
        print(f"  → Saving as: {native_filename}")
        output_path = os.path.join(output_dir, native_filename)
        
        # Save the file with validation
        bytes_written = 0
        with open(output_path, 'wb') as f:
            first_chunk = True
            for chunk in response.iter_content(chunk_size=8192):
                if not chunk:
                    continue
                    
                if first_chunk:
                    print(f"  → First chunk: {len(chunk)} bytes")
                    
                    # Check if this is HTML (error page)
                    if b'<html' in chunk.lower() or b'<!doctype' in chunk.lower():
                        print(f"  ✗ ERROR: Received HTML instead of file")
                        print(f"  → Content preview: {chunk[:200]}")
                        f.close()
                        os.remove(output_path)
                        return False, None
                    
                    # Validate PDF files
                    if native_filename.lower().endswith('.pdf'):
                        if chunk[:4] != b'%PDF':
                            print(f"  ✗ ERROR: File doesn't start with PDF header")
                            print(f"  → First bytes: {chunk[:50]}")
                            if len(chunk) < 100:
                                print(f"  ✗ File too small to be valid PDF")
                                f.close()
                                os.remove(output_path)
                                return False, None
                    
                    first_chunk = False
                
                f.write(chunk)
                bytes_written += len(chunk)
        
        # Final validation
        if bytes_written < 100:
            print(f"  ✗ ERROR: Downloaded file too small ({bytes_written} bytes)")
            if os.path.exists(output_path):
                os.remove(output_path)
            return False, None
        
        # For PDFs, do a final check
        if native_filename.lower().endswith('.pdf'):
            with open(output_path, 'rb') as f:
                header = f.read(4)
                if header != b'%PDF':
                    print(f"  ✗ ERROR: Final PDF validation failed")
                    os.remove(output_path)
                    return False, None
        
        print(f"  ✓ Successfully saved {bytes_written:,} bytes")
        return True, native_filename
        
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error downloading: {e}")
        return False, None
    except Exception as e:
        print(f"  ✗ Error processing: {e}")
        import traceback
        traceback.print_exc()
        return False, None

def process_csv(csv_file, url_columns=None, title_column='title', output_dir='downloads', max_rows=None):
    """
    Process CSV file and download files from multiple URL columns.
    Separates NEPIS URLs into a separate file for manual/Selenium processing.
    """
    successful = 0
    failed = 0
    nepis_skipped = 0
    filename_counter = defaultdict(int)
    filename_usage = {}
    nepis_records = []
    
    try:
        print(f"\n{'='*60}")
        print(f"Starting CSV processing")
        print(f"CSV File: {csv_file}")
        print(f"Output Directory: {output_dir}")
        print(f"Max Rows: {max_rows if max_rows else 'All'}")
        print(f"{'='*60}\n")
        
        # Try different encodings
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        file_content = None
        
        for encoding in encodings:
            try:
                with open(csv_file, 'r', encoding=encoding) as f:
                    file_content = f.read()
                print(f"✓ Successfully opened CSV with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if file_content is None:
            print("Error: Could not decode CSV file")
            return
        
        from io import StringIO
        f = StringIO(file_content)
        
        all_downloads = []
        
        with f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            
            # Auto-detect URL columns
            if url_columns is None:
                url_columns = [col for col in fieldnames if 
                             col.lower().startswith('download_url') or 
                             (col.lower().startswith('url') and col.lower() != 'url_source')]
                if not url_columns:
                    print("Error: No URL columns found")
                    return
                print(f"✓ Auto-detected URL columns: {url_columns}")
            
            # Validate columns
            missing_cols = [col for col in url_columns if col not in fieldnames]
            if missing_cols:
                print(f"Error: Columns not found: {missing_cols}")
                return
            
            if title_column and title_column not in fieldnames:
                print(f"Warning: '{title_column}' column not found")
                title_column = None
            
            # Collect all downloads
            print("\n" + "="*60)
            print("Collecting downloads...")
            print("="*60)
            
            for row_num, row in enumerate(reader, start=2):
                if max_rows and (row_num - 1) > max_rows:
                    break
                
                title = row.get(title_column, '').strip() if title_column else ''
                title_prefix = extract_title_prefix(title)
                
                for url_col in url_columns:
                    url = row.get(url_col, '').strip()
                    if url:
                        all_downloads.append((row_num, row, url_col, url, title_prefix))
        
        print(f"\n✓ Found {len(all_downloads)} downloads to process\n")
        
        if len(all_downloads) == 0:
            print("ERROR: No URLs found!")
            return
        
        # Separate NEPIS from non-NEPIS
        nepis_downloads = [(r, row, c, u, t) for r, row, c, u, t in all_downloads if is_nepis_url(u)]
        non_nepis_downloads = [(r, row, c, u, t) for r, row, c, u, t in all_downloads if not is_nepis_url(u)]
        
        print(f"✓ Found {len(non_nepis_downloads)} NON-NEPIS downloads")
        print(f"✓ Found {len(nepis_downloads)} NEPIS downloads (will be saved to separate CSV)")
        
        # Process NEPIS downloads - save to CSV
        if nepis_downloads:
            nepis_csv_path = os.path.join(os.path.dirname(csv_file), 'nepis_manual_downloads.csv')
            print(f"\n→ Saving NEPIS downloads to: {nepis_csv_path}")
            
            with open(nepis_csv_path, 'w', newline='', encoding='utf-8') as nepis_file:
                nepis_writer = csv.writer(nepis_file)
                nepis_writer.writerow(['row_number', 'dockey', 'original_url', 'search_url', 'title', 'column'])
                
                for row_num, row, url_col, url, title_prefix in nepis_downloads:
                    dockey = extract_nepis_dockey(url)
                    title = row.get(title_column, '').strip() if title_column else ''
                    search_url = f"https://nepis.epa.gov/Exe/ZyNET.exe/QBLK1F2U.txt?ZyActionD=ZyDocument&Client=EPA&Index=1991%20Thru%201994%7C2011%20Thru%202015%7C2006%20Thru%202010%7C2016%20Thru%202020%7C1995%20Thru%201999%7C1976%20Thru%201980%7C1981%20Thru%201985%7C2000%20Thru%202005%7C1986%20Thru%201990%7CPrior%20to%201976&Docs=&Query={dockey}&Time=&EndTime=&SearchMethod=2&TocRestrict=n&Toc=&TocEntry=&QField=pubnumber%5E%22{dockey}%22&QFieldYear=&QFieldMonth=&QFieldDay=&UseQField=pubnumber&IntQFieldOp=1&ExtQFieldOp=1&XmlQuery="
                    nepis_writer.writerow([row_num, dockey or 'N/A', url, search_url, title, url_col])
            
            print(f"✓ Saved {len(nepis_downloads)} NEPIS records to CSV")
            print(f"  → You can use Selenium to automate these downloads")
            print(f"  → Or manually download from the search_url column")
        
        # Download non-NEPIS files
        print("\n" + "="*60)
        print("Downloading NON-NEPIS files...")
        print("="*60)
        
        for idx, download_info in enumerate(non_nepis_downloads, 1):
            row_num, row, url_col, url, title_prefix = download_info
            
            urls_in_row = sum(1 for _, r, _, _, _ in non_nepis_downloads if r == row)
            multiple_urls = urls_in_row > 1
            
            print(f"\n{'='*60}")
            print(f"[{idx}/{len(non_nepis_downloads)}] Row {row_num} - Column: {url_col}")
            print(f"{'='*60}")
            
            success, native_filename = download(url, None, output_dir)
            
            if not success or not native_filename:
                failed += 1
                continue
            
            filename_counter[native_filename] += 1
            if native_filename not in filename_usage:
                filename_usage[native_filename] = []
            filename_usage[native_filename].append((row_num, url_col, title_prefix))
            
            # Determine final filename
            final_filename = native_filename
            needs_prefix = multiple_urls or filename_counter[native_filename] > 1
            
            if needs_prefix and title_prefix:
                name_parts = os.path.splitext(native_filename)
                base_name = name_parts[0]
                extension = name_parts[1]
                
                letter_suffix = ''
                if filename_counter[native_filename] > 1:
                    occurrence_index = filename_counter[native_filename] - 1
                    letter_suffix = '_' + chr(ord('a') + occurrence_index)
                
                final_filename = f"{title_prefix}_{base_name}{letter_suffix}{extension}"
            elif filename_counter[native_filename] > 1 and not title_prefix:
                name_parts = os.path.splitext(native_filename)
                base_name = name_parts[0]
                extension = name_parts[1]
                occurrence_index = filename_counter[native_filename] - 1
                letter_suffix = chr(ord('a') + occurrence_index)
                final_filename = f"{base_name}_{letter_suffix}{extension}"
            
            # Rename if necessary
            if final_filename != native_filename:
                final_filename = sanitize_filename(final_filename)
                old_path = os.path.join(output_dir, native_filename)
                new_path = os.path.join(output_dir, final_filename)
                
                if os.path.exists(old_path):
                    os.rename(old_path, new_path)
                    print(f"  → Renamed to: {final_filename}")
            
            successful += 1
            time.sleep(1)
        
        print(f"\n{'='*60}")
        print(f"COMPLETE!")
        print(f"NON-NEPIS Successful: {successful}")
        print(f"NON-NEPIS Failed: {failed}")
        print(f"NEPIS Skipped (saved to CSV): {len(nepis_downloads)}")
        print(f"{'='*60}")
        
        if nepis_downloads:
            print(f"\n⚠️  NEPIS files require manual download or Selenium automation")
            print(f"📄 See: nepis_manual_downloads.csv")
        
    except FileNotFoundError:
        print(f"✗ Error: Could not find file '{csv_file}'")
    except Exception as e:
        print(f"✗ Error processing CSV: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    csv_file = r'C:\Python313\temp_csv_epa_dls\epa_all_types_pages_nepis_dl.csv'
    
    process_csv(
        csv_file=csv_file,
        url_columns=None,  # Auto-detect all download_url_* columns
        title_column='title',
        output_dir=r'D:\data_archives_2025\EPA_SI\test_nepis',
        max_rows=None  # Process all rows
    )