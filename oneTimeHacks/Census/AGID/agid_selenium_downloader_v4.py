"""
AGID Data Downloader using Selenium - V4
Handles cycling through:
- Multiple batches of 50 checkboxes per data element category
-2 geographies per batch (up to V3 was 5 geographies)
- All data element categories from CSV
- Renames downloaded files with structured naming convention
- Creates lookup table CSV
"""

import time
import logging
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import os
import glob

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class AGIDSeleniumDownloader:
    def __init__(self, download_dir):
        self.download_dir = os.path.abspath(download_dir)
        os.makedirs(self.download_dir, exist_ok=True)

        # Configure Firefox to download files to specific directory
        options = webdriver.FirefoxOptions()
        options.set_preference("browser.download.folderList", 2)
        options.set_preference("browser.download.dir", self.download_dir)
        options.set_preference("browser.download.useDownloadDir", True)
        options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv,application/csv")

        self.driver = webdriver.Firefox(options=options)
        self.wait = WebDriverWait(self.driver, 20)

        # Geography code mapping
        self.geo_codes = {
            'All States Total': 'States',
            'All U.S. Totals': 'USA',
        }

    def create_abbreviation(self, text, max_length=10):
        """Create abbreviation from category text"""
        # Remove common words
        common_words = ['by', 'the', 'and', 'or', 'of', 'to', 'in', 'a', 'an']
        words = [w for w in text.split() if w.lower() not in common_words]

        # Take first letter of each word, capitalize
        abbrev = ''.join([w[0].upper() for w in words if w])

        # If too long, just take first max_length chars
        return abbrev[:max_length]

    def clean_original_filename(self, filename):
        """Clean up AGID's original filename
        - Remove all spaces
        - Strip timestamp pattern like _12_45_54 PM or _12_45_54 AM from the end

        Example: 'Explorer_Data_Title III_12-24-2025-12_45_54 PM.csv'
        Returns: 'Explorer_Data_TitleIII_12-24-2025.csv'
        """
        import re

        # Remove .csv extension temporarily
        name_without_ext = filename.replace('.csv', '')

        # Remove the time pattern at the end: _hh_mm_ss AM/PM
        # Pattern matches: _12_45_54 PM or _12_45_54 AM at the end
        name_without_ext = re.sub(r'[-_]\d{1,2}_\d{2}_\d{2}\s*(AM|PM)$', '', name_without_ext, flags=re.IGNORECASE)

        # Remove all spaces
        name_without_ext = name_without_ext.replace(' ', '')

        # Add .csv back
        return name_without_ext + '.csv'

    def wait_for_download(self, timeout=60, initial_files=None):
        """Wait for a new CSV file to appear in download directory

        Args:
            timeout: Maximum seconds to wait
            initial_files: Dict of {filepath: mtime} captured before clicking Export
        """
        start_time = time.time()

        # Use provided snapshot (captured before Export click)
        if initial_files is None:
            initial_csv_files = {}
            for f in glob.glob(os.path.join(self.download_dir, "*.csv")):
                initial_csv_files[f] = os.path.getmtime(f)
        else:
            initial_csv_files = initial_files

        logging.info(f"  Monitoring download folder: {self.download_dir}")
        logging.info(f"  Initial CSV files: {len(initial_csv_files)}")

        iteration = 0
        last_count = len(initial_csv_files)

        while time.time() - start_time < timeout:
            iteration += 1
            time.sleep(0.5)

            # Get current CSV files
            current_csv_files = glob.glob(os.path.join(self.download_dir, "*.csv"))

            # Log progress every 10 iterations
            if iteration % 20 == 0:
                logging.info(f"  Still waiting... ({int(time.time() - start_time)}s elapsed, {len(current_csv_files)} CSV files)")

            # Check for new files (not in initial set)
            for csv_file in current_csv_files:
                if csv_file not in initial_csv_files:
                    # New file found!
                    file_size = os.path.getsize(csv_file)
                    if file_size > 0:
                        # Wait a bit and verify size is stable
                        time.sleep(1)
                        new_size = os.path.getsize(csv_file)
                        if new_size == file_size:
                            logging.info(f"  ✓ New file detected: {os.path.basename(csv_file)} ({file_size} bytes)")
                            return csv_file
                        else:
                            logging.info(f"  File still growing: {os.path.basename(csv_file)} ({file_size} -> {new_size})")
                            initial_csv_files[csv_file] = None  # Mark as seen but growing

                # Check for modified files (file was updated)
                elif csv_file in initial_csv_files and initial_csv_files[csv_file] is not None:
                    current_mtime = os.path.getmtime(csv_file)
                    if current_mtime > initial_csv_files[csv_file]:
                        file_size = os.path.getsize(csv_file)
                        logging.info(f"  ✓ File updated: {os.path.basename(csv_file)} ({file_size} bytes)")
                        # Verify stable
                        time.sleep(1)
                        if os.path.getsize(csv_file) == file_size:
                            return csv_file

        logging.warning(f"  Download timeout after {timeout}s")
        final_count = len(glob.glob(os.path.join(self.download_dir, "*.csv")))
        logging.warning(f"  Final CSV count: {final_count} (started with {len(initial_csv_files)})")

        # If count increased, return the most recent file
        if final_count > len(initial_csv_files):
            all_csv = glob.glob(os.path.join(self.download_dir, "*.csv"))
            newest = max(all_csv, key=os.path.getmtime)
            logging.warning(f"  Returning newest file as fallback: {os.path.basename(newest)}")
            return newest

        return None

    def download_from_csv(self, csv_path, test_mode=False):
        """
        Read data element selections from CSV and download each dataset
        Each CSV row can generate multiple downloads based on:
        - Number of checkbox batches (ceiling of total_checkboxes / 50)
        - 2 geographies per batch

        Args:
            csv_path: Path to CSV file with data element selections
            test_mode: If True, only process the first row (default: False)
        """
        # Read the CSV
        df = pd.read_csv(csv_path, encoding='utf-8-sig')
        logging.info(f"Loaded {len(df)} data element selections")

        if test_mode:
            df = df.head(1)
            logging.info("*** TEST MODE: Processing only the first row ***")

        # Define the 2 geography categories
        geography_categories = [
            'All States Total',
            'All U.S. Totals',
           
        ]

        results = []
        lookup_table = []

        # LOAD PAGE ONCE AT START
        logging.info("\n" + "="*60)
        logging.info("INITIAL SETUP - Loading Data Explorer")
        logging.info("="*60)
        logging.info("Loading data explorer...")
        self.driver.get("https://agid.acl.gov/data-explorer")
        time.sleep(5)

        # MANUAL DATASET SELECTION AT START
        logging.info("\n" + "="*60)
        logging.info("SELECT DATASET")
        logging.info("="*60)
        logging.info("Which dataset do you want to use?")
        logging.info("  1. Title III (State Programs)")
        logging.info("  2. Title VI (Native Americans)")
        logging.info("  3. Title VII (Vulnerable Elder Rights)")
        logging.info("="*60)
        dataset_choice = input("Enter choice (1/2/3): ").strip()

        if dataset_choice == '1':
            dataset_label = 'Title III'
        elif dataset_choice == '2':
            dataset_label = 'Title VI'
        elif dataset_choice == '3':
            dataset_label = 'Title VII'
        else:
            logging.warning(f"Invalid choice, defaulting to Title III")
            dataset_label = 'Title III'

        logging.info(f"✓ Using dataset: {dataset_label}")

        # Track current dataset/years selection
        current_dataset = None
        current_years = None

        # Flag to stop processing entirely
        stop_all_processing = False

        for idx, row in df.iterrows():
            if stop_all_processing:
                logging.info("Skipping remaining rows due to stop request")
                break

            logging.info(f"\n{'='*60}")
            logging.info(f"Processing CSV row {idx + 1}/{len(df)}")
            logging.info('='*60)

            # Extract data element path from CSV (for reference only)
            csv_categories = []
            for i in range(1, 6):
                cat = row.get(f'Data Elements Category{i}' if i == 1 else f'Data Elements Category {i}', '')
                if pd.notna(cat) and str(cat).strip():
                    csv_categories.append(str(cat).strip())

            # Check if we need to select dataset (first time only)
            if current_dataset != dataset_label:
                logging.info(f"\n{'='*60}")
                logging.info(f"Setting up dataset: {dataset_label}")
                logging.info("="*60)

                # Select new dataset
                logging.info(f"Selecting {dataset_label}...")
                dataset_btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'dataSet-filter-nav')))
                dataset_btn.click()
                time.sleep(2)

                labels = self.driver.find_elements(By.TAG_NAME, 'label')
                for label in labels:
                    if dataset_label in label.text:
                        label.click()
                        break
                time.sleep(2)
                current_dataset = dataset_label

                # Select ALL years (only if dataset changed)
                logging.info("Selecting all years...")
                years_btn = self.wait.until(EC.element_to_be_clickable((By.ID, 'years-filter-nav')))
                years_btn.click()
                time.sleep(2)

                year_labels = self.driver.find_elements(By.TAG_NAME, 'label')
                for label in year_labels:
                    if 'Select All' in label.text or 'select all' in label.text.lower():
                        label.click()
                        logging.info("  Selected: Select All years")
                        break
                time.sleep(2)
                current_years = "All"
            else:
                logging.info(f"Using existing selection: {current_dataset}, {current_years}")

            # MANUAL SETUP: Navigate to data elements ONCE
            logging.info("\n" + "="*60)
            logging.info("INITIAL SETUP FOR THIS CSV ROW")
            logging.info("="*60)
            logging.info(f"Navigate to the data element checkboxes:")
            if csv_categories:
                logging.info(f"   Suggested path from CSV: {' > '.join(csv_categories)}")
                logging.info(f"   (Or navigate to any other path you prefer)")
            logging.info("\nIMPORTANT: Do NOT select any checkboxes yet!")
            logging.info("Just expand to where you can SEE all the checkboxes")
            logging.info("="*60)

            # Ask user to enter the actual path they navigated to
            print("\nEnter the category path you navigated to (separate levels with ' > '):")
            print("Example: Older Adults Characteristics > Gender > Services")
            actual_path = input("Path: ").strip()

            # Parse the actual categories from user input
            categories = [cat.strip() for cat in actual_path.split('>') if cat.strip()]

            if not categories:
                logging.warning("No path entered - using CSV path as fallback")
                categories = csv_categories

            # Create abbreviations from ACTUAL path
            cat_abbrevs = [self.create_abbreviation(cat) for cat in categories]

            # Ask for optional subcategory abbreviation
            print("\nEnter subcategory abbreviation (e.g., 'F' for Female, 'M' for Male):")
            print("Or press Enter to skip if no subcategory")
            subcategory_abbrev = input("Subcategory: ").strip()

            logging.info(f"✓ Using path: {' > '.join(categories)}")
            logging.info(f"✓ Abbreviations: {cat_abbrevs}")
            if subcategory_abbrev:
                logging.info(f"✓ Subcategory: {subcategory_abbrev}\n")
            else:
                logging.info(f"✓ No subcategory\n")

            # Count total available checkboxes (initial scan to determine batch plan)
            time.sleep(2)
            try:
                all_checkboxes = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"]')

                # Filter to valid data element checkboxes (just to count)
                valid_checkbox_count = 0
                for checkbox in all_checkboxes:
                    try:
                        if not checkbox.is_displayed() or not checkbox.is_enabled():
                            continue
                        if checkbox.is_selected():
                            continue
                        checkbox_id = checkbox.get_attribute('id')
                        if not checkbox_id:
                            continue
                        label = self.driver.find_element(By.CSS_SELECTOR, f'label[for="{checkbox_id}"]')
                        element_text = label.text.strip()
                        skip_items = ['Select All', 'Data Set', 'Years', 'Geography', 'Data Elements', '']
                        if element_text not in skip_items:
                            valid_checkbox_count += 1
                    except:
                        continue

                total_checkboxes = valid_checkbox_count
                logging.info(f"Found {total_checkboxes} valid data element checkboxes")

                if total_checkboxes == 0:
                    logging.warning("No checkboxes found - skipping this CSV row")
                    continue

                # Calculate number of batches (50 checkboxes per batch)
                batch_size = 50
                num_batches = (total_checkboxes + batch_size - 1) // batch_size

                logging.info(f"\n{'='*60}")
                logging.info(f"BATCH PLAN:")
                logging.info(f"  Total checkboxes: {total_checkboxes}")
                logging.info(f"  Batch size: {batch_size}")
                logging.info(f"  Number of batches: {num_batches}")
                logging.info(f"  Total downloads for this row: {num_batches * len(geography_categories)}")
                logging.info('='*60)

                # Initialize next_choice for loop control
                next_choice = '1'

                # NOW LOOP THROUGH BATCHES OF 50 CHECKBOXES
                for batch_num in range(num_batches):
                    logging.info(f"\n{'='*60}")
                    logging.info(f"BATCH {batch_num + 1}/{num_batches}")
                    logging.info('='*60)

                    # RE-FIND CHECKBOXES FOR THIS BATCH (avoid stale elements)
                    logging.info("Re-scanning checkboxes for this batch...")
                    time.sleep(1)

                    all_checkboxes = self.driver.find_elements(By.CSS_SELECTOR, 'input[type="checkbox"]')
                    valid_checkboxes = []

                    for checkbox in all_checkboxes:
                        try:
                            if not checkbox.is_displayed() or not checkbox.is_enabled():
                                continue
                            if checkbox.is_selected():
                                continue
                            checkbox_id = checkbox.get_attribute('id')
                            if not checkbox_id:
                                continue
                            label = self.driver.find_element(By.CSS_SELECTOR, f'label[for="{checkbox_id}"]')
                            element_text = label.text.strip()
                            skip_items = ['Select All', 'Data Set', 'Years', 'Geography', 'Data Elements', '']
                            if element_text not in skip_items:
                                valid_checkboxes.append((checkbox, element_text))
                        except:
                            continue

                    # Calculate which checkboxes to select in this batch
                    start_idx = batch_num * batch_size
                    end_idx = min(start_idx + batch_size, total_checkboxes)
                    batch_checkboxes = valid_checkboxes[start_idx:end_idx]

                    logging.info(f"Re-found {len(valid_checkboxes)} unselected checkboxes")
                    logging.info(f"Selecting checkboxes {start_idx + 1} to {end_idx} (batch {batch_num + 1})...")

                    # Select this batch of checkboxes
                    selected_count = 0
                    selected_checkbox_labels = []
                    for checkbox, element_text in batch_checkboxes:
                        try:
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", checkbox)
                            time.sleep(0.2)
                            checkbox.click()
                            selected_count += 1
                            selected_checkbox_labels.append(element_text)
                            logging.info(f"    [{selected_count}] Selected: {element_text}")
                            time.sleep(0.3)
                        except Exception as e:
                            logging.warning(f"    Failed to select '{element_text}': {e}")
                            continue

                    logging.info(f"  ✓ Batch {batch_num + 1}: Selected {selected_count} checkboxes")

                    # NOW LOOP THROUGH 5 GEOGRAPHIES FOR THIS BATCH
                    for geo_idx, geo_category in enumerate(geography_categories, 1):
                        logging.info(f"\n{'='*40}")
                        logging.info(f"Batch {batch_num + 1}/{num_batches} - Geography {geo_idx}/{len(geography_categories)}: {geo_category}")
                        logging.info('='*40)

                        try:
                            # MANUAL: Select this geography
                            logging.info("\n" + "="*60)
                            logging.info("MANUAL GEOGRAPHY SELECTION")
                            logging.info("="*60)
                            logging.info(f"Please select ONLY THIS geography:")
                            logging.info(f"  1. Click 'Geography' button (if not already open)")
                            logging.info(f"  2. Click '{geo_category}' → Click 'Select All'")
                            logging.info(f"\nNOTE: Your {selected_count} data element checkboxes are selected!")
                            logging.info("="*60)
                            input(f"\nPress Enter when you've selected '{geo_category}'...")
                            logging.info(f"✓ Geography '{geo_category}' confirmed!\n")

                            # Click Fetch Data
                            logging.info("Clicking Fetch Data...")
                            fetch_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Fetch Data')]")))
                            fetch_btn.click()
                            time.sleep(5)

                            # Wait for table to appear
                            logging.info("Waiting for table to load...")
                            time.sleep(3)

                            # Click Export to CSV
                            logging.info("Clicking Export to CSV...")

                            # Take snapshot BEFORE clicking export
                            initial_csv_files = {}
                            for f in glob.glob(os.path.join(self.download_dir, "*.csv")):
                                initial_csv_files[f] = os.path.getmtime(f)

                            export_btn = self.wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export')]")))
                            export_btn.click()

                            # Brief pause to let browser start download
                            time.sleep(1)

                            # Wait for download to complete
                            logging.info("Waiting for file to download...")
                            downloaded_file = self.wait_for_download(timeout=60, initial_files=initial_csv_files)

                            if downloaded_file:
                                # Get the original filename and clean it
                                original_filename = os.path.basename(downloaded_file)
                                cleaned_filename = self.clean_original_filename(original_filename)

                                # Create filename: {GeoCode}_{CategoryAbbrevs}_{BatchNum}_{SubCategory}_{cleaned_filename}
                                geo_code = self.geo_codes[geo_category]
                                cat_codes = '_'.join(cat_abbrevs[:2])  # Use first 2 category abbrevs

                                # Build filename parts (prefix)
                                parts = [geo_code, cat_codes]

                                # Add batch number only if more than 1 batch
                                if num_batches > 1:
                                    parts.append(str(batch_num + 1))

                                # Add subcategory if provided
                                if subcategory_abbrev:
                                    parts.append(subcategory_abbrev)

                                # Combine prefix with cleaned original filename
                                prefix = '_'.join(parts)
                                new_filename = f"{prefix}_{cleaned_filename}"
                                new_filepath = os.path.join(self.download_dir, new_filename)

                                # Rename the file
                                os.rename(downloaded_file, new_filepath)
                                logging.info(f"✓ Downloaded and renamed to: {new_filename}")

                                # Add to lookup table
                                lookup_table.append({
                                    'filename': new_filename,
                                    'csv_row': idx + 1,
                                    'batch': batch_num + 1,
                                    'geography_code': geo_code,
                                    'geography_full': geo_category,
                                    'dataset': dataset_label,
                                    'category_path': ' > '.join(categories),
                                    'subcategory': subcategory_abbrev if subcategory_abbrev else '',
                                    'checkboxes_selected': selected_count,
                                    'checkbox_range': f"{start_idx + 1}-{end_idx}",
                                    'checkbox_labels': ', '.join(selected_checkbox_labels)
                                })

                                results.append({
                                    'csv_row': idx + 1,
                                    'batch': batch_num + 1,
                                    'geography': geo_category,
                                    'dataset': dataset_label,
                                    'checkboxes_selected': selected_count,
                                    'filename': new_filename,
                                    'status': 'success'
                                })
                            else:
                                logging.warning("Download timeout - file not found")
                                results.append({
                                    'csv_row': idx + 1,
                                    'batch': batch_num + 1,
                                    'geography': geo_category,
                                    'dataset': dataset_label,
                                    'checkboxes_selected': selected_count,
                                    'filename': None,
                                    'status': 'failed: download timeout'
                                })

                        except Exception as e:
                            logging.error(f"Error: {e}")
                            results.append({
                                'csv_row': idx + 1,
                                'batch': batch_num + 1,
                                'geography': geo_category,
                                'dataset': dataset_label,
                                'checkboxes_selected': selected_count,
                                'filename': None,
                                'status': f'failed: {str(e)}'
                            })

                        # Rate limiting between downloads
                        time.sleep(2)

                        # AFTER EACH DOWNLOAD - prompt for next action
                        logging.info(f"\n{'='*60}")
                        logging.info(f"DOWNLOAD COMPLETE")
                        logging.info(f"  Batch {batch_num + 1}/{num_batches}, Geography {geo_idx}/{len(geography_categories)}")
                        logging.info('='*60)
                        logging.info("What next?")
                        logging.info("  1. Next geography (same batch)")
                        logging.info("  2. Next batch (clear data elements, select next 50)")
                        logging.info("  3. New data elements path")
                        logging.info("  4. Quit")
                        logging.info('='*60)
                        next_choice = input("Enter choice (1/2/3/4): ").strip()

                        if next_choice == '4':
                            logging.info("Quitting...")
                            stop_all_processing = True
                            break
                        elif next_choice == '3':
                            logging.info("Moving to new data elements path...")
                            # Break out of geography loop AND batch loop to get new path
                            break
                        elif next_choice == '2':
                            logging.info("Moving to next batch...")
                            # Break out of geography loop to move to next batch
                            break
                        # else next_choice == '1': continue to next geography

                    # Check if we need to exit batch loop (for options 3 or 4)
                    if stop_all_processing or next_choice == '3':
                        break

                    # After completing all geographies for this batch
                    if geo_idx == len(geography_categories):
                        logging.info(f"\n✓ All geographies complete for batch {batch_num + 1}")

                    # Clear data element selections before next batch
                    if next_choice == '2' and batch_num < num_batches - 1:
                        logging.info(f"\n{'='*60}")
                        logging.info(f"CLEAR DATA ELEMENTS FOR NEXT BATCH")
                        logging.info('='*60)
                        logging.info("Please click 'Clear Selections' under DATA ELEMENTS")
                        logging.info("Keep the same path expanded so script can select next 50")
                        logging.info('='*60)
                        input("Press Enter when ready...")
                        logging.info("✓ Ready for next batch")

                # Check if user wants new path (break out of CSV row loop too)
                if next_choice == '3':
                    logging.info(f"\n{'='*60}")
                    logging.info("NEW DATA ELEMENTS PATH")
                    logging.info('='*60)
                    logging.info("Please click 'Clear Selections' under DATA ELEMENTS")
                    logging.info("Then navigate to your new data elements path")
                    logging.info('='*60)
                    input("Press Enter when ready...")
                    continue  # Continue to next CSV row / prompt for new path

            except Exception as e:
                logging.error(f"Error processing CSV row {idx + 1}: {e}")
                continue

        # Save lookup table
        lookup_df = pd.DataFrame(lookup_table)
        lookup_path = os.path.join(self.download_dir, f"lookup_table_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        lookup_df.to_csv(lookup_path, index=False)

        logging.info(f"\n{'='*60}")
        logging.info(f"Download complete!")
        logging.info(f"CSV rows processed: {len(df)}")
        logging.info(f"Total download attempts: {len(results)}")
        logging.info(f"Successful downloads: {len([r for r in results if r['status'] == 'success'])}")
        logging.info(f"Failed downloads: {len([r for r in results if 'failed' in r['status']])}")
        logging.info(f"Lookup table saved to: {lookup_path}")
        logging.info('='*60)

    def close(self):
        input("\nPress Enter to close browser...")
        self.driver.quit()

if __name__ == "__main__":
    # Path to your data elements CSV
    csv_path = "C:\\main_njy\\agid_Data_Elements.csv"

    # OUTPUT DIRECTORY - Change this to where you want to save the files
    output_dir = "C:\\main_njy\\agid_downloads"

    # TEST MODE - Set to True to process only the first row
    TEST_MODE = False  # Change to False for full run

    downloader = AGIDSeleniumDownloader(output_dir)

    try:
        downloader.download_from_csv(csv_path, test_mode=TEST_MODE)
    finally:
        downloader.close()
