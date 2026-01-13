"""
Simple Dataverse File Uploader (WITH ARCHIVE FILES)
Uploads all files from a directory to an existing Dataverse dataset
Files are added to the draft version - you save/submit manually in the GUI

NOTE: This version INCLUDES .zip, .gz, .7z and other archive files.
      Dataverse may auto-extract these archives upon upload.

Requirements:
    pip install requests

Usage:
    # Option 1: Edit config in script and run
    python dataverse_upload_with_zip.py

    # Option 2: Specify via command line
    python dataverse_upload_with_zip.py --url https://dataverse.harvard.edu --token YOUR_TOKEN --pid doi:10.7910/DVN/XXXXX --dir C:/path/to/files
"""

import requests
import os
import sys
from pathlib import Path

# ============================================================================
# CONFIGURATION - Edit these values OR use command line arguments
# ============================================================================

# Dataverse API settings
DATAVERSE_URL = "https://dataverse.harvard.edu"  # Your Dataverse installation URL
API_TOKEN = "YOUR_API_TOKEN_HERE"                # Your API token

# Dataset identifier (DOI or persistent ID)
DATASET_PID = "doi:10.7910/DVN/XXXXX"            # e.g., "doi:10.7910/DVN/12345"

# Directory containing files to upload
UPLOAD_DIRECTORY = "C:/path/to/your/files"

# ============================================================================


def upload_file_to_dataverse(file_path, dataverse_url, api_token, dataset_pid):
    """
    Upload a single file to a Dataverse dataset (draft version)

    Args:
        file_path: Path to the file to upload
        dataverse_url: Base URL of Dataverse installation
        api_token: API authentication token
        dataset_pid: Persistent identifier of the dataset

    Returns:
        bool: True if successful, False otherwise
    """
    url = f"{dataverse_url}/api/datasets/:persistentId/add"
    filename = os.path.basename(file_path)

    try:
        with open(file_path, 'rb') as f:
            files = {'file': (filename, f)}
            params = {'persistentId': dataset_pid}
            headers = {'X-Dataverse-key': api_token}

            print(f"Uploading: {filename}...", end=' ')

            response = requests.post(
                url,
                files=files,
                params=params,
                headers=headers,
                timeout=300
            )

            if response.status_code == 200:
                print("✓ Success")
                return True
            else:
                print(f"✗ Failed (HTTP {response.status_code})")
                print(f"  Error: {response.text}")
                return False

    except FileNotFoundError:
        print(f"✗ File not found: {file_path}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False


def parse_args():
    """Parse command line arguments"""
    args = {
        'url': DATAVERSE_URL,
        'token': API_TOKEN,
        'pid': DATASET_PID,
        'dir': UPLOAD_DIRECTORY
    }

    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]

        if arg in ['--url', '-u']:
            args['url'] = sys.argv[i + 1]
            i += 2
        elif arg in ['--token', '-t']:
            args['token'] = sys.argv[i + 1]
            i += 2
        elif arg in ['--pid', '-p']:
            args['pid'] = sys.argv[i + 1]
            i += 2
        elif arg in ['--dir', '-d']:
            args['dir'] = sys.argv[i + 1]
            i += 2
        elif arg in ['--help', '-h']:
            print(__doc__)
            print("\nCommand line options:")
            print("  --url, -u     Dataverse URL (e.g., https://dataverse.harvard.edu)")
            print("  --token, -t   API token")
            print("  --pid, -p     Dataset persistent ID (e.g., doi:10.7910/DVN/12345)")
            print("  --dir, -d     Directory containing files to upload")
            sys.exit(0)
        else:
            print(f"Unknown argument: {arg}")
            print("Use --help for usage information")
            sys.exit(1)

    return args


def main():
    """Main upload function"""
    # Parse command line arguments
    config = parse_args()

    dataverse_url = config['url']
    api_token = config['token']
    dataset_pid = config['pid']
    upload_dir = config['dir']

    print("=" * 60)
    print("Dataverse File Uploader (WITH ARCHIVE FILES)")
    print("=" * 60)
    print(f"Dataverse: {dataverse_url}")
    print(f"Dataset: {dataset_pid}")
    print(f"Directory: {upload_dir}")
    print("=" * 60)

    # Check directory exists
    if not os.path.isdir(upload_dir):
        print(f"\n✗ Error: Directory not found: {upload_dir}")
        return

    # Get all files from directory (NO FILTERING)
    files_to_upload = [str(f) for f in Path(upload_dir).iterdir() if f.is_file()]

    if not files_to_upload:
        print(f"\n✗ No files to upload (directory is empty)")
        return

    print(f"\nFound {len(files_to_upload)} files to upload")
    print("⚠️  NOTE: Archive files (.zip, .gz, etc.) will be uploaded")
    print("         Dataverse may auto-extract them upon upload\n")

    # Upload each file
    success_count = 0
    fail_count = 0

    for file_path in files_to_upload:
        success = upload_file_to_dataverse(
            file_path,
            dataverse_url,
            api_token,
            dataset_pid
        )

        if success:
            success_count += 1
        else:
            fail_count += 1

    # Summary
    print("\n" + "=" * 60)
    print("Upload Summary")
    print("=" * 60)
    print(f"Successful: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Total: {len(files_to_upload)}")
    print("\n⚠️  Files uploaded to DRAFT version")
    print("    Go to Dataverse GUI to:")
    print("    - Check if archives were auto-extracted")
    print("    - Remove duplicates")
    print("    - Save changes")
    print("    - Submit for review (if needed)")
    print("=" * 60)


if __name__ == "__main__":
    main()
