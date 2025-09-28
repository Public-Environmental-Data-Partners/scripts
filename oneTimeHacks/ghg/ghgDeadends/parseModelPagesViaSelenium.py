# parseModelPagesViaSelenium.py -- builds a json file of the documentation
# pages for EPA's Green House Gas database model as currently
# documented on the web pages reachable from:
# https://enviro.epa.gov/envirofacts/metadata/model/ghg
# This was intended to form the basis for code to download all the
# GHG model documentation AND the database tables.

# 8 June 2025, I'm putting this code to bed so I can start
# work on a different, probably more dependable approach to getting
# the same data.

# Status: this code runs and finds info for 285 tables.
# But the other method, which uses a current API (DMAP), 
# which finds 350+ tables, rather
# than the apparently-outdated model-documentation web pages. 

# I'm going to preserve this code because its the first example
# I have of using Selenium to drive web interactions as a way
# to get to and then invoke downloads.


from selenium import webdriver
from bs4 import BeautifulSoup
import json
import time
import urllib.parse

# Global variable for the log file object
LOG = None

def get_subpart_page_links(driver, url):
    model_links = []
    try:
        driver.get(url)
        # Wait for the page content to load. Adjust this time as needed.
        time.sleep(2)

        # Get the page source after JavaScript execution
        soup = BeautifulSoup(driver.page_source, "html.parser")
        #print("model soup instantiated OK", file=log_file)

        # Find all <a> tags that have a transform attribute
        for a_tag in soup.find_all("a", transform=True):
            print("a tag found:", a_tag, file=LOG)
            # Extract the value of the 'xlink:href' attribute
            href_value = a_tag["xlink:href"]
            #print(href_value, file=log_file)
            full_link = urllib.parse.urljoin(url, href_value)
            link_info = {
                "label": a_tag.text,
                "subpart_page_link": full_link,
            }
            model_links.append(link_info)
    except Exception as e:
        print(f"An error occurred in get_subpart_page_links: {e}", file=LOG)

    return model_links

def get_table_links(driver, part_info):
    subpart = part_info["label"]
    print("in get_table_links for subpart:", subpart, file=LOG)
    table_links = []
    try:
        driver.get(part_info["subpart_page_link"])
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        #print("part soup instantiated OK", file=log_file)

        # Find all <a> tags that have a transform attribute
        for a_tag in soup.find_all("a", transform=True):
            print("a tag found:", a_tag, file=LOG)
            label = a_tag.text
            # PUB_DIM_FACILITY appears on every page
            # harvest it only on the first page
            if label != "PUB_DIM_FACILITY" or \
                subpart == "GREENHOUSE GAS SUMMARY":
                # A lot of the labels have some sort of failed newline markup.
                # Get rid of it, won't change the ones where \\n is not found
                label = label.replace("\\n", "")
                # Extract the value of the 'xlink:href' attribute
                href_value = a_tag["xlink:href"]
                full_link = urllib.parse.urljoin("https://enviro.epa.gov", href_value)
                link_info = {
                    "label": label,
                    "table_page_link": full_link,
                }
                table_links.append(link_info)

    except Exception as e:
        print(f"An error occurred in get_table_links: {e}", file=LOG)

    print("Have {} table links for {}".format(len(table_links), part_info["label"]), file=LOG)
    return table_links


def main():
    global LOG
    log_file_name = "parseModel.log"

    # Open the log file at the beginning of main()
    try:
        # 'w' mode will overwrite the file each time the script is run
        log_file = open(log_file_name, "w", encoding="utf-8")
        print(f"Log file '{log_file_name}' opened successfully.")
    except IOError as e:
        print(f"Error opening log file: {e}") # This print will go to console as log_file is not open
        # If the log file cannot be opened, we can't log further to it.
        # You might want to exit or handle this error differently.
        log_file = None # Ensure log_file is None if opening failed
        return # Exit main if we can't log

    # Configure Chrome options for headless Browser
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (no browser window)
    options.add_argument("--disable-gpu")  # Recommended for headless mode
    options.add_argument("--no-sandbox")  # Bypass OS security model, required for some environments

    driver = None  # Initialize driver to None for finally block
    try:
        # Initialize the Chrome WebDriver
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        print(f"An error occurred while initializing WebDriver: {e}", file=log_file)
        print("Please ensure ChromeDriver is installed and accessible in your PATH.", file=log_file)

    if driver:
        # URL of the first page to parse
        target_url = "https://enviro.epa.gov/envirofacts/metadata/model/ghg"
        # Get the subpart links
        part_links = get_subpart_page_links(driver, target_url)

        # Print the extracted links
        if part_links:
            print("Found part Links:", file=log_file)
            for link in part_links:
                print(link, file=log_file)
                table_links = get_table_links(driver, link)
                if table_links:
                    link["table_links"] = table_links
                else:
                    link["table_links"] = []
            print("Have {} part links".format(len(part_links)), file=log_file)
        else:
            print("No Subpart links found or an error occurred.", file=log_file)

        driver.quit()  # Ensure the browser is closed
        print("driver/browser closed", file=log_file)

        # Output the JSON structure to its own file
        json_filename = "parseModel.json"
        json_file = open(json_filename, "w", encoding="utf-8")
        print(json.dumps(part_links, indent=2), file=json_file)
        print("json dumped to {}".format(json_filename), file=log_file)

        table_count = 0
        for part in part_links:
            print(part["label"], file=log_file)
            for table_link in part["table_links"]:
                print("\t", table_link["label"], file=log_file)
                table_count += 1

        print("total number of tables: {}".format(table_count), file=log_file)

    # Close the log file when main() finishes
    if log_file:
        log_file.close()
        print(f"Log file '{log_file_name}' closed.") # This print will go to console

if __name__ == "__main__":
    main()