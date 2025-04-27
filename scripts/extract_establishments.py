import csv
import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Setup Chrome options
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")

# Initialize the driver
driver = webdriver.Chrome(options=chrome_options)

# Navigate to the RAMQ website
url = "https://www.ramq.gouv.qc.ca/fr/professionnels/professionnels/Pages/liste-des-etablissements.aspx"
driver.get(url)
print("Navigated to RAMQ website")

# Create CSV file
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments.csv"
with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
    csv_writer = csv.writer(csvfile)
    # Write header
    csv_writer.writerow(['Region', 'Category', 'Code', 'Name'])
    
    # Get the region dropdown
    region_select = Select(driver.find_element(By.TAG_NAME, "select"))
    regions = [option.text for option in region_select.options]
    
    # Skip the first option which is "Choose your region"
    for region_index, region in enumerate(regions[1:], 1):
        print(f"Processing region: {region}")
        
        # Select the region
        region_select = Select(driver.find_element(By.TAG_NAME, "select"))
        region_select.select_by_index(region_index)
        
        # Click the display button
        display_button = driver.find_element(By.XPATH, "//input[@value='Afficher']")
        display_button.click()
        
        # Wait for the page to load
        time.sleep(2)
        
        # Find all establishment categories (buttons that expand to show establishments)
        category_buttons = driver.find_elements(By.XPATH, "//button[contains(@title, '')]")
        
        for button_index, button in enumerate(category_buttons):
            category_name = button.get_attribute("title")
            print(f"  Processing category: {category_name}")
            
            # Click to expand the category
            button.click()
            time.sleep(1)
            
            # Find all establishments in this category
            establishments = driver.find_elements(By.XPATH, f"//div[contains(@id, 'panel{button_index}')]//table//tr")
            
            for establishment in establishments:
                # Skip header rows
                if establishment.get_attribute("class") and "ms-viewheadertr" in establishment.get_attribute("class"):
                    continue
                
                # Extract code and name
                cells = establishment.find_elements(By.TAG_NAME, "td")
                if len(cells) >= 2:
                    code = cells[0].text.strip()
                    name = cells[1].text.strip()
                    
                    if code and name:
                        csv_writer.writerow([region, category_name, code, name])
                        print(f"    Added: {code} - {name}")
            
            # Collapse the category to avoid interference with next categories
            button.click()
            time.sleep(0.5)

print(f"Data extraction complete. CSV file saved to: {csv_file_path}")
driver.quit()
