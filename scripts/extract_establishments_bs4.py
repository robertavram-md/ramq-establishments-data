import csv
import time
import requests
from bs4 import BeautifulSoup
import re

# Base URL
base_url = "https://www.ramq.gouv.qc.ca/fr/professionnels/professionnels/Pages/liste-des-etablissements.aspx"

# Create CSV file
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments.csv"

# Function to extract data from a region
def extract_region_data(region_code, region_name):
    print(f"Processing region: {region_name} ({region_code})")
    
    # Create session to maintain cookies
    session = requests.Session()
    
    # First request to get the form data
    response = session.get(base_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract form data
    viewstate = soup.find('input', {'name': '__VIEWSTATE'})['value']
    viewstategenerator = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})['value']
    eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})['value']
    
    # Prepare form data for POST request
    form_data = {
        '__EVENTTARGET': '',
        '__EVENTARGUMENT': '',
        '__VIEWSTATE': viewstate,
        '__VIEWSTATEGENERATOR': viewstategenerator,
        '__EVENTVALIDATION': eventvalidation,
        'ctl00$PlaceHolderMain$ddlRegion': region_code,
        'ctl00$PlaceHolderMain$btnAfficher': 'Afficher'
    }
    
    # Make POST request to get region data
    response = session.post(base_url, data=form_data)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find all tables with establishment data
    tables = soup.find_all('table', {'class': 'ms-listviewtable'})
    
    establishments = []
    
    # Process each table (each represents a category)
    for table in tables:
        # Find the category name from the preceding heading
        category_elem = table.find_previous('h4')
        if category_elem:
            category = category_elem.text.strip()
        else:
            category = "Unknown Category"
        
        # Extract rows from table
        rows = table.find_all('tr')
        
        # Skip header row
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) >= 2:
                code = cells[0].text.strip()
                name = cells[1].text.strip()
                
                if code and name:
                    establishments.append({
                        'region': region_name,
                        'category': category,
                        'code': code,
                        'name': name
                    })
                    print(f"  Added: {code} - {name}")
    
    return establishments

# Define regions (code, name)
regions = [
    ('08', 'Abitibi-Témiscamingue'),
    ('01', 'Bas-Saint-Laurent'),
    ('03', 'Capitale-Nationale'),
    ('12', 'Chaudière-Appalaches'),
    ('09', 'Côte-Nord'),
    ('05', 'Estrie'),
    ('11', 'Gaspésie–Îles-de-la-Madeleine'),
    ('14', 'Lanaudière'),
    ('15', 'Laurentides'),
    ('13', 'Laval'),
    ('04', 'Mauricie et Centre-du-Québec'),
    ('16', 'Montérégie'),
    ('06', 'Montréal'),
    ('10', 'Nord-du-Québec'),
    ('17', 'Nunavik'),
    ('07', 'Outaouais'),
    ('02', 'Saguenay–Lac-Saint-Jean'),
    ('18', 'Terres-Cries-de-la-Baie-James')
]

# Process each region and collect all establishments
all_establishments = []
for region_code, region_name in regions:
    try:
        region_establishments = extract_region_data(region_code, region_name)
        all_establishments.extend(region_establishments)
        # Add a small delay between regions to avoid overloading the server
        time.sleep(2)
    except Exception as e:
        print(f"Error processing region {region_name}: {str(e)}")

# Write all data to CSV
with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['region', 'category', 'code', 'name']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    
    writer.writeheader()
    for establishment in all_establishments:
        writer.writerow(establishment)

print(f"Data extraction complete. CSV file saved to: {csv_file_path}")
print(f"Total establishments extracted: {len(all_establishments)}")
