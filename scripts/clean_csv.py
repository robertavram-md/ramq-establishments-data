import csv
import re

# Input and output file paths
input_csv = "/home/ubuntu/ramq_data/ramq_establishments.csv"
output_csv = "/home/ubuntu/ramq_data/ramq_establishments_clean.csv"

# Function to clean establishment names
def clean_name(name):
    # Remove page information (e.g., "22 avril 2025 Page 1 sur 10")
    name = re.sub(r'\d{1,2}\s+avril\s+2025\s+Page\s+\d+\s+sur\s+\d+.*$', '', name)
    
    # Remove "Numéro Nom et adresse Catégorie des unités de soins" text
    name = re.sub(r'Numéro\s+Nom\s+et\s+adresse\s+Catégorie\s+des\s+unités\s+de\s+soins.*$', '', name)
    
    # Clean up extra whitespace
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

# Read the input CSV and write the cleaned data to the output CSV
with open(input_csv, 'r', newline='', encoding='utf-8') as infile, \
     open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames
    
    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()
    
    # Track unique codes to avoid duplicates
    unique_codes = set()
    count = 0
    
    for row in reader:
        # Clean the name field
        row['name'] = clean_name(row['name'])
        
        # Skip empty names
        if not row['name']:
            continue
        
        # Skip duplicate codes
        if row['code'] in unique_codes:
            continue
        
        unique_codes.add(row['code'])
        writer.writerow(row)
        count += 1
    
    print(f"Processed {count} unique establishments")
    print(f"Cleaned CSV file saved to {output_csv}")
