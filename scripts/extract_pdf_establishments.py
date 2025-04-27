import os
import csv
import requests
import subprocess
from bs4 import BeautifulSoup

# Create directory for PDFs
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Base URL
base_url = "https://www.ramq.gouv.qc.ca/fr/professionnels/professionnels/Pages/liste-des-etablissements.aspx"

# Define regions with their codes
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

# Function to download PDF for a region
def download_pdf(region_code, region_name):
    # Format region name for URL
    region_name_formatted = region_name.lower().replace(' ', '-').replace('–', '-').replace('é', 'e').replace('è', 'e')
    
    # Construct PDF URL
    pdf_url = f"https://www.ramq.gouv.qc.ca/SiteCollectionDocuments/professionnels/facturation/liste-etablissements/liste_des_numeros_etablissements_{region_name_formatted}_{region_code}.pdf"
    
    # Define local path for PDF
    pdf_path = os.path.join(pdf_dir, f"{region_name_formatted}_{region_code}.pdf")
    
    print(f"Downloading PDF for {region_name} ({region_code}) from {pdf_url}")
    
    try:
        # Download PDF
        response = requests.get(pdf_url)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Save PDF
        with open(pdf_path, 'wb') as f:
            f.write(response.content)
        
        print(f"PDF saved to {pdf_path}")
        return pdf_path
    except Exception as e:
        print(f"Error downloading PDF for {region_name}: {str(e)}")
        return None

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    try:
        # Use pdftotext from poppler-utils to extract text
        result = subprocess.run(['pdftotext', '-layout', pdf_path, '-'], 
                               capture_output=True, text=True, check=True)
        return result.stdout
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {str(e)}")
        return None

# Function to parse PDF text and extract establishments
def parse_establishments(text, region_name):
    establishments = []
    
    if not text:
        return establishments
    
    # Split text into lines
    lines = text.split('\n')
    
    # Process lines to extract code and name
    current_category = None
    
    for line in lines:
        line = line.strip()
        
        # Skip empty lines
        if not line:
            continue
        
        # Check if line is a category header
        if line.isupper() or line.endswith(':'):
            current_category = line
            continue
        
        # Try to extract code and name
        # Codes are typically 5 digits
        import re
        code_match = re.search(r'\b\d{5}\b', line)
        
        if code_match:
            code = code_match.group(0)
            # Name is typically after the code
            name_part = line[code_match.end():].strip()
            
            # Clean up name
            name = re.sub(r'\s+', ' ', name_part).strip()
            
            if code and name:
                establishments.append({
                    'region': region_name,
                    'category': current_category if current_category else "Unknown",
                    'code': code,
                    'name': name
                })
                print(f"  Extracted: {code} - {name}")
    
    return establishments

# Main function to process all regions
def process_all_regions():
    all_establishments = []
    
    for region_code, region_name in regions:
        # Download PDF
        pdf_path = download_pdf(region_code, region_name)
        
        if pdf_path:
            # Extract text from PDF
            pdf_text = extract_text_from_pdf(pdf_path)
            
            # Parse establishments from text
            region_establishments = parse_establishments(pdf_text, region_name)
            
            # Add to overall list
            all_establishments.extend(region_establishments)
            
            print(f"Extracted {len(region_establishments)} establishments from {region_name}")
    
    return all_establishments

# Create CSV file
def create_csv(establishments, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['region', 'category', 'code', 'name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for establishment in establishments:
            writer.writerow(establishment)
    
    print(f"CSV file created at {csv_path}")
    print(f"Total establishments: {len(establishments)}")

# Main execution
if __name__ == "__main__":
    # Process all regions
    establishments = process_all_regions()
    
    # Create CSV file
    csv_path = "/home/ubuntu/ramq_data/ramq_establishments.csv"
    create_csv(establishments, csv_path)
