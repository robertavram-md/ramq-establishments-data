import os
import csv
import re
import subprocess

# Create directory for PDFs if it doesn't exist
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Output CSV file path
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments_with_address.csv"

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    try:
        # Use pdftotext from poppler-utils to extract text with layout preservation
        result = subprocess.run(['pdftotext', '-layout', pdf_path, '-'], 
                               capture_output=True, text=True, check=True)
        return result.stdout
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {str(e)}")
        return None

# Function to parse PDF text and extract establishments with addresses
def parse_establishments_with_address(text, region_name):
    establishments = []
    
    if not text:
        return establishments
    
    # Split text into lines
    lines = text.split('\n')
    
    # Skip header lines (usually first 2-3 lines)
    start_index = 0
    for i, line in enumerate(lines):
        if "Numéro" in line and "Nom et adresse" in line:
            start_index = i + 1
            break
    
    # Process lines to extract code, name, and address
    i = start_index
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Look for code pattern (usually 5 characters with last character being X or a digit)
        code_match = re.search(r'\b\d{4}[0-9X]\b', line)
        
        if code_match:
            code = code_match.group(0)
            
            # Get the name (everything after the code until the categories)
            name_part = line[code_match.end():].strip()
            
            # Find where the categories start (usually digits separated by spaces)
            categories_match = re.search(r'\s+\d(\s+\d)+\s*$', name_part)
            if categories_match:
                name_part = name_part[:categories_match.start()].strip()
            
            name = name_part
            
            # Check if the next line contains an address
            address = ""
            if i + 1 < len(lines) and not re.search(r'\b\d{4}[0-9X]\b', lines[i + 1]):
                address_line = lines[i + 1].strip()
                # Check if this looks like an address (contains postal code or city)
                if re.search(r'[A-Z][0-9][A-Z]\s*[0-9][A-Z][0-9]|QC,', address_line):
                    address = address_line
                    i += 1  # Skip the address line in the next iteration
            
            # Clean up name and address
            name = re.sub(r'\s+', ' ', name).strip()
            address = re.sub(r'\s+', ' ', address).strip()
            
            # Remove page information from name
            name = re.sub(r'\d{1,2}\s+avril\s+2025\s+Page\s+\d+\s+sur\s+\d+.*$', '', name)
            name = re.sub(r'Numéro\s+Nom\s+et\s+adresse\s+Catégorie\s+des\s+unités\s+de\s+soins.*$', '', name)
            name = name.strip()
            
            if code and name:
                establishments.append({
                    'region': region_name,
                    'code': code,
                    'name': name,
                    'address': address
                })
                print(f"  Extracted: {code} - {name} - {address}")
        
        i += 1
    
    return establishments

# Main function to process all PDFs in the directory
def process_all_pdfs():
    all_establishments = []
    
    # Get all PDF files in the directory
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith('.pdf')]
    
    for pdf_file in pdf_files:
        # Extract region name from filename
        region_name = pdf_file.split('_')[0].replace('-', ' ').title()
        
        # Full path to PDF
        pdf_path = os.path.join(pdf_dir, pdf_file)
        
        print(f"Processing {region_name} from {pdf_path}")
        
        # Extract text from PDF
        pdf_text = extract_text_from_pdf(pdf_path)
        
        # Parse establishments from text
        region_establishments = parse_establishments_with_address(pdf_text, region_name)
        
        # Add to overall list
        all_establishments.extend(region_establishments)
        
        print(f"Extracted {len(region_establishments)} establishments from {region_name}")
    
    return all_establishments

# Create CSV file
def create_csv(establishments, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['region', 'code', 'name', 'address']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        
        # Track unique codes to avoid duplicates
        unique_codes = set()
        count = 0
        
        for establishment in establishments:
            # Skip duplicate codes
            if establishment['code'] in unique_codes:
                continue
            
            unique_codes.add(establishment['code'])
            writer.writerow(establishment)
            count += 1
    
    print(f"CSV file created at {csv_path}")
    print(f"Total unique establishments: {count}")

# Main execution
if __name__ == "__main__":
    # Process all PDFs
    establishments = process_all_pdfs()
    
    # Create CSV file
    create_csv(establishments, csv_file_path)
