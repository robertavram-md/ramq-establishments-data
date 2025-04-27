import os
import csv
import re
import subprocess

# Create directory for PDFs if it doesn't exist
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Output CSV file path
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments_with_addresses.csv"

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

# Function to parse PDF text and extract establishments with addresses and categories
def parse_establishments_with_address_and_categories(text, region_name):
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
    
    # Process lines to extract code, name, address, and categories
    i = start_index
    current_code = None
    current_name = None
    current_categories = []
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Look for code pattern (usually 5 characters with last character being X or a digit)
        code_match = re.search(r'^\s*(\d{4}[0-9X])\b', line)
        
        if code_match:
            # If we already have a code and name, save the previous entry
            if current_code and current_name:
                establishments.append({
                    'region': region_name,
                    'code': current_code,
                    'name': current_name,
                    'address': '',  # Will be filled in next pass
                    'categories': ','.join(current_categories)
                })
            
            current_code = code_match.group(1)
            
            # Get the rest of the line after the code
            rest_of_line = line[code_match.end():].strip()
            
            # Find categories at the end of the line (digits separated by spaces)
            current_categories = []
            categories_match = re.search(r'\s+(\d(\s+\d)+)\s*$', rest_of_line)
            if categories_match:
                categories_str = categories_match.group(1)
                current_categories = [cat.strip() for cat in categories_str.split() if cat.strip()]
                # Remove categories part from the rest of the line
                rest_of_line = rest_of_line[:categories_match.start()].strip()
            
            # The remaining part is the name
            current_name = rest_of_line.strip()
            
            # Clean up name
            current_name = re.sub(r'\s+', ' ', current_name).strip()
            
            # Remove page information from name
            current_name = re.sub(r'\d{1,2}\s+avril\s+2025\s+Page\s+\d+\s+sur\s+\d+.*$', '', current_name)
            current_name = re.sub(r'Numéro\s+Nom\s+et\s+adresse\s+Catégorie\s+des\s+unités\s+de\s+soins.*$', '', current_name)
            current_name = current_name.strip()
        
        i += 1
    
    # Add the last entry if there is one
    if current_code and current_name:
        establishments.append({
            'region': region_name,
            'code': current_code,
            'name': current_name,
            'address': '',
            'categories': ','.join(current_categories)
        })
    
    # Second pass to extract addresses
    i = start_index
    current_code = None
    
    while i < len(lines):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            i += 1
            continue
        
        # Look for code pattern
        code_match = re.search(r'^\s*(\d{4}[0-9X])\b', line)
        
        if code_match:
            current_code = code_match.group(1)
            i += 1
            
            # Check if the next line contains an address (doesn't start with a code)
            if i < len(lines):
                next_line = lines[i].strip()
                if next_line and not re.search(r'^\d{4}[0-9X]', next_line):
                    # This is likely an address line
                    address = next_line
                    
                    # Update the address for the corresponding establishment
                    for est in establishments:
                        if est['code'] == current_code:
                            est['address'] = address
                            break
        else:
            i += 1
    
    # Print extraction results
    for est in establishments:
        print(f"  Extracted: {est['code']} - {est['name']} - {est['address']} - Categories: {est['categories']}")
    
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
        region_establishments = parse_establishments_with_address_and_categories(pdf_text, region_name)
        
        # Add to overall list
        all_establishments.extend(region_establishments)
        
        print(f"Extracted {len(region_establishments)} establishments from {region_name}")
    
    return all_establishments

# Create CSV file
def create_csv(establishments, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['region', 'code', 'name', 'address', 'categories']
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
