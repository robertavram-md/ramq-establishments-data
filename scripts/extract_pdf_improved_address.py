import os
import csv
import re
import subprocess

# Create directory for PDFs if it doesn't exist
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Output CSV file path
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments_complete.csv"

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
            code_start = code_match.start()
            code_end = code_match.end()
            
            # Get the rest of the line after the code
            rest_of_line = line[code_end:].strip()
            
            # Find categories at the end of the line (digits separated by spaces)
            categories = []
            categories_match = re.search(r'\s+(\d(\s+\d)+)\s*$', rest_of_line)
            if categories_match:
                categories_str = categories_match.group(1)
                categories = [cat.strip() for cat in categories_str.split() if cat.strip()]
                # Remove categories part from the rest of the line
                rest_of_line = rest_of_line[:categories_match.start()].strip()
            
            # The remaining part is the name
            name = rest_of_line.strip()
            
            # Look for address in the next line
            address = ""
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                # Check if next line doesn't contain a code and has content
                if not re.search(r'^\d{4}[0-9X]\b', next_line) and next_line:
                    # Check if line is indented (address lines are usually indented)
                    if next_line.startswith(' ') or re.search(r'^\d+\s+', next_line):
                        address = next_line
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
                    'address': address,
                    'categories': ','.join(categories)
                })
                print(f"  Extracted: {code} - {name} - {address} - Categories: {','.join(categories)}")
        
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
