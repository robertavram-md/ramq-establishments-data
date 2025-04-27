import os
import csv
import re
import subprocess

# Create directory for PDFs if it doesn't exist
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Output CSV file path
csv_file_path = "/home/ubuntu/ramq_data/ramq_establishments.csv"

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

# Function to parse PDF text and extract establishments
def parse_establishments(text, region_name):
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
    
    # Process lines to extract code and name
    current_code = None
    current_name = None
    
    for i in range(start_index, len(lines)):
        line = lines[i].strip()
        
        # Skip empty lines
        if not line:
            continue
        
        # Look for code pattern (usually 5 characters with last character being X or a digit)
        code_match = re.search(r'\b\d{4}[0-9X]\b', line)
        
        if code_match:
            # If we already have a code and name, save the previous entry
            if current_code and current_name:
                establishments.append({
                    'region': region_name,
                    'code': current_code,
                    'name': current_name
                })
                print(f"  Extracted: {current_code} - {current_name}")
            
            # Get the new code
            current_code = code_match.group(0)
            
            # Get the name (everything after the code until the categories)
            name_part = line[code_match.end():].strip()
            
            # Find where the categories start (usually digits separated by spaces)
            categories_match = re.search(r'\s+\d(\s+\d)+\s*$', name_part)
            if categories_match:
                name_part = name_part[:categories_match.start()].strip()
            
            current_name = name_part
        elif current_code and current_name:
            # This line might be a continuation of the name or address
            # We'll only keep the name part, not the address
            if not re.match(r'^\s*\d+\s+\d+\s+\d+', line):  # Skip lines that are just categories
                # Check if this is an address line (contains postal code or city)
                if re.search(r'[A-Z][0-9][A-Z]\s*[0-9][A-Z][0-9]|QC,', line):
                    # This is likely an address line, skip it
                    continue
                else:
                    # This might be a continuation of the name
                    current_name += " " + line
    
    # Add the last entry if there is one
    if current_code and current_name:
        establishments.append({
            'region': region_name,
            'code': current_code,
            'name': current_name
        })
        print(f"  Extracted: {current_code} - {current_name}")
    
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
        region_establishments = parse_establishments(pdf_text, region_name)
        
        # Add to overall list
        all_establishments.extend(region_establishments)
        
        print(f"Extracted {len(region_establishments)} establishments from {region_name}")
    
    return all_establishments

# Create CSV file
def create_csv(establishments, csv_path):
    with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['region', 'code', 'name']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for establishment in establishments:
            writer.writerow(establishment)
    
    print(f"CSV file created at {csv_path}")
    print(f"Total establishments: {len(establishments)}")

# Main execution
if __name__ == "__main__":
    # Process all PDFs
    establishments = process_all_pdfs()
    
    # Create CSV file
    create_csv(establishments, csv_file_path)
