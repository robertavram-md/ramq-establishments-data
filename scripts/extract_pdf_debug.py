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
        # The -raw option might help preserve the exact layout
        result = subprocess.run(['pdftotext', '-layout', '-raw', pdf_path, '-'], 
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
        code_match = re.search(r'^\s*(\d{4}[0-9X])\b', line)
        
        if code_match:
            code = code_match.group(1)
            
            # Get the rest of the line after the code
            rest_of_line = line[code_match.end():].strip()
            
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
                # Check if next line doesn't start with a code and has content
                if next_line and not re.search(r'^\d{4}[0-9X]', next_line):
                    # This is likely an address line
                    address = next_line
                    i += 1  # Skip the address line in the next iteration
            
            # Clean up name and address
            name = re.sub(r'\s+', ' ', name).strip()
            address = re.sub(r'\s+', ' ', address).strip()
            
            # Remove page information from name
            name = re.sub(r'\d{1,2}\s+avril\s+2025\s+Page\s+\d+\s+sur\s+\d+.*$', '', name)
            name = re.sub(r'Numéro\s+Nom\s+et\s+adresse\s+Catégorie\s+des\s+unités\s+de\s+soins.*$', '', name)
            name = name.strip()
            
            # Print raw lines for debugging
            print(f"DEBUG - Raw line: '{line}'")
            if address:
                print(f"DEBUG - Address line: '{address}'")
            else:
                print(f"DEBUG - No address found in next line")
                # Try to look at the next few lines for an address
                for j in range(i+1, min(i+3, len(lines))):
                    print(f"DEBUG - Next line {j-i}: '{lines[j].strip()}'")
            
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

# Process a single PDF for testing
def process_single_pdf(pdf_file):
    # Extract region name from filename
    region_name = pdf_file.split('_')[0].replace('-', ' ').title()
    
    # Full path to PDF
    pdf_path = os.path.join(pdf_dir, pdf_file)
    
    print(f"Processing {region_name} from {pdf_path}")
    
    # Extract text from PDF
    pdf_text = extract_text_from_pdf(pdf_path)
    
    # Save raw text for inspection
    with open(f"/home/ubuntu/ramq_data/{pdf_file}_raw.txt", 'w', encoding='utf-8') as f:
        f.write(pdf_text)
    
    print(f"Raw text saved to /home/ubuntu/ramq_data/{pdf_file}_raw.txt")
    
    # Parse establishments from text
    region_establishments = parse_establishments_with_address_and_categories(pdf_text, region_name)
    
    print(f"Extracted {len(region_establishments)} establishments from {region_name}")
    
    return region_establishments

# Main execution
if __name__ == "__main__":
    # Process a single PDF for testing
    establishments = process_single_pdf("abitibi-temiscamingue_08.pdf")
    
    # Create CSV file with the results
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['region', 'code', 'name', 'address', 'categories']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for establishment in establishments:
            writer.writerow(establishment)
    
    print(f"CSV file created at {csv_file_path}")
    print(f"Total establishments: {len(establishments)}")
