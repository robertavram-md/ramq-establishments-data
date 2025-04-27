import csv
import os

# Input and output file paths
original_csv_path = "/home/ubuntu/ramq_data/ramq_establishments_final.csv"
enriched_csv_path = "/home/ubuntu/ramq_data/ramq_establishments_enriched_temp.csv"
merged_csv_path = "/home/ubuntu/ramq_data/ramq_establishments_merged_improved.csv"

def merge_csv_files():
    print(f"Starting to merge CSV files...")
    
    # Read the original RAMQ data
    original_data = {}
    with open(original_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        original_fieldnames = reader.fieldnames
        
        # Store original data by code (unique identifier in original data)
        for row in reader:
            code = row['code']
            original_data[code] = row
    
    print(f"Read {len(original_data)} establishments from original CSV")
    
    # Read the enriched data
    enriched_data = {}
    with open(enriched_csv_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        enriched_fieldnames = reader.fieldnames
        
        # Create a mapping from Google Places name to enriched data
        # We'll use this to match with original data
        for row in reader:
            name = row['name']
            enriched_data[name] = row
    
    print(f"Read {len(enriched_data)} establishments from enriched CSV")
    
    # Define the merged CSV structure
    merged_fieldnames = original_fieldnames + [
        field for field in enriched_fieldnames 
        if field not in original_fieldnames and field != 'name'  # Skip duplicate name field
    ]
    
    # Create the merged CSV
    with open(merged_csv_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=merged_fieldnames)
        writer.writeheader()
        
        # Process each establishment in the original data
        merged_count = 0
        for code, original_row in original_data.items():
            merged_row = original_row.copy()
            
            # Try to find a match in the enriched data
            original_name = original_row['name']
            
            # Look for exact match first
            if original_name in enriched_data:
                enriched_row = enriched_data[original_name]
                
                # Add enriched fields to merged row
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field != 'name':
                        merged_row[field] = enriched_row[field]
                
                merged_count += 1
            else:
                # If no exact match, try to find a partial match
                best_match = None
                best_score = 0
                
                for enriched_name, enriched_row in enriched_data.items():
                    # Simple matching score based on common words
                    original_words = set(original_name.upper().split())
                    enriched_words = set(enriched_name.upper().split())
                    common_words = original_words.intersection(enriched_words)
                    
                    if len(common_words) > best_score and len(common_words) >= 2:
                        best_score = len(common_words)
                        best_match = enriched_row
                
                if best_match:
                    # Add enriched fields to merged row
                    for field in enriched_fieldnames:
                        if field not in original_fieldnames and field != 'name':
                            merged_row[field] = best_match[field]
                    
                    merged_count += 1
                else:
                    # No match found, add empty values for enriched fields
                    for field in enriched_fieldnames:
                        if field not in original_fieldnames and field != 'name':
                            merged_row[field] = ""
            
            # Write the merged row
            writer.writerow(merged_row)
    
    print(f"Merged CSV created at {merged_csv_path}")
    print(f"Successfully merged data for {merged_count} establishments")
    print(f"Total establishments in merged file: {len(original_data)}")
    
    return merged_csv_path

if __name__ == "__main__":
    merge_csv_files()
