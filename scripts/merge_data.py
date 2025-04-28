import csv
import os
import argparse

# Default file paths
original_csv_path = "data/ramq_establishments_final.csv"
enriched_csv_path = "data/ramq_establishments_enriched_complete.csv"
merged_csv_path = "data/ramq_establishments_merged_improved.csv"

def merge_csv_files(original_path=None, enriched_path=None, output_path=None):
    # Use provided paths or defaults
    original_path = original_path or original_csv_path
    enriched_path = enriched_path or enriched_csv_path
    output_path = output_path or merged_csv_path
    
    print(f"Starting to merge CSV files...")
    print(f"Original CSV: {original_path}")
    print(f"Enriched CSV: {enriched_path}")
    print(f"Output will be saved to: {output_path}")
    
    # Read original CSV into a dictionary keyed by code
    original_data = {}
    original_fieldnames = None
    with open(original_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        original_fieldnames = reader.fieldnames
        for row in reader:
            code = row['code']
            original_data[code] = row
    
    print(f"Read {len(original_data)} establishments from original CSV")
    
    # Read enriched CSV into dictionaries keyed by both ramq_id and id (if available)
    enriched_by_ramq_id = {}
    enriched_by_id = {}
    enriched_fieldnames = None
    with open(enriched_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        enriched_fieldnames = reader.fieldnames
        for row in reader:
            ramq_id = row['ramq_id']
            if ramq_id:
                enriched_by_ramq_id[ramq_id] = row
            
            # If the row has a Google Place ID, also index by that
            place_id = row.get('id', '')
            if place_id:
                enriched_by_id[place_id] = row
    
    print(f"Read {len(enriched_by_ramq_id)} establishments from enriched CSV")
    print(f"Found {len(enriched_by_id)} establishments with Google Place IDs")
    
    # Define the merged CSV structure
    merged_fieldnames = original_fieldnames + [
        field for field in enriched_fieldnames 
        if field not in original_fieldnames and field != 'ramq_id'  # Skip duplicate ramq_id field
    ]
    
    # Create the merged CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=merged_fieldnames)
        writer.writeheader()
        
        # Process each establishment in the original data
        matched_by_ramq_id = 0
        matched_by_secondary_id = 0
        not_matched = 0
        
        for code, original_row in original_data.items():
            merged_row = original_row.copy()
            matched = False
            
            # Try to find a match in the enriched data using code as ramq_id
            if code in enriched_by_ramq_id:
                enriched_row = enriched_by_ramq_id[code]
                
                # Add enriched fields to merged row
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field != 'ramq_id':
                        merged_row[field] = enriched_row[field]
                
                matched_by_ramq_id += 1
                matched = True
            
            # If not matched, try to find a match using any secondary IDs or fields
            # (This is a placeholder - add more matching logic as needed)
            if not matched:
                # Here you could add more matching logic based on names, addresses, etc.
                not_matched += 1
                
                # No match found, add empty values for enriched fields
                for field in enriched_fieldnames:
                    if field not in original_fieldnames and field != 'ramq_id':
                        merged_row[field] = ""
            
            # Write the merged row
            writer.writerow(merged_row)
    
    print(f"Merged CSV created at {output_path}")
    print(f"Matched by ramq_id: {matched_by_ramq_id}")
    print(f"Matched by secondary identification: {matched_by_secondary_id}")
    print(f"Not matched: {not_matched}")
    print(f"Total establishments in merged file: {len(original_data)}")
    
    return output_path

if __name__ == "__main__":
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Merge RAMQ establishments data with enriched Google Places data')
    parser.add_argument('--original', type=str, help='Path to original CSV file', default=original_csv_path)
    parser.add_argument('--enriched', type=str, help='Path to enriched CSV file', default=enriched_csv_path)
    parser.add_argument('--output', type=str, help='Path to output merged CSV file', default=merged_csv_path)
    args = parser.parse_args()
    
    # Call merge function with provided arguments
    merge_csv_files(args.original, args.enriched, args.output)
