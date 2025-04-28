import subprocess
import csv
import os
import re

# Output file for modified rows
output_csv = 'data/ramq_establishments_to_enrich.csv'

# Run git diff to get all modifications with maximum context
print("Running git diff to find modified rows...")
git_diff = subprocess.check_output(
    ['git', 'diff', '--unified=10000', 'data/ramq_establishments_final.csv'], 
    universal_newlines=True
)

# Parse the diff to find modified lines
print("Parsing git diff output...")
modified_lines = []
in_diff_section = False
is_header_processed = False

for line in git_diff.splitlines():
    # Skip diff header lines
    if line.startswith('diff --git') or line.startswith('index ') or line.startswith('---') or line.startswith('+++'):
        continue
    
    # Identify diff sections
    if line.startswith('@@'):
        in_diff_section = True
        continue
    
    if not in_diff_section:
        continue
    
    # Look for added/modified lines (those starting with +)
    if line.startswith('+'):
        # Skip the header line after we've processed it once
        if line == '+region,code,name,address,categories':
            if not is_header_processed:
                is_header_processed = True
            continue
        
        # Add the modified line (remove the + prefix)
        modified_lines.append(line[1:])

print(f"Found {len(modified_lines)} modified lines")

# Extract the rows with proper CSV parsing
reader = csv.reader(modified_lines)
modified_rows = list(reader)

# Check if we have any results
if not modified_rows:
    print("No modified rows found in the git diff.")
    exit(1)

# Write the modified rows to a CSV file
print(f"Writing {len(modified_rows)} modified rows to {output_csv}...")
with open(output_csv, 'w', newline='') as f:
    writer = csv.writer(f)
    # Write header
    writer.writerow(['region', 'code', 'name', 'address', 'categories'])
    # Write modified rows
    for row in modified_rows:
        if len(row) >= 2:  # Ensure we have at least region and code
            writer.writerow(row)

# Count valid rows
with open(output_csv, 'r', newline='') as f:
    reader = csv.reader(f)
    next(reader)  # Skip header
    valid_rows = list(reader)
    
print(f"Successfully extracted {len(valid_rows)} valid modified rows to {output_csv}")

# Display the codes of modified rows
if valid_rows:
    codes = [row[1] for row in valid_rows if len(row) > 1]
    print(f"Modified codes: {', '.join(codes)}")
    
print("\nNow you can run the enrichment script on these rows:")
print(f"python scripts/enrich_with_google_places.py --input-file={output_csv} --output-file=data/ramq_establishments_enriched_modified.csv")