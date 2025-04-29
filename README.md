# RAMQ Establishments Data

This repository contains essential scripts for extracting healthcare establishment information from the Régie de l'assurance maladie du Québec (RAMQ) PDFs and enriching it with Google Places API data.

## Project Overview

The goal of this project is to:
1. Extract healthcare establishment data (hospitals, clinics, etc.) from RAMQ PDF files
2. Enrich this data with additional information from Google Places API
3. Create a comprehensive CSV file with combined data

## Repository Structure

- **scripts/**: Contains the essential Python scripts for data extraction and processing
- **data/**: Contains the CSV files with RAMQ establishments data

## Essential Scripts

This repository has been streamlined to include only the most useful scripts:

1. **extract_ramq_pdf.py**: Extracts establishment data from RAMQ PDF files
   - Downloads PDF files for all regions of Quebec
   - Extracts establishment codes, names, addresses, and categories
   - Creates a comprehensive CSV with all establishments

2. **enrich_with_google_places.py**: Enriches RAMQ data with Google Places API
   - Searches for each establishment using the Google Places API
   - Retrieves detailed information (Place ID, address, phone, coordinates, etc.)
   - Includes advanced rate limiting handling with exponential backoff
   - Processes establishments in small batches to avoid API limits

3. **merge_data.py**: Merges original RAMQ data with Google Places data
   - Combines data from both sources into a single comprehensive CSV
   - Preserves all original RAMQ information
   - Adds Google Places data where matches are found

## How to Use

### Step 1: Extract RAMQ Data

```python
python scripts/extract_ramq_pdf.py
```

This will create `ramq_establishments_final.csv` with the following fields:
- Region
- Code (5-digit identifier)
- Name
- Address
- Categories of care units

### Step 2: Enrich with Google Places API

```python
python scripts/enrich_with_google_places.py
```

This will create `ramq_establishments_enriched_complete.csv` with Google Places data including:
- Google Place ID
- Formatted address
- Phone numbers
- Coordinates
- Website URLs
- Place types

### Step 3: Merge the Data

```python
python scripts/merge_data.py
```

This will create `ramq_establishments_merged_complete.csv` that combines all data from both sources.

## Data Fields in Final CSV

The final merged CSV file (`ramq_establishments_merged_final.csv`) and enriched file (`ramq_establishments_final_with_keywords.csv`) contain the following columns:

- **region**: Geographic region in Quebec (e.g., "Gaspesie Iles De La Madeleine")
- **code**: RAMQ 5-digit establishment code
- **name**: Official establishment name
- **address**: Complete physical address including street, city, province, and postal code
- **categories**: Categories of care units (comma-separated)
- **id**: Google Place ID
- **formatted_name**: Google Places formatted name of the establishment
- **locality**: City/town where the establishment is located
- **country**: Country code (CA for Canada)
- **administrative_area_level_1**: Province/state (QC for Quebec)
- **administrative_area_level_2**: Postal code
- **international_phone_number**: Phone number in international format
- **fax_number**: Legacy fax number field (if available)
- **type**: Establishment type (hospital, clinic, pharmacy)
- **website**: Official website URL (if available)
- **latitude**: Geographic latitude coordinate
- **longitude**: Geographic longitude coordinate
- **added_time**: Timestamp when the data was added
- **place_type**: Google place type classification
- **is_fax_enabled**: Binary flag indicating whether fax is available (1) or not (0)

Additional fields in `ramq_establishments_final_with_keywords.csv`:

- **fax_numbers**: JSON array of standardized fax numbers in format "1XXXYYYZZZZ" (e.g., "18197574330")
- **fax_keywords**: JSON object mapping fax numbers to their purposes (e.g., {"18197574330": "admin"})
  Common purposes include:
  - general inquiries
  - appointments
  - referrals
  - medical records
  - patient care
  - admin
  - billing

## Requirements

- Python 3.6+
- Required packages:
  - requests
  - poppler-utils (for PDF processing)

## License

This project is available for use under standard open-source terms.
