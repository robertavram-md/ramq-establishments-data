# RAMQ Establishments Data

This repository contains scripts and data for extracting and enriching healthcare establishment information from the Régie de l'assurance maladie du Québec (RAMQ) with Google Places API data.

## Project Overview

The goal of this project is to:
1. Extract healthcare establishment data (hospitals, clinics, etc.) from the RAMQ website
2. Enrich this data with additional information from Google Places API
3. Create a comprehensive CSV file with combined data

## Repository Structure

- **scripts/**: Contains all Python scripts used for data extraction and processing
- **data/**: Contains the CSV files with RAMQ establishments data

## Key Files

### Scripts

- `extract_pdf_final.py`: Extracts establishment data from RAMQ PDF files
- `enrich_with_google_places_improved.py`: Enriches RAMQ data with Google Places API information
- `create_merged_csv.py`: Merges original RAMQ data with Google Places enriched data

### Data

- `ramq_establishments_final.csv`: Original RAMQ establishments data
- `ramq_establishments_merged_improved.csv`: RAMQ data enriched with Google Places information
- `ramq_establishments_merged_complete.csv`: Complete merged dataset with more establishments

## How to Use

### Data Extraction

To extract data from RAMQ PDF files:

```python
python scripts/extract_pdf_final.py
```

This script downloads PDF files for all regions of Quebec from the RAMQ website and extracts establishment codes, names, addresses, and categories.

### Google Places Enrichment

To enrich the data with Google Places API:

```python
python scripts/enrich_with_google_places_improved.py
```

This script uses the Google Places API to search for each establishment and retrieve additional information such as:
- Google Place ID
- Formatted address
- Phone numbers
- Coordinates
- Website URLs
- Place types

The script includes rate limiting handling with exponential backoff to avoid hitting API limits.

### Merging Data

To merge the original RAMQ data with Google Places data:

```python
python scripts/create_merged_csv.py
```

This creates a comprehensive CSV file that preserves all original RAMQ information while adding the Google Places data where available.

## Data Fields

The final merged CSV contains the following fields:

- **region**: Geographic region in Quebec
- **code**: RAMQ 5-digit establishment code
- **name**: Establishment name
- **address**: Physical address
- **categories**: Categories of care units (comma-separated)
- **id**: Google Place ID
- **locality**: City/town
- **country**: Country code
- **administrative_area_level_1**: Province/state
- **administrative_area_level_2**: Postal code
- **international_phone_number**: Phone number
- **fax_number**: Fax number
- **type**: Establishment type (hospital, clinic, pharmacy)
- **website**: Website URL
- **latitude**: Geographic latitude
- **longitude**: Geographic longitude
- **added_time**: Timestamp
- **place_type**: Google place type
- **is_fax_enabled**: Whether fax is available (1) or not (0)

## Requirements

- Python 3.6+
- Required packages:
  - requests
  - beautifulsoup4
  - poppler-utils (for PDF processing)

## License

This project is available for use under standard open-source terms.
