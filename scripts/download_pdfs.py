import os
import requests
import time

# Create directory for PDFs if it doesn't exist
pdf_dir = "/home/ubuntu/ramq_data/pdfs"
os.makedirs(pdf_dir, exist_ok=True)

# Dictionary of regions with their codes
regions = {
    "abitibi-temiscamingue": "08",
    "bas-saint-laurent": "01",
    "capitale-nationale": "03",
    "chaudiere-appalaches": "12",
    "cote-nord": "09",
    "estrie": "05",
    "gaspesie-iles-de-la-madeleine": "11",
    "lanaudiere": "14",
    "laurentides": "15",
    "laval": "13",
    "mauricie-et-centre-du-quebec": "04",
    "monteregie": "16",
    "montreal": "06",
    "nord-du-quebec": "10",
    "nunavik": "17",
    "outaouais": "07",
    "saguenay-lac-saint-jean": "02",
    "terres-cries-de-la-baie-james": "18"
}

# Base URL for PDF downloads
base_url = "https://www.ramq.gouv.qc.ca/SiteCollectionDocuments/professionnels/facturation/liste-etablissements/liste_des_numeros_etablissements_{region}_{code}.pdf"

# Download PDFs for all regions
for region, code in regions.items():
    # Construct the URL
    url = base_url.format(region=region, code=code)
    
    # Construct the output file path
    output_file = os.path.join(pdf_dir, f"{region}_{code}.pdf")
    
    print(f"Downloading {region} (code: {code}) from {url}")
    
    try:
        # Download the PDF
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # Save the PDF
            with open(output_file, 'wb') as f:
                f.write(response.content)
            print(f"Successfully downloaded {output_file}")
        else:
            print(f"Failed to download {region}: HTTP status code {response.status_code}")
        
        # Add a small delay to avoid overwhelming the server
        time.sleep(1)
    except Exception as e:
        print(f"Error downloading {region}: {str(e)}")

print("Download complete!")
