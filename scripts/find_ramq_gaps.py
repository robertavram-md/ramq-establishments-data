import pandas as pd

# Read the CSV file
df = pd.read_csv('data/ramq_establishments_enriched_complete_full.csv')

# Convert ramq_id to numeric
df['ramq_id'] = pd.to_numeric(df['ramq_id'], errors='coerce')

# Sort by ramq_id to ensure we're checking consecutive values properly
df = df.sort_values('ramq_id')

# Create a shifted dataframe to compare consecutive rows
df_shifted = df.shift(-1)

# Find rows where:
# 1. ramq_ids are not consecutive (difference > 1000)
gaps = df[
    (df_shifted['ramq_id'] - df['ramq_id'] > 1000)
]

if len(gaps) > 0:
    print("\nFound gaps in ramq_id sequence larger than 1000:")
    for idx, row in gaps.iterrows():
        next_row = df_shifted.loc[idx]
        print(f"\nGap found between:")
        print(f"Row 1: ramq_id={row['ramq_id']}, id={row['id']}")
        print(f"Row 2: ramq_id={next_row['ramq_id']}, id={next_row['id']}")
        print(f"Gap size: {next_row['ramq_id'] - row['ramq_id'] - 1}")
else:
    print("No gaps larger than 1000 found.")
