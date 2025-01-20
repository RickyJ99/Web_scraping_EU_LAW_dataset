import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import numpy as np

# Create directories for saving HTML files
os.makedirs("html_files", exist_ok=True)

# URL pattern for different years and months
base_url = (
    "https://eur-lex.europa.eu/statistics/{year}/{month:02d}/eu-law-statistics.html"
)

# Years and months range to scrape
years = range(2003, 2026)  # From 2003 to 2025
months = range(1, 13)  # January to December


# Function to add random pauses
def random_pause():
    time.sleep(random.uniform(1, 5))


# Step 1: Download and save HTML files
for year in years:
    for month in months:
        file_path = f"html_files/eu_law_statistics_{year}_{month:02d}.html"
        if os.path.exists(file_path):
            print(f"File already exists: {file_path}")
            continue

        url = base_url.format(year=year, month=month)
        print(f"Downloading: {url}")

        response = requests.get(url)
        if response.status_code == 200:
            with open(file_path, "w", encoding="utf-8") as file:
                file.write(response.text)
            print(f"Saved: {file_path}")
        else:
            print(f"Failed to fetch {url}: {response.status_code}")

        random_pause()

# Step 2: Parse HTML files and extract data
country_data_all = []
sector_data_all = []

# Correct order of country codes from the site
all_country_codes = []

file_path = f"html_files/eu_law_statistics_2003_01.html"

# Read the saved HTML file
with open(file_path, "r", encoding="utf-8") as file:
    soup = BeautifulSoup(file, "html.parser")

# Extract the first table: Country-specific data
text_table = soup.find("table", id="textStatisticsTable")

# Extract countries codes
thead = text_table.find("thead")
headers = [th.text.strip() for th in thead.find_all("th")]
country_codes = headers[1:]  # Exclude the first 'Year' column
all_country_codes = country_codes

sector_table = soup.find("table", id="sectorStatisticsTable")
# Extract headers (codes) from the  second table
sector_headers = [th.text.strip() for th in sector_table.find("tbody").find_all("th")]

for year in years:
    for month in months:
        file_path = f"html_files/eu_law_statistics_{year}_{month:02d}.html"

        # Read the saved HTML file
        with open(file_path, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")

        # Extract the first table: Country-specific data
        text_table = soup.find("table", id="textStatisticsTable")
        if text_table is None:
            print(f"No country-specific table found for {year}-{month:02d}")
            continue

        # Extract headers from the table
        thead = text_table.find("thead")
        headers = [th.text.strip() for th in thead.find_all("th")]
        country_codes = headers[1:]  # Exclude the first 'Year' column

        # Check if headers match the accumulated country codes
        if not all_country_codes:
            all_country_codes = country_codes  # Initialize if empty
        elif country_codes != all_country_codes:
            print(f"Mismatch detected in {year}-{month:02d}!")
            print(f"Expected: {all_country_codes}")
            print(f"Found: {country_codes}")
            # Optionally, decide how to handle mismatches:
            # Raise an error, log a warning, or overwrite all_country_codes.
            all_country_codes = country_codes  # Overwrite to keep processing

        # Process the rows in the table
        rows = text_table.find("tbody").find_all("tr")
        for row in rows:
            # Extract the type of document (e.g., PDF, HTML, etc.)
            type_cells = row.find_all("th")
            # Extract the corresponding data cells
            cells = row.find_all("td")

            for type_document in type_cells:
                # Build the row with Year, Month, Document Type, and Data
                values = [year, month, type_document.text.strip()]

                # Remove the first data point (document type) from the cells
                cell_values = [
                    cell.text.strip() for cell in cells[0:]
                ]  # Skip the first element

                if len(cell_values) < len(all_country_codes):
                    # Pad missing values with np.nan
                    cell_values += [np.nan] * (
                        len(all_country_codes) - len(cell_values)
                    )
                elif len(cell_values) > len(all_country_codes):
                    # Trim extra values if any
                    cell_values = cell_values[: len(all_country_codes)]

                # Combine into final row and append
                values += cell_values
                country_data_all.append(values)

        # Extract the second table: Aggregate sector data
        sector_table = soup.find("table", id="sectorStatisticsTable")
        sector_rows = sector_table.find("tbody").find_all("tr")

        for row in sector_rows:
            # Extract data from <td> cells
            cells = [cell.text.strip() for cell in row.find_all("td")]

            # sector document
            sector_header = sector_headers[
                sector_headers.index(row.find_all("th")[0].text.strip())
            ]
            # Append year, month, and row data
            sector_data_all.append([year, month, sector_header] + cells)


# Step 3: Create DataFrames and merge data
# Create DataFrames for country and sector data
df_country = pd.DataFrame(
    country_data_all, columns=["Year", "Month", "Format"] + all_country_codes
)
df_sector = pd.DataFrame(sector_data_all, columns=["Year", "Month", "Type", "Value"])

# Merge the country and sector data into a panel
panel_data = []

for _, country_row in df_country.iterrows():
    year, month = country_row["Year"], country_row["Month"]

    # Iterate over all country codes to get the format-specific values
    for country in all_country_codes:
        format_value = country_row[country] if country in country_row else None

        # Filter sector data for the same year and month
        sector_rows = df_sector[
            (df_sector["Year"] == year) & (df_sector["Month"] == month)
        ]

        for _, sector_row in sector_rows.iterrows():
            sector_type = sector_row["Type"]
            sector_value = sector_row["Value"]

            # Append to panel data
            panel_data.append(
                [
                    year,
                    month,
                    country,
                    sector_type,
                    sector_value,
                    country_row["Format"],
                    format_value,
                ]
            )

# Create the final panel DataFrame
df_panel = pd.DataFrame(
    panel_data,
    columns=[
        "Year",
        "Month",
        "Country",
        "Type",
        "Type_value",
        "Format",
        "Format_value",
    ],
)

# Debugging: Check the final DataFrame
print(df_panel.head())

# Save the DataFrame to a CSV file
df_panel.to_csv("eu_law_statistics_panel.csv", index=False)


print("Scraping completed and panel data saved to eu_law_statistics_panel.csv")
