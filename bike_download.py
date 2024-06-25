import os
import urllib.error
from urllib.parse import urljoin, urlparse
from urllib.request import urlopen, urlretrieve
from zipfile import ZipFile
from datetime import datetime, timedelta

# Function to generate year_month keys
def generate_year_month_keys(start_year, start_month, count):
    keys = []
    current_date = datetime(start_year, start_month, 1)
    
    for _ in range(count):
        year_month_key = current_date.strftime("%Y%m")
        keys.append(year_month_key)
        # Move to the previous month
        first_day_of_current_month = current_date.replace(day=1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        current_date = last_day_of_previous_month

    return keys

# Function to download and unzip files
def download_and_unzip_files(file_urls, download_folder):
    for url in file_urls:
        try:
            # Extract the ZIP file name and CSV file name
            zip_filename = url.split('/')[-1]
            local_zip_path = os.path.join(download_folder, zip_filename)
            csv_filename = zip_filename.replace('.zip', '.csv')
            clean_csv_filename = zip_filename.replace('.zip','_cleaned.csv')
            local_csv_path = os.path.join(download_folder, csv_filename)
            local_clean_csv_path = os.path.join(download_folder, clean_csv_filename)
            
            # Check if the CSV file already exists
            if os.path.exists(local_csv_path):
                print(f"{local_csv_path} already exists. Skipping download.")
                continue
            elif os.path.exists(local_clean_csv_path):
                print(f"A cleaned version of {local_csv_path} already exists. Skipping download.")
                continue
            
            # Download the ZIP file
            print(f"Downloading {url}...")
            urlretrieve(url, local_zip_path)
            print(f"Downloaded {local_zip_path}")
            
            # Unzip the downloaded file and extract only the CSV files
            print(f"Unzipping {local_zip_path}...")
            with ZipFile(local_zip_path, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    # Ignore '__MACOSX' folders and non-CSV files
                    if not member.startswith('__MACOSX') and member.endswith('.csv'):
                        member_filename = os.path.basename(member)
                        target_path = os.path.join(download_folder, member_filename)
                        with zip_ref.open(member) as source, open(target_path, "wb") as target:
                            target.write(source.read())
            print(f"Unzipped {local_zip_path}")
            # Remove the ZIP file after extraction
            os.remove(local_zip_path)
            print(f"Removed {local_zip_path}")
        except Exception as e:
            print(f"Error processing {url}. Error: {e}")

# The data goes as far back as April of 2020
current_date = datetime.now()
oldest_date = datetime(2020, 4, 1)
max_months = ((current_date.year - oldest_date.year) * 12) + (current_date.month - oldest_date.month)
print("Maximum number of months is ", max_months)
months = input("How many months of data would you like to download?: ")
months = int(months)

if months > max_months:
    months = max_months
    print("Months set to ", max_months, "since requested number exceeded maximum available.")
else:
    months = months

base_url_1 = "https://divvy-tripdata.s3.amazonaws.com/"
base_url_2 = "-divvy-tripdata.zip"
current_year = int(current_date.year)
last_month = int(current_date.month) - 1

if last_month == 0:
    last_month = 12
    current_year = current_year - 1

try:
    month_keys = generate_year_month_keys(current_year, last_month, months)
except Exception as e:
    print("Unable to successfully generate full list of months for download. Error:", e)

# Generate file urls
try:
    file_urls = [base_url_1 + month_key + base_url_2 for month_key in month_keys]
    print("These files will now download: ", file_urls)
except Exception as e:
    print("Unable to successfully generate file URLs. Error:", e)

# Create 'Bike_Data' folder in the current working directory if it doesn't exist
download_folder = os.path.join(os.getcwd(), "Bike_Data")
os.makedirs(download_folder, exist_ok=True)

# Download and unzip files
try:
    download_and_unzip_files(file_urls, download_folder)
    print("All files have finished downloading")
except Exception as e:
    print("Unable to successfully download requested files. Error:", e)