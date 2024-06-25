## README.md

# Divvy Bikes Data Pipeline

This repository contains scripts to download, clean, and manage Divvy Bikes data. The pipeline includes three main scripts that should be executed in the following order:

1. **bike_download.py**
2. **bike_clean.py**
3. **bike_merge.py** / **bike_write.py**

Together, these programs enable a user to download new data from Divvy Bikes, clean the data, and either merge it into a larger dataset or write it to a database that stores previously cleaned data.

## Prerequisites

Before running the scripts, ensure you have the following installed:

- Python 3.x
- Required Python packages (listed in `requirements.txt`)

To install the necessary packages, run:
```bash
pip install -r requirements.txt
```

## Script Descriptions

### bike_download.py

This script downloads the latest Divvy Bikes data from an online source. It saves the data to a specified directory.

**Usage:**
```bash
python3 bike_download.py
```

### bike_clean.py

This script cleans the downloaded Divvy Bikes data. It performs tasks such as handling missing values, correcting data formats, and removing duplicates.

**Usage:**
```bash
python3 bike_clean.py
```

### bike_merge.py

This script merges the newly cleaned data with an existing dataset. It ensures that the data is integrated correctly and avoids duplications.

**Usage:**
```bash
python3 bike_merge.py
```

### bike_write.py

This script writes the cleaned data to a database that stores all previously cleaned data. It ensures that the data is saved correctly and is available for future analysis.

**Usage:**
```bash
python3 bike_write.py
```

## Execution Order

To successfully run the pipeline, execute the scripts in the following order:

1. **Download the data**:
    ```bash
    python3 bike_download.py
    ```

2. **Clean the data**:
    ```bash
    python3 bike_clean.py
    ```

3. **Merge the data**:
    ```bash
    python3 bike_merge.py
    ```

   *OR*

   **Write the data to a database**:
    ```bash
    python3 bike_write.py
    ```

---

This README provides a comprehensive overview of the scripts, their purposes, and how to execute them in the correct order to manage Divvy Bikes data efficiently.
