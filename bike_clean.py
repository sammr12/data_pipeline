import os
import pandas as pd
import re
from tqdm import tqdm

# Function to Choose and Change direcectory
def change_direc():
    direc = ""
    files = list()
    while direc == "":
        direc = input("Enter the path of the folder containing the files you want to clean: ")

        if not os.path.exists(direc):
            print("The entered path does not exist.")
            direc = ""

    try: # Try changing Working Directory
        os.chdir(direc)
        print("New Working Directory: ", os.getcwd())
        files = os.listdir(direc)
        files = [f for f in files if os.path.isfile(direc+'/'+f) and f.endswith('.csv') and 'divvy' in f.lower() and not f.endswith('_cleaned.csv')] 
        if len(files) == 0:
            print("No files to clean in this directory.")
            choice = input("Would you like to enter a new directory? (yes/no): ")
            if choice.lower().strip() == 'no':
                print("Exiting program...")
            else:
                direc = ""
                return direc
        else: 
            print("New Working Directory: ", os.getcwd())
            return direc
    
    except Exception as e:
        print(f"Unable to change working directory. Error: {e}")
        direc = ""

#Function to Load Files from 'Bike_Data'
def load_file(file):
    df = pd.read_csv(file)
    return df

# Function to validate column names
def column_name_validation(df):
    try:
        # Create List of Expected Column Names
        cols_list = ['ride_id', 'start_station_name', 'start_station_id', 'start_lat', 'start_lng', 'end_station_name', 'end_station_id', 'end_lat','end_lng','started_at','ended_at', 'member_casual','rideable_type']
        
        # Convert All Column Names in df to a List
        df_cols_list = df.columns.tolist()

        # Create List for Invalid Column Names
        invalid_cols = list()

        # Check the current file for invalid column names, and adds any invalid columns to 'invalid_cols'
        for col in df_cols_list:
            if col not in cols_list:
                invalid_cols.append(col)
            else:
                cols_list.remove(col)
        
        # Prompt the user to rename or drop the invalid columns
        while len(invalid_cols) > 0:
            for invalid_col in invalid_cols:
                renamed_col = input(f"'{invalid_col}' is not a valid column name. Please select a column that still needs to be mapped ({cols_list}), or enter 'Drop': ")
                if renamed_col == ('Drop' or 'drop'):
                    df.drop(columns=[invalid_col], inplace=True)
                    invalid_cols.remove(invalid_col)
                elif renamed_col in cols_list:
                    df.rename(columns ={invalid_col: renamed_col}, inplace=True)
                    invalid_cols.remove(invalid_col)
                else:
                    continue
        
        # Checks whether there are any expected columns missing in the current file
        if len(cols_list) > 0:
            print("You are missing the following columns {cols_list}. This file is not ready for cleaning.")
            df = "Bad File"
        else: 
            print("All column names validated")

        del cols_list
        del df_cols_list
        del invalid_cols

        return df

    except Exception as e:
        print(f"Unable to validate column names. Error: {e}")

# Function to any rows where 'ride_id', 'start_station_name', 'start_station_id', 'start_lat', 'start_lng', 'end_station_name', 'end_station_id', 'end_lat','end_lng','started_at','ended_at', 'member_casual' or 'rideable_type' is null
def drop_nulls(df):
    try:
        rows_before = df.shape[0]
        df.dropna(inplace=True)
        print(f'Removed ', rows_before - df.shape[0]," null rows")

        return df

    except Exception as e:
        print(f"Error dropping rows with nulls. Error: {e}")

# Function to address issues related to ride start and end times
def drop_invalid_times(df):
    rows_before = df.shape[0]
    # Ensure 'started_at' and 'ended_at' are datetime
    try:
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at'] = pd.to_datetime(df['ended_at'])
    except Exception as e:
        print(f"Error converting 'started_at' and 'ended_at' to datetime. Error: {e}")

    # Drop any rows where 'started_at' is greater than 'ended_at' (Print Rows Dropped)
    try:
        df.drop(df[df.started_at > df.ended_at].index, inplace=True)
        print(f'Removed ', rows_before - df.shape[0], " rows fwith invalid start times.")
    except Exception as e:
        print(f"Error removing rows where 'started_at' is greater than 'ended_at'. Error: {e}")

    # Calculate the time difference in seconds
    try:
        df['duration_sec'] = (df['ended_at'] - df['started_at']).dt.total_seconds()
        df['duration_mins'] = df['duration_sec'] / 60
    except Exception as e:
        print(f"Error creating duration column. Error: {e}")

    # Drop any rows where duration is less than 60 seconds
    try:
        rows_before = df.shape[0]
        df.drop(df[(df.start_station_name == df.end_station_name) & (df['duration_sec'] < 60)].index, inplace=True)
        print(f'Removed ', rows_before - df.shape[0], " rows with trips lasting less than 60 seconds and starting/ending at same station.")

    except Exception as e:
        print(f"Error dropping rides with bad duration values. Error: {e}")

    # Drop any rows where duration is greater than 24 hours
    try:
        rows_before = df.shape[0]
        df.drop(df[(df['duration_sec'] > 86400)].index, inplace=True)
        print(f'Removed ', rows_before - df.shape[0], " rows where trips lasted longer than 24 hours")

        return df

    except Exception as e:
        print(f"Error dropping rides with bad duration values. Error: {e}")

# Function to Create Station Keys
def create_station_keys(df):
     
    # Create a 'start_station_key' column that combines the 'start_station_name' and 'start_station_id' columns (separated by a '_')
    try:
        print("Deriving start_station_keys for each ride...")
        df['start_station_key'] = df['start_station_name'].str.lower().str.replace(" ", "_") + '_' + df['start_station_id'].astype(str)
    except Exception as e:
        print(f"Error creating start_station_keys. Error: {e}")
    
    # Create a 'end_station_key' column that combines the 'end_station_name' and 'end_station_id' columns (separated by a '_')
    try:
        print("Deriving end_station_keys for each ride...")
        df['end_station_key'] = df['end_station_name'].str.lower().str.replace(" ", "_") + '_' + df['end_station_id'].astype(str)

        return df

    except Exception as e:
        print(f"Error creating start_station_keys. Error: {e}")

# Function to Drop Station Columns
def drop_station_columns(df,stations_df):
    # Method to append all remaining unique 'start_station_key', 'start_station_id', 'start_station_name', 'start_lat' and 'start_lng' combinations to stations_df
    try:
        # Extract relevant station columns into a temporary dataframe
        station_df_temp = df[['start_station_key', 'start_station_id', 'start_station_name', 'start_lat', 'start_lng']].copy()
        station_df_temp = station_df_temp.rename(columns={
            'start_station_key': 'station_key',
            'start_station_id': 'station_id_raw',
            'start_station_name': 'station_name_raw',
            'start_lat': 'station_lat',
            'start_lng': 'station_lng'
        })
        station_df_temp = station_df_temp.drop_duplicates()
        stations_df = pd.concat([stations_df, station_df_temp], ignore_index=True)

        del station_df_temp
    
    except Exception as e:
        print(f"Error adding start stations to stations_df. Error: {e}")

    # Method to append all remaining unique 'end_station_id', 'end_station_name', 'end_lat' and 'end_lng' combinations to stations_df
    try:
        # Extract relevant station columns into a temporary dataframe
        station_df_temp = df[['end_station_key', 'end_station_id', 'end_station_name', 'end_lat', 'end_lng']].copy()
        station_df_temp = station_df_temp.rename(columns={
            'end_station_key': 'station_key',
            'end_station_id': 'station_id_raw',
            'end_station_name': 'station_name_raw',
            'end_lat': 'station_lat',
            'end_lng': 'station_lng'
        })
        station_df_temp = station_df_temp.drop_duplicates()
        stations_df = pd.concat([stations_df, station_df_temp], ignore_index=True)
        
        del station_df_temp
    
    except Exception as e:
        print(f"Error adding start stations to stations_df. Error: {e}")

    # Method to drop the 'start_station_name', 'start_station_id', 'start_lat', 'start_lng', 'end_station_name, 'end_station_id', 'end_lat', and 'end_lng' columns
    print("Dropping the all start and end station columns (except for foreign keys)...")
    try:
        df.drop(columns=['start_station_name', 'start_station_id', 'start_lat', 'start_lng', 'end_station_name','end_station_id','end_lat','end_lng'], inplace=True)
    except Exception as e:
        print(f"Error dropping station columns. Error: {e}")
    
    return df, stations_df
    
# Function to Clean Other Columns (rideable_type and member_casual)
def clean_other_columns(df):
    try:
        # The map function in pandas does not have an inplace parameter. Instead, map returns a new Series, and you need to assign it back to the original column.
        df['member_casual'] = df['member_casual'].map({'member': 'm', 'casual': 'c'})
        df['rideable_type'] = df['rideable_type'].map({'electric_bike': 'e', 'docked_bike': 'd', 'classic_bike': 'c'})

        return df
    except Exception as e:
        print(f"Unable to clean 'rideable_type' and/or 'member_casual': {e}")

# Function to Save the Cleaned Data Frame to CSV
def save_cleaned_df(df, file_name):
    try:
        # Ensure the directory exists
        if not os.path.exists(direc):
            os.makedirs(direc)

        # Define the file name and path
        if file_name == 'stations.csv':
            file_name = file_name
        else:
            file_name = file_name.replace('.csv','_cleaned.csv')

        file_path = os.path.join(direc, file_name)

        # Check if the file already exists
        if os.path.exists(file_path):
            # Ask for user confirmation to overwrite the file
            overwrite = input(f"{file_path} already exists. Do you want to overwrite it? (yes/no): ")
            if overwrite.lower() != 'yes':
                print("File not overwritten.")
            else:
                print(f"Saving cleaned version of {file_name}...")
                # Save the DataFrame to a CSV file
                df.to_csv(file_path, index=False)
                print(f"DataFrame saved to {file_path}")
        else:
            print(f"Saving cleaned version of {file_name}...")
            # Save the DataFrame to a CSV file
            df.to_csv(file_path, index=False)
            print(f"DataFrame saved to {file_path}")

        del file_path
        return file_name
    except Exception as e:
        print(f"Unable to save {file_name} in submitted path: {e}")

# Function to Delete Raw File From Computer
def delete_raw_csv(file,file_path):
    try:
        if os.path.exists(file_path):
            if os.path.isfile(file_path):
                os.remove(file_path)
                print(f"File {file_path} deleted successfully.")
            else:
                print(f"{file_path} is not a file.")
        else:
            print(f"File {file_path} not found.")
    except Exception as e:
        print(f"Unable to delete {file}")

# Function to Clean Station Names    
def clean_station_name(station_name):
    # Remove leading and trailing whitespace
    cleaned_name = station_name.strip()
    # Remove "Public Rack - " from the station name
    cleaned_name = re.sub(r'Public Rack - ', '', cleaned_name, flags=re.IGNORECASE)
    # Standardize capitalization (e.g., title case)
    cleaned_name = cleaned_name.title()
    # Remove text within parentheses and ensure there is a space
    cleaned_name = re.sub(r'\s*\(.*?\)\s*', ' ', cleaned_name).strip()
    # Correct the capitalization for ordinal numbers (10th, 33rd, etc.)
    cleaned_name = re.sub(r'(\d+)(Th|Rd|St|Nd)', lambda x: x.group(1) + x.group(2).lower(), cleaned_name)
    # Ensure single spaces between words
    cleaned_name = re.sub(r'\s+', ' ', cleaned_name)
    # Ensure directional abbreviations remain in uppercase
    cleaned_name = re.sub(r'\b(N|S|E|W|Sw|Se|Nw|Ne)\b', lambda x: x.group(1).upper(), cleaned_name)

    return cleaned_name

# Function to Clean the Stations Table
def clean_stations(stations_df):
    # Drop Duplicates Based on 'duplicate_key'
    stations_df.drop_duplicates(subset=['station_key'], inplace=True)
    stations_df['station_name_cleaned'] = stations_df['station_name_raw'].apply(clean_station_name)
    return stations_df

# Have the User Identify the Bike_Data Folder Path
direc = ""

while direc == "":
    direc = change_direc()

# Create stations_df
stations_df = pd.DataFrame({'station_key': pd.Series(dtype='str'),
                   'station_id_raw': pd.Series(dtype='str'),
                   'station_name_raw': pd.Series(dtype='str'),
                   'station_lat': pd.Series(dtype='float'),
                   'station_lng': pd.Series(dtype='float')})

# Get List of Files to Clean
try:
    files = os.listdir(direc)
    files = [f for f in files if os.path.isfile(direc+'/'+f) and f.endswith('.csv') and not f.endswith('_cleaned.csv') and 'divvy' in f.lower()]
    print(*files, sep="\n")
except Exception as e:
    print(f"Unable to enter files in submitted path: {e}")

# Loop to Load and Clean Each File
cleaned_files = list()

for file in files:
    # Load the current file in the loop
    try:
        df = load_file(file)
    except Exception as e:
        print(f"Unable to load {file} in submitted path: {e}")
        continue
    
    # Clean the current file in the loop
    try:
        rows_before = df.shape[0]
        print("{File} has {rows_before} rows before cleaning")
    
        df = column_name_validation(df)
    
        if isinstance(df, pd.DataFrame):
            df = drop_nulls(df)
            df = drop_invalid_times(df)
            df = create_station_keys(df)
            result = drop_station_columns(df,stations_df)
            df = result[0]
            stations_df = result[1]
            df = clean_other_columns(df)
            print(f"{df.shape[0]} rows in {file} are left after cleaning")
        else:
            continue
    except:
        print(f"Unable to clean {file} in submitted path: {e}")
        continue

    # Save the Cleaned File to the 'Bike_Data' Folder
    try:
        cleaned_file = save_cleaned_df(df, file)
        cleaned_files.append(cleaned_file)
    except Exception as e:
        continue
    
    del cleaned_file
    del df

    # Delete the Raw File from the 'Bike_Data' Folder
    try:
        file_path = direc+'/'+file
        delete_raw_csv(file,file_path)
    except:
        continue

# Start Cleaning Stations Table
print("Starting to clean stations table...")
stations_df = clean_stations(stations_df)
file_name = 'stations.csv'
save_cleaned_df(stations_df,file_name)



print("File cleaning complete. Run 'bike_merge.py' to merge all of the data tables.")
print("IMPORTANT: Review 'stations.csv for any remaining station name inaccuracies not caught by this program.")
print("Run 'bike_merge.py' to merge all of the data tables. Or, run 'bike_write.py to write the data to a database.")
