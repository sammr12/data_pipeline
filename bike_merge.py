import os
import pandas as pd
from tqdm import tqdm

# Function to Choose and Change direcectory
def change_direc():
    direc = ""
    files = list()
    while direc == "":
        direc = input("Enter the path of the folder containing the files you want to merge: ")

        if not os.path.exists(direc):
            print("The entered path does not exist.")
            direc = ""

    try: # Try changing Working Directory
        os.chdir(direc)
        print("New Working Directory: ", os.getcwd())
        files = os.listdir(direc)
        files = [f for f in files if os.path.isfile(direc+'/'+f) and f.endswith('.csv') and 'divvy' in f.lower() and f.endswith('_cleaned.csv')] 
        if len(files) == 0:
            print("No files cleaned files in this directory.")
            choice = input("Would you like to enter a new directory? (yes/no): ")
            if choice.lower().strip() == 'no':
                print("Exiting program...")
            else:
                direc = ""
                return direc
        else: 
            print("New Working Directory: ", os.getcwd())
            return direc, files
    
    except Exception as e:
        print(f"Unable to change working directory. Error: {e}")
        direc = ""

#Function to Load Files from 'Bike_Data'
def load_file(file):
    df = pd.read_csv(file)
    return df

# Function to Merge the Current File with 'merged_df'
def merge_file(df, merged_df):
    try:
        merged_df = pd.concat([merged_df,df], ignore_index=True)
        return merged_df
    except Exception as e:
        print(f"Error while attempting to merge {file} with merged_df")

# Function to Save the Merged Data Frame to CSV
def save_cleaned_df(df, file_name):
    try:
        print("Saving merged table to CSV file...")
        # Ensure the directory exists
        if not os.path.exists(direc):
            os.makedirs(direc)

        # Define the file name and path
        file_name = 'bike_trips_merged.csv'
        file_path = os.path.join(direc, file_name)

        # Check if the file already exists
        if os.path.exists(file_path):
            # Ask for user confirmation to overwrite the file
            overwrite = input(f"{file_path} already exists. Do you want to overwrite it? (yes/no): ")
            if overwrite.lower() != 'yes':
                print("File not overwritten.")
            else:
                # Save the DataFrame to a CSV file
                df.to_csv(file_path, index=False)
                print(f"DataFrame saved to {file_path}")
        else:
            # Save the DataFrame to a CSV file
            df.to_csv(file_path, index=False)
            print(f"DataFrame saved to {file_path}")

        del file_path
        return file_name
    except Exception as e:
        print(f"Unable to save {file_name} in submitted path: {e}")


# Have the User Identify the Bike_Data Folder Path
direc = ""

while direc == "":
    results = change_direc()
    direc = results[0]
    files = results[1]

merged_df = pd.DataFrame({'ride_id': pd.Series(dtype='str'),
                   'rideable_type': pd.Series(dtype='str'),
                   'started_at': pd.Series(dtype='datetime64[ns]'),
                   'ended_at': pd.Series(dtype='datetime64[ns]'),
                   'member_casual': pd.Series(dtype='int'),
                   'duration': pd.Series(dtype='int'),
                   'start_station_key': pd.Series(dtype='str'),
                   'end_station_key': pd.Series(dtype='str')})

print("Merging tables...")
# Adding tqdm progress bar to the loop
for file in tqdm(files, desc="Processing files"):
    # Load the current file in the loop
    try:
        df = load_file(file)
    except Exception as e:
        print(f"Unable to load {file} in submitted path: {e}")
        continue

    try:
        merged_df = merge_file(df, merged_df)
    except Exception as e:
        print(f"Unable to merge {file} in submitted path: {e}")
        continue

# Save Merged Data Frame
file_name = 'bike_trips_merged.csv'
try:
    save_cleaned_df(merged_df,file_name)
    print("File merging complete. You may now begin your analysis with bike_analysis.py")
except Exception as e:
    print("Unable to save merged file.")
