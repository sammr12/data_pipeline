import os
import sqlite3

# Function to Choose and Change direcectory
def change_direc():
    direc = ""
    while direc == "":
        direc = input("Enter the path of the directory with the Bike Data and Database: ")
        print("The entered path does not point to the 'Bike_Data' folder.")

        if not os.path.exists(direc):
            print("The entered path does not exist.")
            direc = ""

    try: # Try changing Working Directory
        os.chdir(direc)
        print("New Working Directory: ", os.getcwd())
        files = os.listdir(direc)
        files = [f for f in files if os.path.isfile(direc+'/'+f)]
        files.sort()
        print("The folder contains the following files: ",*files, sep="\n")

        return direc, files
    
    except Exception as e:
        print("Unable to change working directory")
        direc = ""

# Function to Select Files to Write to DB
def select_files(direc, files):
    files = [f for f in files if os.path.isfile(direc+'/'+f) and f.endswith('.csv') and 'divvy' in f.lower()]
    add_another = True
    selected_files = list()

    while add_another == True:
        print("The following files are available: ")
        for index, item in enumerate(files, start=1):
            print(f"[{index}] {item}")

        file_input = input("Enter the name or number of the file to write to the database (Type 'all' to select all files): ")

        if file_input.lower() == 'all':
            selected_files = files
            break

        if file_input.isdigit():
            file_index = int(file_input)-1 # Convert to 0-based index
            if 0 <= file_index < len(files):
                file_name = files[file_index]
            else:
                print("Invalid file number. Please select a valid file.")
                continue
        elif file_input in files:
            file_name = file_input
        else: 
            print("The selected file does not exist. Please select a valid file. (Type 'done' to stop selecting files): ")
            continue

        print(f"File name: {file_name}")
        selected_files.append(file_name)
        files.remove(file_name)  # Remove the Selected File from the List of Files

        if files:
            add_another = input("Do you want to add another file? (yes/no): ").lower() in ['yes', 'y']
        else:
            print("No more files to select.")
            add_another = False
        
    print("Selected Files: ",*selected_files, sep="\n")

    return selected_files

# Function to Create a New Database
def new_db(direc):
    conn = None
    overwrite_file = False
    new_db_name = input("Enter the name of the new database: ")
    new_db_name_pieces = new_db_name.strip().lower().split('.')
    if len(new_db_name_pieces) > 1:
        new_db_name = new_db_name_pieces[0]
    else:
        new_db_name = new_db_name.strip().lower()
    
    try:
        if not os.path.exists(direc): # Ensure the directory exists
            os.makedirs(direc)

        # Define the file name and path
        file_name = new_db_name+'.sqlite'

        file_path = os.path.join(direc, file_name)

        if os.path.exists(file_path): # Check if the file already exists

            overwrite = input(f"{file_path} already exists. Do you want to overwrite it? (yes/no): ") # Ask for user confirmation to overwrite the file
            if overwrite.lower() != 'yes':
                print("File not overwritten.")
            else:
                overwrite_file = True
                
        elif not os.path.exists(file_path) or overwrite_file == True:
            
            print(f"Saving new database...")
            conn = sqlite3.connect(file_path)
            cur = conn.cursor()
            print("New Database Created")
            

        return file_name
    except Exception as e:
        print(f"Error creating new database: {e}")

# Function to Select an Existing Database
def select_db(files, direc):
    files = [f for f in files if os.path.isfile(direc+'/'+f) and f.endswith('.sqlite')]
    
    db_name = ""

    while db_name == "":
        print("The following databases are available: ",*files, sep="\n")
        db_name = input("Enter the name of the database to write to: ")

        if not db_name.endswith('.sqlite'):
            print("The database name must end with '.sqlite'")
            db_name = ""
        if db_name not in files:
            print("The selected database does not exist!")
            choice = input("Do you want to select another database or create a new one? (new/existing): ")

            if choice.lower() == 'new':
                db_name = new_db(direc)
            else:
                db_name = ""
        
        else:
            print(f"Database name: {db_name}")
    
    return db_name

# Function to Connect to the Database
def connect_db(db_name):
    try:
        print(f"Connecting to {db_name}...")
        conn = sqlite3.connect(db_name)
        cur = conn.cursor()
        print("Connection Successful")

        return cur, conn
    except Exception as e:
        print(f"Error connecting to {db_name}: {e}")
        exit()

def check_table (cur, conn):

    table_name = 'rides'
    table_connected = False

    try:
        cur.executescript(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                ride_id TEXT PRIMARY KEY,
                rideable_type TEXT,
                started_at DATETIME,
                ended_at DATETIME,
                member_casual TEXT,
                duration_sec INTEGER,
                duration_min FLOAT,
                start_station_key TEXT,
                end_station_key TEXT
        );
        ''')

        conn.commit()
        cur.close()
        conn.close

        table_connected = True
        print(f"Table {table_name} created successfully")
        return table_connected
    
    except Exception as e:
        print(f"Error while creating table {table_name}: {e}")

# Function to Write the Selected Files to the Selected Database
def write_to_db(file,db_name):
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    try:
        handle = open(file)
        print(f"Writing {file} to {db_name}...")
        i = 0
        for line in handle:
            i += 1
            if i != 1: # Skip the Header Row

                line = line.strip();
                pieces = line.split(',')
                if len(pieces) < 9 : continue

                ride_id = pieces[0]
                rideable_type = pieces[1]
                started_at = pieces[2]
                ended_at = pieces[3]
                member_casual = pieces[4]
                duration_sec = pieces[5]
                duration_min = pieces[6]
                start_station_key = pieces[7]
                end_station_key = pieces[8]

                cur.execute('''INSERT OR IGNORE INTO Rides (ride_id, rideable_type, started_at, ended_at, member_casual, duration_sec, duration_min, start_station_key, end_station_key) 
                    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ? )''', (ride_id, rideable_type, started_at, ended_at, member_casual, duration_sec, duration_min, start_station_key, end_station_key) )

        conn.commit()
        print(f"Inserted {i-1} rows into the database.")
    except Exception as e:
        print("An error occurred:", e)
        conn.rollback()  # Rollback in case of error
    finally:
        handle.close()  # Ensure the file is closed in any case


direc = ""
db_name = ""
table_connected = False
files = list()

while db_name == "":

    while direc == "":
        results = change_direc() # Prompt the User to Select a Folder
        direc = results[0]
        files = results[1]
        

    selected_files = select_files(direc, files) # Prompt the User to Select Files to Write to DB

    if len(selected_files) == 0:
        print("No files selected. Exiting program...")
    
    choice = input("Do you want to write to a new database or an existing database? (new/existing): ")
    if choice.lower() == 'new':
        db_name = new_db(direc) # Prompt the User to Create a New Database
    else:
        db_name = select_db(files, direc) # Prompt the User to Select a Database
    
    if db_name == "":
        print("No database selected. Exiting program...")
        exit()
    else:
        print(f"Selected Database: {db_name}")


db_results = connect_db(db_name) # Connect to the Database
cur = db_results[0]
conn = db_results[1]

table_connected = check_table(cur, conn) # Check if the Table Exists

if table_connected == False:
    print("No table connected. Exiting program...")
    conn.close()
    exit()

for file in selected_files:
    write_to_db(file, db_name) # Write the Selected Files to the Selected Database

print("All files written to the database. Exiting program...")
conn.close() # Close the Connection to the Database