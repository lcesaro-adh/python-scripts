import pandas as pd
pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None

sourcefiles = ["persons.csv", "policies.csv", "claims.csv"]
    
keys = ["PERSON_ID", "POLICY_ID"]

path = "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/input/"

def readRidm(path, sourcefiles): 
    print("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(path + filename, dtype=object)
        print(filename)
    print(str(len(sourcefiles)) + " files loaded")
    return read_files

def double_df(df, list_columns): # Create new Ids from it
    for column in list_columns:
        for real in df.columns:
            print(column, "COLUMN")
            print(real, "REAL")
            if column==real:
                print("YESSSS")
                new_df = df.copy()
                new_df[column] = new_df[column] + "ABC"
                df = pd.concat([df, new_df])

# Read all tables
tableread = readRidm(path, sourcefiles)

# Asks how many times the enlargement is needed
times = int(
        input(
            "Please enter how many times you want to enlarge exponentially the initial datasets\n"
        )
    )
print(f"The enlargment will be performed by {times} times")

for time in range(times): # Enlarge times
    for table in tableread:  # For these tables
        double_df(tableread[table],keys)
    # Asks if the size are okay

    # If not okay decrease by percentage