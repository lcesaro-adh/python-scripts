import pandas as pd
pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None

path = "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/input/"
sourcefiles = ["claims.csv","policies.csv", "persons.csv", "personPolicyAssociations.csv", "diagnosis.csv", "providers.csv"]
keys = ["ID", "PERSON_ID", "POLICY_ID", "CLAIM_ID", "PROVIDER_ID"]

def readRidm(path, sourcefiles): # Read ridm and save as dictionary
    print("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(path + filename, dtype=object)
        print(filename)
    print(str(len(sourcefiles)) + " files loaded")
    return read_files

def double_df(df, list_columns): # Enlarge only creating keys for the list_columns
    df_copy = df.copy()
    for column in list_columns:
            for real_column in df.columns:
                print(real_column, column)
                if real_column==column:
                    df_copy[column] = df_copy[column] + "ABC"
    df = pd.concat([df, df_copy])
    df.reset_index(inplace=True, drop=True)
    print(df, 'df')

def decrease_tablesize():
    print('decrease')

# Read all tables
tableread = readRidm(path, sourcefiles)

# # Asks how many times the enlargement is needed
# times = int(
#         input(
#             "Please enter how many times you want to enlarge exponentially the initial datasets\n"
#         )
#     )
# print(f"The enlargment will be performed by {times} times")
# for time in range(times): # Enlarge times

for table in tableread:  # For these tables
    print('running',table)
    double_df(tableread[table],keys)

    # Asks if the size are okay

    # If not okay decrease by percentage