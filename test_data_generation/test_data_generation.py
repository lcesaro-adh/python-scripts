import pandas as pd
pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None

sourcefiles = ["claims.csv","policies.csv", "persons.csv"]
    
keys = ["ID", "PERSON_ID", "POLICY_ID"]

path = "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/input/"

def readRidm(path, sourcefiles): # Read ridm and save as dictionary
    print("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(path + filename, dtype=object, index_col=[0])
        print(filename)
    print(str(len(sourcefiles)) + " files loaded")
    return read_files

def double_df(df, list_columns): # Enlarge only creating keys for the list_columns
        df_copy = df.copy()
        for column in list_columns:
            for real_column in df.columns:
                if real_column==column:
                    print('enlarging')
                    df_copy[column] = df_copy[column] + "ABC"
        df = pd.concat([df, df_copy])
        df.reset_index(inplace=True)
        df.drop(['index'], axis=1, inplace=True)
        print(df, 'df')

def match_f_keys(df, list_columns):
    for column in list_columns:
        for real in df.columns:
            if real==column:
                print('enlarging')
                # df_copy = df.copy()
                # df_copy[column] = df_copy[column] + "ABC"
                # df = pd.concat([df, df_copy])
                # df.reset_index(inplace=True)
                # df.drop(['index'], axis=1, inplace=True)
                # print(df, 'df')

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