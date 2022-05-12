import pandas as pd
pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None
byte = 1000000

path = "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/input/"
sourcefiles = ["claims.csv","policies.csv", "persons.csv"]
keys = ["ID", "PERSON_ID", "POLICY_ID", "CLAIM_ID", "PROVIDER_ID", "OE_ID", "BUSINESS_DIVISION_ID", "CLIENT_ID", "BUSINESS_PARTNER_ID", "BORDERAUX_ITEM_ID", "BUSINESS_RELATION_ID","CERTIFICATE_ID", "CONTRACT_ID", "LOCAL_POLICY_ID", "PRODUCT_ID", "LOCAL_PRODUCT_ID", "TERRITORY_ID", "COST_CONTAINMENT_BUNDLE_ID", "COST_CONTAINMENT_SUB_COVERAGE_ID", "SUB_COVERAGE_ID", "LOCAL_CLAIM_ID"]

def readRidm(path, sourcefiles): # Read ridm and save as dictionary
    print("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(path + filename, dtype=object)
        print(filename)
    print(str(len(sourcefiles)) + " files loaded")
    return read_files

def double_df(df, list_columns, table): # Enlarge only creating keys for the list_columns
    df_copy = df.copy()
    for column in list_columns:
            for real_column in df.columns:
                if real_column==column:
                    df_copy[column] = df_copy[column] + "_A"
    df = pd.concat([df, df_copy])
    df.reset_index(inplace=True, drop=True)
    print(df)
    return df

def ask_decrease():
    answer = input("Do you accept the current sizes? y/n: ")
    if answer == "n":
        decrease = int(
        input("How much do you want to decrease the size in percentage? "))
    return decrease

def decrease_tablesize(df, decrease):
    total = len(df.index)
    print(df.info())
    toremove = round(total * decrease / 100)
    last = total - toremove
    data_decreased = df[:last]
    print(
        "reduced. Current size",
        data_decreased.memory_usage().sum() / byte,
        "Mb",
    )
    #data_decreased.to_csv(("test_data_generation/output/{}.csv").format(table))
        
# Read all tables
tableread = readRidm(path, sourcefiles)

def round_table(tr):
    df = None
    for table in tr:  # For these tables
        print('running',table)
        print(keys)
        df = double_df(tableread[table],keys, table)
    return df

decrease = ask_decrease()

df = round_table(tableread)
print(df)

print(df, 'decrease table')
decrease_tablesize(df, decrease)
    # Asks if the size are okay
    # If not okay decrease by percentage