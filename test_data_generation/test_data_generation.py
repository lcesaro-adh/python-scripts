import pandas as pd

pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None
byte = 1000000

path = "/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/rids/"
sourcefiles = [
    "providers.csv",
    "diagnosis.csv",
    "treatments.csv",
    "claims.csv",
    "policies.csv",
    "persons.csv",
    "personPolicyAssociations.csv",
    "subCoverages.csv",
    "contracts.csv",
    "operationalEntities.csv",
    "businessRelations.csv",
    "products.csv",
    "exchangeRates.csv",
    "clients.csv",
    "clientEntities.csv",
    "certificates.csv",
    "retailClients.csv",
    "cashflows.csv",
    "territories.csv",
    "uwTerritoryAssociations.csv",
    "borderauxItems.csv",
    "businessDivisions.csv",
    "productSubCovAssociations.csv",
    "costContainmentProducts.csv",
    "costContainmentBundles.csv",
    "costContainmentSubCoverages.csv",
]
keys = [
    "ID",
    "PERSON_ID",
    "POLICY_ID",
    "CLAIM_ID",
    "PROVIDER_ID",
    "OE_ID",
    "BUSINESS_DIVISION_ID",
    "CLIENT_ID",
    "BUSINESS_PARTNER_ID",
    "BORDERAUX_ITEM_ID",
    "BUSINESS_RELATION_ID",
    "CERTIFICATE_ID",
    "CONTRACT_ID",
    "LOCAL_POLICY_ID",
    "PRODUCT_ID",
    "LOCAL_PRODUCT_ID",
    "TERRITORY_ID",
    "COST_CONTAINMENT_BUNDLE_ID",
    "COST_CONTAINMENT_SUB_COVERAGE_ID",
    "SUB_COVERAGE_ID",
    "LOCAL_CLAIM_ID",
]


def readRidm(path, sourcefiles):  # Read ridm and save as dictionary
    print("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(
            path + filename, dtype=object, index_col=0
        )
        print(filename)
    print(str(len(sourcefiles)) + " files loaded")
    return read_files


def double_df(df, list_columns):  # Enlarge df creating keys for the list_columns
    df_copy = df.copy()
    for column in list_columns:
        for real_column in df.columns:
            if real_column == column:
                df_copy[column] = df_copy[column] + "_A"
    df = pd.concat([df, df_copy])
    df.reset_index(inplace=True, drop=True)
    # print(df)
    return df


def ask_decrease():
    print("Enter 0 to increase the size, any other to decrease")
    decrease = int(input("How much do you want to decrease the size in percentage? "))
    return decrease


def log_size(df, table):
    print(
        table,
        "current size",
        df.memory_usage().sum() / byte,
        "Mb",
    )


def decrease_tablesize(df, table, decrease):  # Decrease df and save csv
    total = len(df.index)
    toremove = round(total * decrease / 100)
    last = total - toremove
    data_decreased = df[:last]
    log_size(data_decreased, table)
    data_decreased.to_csv(("test_data_generation/rids/{}.csv").format(table))


# Read all tables
tableread = readRidm(path, sourcefiles)

for table in tableread:
    log_size(tableread[table], table)

# asks decrease after telling how big is the table
decrease = ask_decrease()  # ask decrease before because of logic issue

# Double content of tables
for table in tableread:
    df = double_df(tableread[table], keys)
    print("Saving...")
    decrease_tablesize(df, table, decrease)
