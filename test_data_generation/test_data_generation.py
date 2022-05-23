import pandas as pd
import os
import logging

pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None
BYTE = 1000000

custom_path = os.getcwd()  # to be changed
path = custom_path + "/test_data_generation/rids/"
sourcefiles = ["claims.csv", "policies.csv", "persons.csv"]
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

def read_ridm(path, sourcefiles):
    """Read ridm and save as dictionary"""
    logging.warning("start read ridm from path: " + path)
    read_files = {}
    for filename in sourcefiles:
        read_files[filename.split(".")[0]] = pd.read_csv(path + filename, dtype=object)
        logging.warning(filename)
    logging.warning(str(len(sourcefiles)) + " files loaded")
    return read_files


def double_df(df, list_columns):
    """Enlarge df creating keys for the list_columns
    df -> type: Dataframe
    list_columns -> type: List"""
    df_copy = df.copy()
    for column in list_columns:
        for real_column in df.columns:
            if real_column == column:
                df_copy[column] = df_copy[column] + "_A"
    df = pd.concat([df, df_copy])
    return df


def ask_decrease():
    print("Enter 0 to increase the size, any other to decrease")
    decrease = int(input("How much do you want to decrease the size in percentage? "))
    return decrease


def log_size(df, table):
    """Log tablename and size in Mb
    df -> type: Dataframe
    table -> type: string
    """
    message = (table, "current size", df.memory_usage().sum() / BYTE, "Mb")
    logging.warning(message)


def decrease_tablesize(df, table, decrease):
    """Decrease df and save csv
    df -> type: Dataframe
    table -> type: string
    decrease -> type: int
    """
    total = len(df.index)
    toremove = round(total * decrease / 100)
    last = total - toremove
    data_decreased = df[:last]
    log_size(data_decreased, table)
    data_decreased.to_csv(
        ("{}/test_data_generation/rids/{}.csv").format(custom_path, table), index=False
    )


if __name__ == "__main__":
    # Read all tables
    tableread = read_ridm(path, sourcefiles)

    # Log size of initial tables
    for table in tableread:
        log_size(tableread[table], table)

    # asks decrease after telling how big is the table
    decrease = ask_decrease()

    # Double content of tables
    for table in tableread:
        df = double_df(tableread[table], keys)
        decrease_tablesize(df, table, decrease)