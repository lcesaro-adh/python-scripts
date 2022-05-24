import pandas as pd
import os
import logging

pd.set_option("display.max_columns", None)
pd.options.mode.chained_assignment = None
BYTE = 1000000

custom_path = os.getcwd()
path = custom_path + "/test_data_generation/ridm/"
sourcefiles = ["providers.csv", "diagnosis.csv", "treatments.csv", "claims.csv",
                    "policies.csv", "persons.csv", "personPolicyAssociations.csv",
                    "subCoverages.csv", "contracts.csv", "operationalEntities.csv", "businessRelations.csv", "products.csv",
                    "exchangeRates.csv", "clients.csv", "clientEntities.csv", "certificates.csv", "retailClients.csv", "cashflows.csv", "territories.csv",
                    "uwTerritoryAssociations.csv", "borderauxItems.csv", "businessDivisions.csv", "productSubCovAssociations.csv", "costContainmentProducts.csv",
                    "costContainmentBundles.csv", "costContainmentSubCoverages.csv"
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


def ask_operation():
    answer = input("Do you want to increase (inc) or decrease (dec) the current size?")
    return answer


def log_size(df, table):
    """Log tablename and size in Mb
    df -> type: Dataframe
    table -> type: string
    """
    message = (table, "current size", df.memory_usage().sum() / BYTE, "Mb")
    logging.warning(message)


def increase_tablesize(df, table, increase):
    """Increase df and save csv
    df -> type: Dataframe
    table -> type: string
    increase -> type: int"""
    for i in range(increase):
        if i == 0:
            df = tablesread[table]
        df = double_df(df, keys)
        log_size(df, table)
        df.to_csv(
            ("{}/test_data_generation/ridm/{}.csv").format(custom_path, table),
            index=False,
        )


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
        ("{}/test_data_generation/ridm/{}.csv").format(custom_path, table), index=False
    )

if __name__ == "__main__":
    tablesread = read_ridm(path, sourcefiles)

    for table in tablesread:
        log_size(tablesread[table], table)

    answer = ask_operation()
    if answer == "inc":
        increase = int(input("How many times you want to increase?"))
        for table in tablesread:
            increase_tablesize(tablesread[table], table, increase)
    else:
        decrease = int(
            input("How much do you want to decrease the sizes? (in percentage)")
        )
        for table in tablesread:
            decrease_tablesize(tablesread[table], table, decrease)
