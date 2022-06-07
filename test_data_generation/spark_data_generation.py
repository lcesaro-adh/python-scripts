import string
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import DataFrame as SparkDataFrame
import os, glob
import logging
import click

@click.command()
@click.option("--action", "-a", required=True, type=str, help="Action you want to do inc/dec")
@click.option(
    "--amount", "-a", required=True, type=int, help="Quantity of enlargement/decrease"
)
@click.option("--context", "-c", default="slamApp", type=str, help="Spark context name")
def main(action: str, amount: int, context: str) -> None:
    spark = SparkSession.builder.appName(context).getOrCreate()
    generate(action, amount, spark)

def generate(action: str, amount: int, spark: str):
    custom_path = os.getcwd()

    BYTE = 1000000
    path = custom_path + "/ridm/"
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


    def read_ridm(path: str, sourcefiles: dict):
        """Read ridm and save as dictionary
        """
        logging.warning("start read ridm from path: " + path)
        read_files = {}
        for filename in sourcefiles:
            read_files[filename.split(".")[0]] = (
                spark.read.option("delimiter", ";")
                .option("header", True)
                .csv(path + filename)
            )
            logging.warning(filename)
        logging.warning(str(len(sourcefiles)) + " files loaded")
        return read_files


    def double_df(df: SparkDataFrame, list_columns: list):
        """Enlarge df creating keys for the list_columns
        """
        print(df)
        # df_copy = df.copy()
        df_copy = df.select("*")
        for column in list_columns:
            for real_column in df.columns:
                if real_column == column:
                    df_copy[column] = df_copy[column] + "_A"
        # df = pd.concat([df, df_copy])
        df = df.union(df_copy)
        return df

    def log_size(df: SparkDataFrame, table: str):
        """Log tablename and size in Mb
        """
        df = df.toPandas()  # !--- still Pandas for logging size
        message = (table, "current size", df.memory_usage().sum() / BYTE, "Mb")
        logging.warning(message)


    def increase_tablesize(df: SparkDataFrame, table: str, increase: int):
        """Increase df and save csv in the respective folder
        """
        for i in range(increase):
            if i == 0:
                df = tablesread[table]
            df = double_df(df, keys)
            log_size(df, table)

            df.coalesce(1).write.format("csv").mode("overwrite").options(
                header="true"
            ).save(path=f"{custom_path}/{table}")
            os.chdir(
                f"/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/{table}"
            )
            for file in glob.glob("*.csv"):
                os.rename(file, f"{table}.csv")

            print("increased and saved correctly")


    def decrease_tablesize(df: SparkDataFrame, table: string, decrease: int):
        """Decrease df and save csv in its folder"""
        total = df.count()
        toremove = round(total * decrease / 100)
        last = total - toremove
        data_decreased = df.withColumn(
            "index", f.monotonically_increasing_id()
        )
        data_decreased = data_decreased.sort("index").limit(last)
        print(data_decreased.count())
        log_size(data_decreased, table)

        data_decreased.coalesce(1).write.format("csv").mode("overwrite").options(header="true").save(
            path=f"{custom_path}/{table}"
        )
        os.chdir(
            f"/Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Other/Scripts/test_data/test_data_generation/{table}"
        )
        for file in glob.glob("*.csv"):
            os.rename(file, f"{table}.csv")

    print("decreased and saved correctly")

    tablesread = read_ridm(path, sourcefiles)

    for table in tablesread:
        log_size(tablesread[table], table)

    #answer = ask_operation()
    if action == "inc":
        #increase = int(input("How many times you want to increase?"))
        for table in tablesread:
            increase_tablesize(tablesread[table], table, amount)
    else:
        #decrease = int(input("How much do you want to decrease the sizes? (in percentage)"))
        for table in tablesread:
            decrease_tablesize(tablesread[table], table, amount)

if __name__ == "__main__":
    main()
