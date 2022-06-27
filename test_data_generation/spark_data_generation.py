import string
from typing import List
import click
import pyspark.sql.functions as F
from pyspark import SparkContext
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, desc, lit
from tasks.common.pyspark.loading import load_dataset
from tasks.common.pyspark.saving import save_dataset
from tasks.common.ridm.ridm_spec_processing import get_ridm_spec
from tasks.logs.logger import Logger

DEFAULT_RIDM_VERSION = "v1"
logger = Logger()

@click.command()
@click.option(
    "--input",
    "-i",
    required=True,
    type=str,
    help="Input folder with the base ridm to start",
)
@click.option(
    "--output", "-o", required=True, type=str, help="Location folder where to output"
)
@click.option(
    "--action", "-a", required=True, type=str, help="Action you want to do inc/dec"
)
@click.option(
    "--amount",
    "-am",
    required=True,
    type=int,
    help="Quantity enlargement(times)/decrease(percentage)",
)
@click.option(
    "--test",
    "-t",
    is_flag=True,
    default=False,
    help="Set flag to true to run test case.",
)
@click.option("--context", "-c", default="slamApp", type=str, help="Spark context name")
def main(
    input: str, output: str, action: str, amount: int, test: bool, context: str
) -> None:
    spark = SparkSession.builder.appName(context).getOrCreate()

    if test:
        sourcefiles = ["claims"]
    else:
        sourcefiles = [
            "providers",
            "diagnosis",
            "treatments",
            "claims",
            "policies",
            "persons",
            "personPolicyAssociations",
            "subCoverages",
            "contracts",
            "operationalEntities",
            "businessRelations",
            "products",
            "exchangeRates",
            "clients",
            "clientEntities",
            "certificates",
            "retailClients",
            "cashflows",
            "territories",
            "uwTerritoryAssociations",
            "borderauxItems",
            "businessDivisions",
            "productSubCovAssociations",
            "costContainmentProducts",
            "costContainmentBundles",
            "costContainmentSubCoverages",
        ]

    ridm_relations = get_ridm_spec(DEFAULT_RIDM_VERSION)
    keys_list = []
    for a in ridm_relations[1]:
        key = ridm_relations[1][a]["right_field"]
        keys_list.append(key)
    keys_list.append("ID")  # force append ID because ID key missing
    keys_list = list(dict.fromkeys(keys_list))  # removing double keys

    tables_read = read_ridm(input, output, sourcefiles, spark)

    if action == "inc":
        for table in tables_read:
            increase_tablesize(
                tables_read[table], table, amount, tables_read, keys_list, output
            )
    else:
        for table in tables_read:
            decrease_tablesize(tables_read[table], table, amount, output)


def read_ridm(input: str, path: str, sourcefiles: dict, spark: SparkContext):
    """Read ridm and save as dictionary"""
    logger._logger_technical.info("start reading ridm from path:"+path)
    input_tables = {
        input: sourcefiles,
    }
    read_files = {}
    for filename in sourcefiles:
        read_files = load_dataset(input_tables, spark, "<NA>")
        logger._logger_technical.info(filename)
    logger._logger_technical.info(str(len(sourcefiles))+"files loaded")
    return read_files


def double_df(df: SparkDataFrame, list_columns: List[str]):
    """Enlarge the size of the df creating new keys for the matching"""
    df_copy = df.select("*")
    for column in list_columns:
        for real_column in df.columns:
            if real_column == column:
                add_char = [column, lit("_A")]
                df_copy = df_copy.withColumn(column, concat(*add_char))
    df = df.union(df_copy)
    return df


def increase_tablesize(
    df: SparkDataFrame,
    table: str,
    increase: int,
    tablesread: dict,
    keys_list: list,
    output: str,
):
    """Increase the size of the df and saves its csv in the output folder"""
    for i in range(increase):
        if i == 0:
            df = tablesread[table]
        df = double_df(df, keys_list)
        if "ID".upper() in (name.upper() for name in df.columns):
            df = df.sort(desc("ID"))
        table_dict = {table: df}
        mod_path = output + table
        save_dataset(table_dict, mod_path, "")
        logger._logger_technical.info(f"{table}increased and saved correctly")


def decrease_tablesize(
    df: SparkDataFrame, table: string, decrease: int, output: str
):
    """Decrease the size of the df by percentage and saves its csv in the output folder"""
    total = df.count()
    to_remove = round(total * decrease / 100)
    last = total - to_remove
    data_decreased = df.withColumn("index", F.monotonically_increasing_id())
    data_decreased = data_decreased.sort("index").limit(last)
    if "ID" in (name.upper() for name in df.columns):
        data_decreased = data_decreased.sort(desc("ID"))
    table_dict = {table: data_decreased}
    mod_path = output + table
    save_dataset(table_dict, mod_path, "")
    logger._logger_technical.info(f"{table}decreased and saved correctly")


if __name__ == "__main__":
    """
    Script to generate/reduce RIDMs size with the matchings
    Given the path to a directory containing a defined subset of RIDM tables,
    an output folder, the operation and the amount wanted it enlarges/reduces the input folder and then saves it

    Example increment by 1 time :
    python data_generation.py -input /ridm/ -output /output_ridm/ -action inc -amount 1

    Example decrement by 50% :
    python data_generation.py -input /ridm/ -output /output_ridm/ -action dec -amount 50
    """
    main()
