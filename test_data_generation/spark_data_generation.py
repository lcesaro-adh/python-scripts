import string
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import desc
import click
from pyspark import SparkContext
from tasks.logs.logger import Logger
from tasks.common.pyspark.loading import load_dataset
from tasks.common.pyspark.saving import save_dataset
from tasks.common.ridm.ridm_spec_processing import get_ridm_spec

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
    "--output", "-o", required=True, type=str, help="Location folder where to output (should be the same as input)"
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
@click.option("--context", "-c", default="slamApp", type=str, help="Spark context name")
def main(input: str, output: str, action: str, amount: int, context: str) -> None:
    spark = SparkSession.builder.appName(context).getOrCreate()
    path = input
    output_path = output
    sourcefiles = ["claims"]#, "policies", "persons"] # temporarily removed to make test works

    ridm_relations = get_ridm_spec(DEFAULT_RIDM_VERSION)

    keys_list = []
    for a in ridm_relations[1]:
        key = ridm_relations[1][a]['right_field']
        keys_list.append(key)
    keys_list.append("ID")  # force append ID because key missing
    print(keys_list)

    tablesread = read_ridm(input, path, sourcefiles, spark)

    if action == "inc":
        for table in tablesread:
            increase_tablesize(tablesread[table], table, amount, tablesread, keys_list, output_path)
    else:
        for table in tablesread:
            decrease_tablesize(tablesread[table], table, amount, output_path)

def read_ridm(input: str, path: str, sourcefiles: dict, spark: SparkContext):
    """Read ridm and save as dictionary"""
    # logger._logger_technical.info("start reading ridm from path:"+path)
    print("start reading ridm from part:" + path)
    input_tables = {
        input: sourcefiles,
    }
    read_files = {}
    for filename in sourcefiles:
        read_files = load_dataset(input_tables, spark, "<NA>")
        #logger._logger_technical.info(filename)
        print(filename)
    #logger._logger_technical.info(str(len(sourcefiles))+"files loaded")
    print(str(len(sourcefiles)) + " files loaded")
    return read_files

def double_df(df: SparkDataFrame, list_columns: list):
    """Enlarge df creating keys for the list_columns"""
    df_copy = df.select("*")
    for column in list_columns:
        for real_column in df.columns:
            if real_column == column:

                #df_copy[column] = df_copy[column] + "_A"
                df_copy = df_copy.withColumn(  # !!- Matching does not work fine
                    column,
                    F.concat(
                        F.expr(f"substring({column}, 0, length({column}))"),
                        F.lit("_A"),
                        F.substring(f"{column}", 0, 0),
                    ),
                )

               #df_copy[column] = df_copy.select(concat(col(column), lit("_A"))).withColumnRenamed(f"concat({column},_A)", column)
               # above line not working -> TypeError: 'DataFrame' object does not support item assignment

    df = df.union(df_copy)
    return df

def increase_tablesize(df: SparkDataFrame, table: str, increase: int, tablesread: dict, keys_list: list, output_path: str):
    """Increase df and save csv in the respective folder"""
    for i in range(increase):
        if i == 0:
            df = tablesread[table]
        df = double_df(df, keys_list)
        df = df.sort(desc("ID"))

        table_dict = {table: df}
        mod_path = output_path + table
        save_dataset(table_dict, mod_path, "")

        print("increased and saved correctly")
        # logger._logger_technical.info(f"{table}increased and saved correctly")

def decrease_tablesize(df: SparkDataFrame, table: string, decrease: int, output_path: str):
    """Decrease df and save csv in its folder"""
    total = df.count()
    toremove = round(total * decrease / 100)
    last = total - toremove
    data_decreased = df.withColumn("index", F.monotonically_increasing_id())
    data_decreased = data_decreased.sort("index").limit(last)
    data_decreased = data_decreased.sort(desc("ID"))

    table_dict = {table: data_decreased}
    mod_path = output_path + table
    save_dataset(table_dict, mod_path, "")

    print("decreased and saved correctly")


if __name__ == "__main__":
    """
    Script to generate/reduce RIDMs size
    Given the path to a directory containing a defined subset of RIDM tables,
    the operation wanted to perform and the amount it enlarges/reduces the
    content of the folder and saves it

    Example increment:
    python data_generation.py -input /ridm/ -action inc -amount 1

    Example decrement:
    python data_generation.py -input /ridm/ -action dec -amount 50
    """
    main()
