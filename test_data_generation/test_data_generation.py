import os
from tempfile import TemporaryDirectory

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession

from tasks.common.utilities.io import (
    get_directory_path_from_file_path,
    get_directory_path_x_levels_up_from_file_path,
)
from tasks.logs.logger import Logger

logger = Logger()

SRC_DIRECTORY = f"{get_directory_path_x_levels_up_from_file_path(__file__, 5)}Allianz/Projects/adhdh-semantic-layer-aggregate-model/test_data_generation/"
_THIS_DIRECTORY_PATH = get_directory_path_from_file_path(__file__)

# For the test I need
# src directory with very small amount of data ( 1 line is okay )
# expected output with the command already ran with the data

def test_generate(spark_session: SparkSession) -> None:
    # Arrange
    with TemporaryDirectory() as temp_dir:
        command = (
            f" python {SRC_DIRECTORY}data_generation.py -i /Users/ludovicocesaro/Desktop/Files/Reply/Allianz/Projects/adhdh-semantic-layer-aggregate-model/tests/test_data_generation/start_test_ridm/ -o {temp_dir}/ -a inc -am 1"
        )
        print(temp_dir, "TEMP_DIR")
        expected_output_file_path = f"{_THIS_DIRECTORY_PATH}ridm/claims.csv"
        print(expected_output_file_path, "EXP_OUT")
        expected_output_df = pd.read_csv(expected_output_file_path)

        # Act
        os.system(command)
        # Assert
        result_output_file_path = f"{temp_dir}/claims.csv"
        print(result_output_file_path)
        result_df = pd.read_csv(result_output_file_path)
        pd.testing.assert_frame_equal(result_df, expected_output_df, check_dtype=False)
