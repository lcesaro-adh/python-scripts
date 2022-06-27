import os
from tempfile import TemporaryDirectory
import pandas as pd
from pyspark.sql import SparkSession

from tasks.common.utilities.io import (
    get_directory_path_from_file_path,
    get_directory_path_x_levels_up_from_file_path,
)
from tasks.logs.logger import Logger

logger = Logger()

SRC_DIRECTORY = f"{get_directory_path_x_levels_up_from_file_path(__file__, 2)}/test_data_generation/"
_THIS_DIRECTORY_PATH = get_directory_path_from_file_path(__file__)

def test_generate(spark_session: SparkSession) -> None:
    # Arrange
    with TemporaryDirectory() as temp_dir:
        command = (
            f" python {SRC_DIRECTORY}data_generation.py -i {_THIS_DIRECTORY_PATH}start_test_ridm/ -o {temp_dir}/ -a inc -am 1"
        )
        expected_output_file_path = f"{_THIS_DIRECTORY_PATH}ridm/claims.csv"
        expected_output_df = pd.read_csv(expected_output_file_path)
        # Act
        os.system(command)
        # Assert
        result_output_file_path = f"{temp_dir}/claims.csv"
        print(result_output_file_path)
        result_df = pd.read_csv(result_output_file_path)
        pd.testing.assert_frame_equal(result_df, expected_output_df, check_dtype=False)
