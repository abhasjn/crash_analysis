import os
import shutil

import pandas as pd

from analysis.analysis import CarCrashAnalysis
from utils.io import get_spark_session, read_config, read_csv, save_df

APP_NAME = "CarCrashAnalysis"


def run_analysis(analysis_num="all"):
    """
    Main function to run the car crash analysis.
    """

    input_config = read_config('config/input_config.json')
    output_config = read_config('config/output_config.json')
    java_home = input_config['java']['java_home']

    spark = get_spark_session(java_home, APP_NAME)

    folder_name = "output"

    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' created successfully.")

    try:
        # Read all necessary CSV files into Spark DataFrames
        data_paths = {
            "charges": input_config["charges_data"],
            "damages": input_config["damages_data"],
            "endorse": input_config["endorse_data"],
            "primary_person": input_config["primary_person_data"],
            "restrict": input_config["restrict_data"],
            "units": input_config["units_data"],
        }

        dataframes = {name: read_csv(spark, path) for name, path in data_paths.items()}

        # Initialize analysis class
        analysis = CarCrashAnalysis(dataframes['charges'], dataframes['damages'], dataframes['endorse'],
                                    dataframes['primary_person'], dataframes['restrict'], dataframes['units'])

        # List to store results of single value analyses
        single_value_results = []

        if isinstance(analysis_num, str) and analysis_num == "all":
            for i in range(1, 11):
                analysis.add_analysis(i, single_value_results, output_config)
        else:
            for i in analysis_num:
                analysis.add_analysis(int(i), single_value_results, output_config)

        # Saving single value results
        answers_df = pd.DataFrame(single_value_results)
        save_df(answers_df, output_config['single_value_answers'])

    finally:
        spark.stop()


if __name__ == "__main__":
    run_analysis()
