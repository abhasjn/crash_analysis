import json

import pandas as pd
from pyspark.sql import SparkSession


def get_spark_session(java_home, app_name):
    """
    Create a Spark session.
    
    Parameters:
    app_name (str): The name of the Spark application.

    Returns:
    SparkSession: A Spark session object.
    """

    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.executorEnv.JAVA_HOME", java_home) \
        .getOrCreate()
    return spark


def read_config(file_path):
    """
    Read a JSON configuration file.

    Parameters:
    file_path (str): The path to the JSON configuration file.

    Returns:
    dict: The configuration as a dictionary.
    """
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config


def read_csv(spark, path):
    """
    Read a CSV file into a Spark DataFrame.

    Parameters:
    spark (SparkSession): The Spark session.
    path (str): The path to the CSV file.

    Returns:
    DataFrame: A Spark DataFrame.
    """
    return spark.read.csv(path, header=True, inferSchema=True)


def save_df(df, path):
    """
    Save a Spark DataFrame to a CSV file.

    Parameters:
    df (DataFrame): The Spark DataFrame.
    path (str): The path to save the CSV file.
    """
    # df.coalesce(1).write.csv(path, header=True, mode='overwrite')

    # Convert Spark DataFrame to Pandas DataFrame
    if not isinstance(df, pd.DataFrame):
        pd_df = df.toPandas()
    else:
        pd_df = df
    pd_df.to_csv(path, index=False)
