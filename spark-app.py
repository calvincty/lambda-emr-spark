#!/usr/bin/python

import sys
from pyspark.sql import SparkSession
import pyspark.sql.types as T

def convert(input, output):
    """CSV2Parq transformation."""
    spark = SparkSession.builder.appName('CSV2Parq demo').getOrCreate()
    df_data = spark.read.csv(
        input,
        header=True,
        sep=','
    )
    df_data.write.parquet(output, mode='overwrite')
    # df_data.write.csv(output, sep='\t', compression='gzip', mode='overwrite')

if __name__ == "__main__":
    convert(sys.argv[1], sys.argv[2])