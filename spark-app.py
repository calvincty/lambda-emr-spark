#!/usr/bin/python

import argparse 
from pyspark.sql import SparkSession
import pyspark.sql.types as T

def parse_args():
    """Parse comman line arguements."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", help="Input", required=True)
    parser.add_argument("-o", "--output", help="Output", required=True)

    args = parser.parse_args()

    return args

def convert(input, output):
    """CSV2Parq transformation."""
    spark = SparkSession.builder.appName('CSV2Parq demo').getOrCreate()
    df_data = spark.read.csv(
        input,
        header=True,
        sep=',',
        schema=T.StructType([
            T.StructField('unit_id', T.StringType()),
            T.StructField('relevance', T.DoubleType()),
            T.StructField('relevance_variance', T.DoubleType()),
            T.StructField('product_image', T.StringType()),
            T.StructField('product_link', T.StringType()),
            T.StructField('product_price', T.DoubleType()),
            T.StructField('product_title', T.StringType()),
            T.StructField('query', T.StringType()),
            T.StructField('rank', T.IntegerType()),
            T.StructField('source', T.StringType()),
            T.StructField('url', T.StringType()),
            T.StructField('product_description', T.StringType())]
        )
    )
    df_data.write.parquet(output, mode='overwrite')
    # df_data.write.csv(output, sep='\t', compression='gzip', mode='overwrite')

if __name__ == "__main__":
    args = parse_args()
    convert(args.input, args.output)