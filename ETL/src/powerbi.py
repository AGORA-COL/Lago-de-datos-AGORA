'''
ETL de PowerBI
Autor: Pedro Fabian Perez Arteaga
Descripción: Este script toma datos desde stagedata en HDFS y genera tablas analíticas para PowerBI en analyticsdata/powerbi
'''

import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring


def load_config(config_path="config/powerbi.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def create_spark_session(app_name="ETL PowerBI"):
    return SparkSession.builder.appName(app_name).getOrCreate()


def process_nacimientos(df, filters):
    df = df.withColumn("anonacimiento", substring(col("fechanacimiento"), 1, 4))
    if filters:
        min_year = filters.get("year_min")
        max_year = filters.get("year_max")
        if min_year:
            df = df.filter(col("anonacimiento") >= str(min_year))
        if max_year:
            df = df.filter(col("anonacimiento") <= str(max_year))
    return df


def main():
    config = load_config()
    spark = create_spark_session()

    for source in config["sources"]:
        if not source.get("process", False):
            continue

        print(f"Procesando fuente: {source['name']}")
        df = spark.read.parquet(source["input_path"])

        if source.get("records_to_process") != "all":
            df = df.limit(int(source["records_to_process"]))

        if source['name'] == "nacimientos":
            df = process_nacimientos(df, source.get("filters"))

        output_path = source["output_path"]
        partition_col = source.get("partition_by")

        if partition_col:
            df.write.mode("overwrite").partitionBy(partition_col).parquet(output_path)
        else:
            df.write.mode("overwrite").parquet(output_path)

        print(f"Salida escrita en: {output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
