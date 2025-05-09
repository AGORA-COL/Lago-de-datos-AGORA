'''
ETL de PowerBI
Autor: Pedro Fabian Perez Arteaga
Descripción: Este script toma datos desde stagedata en HDFS y genera tablas analíticas para PowerBI en analyticsdata/powerbi
'''
import os
import yaml
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, count, countDistinct

def load_config(path):
    with open(path, 'r') as file:
        return yaml.safe_load(file)

def apply_limit(df, limit):
    return df if limit == "all" else df.limit(int(limit))

def process_rips(spark, source, global_limit):
    input_path = source["input_path"]
    input_subdirs = source["input_subdirs"]
    output_path = source["output_path"]
    outputs = source["output_files"]
    
    # Procesar solo los años habilitados
    for year, is_enabled in source["years"].items():
        if is_enabled:
            input_year_path = os.path.join(input_path, f"rips_parquet_{year}")
            df = spark.read.parquet(input_year_path)
            df = apply_limit(df, global_limit)

            # Usar 'fechaid' para extraer año y mes
            df = df.withColumn("anio", col("fechaid").substr(1, 4).cast("int")) \
                   .withColumn("mes", col("fechaid").substr(5, 2).cast("int"))

            archivos_generados = []
            
            # Procesar las tablas de salida
            for out in outputs:
                file_name = out["file_name"]
                # Añadir el año al final del nombre del archivo
                file_name_with_year = f"{file_name}_{year}.parquet"
                full_output_path = os.path.join(output_path, file_name_with_year)
                
                # Agrupar los datos por año y mes
                grouped = df.groupBy("anio", "mes").agg(count("*").alias("total"))
                grouped.write.mode("overwrite").parquet(full_output_path)
                
                archivos_generados.append(file_name_with_year)
                print(f"✅ Archivo generado: {file_name_with_year}")
            
            print("\n📦 Resumen de archivos generados:")
            for i, nombre in enumerate(archivos_generados, 1):
                print(f"{i:02d}. {nombre}")
            print(f"Total: {len(archivos_generados)} archivos.")

def process_segcovid(spark, source, global_limit):
    input_path = source["input_path"]
    input_subdirs = source["input_subdirs"]
    output_path = source["output_path"]
    outputs = source["output_files"]

    input_paths = [os.path.join(input_path, subdir) for subdir in input_subdirs]
    df = spark.read.parquet(*input_paths)
    df = apply_limit(df, global_limit)

    df = df.withColumn("anio", year(col("FechaRegistro"))) \
           .withColumn("mes", month(col("FechaRegistro")))

    archivos_generados = []

    for out in outputs:
        var = out["variable"]
        file_name = out["file_name"]

        grouped = df.groupBy("anio", "mes", var).agg(count("*").alias("total"))
        grouped.write.mode("overwrite").parquet(os.path.join(output_path, file_name))

        archivos_generados.append(file_name)
        print(f"✅ Archivo generado: {file_name}")

    print("\n📦 Resumen de archivos generados:")
    for i, nombre in enumerate(archivos_generados, 1):
        print(f"{i:02d}. {nombre}")
    print(f"Total: {len(archivos_generados)} archivos.")    

def process_sivigila(spark, source, global_limit):
    input_path = source["input_path"]
    input_subdirs = source["input_subdirs"]
    output_path = source["output_path"]
    outputs = source["output_files"]

    input_paths = [os.path.join(input_path, subdir) for subdir in input_subdirs]
    df = spark.read.parquet(*input_paths)
    df = apply_limit(df, global_limit)

    df = df.withColumn("anio", year(col("FechaNotificacion"))) \
           .withColumn("mes", month(col("FechaNotificacion")))

    archivos_generados = []

    for out in outputs:
        var = out["variable"]
        file_name = out["file_name"]

        grouped = df.groupBy("anio", "mes", "Edad", "sexo", var).agg(count("*").alias("total"))
        grouped.write.mode("overwrite").parquet(os.path.join(output_path, file_name))

        archivos_generados.append(file_name)
        print(f"✅ Archivo generado: {file_name}")

    print("\n📦 Resumen de archivos generados:")
    for i, nombre in enumerate(archivos_generados, 1):
        print(f"{i:02d}. {nombre}")
    print(f"Total: {len(archivos_generados)} archivos.")

def process_vacunascovid(spark, source, global_limit):
    input_path = source["input_path"]
    input_subdirs = source["input_subdirs"]
    output_path = source["output_path"]
    outputs = source["output_files"]

    input_paths = [os.path.join(input_path, subdir) for subdir in input_subdirs]
    df = spark.read.parquet(*input_paths)
    df = apply_limit(df, global_limit)

    df = df.withColumn("anio", year(col("FechaAplicacion"))) \
           .withColumn("mes", month(col("FechaAplicacion")))

    group_cols = [
        "anio", "mes", "Sexo", "Edad", "TipoRegimenAfiliacion",
        "NroDosis", "Biologico",
        "CAC_HTA", "CAC_DM", "CAC_ERC", "CAC_VIH", "CAC_Cancer",
        "CausaBasicaDefuncion", "DefuncionSospechosoCOVID"
    ]

    grouped = df.groupBy(*group_cols).agg(
        countDistinct("IDAnonimizado").alias("Número_de_Personas"),
        count("*").alias("Número_de_Atenciones")
    )

    archivos_generados = []

    for out in outputs:
        file_name = out["file_name"]
        grouped.write.mode("overwrite").parquet(os.path.join(output_path, file_name))
        archivos_generados.append(file_name)
        print(f"✅ Archivo generado: {file_name}")

    print("\n📦 Resumen de archivos generados:")
    for i, nombre in enumerate(archivos_generados, 1):
        print(f"{i:02d}. {nombre}")
    print(f"Total: {len(archivos_generados)} archivos.")

def process_nacimientos(spark, source, limit):
    from pyspark.sql.functions import col, count, countDistinct

    input_path = source['input_path']
    output_path = source['output_path']
    file_name = source['output_files'][0]

    df = spark.read.parquet(input_path)
    if limit is not None:
        df = df.limit(limit)

    df = df.withColumn("anio", col("fechanacimiento").substr(1, 4).cast("int")) \
           .withColumn("mes", col("fechanacimiento").substr(5, 2).cast("int"))

    grouped = df.groupBy(
        "anio", "mes", "sexo", "edadmadre", "regimenafiliacion"
    ).agg(
        countDistinct("personaid").alias("Número_de_Personas"),
        count("*").alias("Número_de_Atenciones")
    )

    full_output_path = os.path.join(output_path, file_name)

    grouped.write.mode("overwrite").parquet(full_output_path)
    print(f"✅ Archivo generado: {file_name}")

    print("\n📦 Resumen de archivos generados:")
    print(f"01. {file_name}")
    print("Total: 1 archivos.")

def process_defunciones(spark, source, limit):
    from pyspark.sql.functions import col, count, countDistinct

    input_path = source['input_path']
    output_path = source['output_path']
    file_name = source['output_files'][0]

    df = spark.read.parquet(input_path)
    if limit is not None:
        df = df.limit(limit)

    df = df.withColumn("anio", col("fechadefuncion").substr(1, 4).cast("int")) \
           .withColumn("mes", col("fechadefuncion").substr(5, 2).cast("int"))

    grouped = df.groupBy(
        "anio", "mes", "sexo", "edadmadre", "regimenafiliacion"
    ).agg(
        countDistinct("personaid").alias("Número_de_Personas"),
        count("*").alias("Número_de_Atenciones")
    )

    full_output_path = os.path.join(output_path, file_name)

    grouped.write.mode("overwrite").parquet(full_output_path)
    print(f"✅ Archivo generado: {file_name}")

    print("\n📦 Resumen de archivos generados:")
    print(f"01. {file_name}")
    print("Total: 1 archivos.")

def main():
    # Cargar la configuración desde el archivo YAML
    config = load_config("config/powerbi.yaml")
    
    # Crear la sesión de Spark
    spark = SparkSession.builder.appName("ETL PowerBI").getOrCreate()
    
    # Obtener el límite global de registros
    global_limit = config.get("records_to_process", "all")
    
    # Iterar sobre las fuentes configuradas
    for source in config["sources"]:
        if source["process"]:
            print(f"Procesando fuente: {source['name']}")
            
            # Llamar a la función de procesamiento según el nombre de la fuente
            if source["name"] == "rips":
                process_rips(spark, source, global_limit)
            # Puedes agregar otros casos aquí para diferentes fuentes
            elif source["name"] == "segcovid":
                process_segcovid(spark, source, global_limit)
            elif source["name"] == "sivigila":
                process_sivigila(spark, source, global_limit)
            elif source["name"] == "vacunascovid":
                process_vacunascovid(spark, source, global_limit)
            elif source["name"] == "nacimientos":
                process_nacimientos(spark, source, global_limit)
            elif source["name"] == "defunciones":
                process_defunciones(spark, source, global_limit)

if __name__ == "__main__":
    main()
