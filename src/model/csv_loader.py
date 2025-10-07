from pyspark.sql import SparkSession, DataFrame

"""
Lectura de fuentes CSV (por ejemplo, IBEXT 2024).
"""

def load_csv(spark: SparkSession) -> DataFrame:
    """
    Lee el CSV origanal y devuelve un DataFrame de Spark.
    """
    raise NotImplementedError("Función no implementada")

def load_csv_long()