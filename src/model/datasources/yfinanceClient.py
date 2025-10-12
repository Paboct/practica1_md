import yfinance as yf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType
from model.persistence.schema_definition import _SCHEMA

def download_historic_data(spark:SparkSession, ticker:str, start:str, end:str, interval:str="1d") -> DataFrame:
    """Ej1a: Descarga datos históricos con yfinances, convierte los ínides Date
    a columna y crea un DataFrame Spark con la estructura adecuada.
    """
    df_pd = yf.download(ticker, start=start, end=end, interval=interval, progress=False) # Devuelve un DF de pandas con la fecha como índice
    df_pd.reset_index(inplace=True)  # Convertir el índice en una columna
    df_pd["Ticker"] = ticker  # Añadir la columna de la acción concreta

    # Crear el DataFrame de Spark con el esquema definido
    df_spark = spark.createDataFrame(df_pd, schema=_SCHEMA)
    return df_spark