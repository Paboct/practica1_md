from pyspark.sql import DataFrame
from config.settings import _PARQUET_BATCH_PATH, _PARQUET_STREAM_PATH
import os
from pyspark.sql import SparkSession
import shutil

def _get_parquet_path(ticker:str, path:str=_PARQUET_BATCH_PATH) -> str:
    """
    Función auxiliar para obtener la ruta completa del parquet de un ticker
    """
    reg_exp = ticker.replace(".", "_")
    return os.path.join(path, f"{reg_exp}")

def _check_ticker_existance(path:str) -> bool:
    """
    Función auxiliar para comprobar si ya existen datos de un ticker en la ruta dada
    """
    return os.path.exists(path)

def save_if_not_exists(df: DataFrame, ticker:str) -> None:
    """
    Ej1c: Guarda el DataFrame en formato Parquet en una ruta específica, si 
    no hay datos de ese ticker
    """
    path = _get_parquet_path(ticker)
    if not _check_ticker_existance(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        df.write.mode("overwrite").parquet(path)
        print(f"Datos del ticker {ticker} guardados correctamente en la ruta {path}.")

    else:
        print(f"Los datos del ticker {ticker} ya existen en la ruta {path}. No se guardan de nuevo.")

def delete_data_from_ticker(ticker:str) -> None:
    """
    Ej1d: Elimina el parquet de un ticker específico.
    """
    path = _get_parquet_path(ticker)

    # Compruebo que existen los datos del ticker
    if _check_ticker_existance(path):
        print(f"Datos del ticker {ticker} encontrados en la ruta {path}. Eliminando...")
        shutil.rmtree(path)
        print(f"Datos del ticker {ticker} eliminados correctamente.")

    else:
        print(f"·No existen datos del ticker {ticker} en la ruta {path}. No se puede eliminar.")

def append_data_to_ticker(df: DataFrame, ticker:str, path_espec:str=_PARQUET_BATCH_PATH) -> None:
    """
    Ej1d: Añade datos al parquet de un ticker específico.
    """
    path = _get_parquet_path(ticker, path_espec)

    # Compruebo que existen los datos del ticker
    if not _check_ticker_existance(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        print(f"Datos del ticker {ticker} encontrados en la ruta {path}. Añadiendo nuevos datos...")
        
    # Ahora añado los datos
    df.write.mode("append").parquet(path)
    print(f"Nuevos datos del ticker {ticker} añadidos correctamente.")

def read_ticker_from_parquet(spark:SparkSession, ticker:str, path_espec:str=_PARQUET_BATCH_PATH) -> DataFrame:
    """
    Lee un parquet de un ticker específico y devuelve el DataFrame correspondiente.
    """
    path = _get_parquet_path(ticker, path_espec)

    # Compruebo que existen los datos del ticker
    if not _check_ticker_existance(path):
        raise FileNotFoundError(f"No existen datos del ticker {ticker} en la ruta {path}.")

    df = spark.read.parquet(path, index=False)
    return df