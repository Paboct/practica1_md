from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from model.persistence.parquet_store import read_ticker_from_parquet


class PlottingController:
    """
    Controlador para gestionar la visualización de los datos.
    """

    def __init__(self, spark_session:SparkSession) -> None:
        self.session = spark_session

    def get_data_for_ticker(self, ticker:str) -> DataFrame:
        """
        Dado un ticker, devuelve su DataFrame, que está almacenado en un parquet.
        """
        # Vamos a leer los datos del histórico, por eso no
        # especificamos otra ruta
        return read_ticker_from_parquet(self.session, ticker)