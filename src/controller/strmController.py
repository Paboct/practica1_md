from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.streaming import StreamingContext
from model.logic.additioners import compute_daily_open_high_low_close
from model.datasources.streamingClient import start_streaming_context, get_dataframe
from config.settings import STREAM_HOST, STREAM_PORT, _PARQUET_STREAM_PATH
from view import *
from typing import List

class StreamingController:
    """
    Controlador para gestionar la lógica de streaming.
    Crea el contexto de streaming y ejecuta el pipeline.
    """

    def __init__(self, spark_session:SparkSession, tickers:List[str]) -> None:
        self.session = spark_session
        self.tickers = tickers

    def start_streaming(self) -> None:
        """
        Inicia el contexto de streaming y comienza a procesar los datos entrantes.
        """
        start_streaming_context(self.session)

    def get_df_of_ticker_streaming(self, ticker:str) -> DataFrame:
        """
        Devuelve el dataframe de un ticker que se ha
        obtenido en tiempo real
        """
        return get_dataframe(self.session, ticker, _PARQUET_STREAM_PATH)
    
    def compute_streaming_metrics(self, df:DataFrame) -> DataFrame:
        """
        Devuelve el dataframe, una vez aplicados los cambios
        del ejercicio 6
        """
        return compute_daily_open_high_low_close(df)
    
    def show_info(self, ) -> None:
        """
        Muestra la información relevante del streaming
        """