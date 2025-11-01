from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.streaming import StreamingContext
from model.logic.additioners import compute_daily_open_high_low_close
from model.datasources.streamingClient import start_streaming_context, get_dataframe
from model.logic.auxiliars import compute_perc_variation, compute_medium
from config.settings import STREAM_HOST, STREAM_PORT, _PARQUET_STREAM_PATH
from view.console_view import show_head
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
        del ejercicio 6.
        """
        df = compute_daily_open_high_low_close(df)
        return df

    def compute_additional_metrics(self, df:DataFrame, n_rows:int) -> DataFrame:
        """
        Añade las métricas adicionales para visualización.
        - Variación porcentual del precio respecto al anterior
        - Media móvil del precio de las últimas 5 filas
        """
        df = compute_perc_variation(df)
        df = compute_medium(df, n_rows=n_rows)
        return df
    

    def _show_head(self, df:DataFrame, n:int, title:str) -> None:
        """Muestra el head del dataframe"""
        show_head(df, n, title)

    def show_info(self) -> None:
        """
        Muestra la información relevante del streaming
        de cada uno de los tickers
        """
        for ticker in self.tickers:
            df = self.get_df_of_ticker_streaming(ticker)
            self._show_head(df, 15, f"Top {15} streaming {ticker}")
            df_open_high_low_close = self.compute_streaming_metrics(df)
            self._show_head(df_open_high_low_close, 15, f"Top {15} streaming {ticker}")
