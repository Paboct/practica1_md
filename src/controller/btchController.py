import pyspark.sql.functions as F

from typing import List, Dict
from model.datasources.yfinanceClient import download_historic_data
from model.logic.additioners import add_open_gap, add_weekday_column
from model.logic.cleaners import clean_and_validate_ticker
from model.persistence.parquet_store import append_data_to_ticker, delete_data_from_ticker, save_if_not_exists
from pyspark.sql import SparkSession, DataFrame

class BatchController:
    """
    Controlador para el procesamiento por lotes (batch).
    - Descarga datos históricos (yfinance).
    - Limpia y valida los datos.
    - Persistencia por ticker (Parquet).
    - Adiciñon de columnas (weekday, openg gap).
    - Visualización de los resultados
    """

    def __init__(self, spark: SparkSession, tickers: List[str], start: str, end: str) -> None:
        self.spark = spark
        self.tickers = tickers
        self.start = start
        self.end = end

    def _download_data(self) -> DataFrame:
        """
        Descarga los datos históricos para cada ticker y devuelve un DataFrame combinado.
        """
        if not self.tickers:
            raise ValueError("La lista de tickers está vacía.")

        # Lista de DataFrames
        dfs = [download_historic_data(self.spark, ticker, self.start, self.end) for ticker in self.tickers]
        
        # Concateno los dataframes
        combined_df = dfs[0]

        for df in dfs[1:]:
            combined_df = combined_df.unionByName(df) # unionByName para que no importe el orden de las columnas

        return combined_df

    def _clean(self, df:DataFrame) -> DataFrame:
        """
        Limpia y valida los datos del DataFrame.
        """
        self._df_clean = clean_and_validate_ticker(df)
        return self._df_clean

    def _save_parquet(self, df:DataFrame) -> None:
        """
        Guarda los datos en formato Parquet, si no existen ya.
        """
        for ticker in self.tickers:
            save_if_not_exists(df, ticker)

    def _add_weekday(self, df:DataFrame) -> DataFrame:
        """
        Añade una columna que indica el día de la semana.
        """
        return add_weekday_column(df)

    
    def _add_open_gap(self, df:DataFrame) -> DataFrame:
        """
        Añade la columna de open gap entre sesiones (%).
        """
        return add_open_gap(df)

    def exec_pipeline(self) -> Dict:
        """
        Ejecuta el flujo requerido en el ejercicio 4:
        - Descarga los datos históricos.
        - Limpia y valida los datos.
        - Guarda los datos en formato Parquet (si no existen).
        - Añade la columna de día de la semana.
        - Añade la columna de open gap entre sesiones (%).
        - Devolver DFs para mostrar

        Returns:
        {
            "historic": DataFrame (limpio con todos los tickers),
            "weeday": DataFrame (con columna weekday),
            "opengap": DataFrame (con columna open gap)
            }
        """
        
        df_all = self._download_data()
        df_clean = self._clean(df_all)
        self._save_parquet(df_clean)
        df_weekday = self._add_weekday(df_clean)
        df_gap = self._add_open_gap(df_weekday)

        return {
            "historic": df_clean,
            "weekday": df_weekday,
            "opengap": df_gap
        }