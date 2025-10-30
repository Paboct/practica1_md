import pyspark.sql.functions as F

from typing import List, Dict
from model.datasources.yfinanceClient import download_historic_data
from model.logic.additioners import add_open_gap, add_weekday_column
from model.logic.cleaners import clean_and_validate_ticker
from model.persistence.parquet_store import append_data_to_ticker, delete_data_from_ticker, save_if_not_exists
from view.console_view import show_schema, show_head, show_weekday, show_opengap, show_response_to_question_1, show_comments_to_question_1b
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

    def _delete_parquet(self, tickers:List[str]) -> None:
        """
        Elimina los datos en formato Parquet de una lista
        de tickers específicos.
        """
        for ticker in tickers:
            delete_data_from_ticker(ticker)

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
    
    def _show_df_schema(self, df:DataFrame, title:str="Schema histórico limpio") -> None:
        """
        Muestra el esquema del DataFrame.
        """
        show_schema(df, title)

    
    def _show_head(self, df:DataFrame, n:int) -> None:
        """
        Muestra las primeras n filas del DataFrame.
        """
        show_head(df, n, title=f"Top {n} histórico {df.collect()[0]['Ticker']}")


    def _show_weekday_or_open_gap(self, output: Dict, n:int, tp:str) -> None:
        """
        Muestra el día de la semana de los datos procesados.
        tp: "weekday" o "opengap"
        """
        if tp == "opengap":
            show_opengap(self.tickers, output, n)
        else:
            show_weekday(self.tickers, output, n)

    
    def _show_arguments(self) -> None:
        """
        Muestra los argumentos para la pregunta 1.
        """
        show_response_to_question_1()
        print("\n")
        show_comments_to_question_1b()


    def exec_pipeline(self) -> None:
        """
        Ejecuta el flujo requerido en el ejercicio 4:
        - Descarga los datos históricos.
        - Limpia y valida los datos.
        - Guarda los datos en formato Parquet (si no existen).
        - Añade la columna de día de la semana.
        - Añade la columna de open gap entre sesiones (%).
        - Mustra la información requerida en consola.
        """
        
        df_all = self._download_data()
        df_clean = self._clean(df_all)
        #self._save_parquet(df_clean)
        print(df_clean.collect()[0][-1])
        df_weekday = self._add_weekday(df_clean)
        df_gap = self._add_open_gap(df_weekday)
        # Tengo que guardar el df_gap para el parquet de sus ticker correspondiente, es decir
        # Tengo que reescribir los datos de los ticker con la nueva columna

        # Actualizo los datos en el parquet
        # Primero elimino los datos en el parquet anterior (Con parquet, no tendría por qué borrar para quitar esos datos y añadir los nuevos,
        # podría simplemente hacer un overwrite, pero así hago uso de la función de delete que he implementado)
        #self._delete_parquet(self.tickers)

        # Luego añado los datos nuevos en la columna adicional
        #self._save_parquet(df_gap)
    
        output ={
            "historic": df_clean,
            "weekday": df_weekday,
            "opengap": df_gap
        }

        # Muestro los argumentos para la pregunta 1
        self._show_arguments()

        # Mostramos el esquema del df limpio
        self._show_df_schema(df_clean)
        
        print("\n")
        
        # Mostramos el histórico de cada ticker
        for ticker in self.tickers:
            self._show_head(df_clean.filter(F.col("Ticker") == ticker), 5)

        # Mostramos el weekday de cada ticker
        print("\n")
        self._show_weekday_or_open_gap(output, 5, "weekday")
        
        print("\n")
        print("\n")

        # Mostramos el opengap de cada ticker
        self._show_weekday_or_open_gap(output, 5, "opengap")
