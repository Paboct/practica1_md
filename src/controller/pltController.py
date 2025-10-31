from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from model.persistence.parquet_store import read_ticker_from_parquet
from model.logic.data_plotting import PlottingFactory
from model.logic.auxiliars import friday_or_not_weekday_col, compute_close_gap, is_in_season
from model.logic.additioners import add_season_column
from view.plot_view import PlotView

class PlottingController:
    """
    Controlador para gestionar la visualización de los datos.
    """

    def __init__(self, spark_session:SparkSession) -> None:
        self.session = spark_session

    def _get_data_for_ticker(self, ticker:str) -> DataFrame:
        """
        Dado un ticker, devuelve su DataFrame, que está almacenado en un parquet.
        """
        # Vamos a leer los datos del histórico, por eso no
        # especificamos otra ruta
        return read_ticker_from_parquet(self.session, ticker)
    
    def _friday_effect_df(self, ticker:str) -> DataFrame:
        """
        Devuelve un DataFrame con la información necesaria para analizar el efecto viernes.
        """
        df = self._get_data_for_ticker(ticker)

        # Modificamos el DataFrame para añadir la columna categórica
        df =  compute_close_gap(df)
        df = friday_or_not_weekday_col(df)

        return df.select("CloseGap", "is_friday")
    
    def _stationary_effect_df(self, ticker:str, start_month:int, end_month:int) -> DataFrame:
        """
        Devuelve un DataFrame al que se le agrega una columna que indica la pertenencia o no
        a un periodo estacional (meses).
        """
        # Agregamos la columna Month
        df = self._get_data_for_ticker(ticker)
        
        # Modificamos el DataFrame para añadir la columna categórica
        df = add_season_column(df)
        df = compute_close_gap(df)
        df = is_in_season(df, "summer")

        return df.select("CloseGap", "is_in_season")

    def plotting_close_gap(self, ticker:str, plot_type:str) -> None:
        """
        Ejecuta la pipeline de visualización para un ticker y tipo de gráfico dado.
        """
        df = self._friday_effect_df(ticker)
        df = df.dropna() # Eliminamos filas con valores nulos (Gap del primer dia)

        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        fig = plot_factory.build_plot(plot_type, df,
                                      x="is_friday",
                                      y="CloseGap",
                                      title=f"Efecto viernes en el ticker {ticker}",
                                      x_label="Día de la semana",
                                      y_label="Retorno diario (%)")
        
        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)

    def plotting_seasonal_effect(self, ticker:str, plot_type:str, start_mont_season:int, end_month_season:int) -> None:
        """
        Ejecuta la pipeline de visualización para un ticker y tipo de gráfico dado,
        filtrando por meses para analizar el efecto estacionalidad.
        """
        df = self._stationary_effect_df(ticker, start_mont_season, end_month_season)
        df = df.dropna() # Eliminamos filas con valores nulos (Gap del primer dia)

        df.show(200)
        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        fig = plot_factory.build_plot(plot_type, df,
                                        x="is_in_season",
                                        y="CloseGap",
                                        title=f"Efecto estacionalidad en el ticker {ticker}",
                                        x_label="Pertenencia a estación",
                                        y_label="Retorno diario (%)")
        
        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)