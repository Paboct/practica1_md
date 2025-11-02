from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from model.persistence.parquet_store import read_ticker_from_parquet
from model.logic.data_plotting import PlottingFactory
from model.logic.auxiliars import friday_or_not_weekday_col, compute_close_gap, is_in_season
from model.logic.additioners import add_season_column, add_gap_behaviour
from view.plot_view import PlotView

class PlottingController:
    """
    Controlador para gestionar la visualización de los datos.
    """

    def __init__(self, spark_session:SparkSession, str_ctrl) -> None:
        self.session = spark_session
        self.streamingController = str_ctrl


    def _build_title(self, ticker:str, plot_type:str, var_name:str) -> str:
        """
        Construye un título para el gráfico basado en el ticker y tipo de gráfico.
        """
        if plot_type == "boxplot":
            return f"Gráfico de Cajas: comparación del retorno diario (%) entre {var_name} y Not {var_name} — {ticker}"

        elif plot_type == "violin":
            return f"Gráfico de Violín: distribución del retorno diario (%) para {var_name} y Not {var_name} — {ticker}"

        elif plot_type == "hist":
            return f"Histograma: distribución del retorno diario (%) — {var_name} vs Not {var_name} — {ticker}"
        else:
            return f"{plot_type} — {ticker}"


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
    
    
    def _seasonal_effect_df(self, ticker:str, season:str) -> DataFrame:
        """
        Devuelve un DataFrame al que se le agrega una columna que indica la pertenencia o no
        a un periodo estacional (meses). E.g., season = "summer" -> meses 6,7,8
        Parámetros:
        - ticker: str : ticker a analizar
        - season: str : estación del año (winter, spring, summer, autumn)

        Returns:
        DataFrame con las columnas CloseGap e is_in_season

        """
        # Agregamos la columna Month
        df = self._get_data_for_ticker(ticker)
        
        # Modificamos el DataFrame para añadir la columna categórica
        df = add_season_column(df)
        df = compute_close_gap(df)
        df = is_in_season(df, season)

        return df.select("CloseGap", "is_in_season")

    def plotting_close_gap(self, tickers:list, plot_type:str) -> None:
        """
        Ejecuta la pipeline de visualización para una lista de tickers y tipo de gráfico dado,
        """
        for ticker in tickers:
            df = self._friday_effect_df(ticker)
            df.show(5)
            df = df.dropna() # Eliminamos filas con valores nulos (Gap del primer dia)

            # Creo el gráfico invocando la factoría
            plot_factory = PlottingFactory()
            fig = plot_factory.build_plot(plot_type, df,
                                          x="is_friday",
                                          y="CloseGap",
                                          bins=30,
                                          title=self._build_title(ticker, plot_type, "Friday"),
                                          x_label="Día de la semana",
                                          y_label="Retorno diario (%)")

            # Mostrar el gráfico
            plot_view = PlotView()
            plot_view.show(fig)

            # Guardar gráfico
            plot_view.save(fig, ticker, f"friday_effect_{plot_type}.png")
            df.show(5)


    def plotting_seasonal_effect(self, ticker:str, plot_type:str, season:str) -> None:
        """
        Ejecuta la pipeline de visualización para un ticker y tipo de gráfico dado,
        filtrando por estación para analizar el efecto estacionalidad.
        Parámetros:
        - season: str : estación del año (winter, spring, summer, autumn)
            - summer: 6,7,8
            - autumn: 9,10,11
            - winter: 12,1,2
            - spring: 3,4,5

        - ticker: str : ticker a analizar
        - plot_type: str : tipo de gráfico (boxplot, violin, etc.)

        Returns:
            None
        """
        df = self._seasonal_effect_df(ticker, season)
        df = df.dropna() # Eliminamos filas con valores nulos (Gap del primer dia)

        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        fig = plot_factory.build_plot(plot_type, df,
                                        x="is_in_season",
                                        y="CloseGap",
                                        title=self._build_title(ticker, plot_type, season),
                                        x_label="Pertenencia a estación",
                                        y_label="Retorno diario (%)")
        
        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)

        # Guardar gráfico
        plot_view.save(fig, ticker, f"{season}_seasonal_effect_{plot_type}.png")

    
    def plotting_numerics_features_corr(self, ticker:str, var1:str, var2:str, tpe:str='scatter') -> None:
        """
        Ejecuta la pipeline de visualización para un ticker y un gráfico de dispersión.
        Parámetros:
        - ticker: str : ticker a analizar
        - var1: str : nombre de la primera variable a comparar (La variable debe ser numérica)
        - var2: str : nombre de la segunda variable a comparar (La variable debe ser numérica)
        - tpe: str : tipo de gráfico (por defecto scatter) [scatter, jointplot]
        Returns:
            None
        """
        df = self._get_data_for_ticker(ticker)
        df = df.dropna(subset=[var1, var2]) # Eliminamos filas con valores nulos en las variables

        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        fig = plot_factory.build_plot(tpe, df,
                                      x=var1,
                                      y=var2,
                                      title=f"Gráfico de Dispersión: {var1} vs {var2} — {ticker}",
                                      x_label=var1,
                                      y_label=var2)
        
        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)


    def plotting_streaming_data(self, ticker:str, tpe:str="line") -> None:
        """
        Ejecuta la pipeline de visualización para los datos recibidos en streaming,
        agregando previamente una serie de transformaciones.
        Parámetros:
        - ticker: str : ticker a analizar
        Returns:
            None
        """
        # Datos originales en streaming
        df = self.streamingController.get_df_of_ticker_streaming(ticker)
        
        # Agregación de las métricas adicionales
        n_rows = 5
        df = self.streamingController.compute_additional_metrics(df, n_rows=n_rows)

        # Ordenamos por Timestamp
        df = df.orderBy(F.col("Timestamp").asc())

        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        if tpe == "line":
            fig = plot_factory.build_plot(tpe, df,
                                          title=f"Evolución intradía en tiempo real — {ticker}")
        
        elif tpe == "scatter":
            fig = plot_factory.build_plot("scatter", df, 
                                          x="price", 
                                          y="volume",
                                          hue="Perc_Variation",
                                          title=f"{ticker} — Relación Precio–Volumen (streaming)",
                                          x_label="Precio (€)",
                                          y_label="Volumen",
                                          palette="coolwarm")
            
        elif tpe == "kde":
            fig = plot_factory.build_plot("kde", df,
                                            x="Perc_Variation",
                                            title=f"{ticker} — Distribución de la variación porcentual (streaming)",
                                            x_label="Variación porcentual (%)",
                                            y_label="Densidad")

        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)

    
    def plotting_gap_behaviour(self, ticker:str) -> None:
        """
        Ejecuta la pipeline de visualización para un ticker y tipo de gráfico dado,
        mostrando el comportamiento del gap.
        Parámetros:
        - ticker: str : ticker a analizar

        Returns:
            None
        """
        df = self._get_data_for_ticker(ticker)

        # Modificamos el DataFrame para añadir la columna categórica
        df = add_gap_behaviour(df)

        df = df.select("OpenGap", "GapBehaviour", "IntraDayReturn").dropna()

        # Creo el gráfico invocando la factoría
        plot_factory = PlottingFactory()
        fig = plot_factory.build_plot("scatter", df,
                                      x="OpenGap",
                                      y ="IntraDayReturn",
                                      hue="GapBehaviour",
                                      title=f"{ticker} - Retorno intradía vs Gap de apertura",
                                      x_label="Open Gap (%)",
                                      y_label="Retorno intradía (%)",
                                      palette={"Reversión": "green", "Continuación": "orange"},
                                      figsize=(10, 8))

        
        # Mostrar el gráfico
        plot_view = PlotView()
        plot_view.show(fig)