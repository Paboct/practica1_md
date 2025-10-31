from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
import seaborn as sns
import matplotlib.pyplot as plt

class PlottingFactory:
    """
    Clase Factoría para crear una variedad de gráficos basados en los datos de precios de acciones.
    """

    def __init__(self) -> None:
        self._graphs_types = {"boxplot": self._create_boxplot,
                              "violin": self._create_violin,
                              #"scatter": self._create_scatter,
                              #"line": self._create_line,
                              #"hist": self._create_hist
                              }

    def build_plot(self, plot_type: str, df: DataFrame, **kwargs) -> plt.figure:
        """
        Construye y retorna una figura de gráfico basada en el tipo especificado.
        
        Parámetros:
        - plot_type: str : Tipo de gráfico a crear (boxplot, violin, scatter, line, hist).
        - data: DataFrame : DataFrame de PySpark que contiene los datos a graficar.
        - kwargs: Argumentos adicionales específicos del gráfico. como 'x', 'y', 'hue', 'title', etc.
        """

        if plot_type not in self._graphs_types:
            raise ValueError(f"Tipo de gráfico '{plot_type}' no soportado.")
        
        # Convertimos a pandas para graficar
        pd_df = df.toPandas()

        constructor = self._graphs_types[plot_type]
        fig = constructor(pd_df, **kwargs)

        if not isinstance(fig, plt.Figure):
            raise TypeError("El gráfico debe ser un objeto de matplotlib.")

        return fig
    

    def _create_violin(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Gráfico de violín para visualizar la distribución de una variable numérica 
        entre categorías.

        kwargs:
        - x: str : Nombre de la columna para el eje x (categoría).
        - y: str : Nombre de la columna para el eje y (numérica).
        - title: str : Título del gráfico. (Opcional)
        - x_label: str : Etiqueta del eje x. (Opcional)
        - y_label: str : Etiqueta del eje y. (Opcional)
        """
        # Extraemos los parámetros necesarios
        x = kwargs.get("x")
        y = kwargs.get("y")
        title = kwargs.get("title", "Violin Plot")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", y)
        figsize = kwargs.get("figsize", (10, 8))
        palette = kwargs.get("palette", "Set2")

        sns.set_theme(style="whitegrid")
        fig, ax = plt.subplots(figsize=figsize)
        sns.violinplot(data=df, x=x, y=y, ax=ax, palette=palette)

        ax.axhline(0, color='red', linestyle='--') # Línea en 0 como referencia
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        return fig


    def _create_boxplot(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Boxplot para comprobar la distribución de una variable numérica 
        entre categorías.

        kwargs:
        - x: str : Nombre de la columna para el eje x (categoría).
        - y: str : Nombre de la columna para el eje y (numérica).
        - title: str : Título del gráfico. (Opcional)
        - x_label: str : Etiqueta del eje x. (Opcional)
        - y_label: str : Etiqueta del eje y. (Opcional)
        """
        # Extraemos los parámetros necesarios
        x = kwargs.get("x")
        y = kwargs.get("y")
        title = kwargs.get("title", "Boxplot")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", y)
        figsize = kwargs.get("figsize", (10, 8))
        show_points = kwargs.get("show_points", False)
        palette = kwargs.get("palette", "Set2")


        sns.set_theme(style="whitegrid")
        fig, ax = plt.subplots(figsize=figsize)
        sns.boxplot(data=df, x=x, y=y, ax=ax, hue=x, palette=palette, legend=False)
        # Opcional: mostrar puntos individuales
        if show_points:
            sns.stripplot(data=df, x=x, y=y, color='black', alpha=0.4, ax=ax, size=3)

        ax.axhline(0, color='red', linestyle='--') # Línea en 0 como referencia
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        # Añadimos la mediana como punto en el gráfico
        medians = df.groupby(x)[y].median().reset_index()
        
        for idx, row in enumerate(medians.itertuples(), start=0):
            ax.text(idx, row[2], f"{row[2]:.2f}",
                    color="black", ha="center", va="bottom", fontweight="bold",
                    fontsize=10)

        return fig