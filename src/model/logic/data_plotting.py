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
                              "hist": self._create_hist,
                              "scatter": self._create_scatter,
                              "jointplot": self._create_jointplot,
                              "line": self._create_lineplot,
                              "kde": self._create_densityplot
                              }

    def build_plot(self, plot_type: str, df: DataFrame, **kwargs) -> plt.figure:
        """
        Construye y retorna una figura de gráfico basada en el tipo especificado.
        
        Parámetros:
        - plot_type: str : Tipo de gráfico a crear (boxplot, violin, scatter, jointplot, hist).
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
    

    def _create_lineplot(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Gráfico temporal en tiempo real:
        - Línea del precio
        - Línea de la media móvil (RollingMean_5)
        - Variación porcentual tick a tick (Perc_Variation) en eje secundario.
        """

        sns.set_theme(style="whitegrid")
        fig, ax1 = plt.subplots(figsize=(12, 6))

        # Eje del precio y media móvil
        sns.lineplot(x="Timestamp", y="price", data=df, ax=ax1, label="Precio", color="blue", linewidth=1.5)
        sns.lineplot(x="Timestamp", y="RollingMean_5", data=df, ax=ax1, label="Media móvil (5)", color="orange", linestyle="--", linewidth=1.2)

        ax1.set_xlabel("Tiempo")
        ax1.set_ylabel("Precio (€)")
        ax1.set_title(kwargs.get("title", "Evolución intradía en tiempo real"))

        # Eje de la variación porcentual
        ax2 = ax1.twinx()
        sns.lineplot(x="Timestamp", y="Perc_Variation", data=df, ax=ax2, color="red", label="Variación %", linewidth=1.0)
        ax2.set_ylabel("Variación (%)")

        # Combinar las leyendas
        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(h1 + h2, l1 + l2, loc="upper left")

        fig.autofmt_xdate(rotation=45)
        fig.tight_layout()
        return fig


    def _create_scatter(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Gráfico de dispersión para visualizar la relación entre dos variables numéricas.
        
        Parámetros:
        - df: pd.DataFrame : DataFrame de pandas con los datos a graficar.
        - kwargs: diccionario con parámetros adicionales para personalizar el gráfico.
            - x: str : Nombre de la columna para el eje x.
            - y: str : Nombre de la columna para el eje y.
            - title: str : Título del gráfico. (Opcional)
            - x_label: str : Etiqueta del eje x. (Opcional)
            - y_label: str : Etiqueta del eje y. (Opcional)
            - hue: str : Nombre de la columna para el mapeo de color. (Opcional)
        """

        # Extraemos los parámetros necesarios
        x = kwargs.get("x")
        y = kwargs.get("y")
        title = kwargs.get("title", "Gráfico de Dispersión")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", y)
        figsize = kwargs.get("figsize", (10, 8))
        palette = kwargs.get("palette", "Set2")
        hue = kwargs.get("hue", None)

        sns.set_theme(style="darkgrid")

        # Figura
        fig, ax = plt.subplots(figsize=figsize)
        sns.scatterplot(data=df, x=x, y=y, ax=ax, alpha=0.7, palette=palette if hue else None, hue=hue)
        # Ahora añadimos la línea de regresión
        sns.regplot(data=df, x=x, y=y, ax=ax, scatter=False, color='red')

        # Correlación de Pearson
        corr = df[x].corr(df[y])
        ax.text(0.05, 0.95, f'Corr Pearson: {corr:.2f}', transform=ax.transAxes,
                fontsize=12, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.5))
        
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        return fig
    

    def _create_densityplot(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Gráfico de densidad para visualizar la distribución de una variable numérica.
        
        Parámetros:
        - df: pd.DataFrame : DataFrame de pandas con los datos a graficar.
        - kwargs: diccionario con parámetros adicionales para personalizar el gráfico.
            - x: str : Nombre de la columna para el eje x.
            - title: str : Título del gráfico. (Opcional)
            - x_label: str : Etiqueta del eje x. (Opcional)
            - y_label: str : Etiqueta del eje y. (Opcional)
        """

        # Extraemos los parámetros necesarios
        x = kwargs.get("x")
        title = kwargs.get("title", "Gráfico de Densidad")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", "Densidad")
        figsize = kwargs.get("figsize", (10, 8))
        palette = kwargs.get("palette", "Set2")

        sns.set_theme(style="whitegrid")

        # Figura
        fig, ax = plt.subplots(figsize=figsize)
        sns.kdeplot(data=df, x=x, ax=ax, fill=True, alpha=0.6, linewidth=1.5, color="skyblue")

        ax.axvline(0, color='red', linestyle='--') # Línea en 0 como referencia
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        return fig
    

    def _create_jointplot(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Gráfico conjunto para visualizar la relación entre dos variables numéricas 
        junto con sus distribuciones marginales.
        
        Parámetros:
        - df: pd.DataFrame : DataFrame de pandas con los datos a graficar.
        - kwargs: diccionario con parámetros adicionales para personalizar el gráfico.
            - x: str : Nombre de la columna para el eje x.
            - y: str : Nombre de la columna para el eje y.
            - title: str : Título del gráfico. (Opcional)
            - x_label: str : Etiqueta del eje x. (Opcional)
            - y_label: str : Etiqueta del eje y. (Opcional)
        """

        # Extraemos los parámetros necesarios
        x = kwargs.get("x")
        y = kwargs.get("y")
        title = kwargs.get("title", "Gráfico Conjunto")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", y)
        kind = kwargs.get("kind", "scatter")  # scatter, reg, resid, kde, hex
        height = kwargs.get("height", 8)

        sns.set_theme(style="darkgrid")

        # Gráfico conjunto
        fig = sns.jointplot(data=df, x=x, y=y, kind=kind, height=height, space=0, color="C0", alpha=0.6)

        # Correlación de Pearson
        corr = df[x].corr(df[y])
        fig.figure.suptitle(title + f'\nCorr Pearson: {corr:.2f}', fontsize=16)
        fig.set_axis_labels(x_label, y_label)
        fig.figure.tight_layout()

        return fig.figure


    def _create_hist(self, df: pd.DataFrame, **kwargs) -> plt.Figure:
        """
        Histograma para visualizar la distribución de una variable numérica.
        
        kwargs:
        - x: str : Nombre de la columna para el eje x (categoría).
        - bins: int : Número de bins para el histograma. (Opcional)
        - title: str : Título del gráfico. (Opcional)
        - x_label: str : Etiqueta del eje x. (Opcional)
        - y_label: str : Etiqueta del eje y. (Opcional)
        """

        # Extraemos los parámetros necesarios
        categories = kwargs.get("x")
        values = kwargs.get("y")
        title = kwargs.get("title", "Histograma")
        y_label = kwargs.get("x_label", categories)
        x_label = kwargs.get("y_label", "Frecuencia")
        bins = kwargs.get("bins", 30)
        figsize = kwargs.get("figsize", (10, 8))
        palette = kwargs.get("palette", "Set2")
        
        sns.set_theme(style="whitegrid")

        # Figura
        fig, ax = plt.subplots(figsize=figsize)

        # hacemos el histograma para cada categoría
        for cat in df[categories].unique():
            subset = df[df[categories] == cat]
            sns.histplot(subset[values], bins=bins, kde=True, label=str(cat), ax=ax, 
                         stat="density", element="step", fill=False)
            # density para normalizar (ya que estás comparando un día frente a 6), element me define el estilo
            # kde sirve para añadir la curva de densidad
            
        ax.axvline(0, color='red', linestyle='--') # Línea en 0 como referencia
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)
        ax.legend(title=categories)

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