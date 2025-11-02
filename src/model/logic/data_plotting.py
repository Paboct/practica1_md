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

        title = kwargs.get("title", "Evolución intradía en tiempo real")

        sns.set_theme(style="whitegrid")
        fig, ax_price = plt.subplots(figsize=(12, 6))

        # Preccio y media móvi
        sns.lineplot(
            x="Timestamp", y="price", data=df,
            ax=ax_price,
            label="Precio",
            color="blue",
            linewidth=1.5,
            marker="o",
            markersize=4,
            ci=None
        )

        sns.lineplot(
            x="Timestamp", y="RollingMean_5", data=df,
            ax=ax_price,
            label="Media móvil (5)",
            color="orange",
            linestyle="--",
            linewidth=1.4,
            marker=None,
            ci=None
        )

        # Relleno entre precio y media móvil
        ax_price.fill_between(
            df["Timestamp"],
            df["price"],
            df["RollingMean_5"],
            color="orange",
            alpha=0.08,
            linewidth=0
        )

        ax_price.set_xlabel("Tiempo")
        ax_price.set_ylabel("Precio (€)")
        ax_price.set_title(title)

        # Variación porcentual en eje secundario
        ax_var = ax_price.twinx()
        sns.lineplot(
            x="Timestamp", y="Perc_Variation", data=df,
            ax=ax_var,
            color="red",
            linewidth=1.0,
            marker="o",
            markersize=3,
            alpha=0.8,
            ci=None,
            label="Variación %"
        )
        ax_var.set_ylabel("Variación (%)")

        # Centramos el eje y de variación porcentual
        ypad = max(abs(df["Perc_Variation"].min()), abs(df["Perc_Variation"].max())) * 1.2
        ax_var.set_ylim(-ypad, ypad)

        # Agrupamos leyendas
        h_price, l_price = ax_price.get_legend_handles_labels()
        h_var,   l_var   = ax_var.get_legend_handles_labels()

        ax_price.legend(
            h_price + h_var,
            l_price + l_var,
            loc="upper right",
            frameon=True,
            framealpha=0.9
        )

        fig.autofmt_xdate(rotation=45)

        # Menos ruido en cuadrícula
        ax_price.grid(True, which="major", axis="both", alpha=0.3)
        ax_var.grid(False)

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
        title = kwargs.get("title", "Distribución de la variación porcentual (streaming)")
        x_label = kwargs.get("x_label", x)
        y_label = kwargs.get("y_label", "Densidad")
        figsize = kwargs.get("figsize", (10, 6))

        sns.set_theme(style="whitegrid")
        fig, ax = plt.subplots(figsize=figsize)

        # Recorte automático para que no dominen outliers muy extremos
        x_vals = df[x].dropna()
        p_low, p_high = x_vals.quantile([0.01, 0.99])
        ax.set_xlim(p_low, p_high)

        # Gráfico de densidad
        sns.kdeplot(
            data=df, x=x,
            ax=ax,
            fill=True,
            alpha=0.5,
            linewidth=1.5,
            color="skyblue"
        )
        sns.kdeplot(
            data=df, x=x,
            ax=ax,
            fill=False,
            alpha=1.0,
            linewidth=1.0,
            color="steelblue"
        )

        # Línea vertical en 0 como referencia neutra
        ax.axvline(0, color='red', linestyle='--', linewidth=1.2)
        ax.text(
            0, ax.get_ylim()[1]*0.9,
            "0%",
            color="red",
            ha="left",
            va="top",
            fontsize=9,
            alpha=0.8
        )

        # Media de la distribución
        mean_val = x_vals.mean()
        ax.axvline(mean_val, color='black', linestyle=':', linewidth=1.2)
        ax.text(
            mean_val,
            ax.get_ylim()[1]*0.9,
            f"Media: {mean_val:.2f}%",
            color="black",
            ha="right" if mean_val > 0 else "left",
            va="top",
            fontsize=9,
            alpha=0.8
        )

        # Etiquetas
        ax.set_title(title)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        # Grid más fino
        ax.grid(True, alpha=0.2)

        fig.tight_layout()
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
        color = kwargs.get("color", "#2A9D8F")

        sns.set_theme(style="darkgrid")
        plt.rcParams["axes.edgecolor"] = "0.8"
        plt.rcParams["axes.linewidth"] = 0.8

        # Gráfico conjunto
        fig = sns.jointplot(data=df, x=x, y=y, kind=kind, height=height, space=0, color=color, alpha=0.6,
                            marginal_kws=dict(bins=25, fill=True, alpha=0.7), edgecolor="0.2")


        # Correlación de Pearson
        corr = df[x].corr(df[y])

        # Título principal
        fig.figure.suptitle(title, fontsize=16, weight="bold", y=1.02)

        # Etiquetas
        fig.set_axis_labels(x_label, y_label, fontsize=13)

        ax = fig.ax_joint
        ax.text(
            0.05, 0.95,
            f"Corr Pearson: {corr:.2f}",
            transform=ax.transAxes,
            fontsize=12,
            weight="bold",
            color="black",
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="0.7", boxstyle="round,pad=0.3")
        )

        ax.grid(alpha=0.25, linestyle="--")
        plt.tight_layout()
        plt.subplots_adjust(top=0.92)

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

        # hacemos el histograma para cada categoría y añadimos una etiqueta de la media de cada distribución
        for cat in df[categories].unique():
            subset = df[df[categories] == cat]
            sns.histplot(subset[values], bins=bins, kde=True, label=str(cat), ax=ax,
                         stat="density", element="step", fill=True, alpha=0.2)
            # density para normalizar (ya que estás comparando un día frente a 6), element me define el estilo
            # kde sirve para añadir la curva de densidad
            mean_val = subset[values].mean()
            ax.axvline(mean_val, linestyle='--', label=f'Media {cat}: {mean_val:.2f}', color='C'+str(list(df[categories].unique()).index(cat)))
            
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
        sns.violinplot(data=df, x=x, y=y, ax=ax, palette=palette, alpha=0.4, inner="quartile", linewidth=1.2)

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
        sns.boxplot(data=df, x=x, y=y, ax=ax, hue=x, palette=palette, legend=False,
                    meanprops={"marker":"o", "markerfacecolor":"white", "markeredgecolor":"black"})
        # Opcional: mostrar puntos individuales
        if show_points:
            sns.stripplot(data=df, x=x, y=y, color='black', alpha=0.4, ax=ax, size=2)

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