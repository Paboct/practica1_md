from matplotlib.figure import Figure
import matplotlib.pyplot as plt

class PlotView:
    """
    Vista encargada de mostrar o guardar los gráficos generados por la PlottingFactory.
    """

    def show(self, fig: Figure) -> None:
        """
        Muestra el gráfico en pantalla.

        Parámetros:
        - fig: matplotlib.figure.Figure
        """
        if fig is not None:
            plt.figure(fig.number)
            plt.show(block=True)
        else:
            print("No hay figura para mostrar.")

    def save(self, fig: Figure, path: str, dpi: int = 300) -> None:
        """
        Guarda el gráfico en un archivo.

        Parámetros:
        - fig: matplotlib.figure.Figure
        - path: str : ruta completa del archivo (incluye .png, .pdf, etc.)
        - dpi: int : resolución en puntos por pulgada (por defecto 300)
        """
        if fig is not None:
            fig.savefig(path, dpi=dpi, bbox_inches="tight")
            print(f"Gráfico guardado correctamente en: {path}")
        
        else:
            print("No hay figura para guardar.")
