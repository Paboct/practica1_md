from matplotlib.figure import Figure
from config.settings import _GRAPHS_PATH
import os
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

    def save(self, fig: Figure, ticker: str, filename: str, dpi: int = 300) -> None:
        """
        Guarda el gráfico en un archivo.

        Parámetros:
        - fig: matplotlib.figure.Figure
        - ticker: str : Ticker asociado al gráfico (para organizar carpetas)
        - filename: str : Nombre del archivo donde se guardará el gráfico
        - dpi: int : Resolución del gráfico guardado (por defecto 300)
        """
        if not fig:
            print("No hay figura para guardar.")
            return
    
        _ticker = _ticker.replace(".MC", "").lower()

        ticker_path = os.path.join(_GRAPHS_PATH, _ticker)
        os.makedirs(ticker_path, exist_ok=True)

        file_path = os.path.join(ticker_path, filename)
        fig.savefig(file_path, dpi=dpi)
        print(f"Gráfico guardado en {file_path}.")