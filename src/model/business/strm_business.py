from pyspark.sql import SparkSession, DataFrame

class StreamBusiness:
    """
    Lógica de negocio para el pipeline en streaming.
    Encapsula la gestión de fuentes en tiempo real y la escritura continua.
    """

    def __init__(self, output_dir: str, checkpoint_dir: str) -> None:
        """
        Parameters
        ----------
        output_dir : str
            Ruta de salida para los micro-lotes procesados.
        checkpoint_dir : str
            Ruta para checkpoints de streaming (estado y commits).
        """
        self.output_dir = output_dir
        self.checkpoint_dir = checkpoint_dir
