from pyspark.sql import DataFrame, SparkSession

class BatchBusiness:
    """
    Lógica de negocio para el pipeline batch.
    Encapsula operaciones de carga, transformación y persistencia sobre datos históricos.
    """

    def __init__(self, parquet_dir: str) -> None:
        """
        Parameters
        ----------
        parquet_dir : str
            Ruta donde se guardarán los resultados en formato Parquet.
        """
        self.parquet_dir = parquet_dir
