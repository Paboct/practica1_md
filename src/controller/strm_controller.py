from pyspark.sql import SparkSession
from model.business.strm_business import StreamBusiness

class StreamController:
    """
    Controlador para el procesamiento en streaming.
    Coordina la vista y la lógica de negocio (StreamBusiness).
    """

    def __init__(self, spark: SparkSession, view, business: StreamBusiness) -> None:
        """
        Parameters
        ----------
        spark : SparkSession
            Sesión activa de Spark.
        view : Any
            Vista encargada de mostrar los resultados.
        business : StreamBusiness
            Objeto de negocio con la lógica streaming.
        """
        self.spark = spark
        self.view = view
        self.business = business
