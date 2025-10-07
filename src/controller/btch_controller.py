from pyspark.sql import SparkSession
from model.business.btch_business import BatchBusiness

class BatchController:
    """
    Controlador para el procesamiento batch.
    Coordina la interacción entre la vista, el modelo y la lógica de negocio (BatchBusiness).
    """

    def __init__(self, spark: SparkSession, view, business: BatchBusiness) -> None:
        """
        Parameters
        ----------
        spark : SparkSession
            Sesión activa de Spark.
        view : Any
            Vista encargada de mostrar los resultados.
        business : BatchBusiness
            Objeto de negocio con la lógica batch.
        """
        self.spark = spark
        self.view = view
        self.business = business
