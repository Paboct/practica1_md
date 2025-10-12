from pyspark.sql import SparkSession
from model.datasources.streamingClient import create_streaming_context

class StreamingController:
    """
    Controlador para gestionar la lógica de streaming.
    Coordina la conexión, procesamiento y visualización de datos en tiempo real.
    """

    def __init__(self, spark:SparkSession):
        self.spark = spark

    def run_streaming_pipeline(self):
        """
        Inicia el proceso de streaming.
        """
        print("Iniciando el proceso de streaming...")
        self.ssc = create_streaming_context(self.spark)
        self.ssc.start()
        self.ssc.awaitTermination()