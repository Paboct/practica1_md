from pyspark.sql import SparkSession
from model.datasources.streamingClient import create_streaming_context

class StreamingController:
    """
    Controlador para gestionar la lógica de streaming.
    Coordina la conexión, procesamiento y visualización de datos en tiempo real.
    """

    def __init__(self, spark:SparkSession):
        self.spark = spark
        self.ssc = None

    def start(self) -> None:
        """
        Inicializa el contexto de streaming.
        """
        if self.ssc is None:
            self.ssc = create_streaming_context(self.spark)
            self.ssc.start()

    def await_termination(self) -> None:
        """
        Espera a que el streaming termine.
        """
        if self.ssc is not None:
            self.ssc.awaitTermination()

    def stop(self) -> None:
        """
        Detiene el contexto de streaming.
        Aunque la sesión de Spark permanece activa.
        """
        if self.ssc is not None:
            self.ssc.stop(stopSparkContext=False, stopGraceFully=True)
            self.ssc = None

    def run_streaming_pipeline(self) -> None:
        """
        Inicia el proceso de streaming.
        """
        print("Iniciando el proceso de streaming...")
        self.start()
        try:
            self.await_termination()
        
        except KeyboardInterrupt:
            print("Deteniendo el streaming...")
        
        finally:
            self.stop()
