from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.streaming import StreamingContext
from model.datasources.streamingClient import process_rdd, DFF_GLOBAL
from config.settings import STREAM_HOST, STREAM_PORT

class StreamingController:
    """
    Controlador para gestionar la lógica de streaming.
    Crea el contexto de streaming y ejecuta el pipeline.
    """

    def __init__(self, spark_session:SparkSession) -> None:
        self.ssc = StreamingContext(spark_session.sparkContext, batchDuration=10)
        self.spark = spark_session

    def start_streaming(self) -> None:
        """
        Ej4: Crea una conexión que permita recibir los datos en tiempo real.
        Inicia el contexto de streaming.
        """
        lines = self.ssc.socketTextStream(STREAM_HOST, STREAM_PORT)
        
        # Aplico la función de proceso a cada RDD recibido
        lines.foreachRDD(lambda rdd: process_rdd(self.spark, rdd))

        # Inicia el proceso streaming
        self.ssc.start()

        try:
            # Bloqueo el hilo principal hasta que se detenga el streaming
            self.ssc.awaitTerminationOrTimeout(140)

        except Exception as e:
            print(f"Error en el streaming: {e}")
            self.ssc.stop(stopSparkContext=False, stopGraceFully=True)

        # Una vez finalizado, detengo el streaming
        self.ssc.stop(stopSparkContext=False, stopGraceFully=True)

        # Una vez finalizado el streaming, imprimo mensaje
        print("Streaming finalizado.")

    def get_dataframe(self) -> DataFrame:
        """
        Devuelve el DataFrame global con todos los datos recibidos en streaming.
        """
        global DFF_GLOBAL
        return DFF_GLOBAL