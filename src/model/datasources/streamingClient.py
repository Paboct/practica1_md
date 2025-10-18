from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import yfinance as yf
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from model.logic.additioners import compute_daily_open_high_low_close
from model.persistence.schema_definition import _STREAM_SCHEMA
from config.settings import STREAM_HOST, STREAM_PORT
import os

def _get_eurusd() -> float:
    """
    Función auxiliar que obtiene el valor actual del EURUSD
    """
    try:
        ticker = yf.Ticker("EURUSD=X") # Crear el objeto Ticker para EURUSD
        price = ticker.fast_info['lastPrice']  # Obtener el precio actual
        
        if price is None:
            raise ValueError("No se pudo obtener el precio de EUR/USD")        
        
        return float(price)
    
    except Exception as e:
        print(f"Error al obtener el precio de EUR/USD: {e}")
        return 1.0

def create_streaming_context(spark:SparkSession) -> StreamingContext:
    """
    Ej4: Crea una conexión que permite recibir datos en streaming
    Ej5: Recoge TODOS los datos JSON recibidos y, por micro-lote:
         - los convierte en DataFrame,
         - añade la fecha (Date = timestamp de recepción),
         - añade el EUR/USD del momento,
         - y los muestra por consola.
    """
    # Creo el contexto de streaming y la frecuencia de batches
    sc = spark.sparkContext
    ssc = StreamingContext(sc, batchDuration=10)

    # Conexión al socket
    lines = ssc.socketTextStream(STREAM_HOST, STREAM_PORT) # Por socket llega siempre texto
    
    def process_rdd(rdd):
        """
        Función que procesa cada RDD (micro-lote) recibido en streaming
        """
        if rdd.isEmpty():
            return
        
        # Convierto el RDD (Colección distribuida de datos) en DataFrame
        df = spark.read.json(rdd, schema=_STREAM_SCHEMA)

        # Obtengo el timestamp actual
        now = datetime.now()
        actual_date = now.strftime("%Y-%m-%d")
        actual_time = now.strftime("%H:%M:%S")
        weekday = now.weekday() + 1 # Lunes=1 ... Domingo=7

        # Obtengo el timestamp completo
        eurusd = _get_eurusd()

        # Limpio y añado las nuevas columnas
        df = (df.dropna(subset=["ticker", "price"])
              .withColumn("Date", F.lit(actual_date).cast("date"))
              .withColumn("Time", F.lit(actual_time).cast("string"))
              .withColumn("Weekday", F.lit(weekday).cast("int"))
              .withColumn("eurusd", F.lit(eurusd).cast("double"))
              .withColumn("Timestamp", F.current_timestamp()) #
        )

        print(f"Batch recibido el {actual_date} a las {actual_time}")
        df.show(10, truncate=False)

        # Ejercicio6
        print("Cálculo de valores diarios por ticker:")
        df = compute_daily_open_high_low_close(df)
        df.show(10, truncate=False)


    # Por cada RDD recibido, aplico la función de proceso
    lines.foreachRDD(process_rdd)

    return ssc