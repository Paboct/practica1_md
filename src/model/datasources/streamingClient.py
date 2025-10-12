from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
import yfinance as yf
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from model.logic.additioners import compute_daily_open_hgh_lw_close
from model.persistence.schema_definition import _STREAM_SCHEMA
from config.settings import STREAM_HOST, STREAM_PORT
import os

def _get_eurusd() -> float:
    """
    Función auxiliar que obtiene el valor actual del EURUSD
    """
    df = yf.download("EURUSD=X", period="1d", interval="1m", progress=False, auto_adjust=True)
    return df["Close"].iloc[-1]

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
    lines = ssc.socketTextStream(STREAM_HOST, STREAM_PORT)
    
    def process_rdd(time, rdd):
        if rdd.isEmpty():
            return
        
        # Parseo JSON y lo convierto en DataFrame
        df = spark.read.json(rdd, schema=_STREAM_SCHEMA)

        # Añadir la fecha y el EUR/USD
        ts = datetime.now()
        eurusd = _get_eurusd()

        # Limpio y transformo los datos
        df = (df
              .dropna(subset=["Ticker", "price"])
              .withColumn("Date",   F.lit(ts).cast("timestamp"))
              .withColumn("eurusd", F.lit(eurusd).cast("double"))
        )

        print(f"========= Batch recibido a las {str(ts)} =========")
        df.show(10, truncate=False)

        # Ejercicio6
        df = compute_daily_open_hgh_lw_close(df)
        df.show(10, truncate=False)


    # Por cada RDD recibido, aplico la función de proceso
    lines.foreachRDD(process_rdd)

    return ssc