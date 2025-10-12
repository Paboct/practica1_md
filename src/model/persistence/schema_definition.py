from pyspark.sql.types import StructType, LongType, StructField, DoubleType, StringType, FloatType, IntegerType, DateType, TimestampType
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Ej1a - Definición del esquema para los datos del IBEX35 (estructuar para batch)
_SCHEMA = StructType([
    StructField("Date", DateType(), False), # Día de la sesión, en formato yyyy-MM-dd
    StructField("Open", DoubleType(), False), # Precio de apertura de la sesión
    StructField("High", DoubleType(), False), # Máximo intradía
    StructField("Low", DoubleType(), False), # Mínimo intradía
    StructField("Close", DoubleType(), False), # Precio de cierre de la sesión
    StructField("Volume", LongType(), True), # Acciones negociadas en la sesión
    StructField("Ticker", StringType(), False) # Símbolo único que identifica un activo financiero
])


# Esquema para datos streaming
_STREAM_SCHEMA = StructType([
    StructField("Ticker",  StringType(), True),
    StructField("price",   DoubleType(), True),
    StructField("high",    DoubleType(), True),
    StructField("low",     DoubleType(), True),
    StructField("volume",  LongType(),   True),
])