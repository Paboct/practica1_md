import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

def add_weekday_column(df: DataFrame) -> DataFrame:
    """
    Ej2: Añade una columna que me dice el día de la semana, de
    cuando se registró la sesión (1=Monday, 7=Sunday) cuyo tipo es Integer.
    La columna de Date tiene formato yyyy-MM-dd
    """
    return df.withColumn("Weekday", F.dayofweek(F.col("Date")))

def add_open_gap(df: DataFrame) -> DataFrame:
    """
    Ej3: Añade el open gap entre sesiones (%): ((Close del día anterior / Open de hoy) - 1) * 100
    """
    w = Window.partitionBy("Ticker").orderBy(F.col("Date").asc())
    df_prev = df.withColumn("Prev_Close", F.lag(F.col("Close")).over(w))

    return df_prev.withColumn("OpenGap", ((F.col("Prev_Close") / F.col("Open")) - 1) * 100).drop("Prev_Close")

def compute_daily_open_high_low_close(df: DataFrame) -> DataFrame:
    """
    Ej6: Calcula Open, High, Low y Close diarios a partir de ticks en tiempo real.
    Requisitos de columnas:
      - ticker (string)
      - price  (double)
      - Date   (date)         # día de la sesión
      - Timestamp (timestamp) # instante de llegada/lectura (para ordenar)
      - volume (long)         # opcional
    Devuelve: ticker, day, Open, High, Low, Close, volume
    """
    df = df.withColumn("day", F.col("Date").cast("date"))

    w_open  = Window.partitionBy("ticker", "day").orderBy(F.col("Timestamp").asc())
    w_close = Window.partitionBy("ticker", "day").orderBy(F.col("Timestamp").desc())

    # Open y Close por ventana
    df_oc = (df
             .withColumn("Open",  F.first("price").over(w_open))
             .withColumn("Close", F.first("price").over(w_close)))

    # Agregación ¡High/Low, Open/Close y volume
    result = (df_oc
              .groupBy("ticker", "day")
              .agg(F.max("price").alias("High"),
                   F.min("price").alias("Low"),
                   F.first("Open").alias("Open"),
                   F.first("Close").alias("Close"),
                   F.sum("volume").cast("long").alias("volume"))
              .orderBy("ticker", "day"))

    return result