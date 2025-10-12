import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

def add_weekday_column(df: DataFrame) -> DataFrame:
    """
    Ej2: Añade una columna que me dice el día de la semana, de
    cuando se registró la sesión (1=Monday, 7=Sunday) cuyo tipo es Integer.
    La columna de Date tiene formato yyyy-MM-dd
    """
    return df.withColumn("Weekday", F.date_format(F.col("Date"), "u").cast("int"))

def add_open_gap(df: DataFrame) -> DataFrame:
    """
    Ej3: Añade el open gap entre sesiones (%): ((Close de hoy / Open de hoy) - 1) * 100
    """
    w = Window.partitionBy("Ticker").orderBy(F.col("Date").asc())
    df_prev = df.withColumn("Prev_Close", F.lag(F.col("Close")).over(w))

    return df_prev.withColumn("OpenGap", ((F.col("Open") / F.col("Prev_Close")) - 1) * 100).drop("Prev_Close")

def compute_daily_open_high_low_close(df: DataFrame) -> DataFrame:
    """
    Ej6: Calcula Open, High, Low y Close diarios a partir de los tickers:
    - Ticker: string
    - price: double
    - Date: timestamp (momento de recepción del dato)
    -volume: long
    Devuelve: Ticker, day (date), Open, High, Low, Close
    """
    # Agrego la clave day
    df = df.withColumn("day", F.to_date("Date"))

    w_open = Window.partitionBy("Ticker", "day").orderBy(F.col("Date").asc())
    w_close = Window.partitionBy("Ticker", "day").orderBy(F.col("Date").desc())

    # Añadir columnas Open y Close, donde sus valores son los primeros de la ventana
    df_open_close = df.withColumn("Open", F.first("price").over(w_open))
    df_open_close = df_open_close.withColumn("Close", F.first("price").over(w_close))
    
    # Añado las columnas High y Low
    df_open_close = df_open_close.groupBy("Ticker", F.to_date("Date").alias("day")) \
                        .agg(F.max("price").alias("High"),
                             F.min("price").alias("Low"),
                                F.first("Open").alias("Open"),
                                F.first("Close").alias("Close"),
                                F.sum("volume").cast("long").alias("volume") if "volume" in df.columns else F.lit(0).cast("long").alias("volume")
                             ).orderBy("Ticker", "day")

    return df_open_close