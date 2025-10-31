import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import Window

def friday_or_not_weekday_col(df: DataFrame) -> DataFrame:
    """
    Transforma la columna de Weekday en una columna categórica
    que indica si es viernes o no.
    """
    return df.withColumn("is_friday", F.when(F.col("Weekday") == 6, "Friday")\
                       .otherwise("Not Friday").cast("string"))

def is_in_season(df: DataFrame, season:str) -> DataFrame:
    """
    Devuelve un DataFrame con una columna categórica que indica si la fila
    pertenece o no a la estación dada (season).
    """
    return df.withColumn("is_in_season", F.when(F.col("Season") == season, season)\
                       .otherwise(f"Not {season}").cast("string"))

def compute_close_gap(df: DataFrame) -> DataFrame:
    """
    Añade una columna que almacena con el retorno diario en %:
    ((Close_t - Close_(t-1)) / Close_(t-1)) * 100
    """
    w = Window.partitionBy("Ticker").orderBy(F.col("Date").asc())
    df_prev = df.withColumn("Prev_Close", F.lag(F.col("Close")).over(w))

    return df_prev.withColumn("CloseGap", ((F.col("Close") - F.col("Prev_Close")) / F.col("Prev_Close")) * 100)\
                    .drop("Prev_Close")
