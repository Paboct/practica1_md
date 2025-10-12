from pyspark.sql import DataFrame
import pyspark.sql.functions as F


def clean_and_validate_ticker(df: DataFrame) -> DataFrame:
    """
    Ej1b: Limpia y valida los datos del DataFrame.
    """
    # Eliminar duplicados de fecha y ticker
    df_cleaned_validated = df.dropDuplicates(subset=["Date", "Ticker"])

    # Eliminar filas donde Date o Ticker sean nulos
    df_cleaned_validated = df_cleaned_validated.dropna(subset=["Date", "Ticker"])
    
    # Filtramos para quedarnos con precios positivos
    df_cleaned_validated = df_cleaned_validated.filter((F.col("Open") > 0) & (F.col("High") > 0) & 
                                                       (F.col("Low") > 0) & (F.col("Close") > 0))
    
    # Verificar la consistencia de los precios de apertura, cierre, máximo y mínimo
    df_cleaned_validated = df_cleaned_validated.filter((F.col("High") >= F.col("Low")) &
                                                       (F.col("High") >= F.col("Open")) &
                                                       (F.col("High") >= F.col("Close")) &
                                                       (F.col("Low") <= F.col("Open")) &
                                                       (F.col("Low") <= F.col("Close")))
    
    return df_cleaned_validated