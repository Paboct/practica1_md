from pyspark.sql import DataFrame

def show_head(df: DataFrame, n: int, title: str = "") -> None:
    """
    Muestra las primeras n filas del DataFrame con un título opcional.
    """
    if title:
        print(f"\n----{title}----")

    df.show(n, truncate=False)

def show_schema(df: DataFrame, title: str = "") -> None:
    """
    Muestra el esquema del DataFrame con un título opcional.
    """
    if title:
        print(f"\n----{title}----")

    df.printSchema()

def show_streaming_batch_info(df:DataFrame, time, eurusd:float) -> None:
    """
    Muestra información adicional de cada batch recibido en streaming
    """
    print(f"\n========= Batch recibido a las {time} | EUR/USD = {eurusd} =========")
    df.show(truncate=False)