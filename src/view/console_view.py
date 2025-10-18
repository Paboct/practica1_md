from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def show_response_to_question_1() -> None:
    """
    Muestra la respuesta a la pregunta 1 en consola.
    """
    print("Ej-1")
    print("Reflexión")
    print("La fecha no puede ser un campo nulo, porque es un dato esencial, ya que representará una sesión concreta.")
    print("Open representa el precio de apertura de la sesión, y si no tenemos ese dato, no podemos analizar correctamente el comportamiento de la acción.")
    print("High y Low representan los precios máximos y mínimos alcanzados en una sesión, por lo que son datos críticos para entender la volatilidad y el rango de precios.")
    print("Close, al igual que Open, es fundamental para evaluar el rendimiento de la acción durante la sesión.")
    print("Volume, puede ser nulo, ya que puede haber sesiones donde no se hayan negociado acciones.")
    print("Ticker no puede ser nulo, ya que identifica de manera única el activo financiero.")

def show_comments_to_question_1b() -> None:
    """
    Muestra los comentarios a la pregunta 1b en consola.
    """
    print("Ej-1b")
    print("Comentarios sobre la limpieza y validación de datos:")
    print("- Eliminamos duplicados basándonos en la combinación de 'Date' y 'Ticker' para asegurar que cada sesión de trading sea única por activo.")
    print("- Eliminamos filas con valores nulos en 'Date' o 'Ticker', ya que estos campos son esenciales para identificar y analizar los datos.")
    print("- Filtramos los precios para asegurarnos de que sean positivos, ya que los precios negativos no tienen sentido en este contexto.")
    print("- Verificamos la consistencia entre los precios de apertura, cierre, máximo y mínimo para garantizar que los datos reflejen correctamente el comportamiento del mercado.")

def show_head(df: DataFrame, n: int, title: str = "") -> None:
    """
    Muestra las primeras n filas del DataFrame con un título opcional.
    """
    if title:
        print(f"\n----{title}----")

    cols = [col for col in df.columns if col != "Ticker"]
    df.select(*cols).show(n, truncate=False)

def show_weekday(tickers, output: dict, n: int,) -> None:
    """
    Muestra las primeras n filas de cada  ticker del DataFrame 'weekday'.
    """
    for ticker in tickers:
        print(f"\n-----------Top {n} de {ticker} en weekday------------")
        df = output["weekday"].filter(F.col("Ticker") == ticker)    
        cols = [col for col in df.columns if col != "Ticker"]        
        df.select(*cols).show(n, truncate=False)

def show_opengap(tickers, output: dict, n: int,) -> None:
    """
    Muestra las primeras n filas de cada  ticker del DataFrame 'opengap'.
    """
    for ticker in tickers:
        print(f"\n-----------Top {n} de {ticker} en opengap------------")
        df = output["opengap"].filter(F.col("Ticker") == ticker)    
        cols = [col for col in df.columns if col != "Ticker"]        
        df.select(*cols).show(n, truncate=False)

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
    print(f"\nBatch recibido a las {time} | EUR/USD = {eurusd}")
    df.show(truncate=False)