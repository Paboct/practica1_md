from controller.btchController import BatchController
from controller.strmController import StreamingController
from model.sparkSession import SparkSessionSingleton
from view.console_view import show_head, show_schema, show_streaming_batch_info
from config.settings import TICKERS, START_DATE, END_DATE
import pyspark.sql.functions as F
import os
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

def main():

    spark = SparkSessionSingleton.get_instance("IBEX35-Practice1")
    btch_ctrl = BatchController(spark, TICKERS, START_DATE, END_DATE)
    str_ctrl = StreamingController(spark, TICKERS)

    # Silenciar logs de spark
    spark.sparkContext.setLogLevel("ERROR")

    output = btch_ctrl.exec_pipeline()

    print("\n=== Resultados del procesamiento por lotes ===")
    show_schema(output["historic"], "Schema histórico limpio")
    show_head(output["historic"], 5, "Top 5 histórico (todos los tickers)")

    for ticker in TICKERS:
        show_head(output["weekday"].filter(F.col("Ticker") == ticker), 5, f"Top 5 {ticker} con columna weekday")

    for ticker in TICKERS:
        show_head(output["opengap"].filter(F.col("Ticker") == ticker), 5, f"Top 5 {ticker} con columna open gap")

    print("\n=== Resultados del procesamiento en streaming ===")
    str_ctrl.run_streaming_pipeline()

if __name__ == "__main__":
    main()