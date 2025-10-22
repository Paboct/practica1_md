from controller.btchController import BatchController
from controller.strmController import StreamingController
from model.sparkSession import SparkSessionSingleton
from view.console_view import show_weekday, show_head, show_opengap, show_schema, show_comments_to_question_1b, show_response_to_question_1
from config.settings import TICKERS, START_DATE, END_DATE
import pyspark.sql.functions as F
import time
import os
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

def main():

    spark = SparkSessionSingleton.get_instance("IBEX35-Practice1")
    #btch_ctrl = BatchController(spark, TICKERS, START_DATE, END_DATE)
    str_ctrl = StreamingController(spark)

    # Silenciar logs de spark
    spark.sparkContext.setLogLevel("FATAL")

    #output = btch_ctrl.exec_pipeline()

    #print("\nResultados del procesamiento por lotes")
    #show_response_to_question_1()
    #print("\n")
    #show_comments_to_question_1b()
    #show_schema(output["historic"], "Schema histórico limpio")
    #for ticker in TICKERS:
    #    show_head(output["historic"].filter(F.col("Ticker") == ticker), 5, f"Top 5 histórico {ticker}")
    #
    #print("\n")
    #show_weekday(TICKERS, output, 5)
    #print("\n")
    #print("\n")
    #show_opengap(TICKERS, output, 5)
    print("\nResultados del procesamiento en streaming")
    #str_ctrl.start_streaming()

    # Una vez recibidos los muestro
    for ticker in TICKERS:
        df_stream = str_ctrl.get_df_of_ticker_streaming(ticker)
        show_head(df_stream, 15, f"Top 15 streaming {ticker}")
        df_metrics = str_ctrl.compute_streaming_metrics(df_stream)
        print()
        show_head(df_metrics, 15, f"Top 15 streaming Open, High, Low, Volume {ticker}")

if __name__ == "__main__":
    main()