from controller.btchController import BatchController
from controller.strmController import StreamingController
from model.sparkSession import SparkSessionSingleton
from view.console_view import show_weekday, show_head, show_opengap, show_schema, show_comments_to_question_1b, show_response_to_question_1
from config.settings import TICKERS, START_DATE, END_DATE
import pyspark.sql.functions as F
import os
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

def main():

    spark = SparkSessionSingleton.get_instance("IBEX35-Practice1")
    btch_ctrl = BatchController(spark, TICKERS, START_DATE, END_DATE)
    str_ctrl = StreamingController(spark)

    # Silenciar logs de spark
    spark.sparkContext.setLogLevel("FATAL")

    output = btch_ctrl.exec_pipeline()

    #print("\nResultados del procesamiento por lotes")
    show_response_to_question_1()
    print("\n")
    show_comments_to_question_1b()
    show_schema(output["historic"], "Schema histórico limpio")
    for ticker in TICKERS:
        show_head(output["historic"].filter(F.col("Ticker") == ticker), 5, f"Top 5 histórico {ticker}")
    
    print("\n")
    show_weekday(TICKERS, output, 5)
    print("\n")
    print("\n")
    show_opengap(TICKERS, output, 5)
    print("\nResultados del procesamiento en streaming")
    str_ctrl.start_streaming()

    
if __name__ == "__main__":
    main()