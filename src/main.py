from controller.btchController import BatchController
from controller.strmController import StreamingController
from controller.pltController import PlottingController
from model.sparkSession import SparkSessionSingleton
from view.console_view import show_weekday, show_head, show_opengap, show_schema, show_comments_to_question_1b, show_response_to_question_1
from config.settings import TICKERS, START_DATE, END_DATE
import pyspark.sql.functions as F
import time
import os
os.environ.setdefault("HADOOP_HOME", r"C:\hadoop")

def main():

    spark = SparkSessionSingleton.get_instance("IBEX35-Practice1")
    btch_ctrl = BatchController(spark, TICKERS, START_DATE, END_DATE)
    str_ctrl = StreamingController(spark, TICKERS)
    plt_ctrl = PlottingController(spark, str_ctrl)

    # Silenciar logs de spark
    spark.sparkContext.setLogLevel("FATAL")

    # Procesamiento por lotes
    print("\nResultados del procesamiento por lotes")
    #btch_ctrl.exec_pipeline()
    
    print("\nResultados del procesamiento en streaming")
    #str_ctrl.start_streaming()

    # Una vez recibidos los muestro
    #str_ctrl.show_info()
    
    
    print("---- Visualización de datos ----")
    # Efecto viernes sobre el retorno diario (%)
    #plt_ctrl.plotting_close_gap(TICKERS, "boxplot")
    #plt_ctrl.plotting_close_gap(TICKERS, "violin")
    #plt_ctrl.plotting_close_gap(TICKERS, "hist")

    # Efecto estacional sobre el retorno diario (%)
    #plt_ctrl.plotting_seasonal_effect(TICKERS, "boxplot", "summer")
    #plt_ctrl.plotting_seasonal_effect(TICKERS, "violin", "summer")
    #plt_ctrl.plotting_seasonal_effect(TICKERS, "hist", "summer")

    # Relación precio volumen
    #plt_ctrl.plotting_numerics_features_corr(TICKERS, "Close", "Volume", "scatter")
    #plt_ctrl.plotting_numerics_features_corr(TICKERS, "Close", "Volume", "jointplot")

    # Gráfico datos recibidos en streaming
    #plt_ctrl.plotting_streaming_data(TICKERS, "line")
    #plt_ctrl.plotting_streaming_data(TICKERS, "scatter")
    #plt_ctrl.plotting_streaming_data(TICKERS, "kde")

    # Comportamiento del gap de apertura
    #plt_ctrl.plotting_gap_behaviour(TICKERS)

    # Cerrar la sesión de Spark
    #spark.stop()

if __name__ == "__main__":
    main()