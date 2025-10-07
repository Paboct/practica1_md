from src.model.connection.spark_ssn import SparkSessionManager
from src.controller.btch_controller import Controller
from model.Business_object import BusinessObject


if __name__ == "__main__":
    # Inicializamos sesión de spark
    spark_session = SparkSessionManager.get_session()
    spark_session.sparkContext.setLogLevel("ERROR") # Ocultamos logs innecesarios

    # Inicializamos Business Object
    csv_path = "data/ibex35_close-2024.csv"
    bo = BusinessObject(spark_session)
    controller = Controller(bo)

    # Detenemos la sesión de spark
    SparkSessionManager.stop()