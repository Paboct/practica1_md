from pyspark.sql import SparkSession

class SparkSessionSingleton:
    """
    Clase Singleton para gestionar una única instancia de SparkSession.
    """

    _instance = None

    @classmethod
    def get_instance(cls, app_name:str="IBEX35-Practice1") -> SparkSession:
        """
        Devuelve una única instancia de SparkSession.
        """

        if cls._instance is None:
            cls._instance = SparkSession.builder.appName(app_name).getOrCreate()

        return cls._instance
    
    @classmethod
    def stop_instance(cls) -> None:
        """
        Detiene la instancia de SparkSession en caso de que exista.
        """

        if cls._instance is not None:
            cls._instance.stop()
            cls._instance = None

    @classmethod
    def is_running(cls) -> bool:
        """
        Comprueba si la instancia de SparkSession está en ejecución.
        """
        return cls._instance is not None