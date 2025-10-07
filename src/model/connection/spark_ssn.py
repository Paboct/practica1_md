from pyspark.sql import SparkSession

class SparkSessionManager:
    """
    Clase Singleton para manejar una única SparkSession.
    """

    _spark = None  # atributo de clase para guardar la sesión
    _path = "./lib/mysql-connector-j-9.4.0.jar"
    _app_name = "IBEX35App"

    @classmethod # Para que main use la clase sin instanciarla, es decir, la global
    def get_session(cls) -> SparkSession:
        """
        Devuelve la SparkSession, creándola si no existe.
        """
        if cls._spark is None:
            cls._spark = SparkSession.builder.appName("PracticaIBEX").config("spark.driver.extraClassPath", cls._path).getOrCreate()
        
        return cls._spark
    
    @classmethod
    def stop(cls):
        """
        Detiene la SparkSession si existe.
        """
        if cls._spark is not None:
            cls._spark.stop()
            cls._spark = None