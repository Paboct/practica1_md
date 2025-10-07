from pyspark.sql import DataFrame

class Conexion:
    """Clase qque maneja la conexion a la base de datos."""

    _DRIVER = "com.mysql.cj.jdbc.Driver"

    def write(df:DataFrame, url:str, table:str, user:str, password:str, mode:str="overwrite") -> None:
        """
        Escribe el DataFrame en una base de datos JDBC.
        """

        df.write.jdbc(url=url, table=table, mode=mode, properties={'user':user, 'password':password, "driver":Conexion._DRIVER})
