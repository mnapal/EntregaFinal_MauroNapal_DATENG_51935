from datetime import datetime
from os import environ as env
import yfinance as yf
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DecimalType
from commons import ETL_Spark


class ETL_entrega_final(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)

    def run(self):
        fecha = env['PARAM_PROCESS_DATE']
        date_obj = datetime.strptime(fecha, "%Y-%m-%d")
        self.process_date = date_obj.strftime("%Y-%m-%d")
        if not fecha:
            self.process_date = datetime.now().strftime("%Y-%m-%d")
        self.execute(fecha)


    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")
        ticker = env['PARAM_TICKER_EMPRESA']
        hist = yf.Ticker(ticker).history(period="1y")
        hist['Date'] = hist.index
        data = hist.reset_index(drop=True)
        if len(data) < 1:
            raise Exception("Error al obtener datos de Yahoo Finance!")
        else:
            print("Muestro datos de la API...")
            df = self.spark.createDataFrame(data, ['Open', 'High', 'Low', 'Close', 'v', 'd', 's', 'date'])
            df.printSchema()
            # selecciono las columnas
            df = df.select('Open', 'High', 'Low', 'Close', 'date')
            df.printSchema()
            df.show()
            print(">>> [E] Fin Extrayendo datos de la API...")
            return df


    def transform(self, df):
        """
        Transforma los datos
        """
        print(">>> [T] Transformando datos...")
        df = df.withColumn("Open", df["Open"].cast(DecimalType(precision=10, scale=2)))
        df = df.withColumn("High", df["High"].cast(DecimalType(precision=10, scale=2)))
        df = df.withColumn("Low", df["Low"].cast(DecimalType(precision=10, scale=2)))
        df = df.withColumn("Close", df["Close"].cast(DecimalType(precision=10, scale=2)))
        df = df.withColumn('Difference', col('Close') - col('Open'))
        df = df.withColumn("Difference", df["Difference"].cast(DecimalType(precision=10, scale=2)))
        df = df.withColumnRenamed("Open", "openn")
        df = df.withColumn("process_date", lit(self.process_date))
        print("printSchema")
        df.printSchema()
        print("Muestro los valores modificados...")
        df.show()
        print(">>> [T] Fin Transformando datos...")
        return df


    def load(self, df_final):
        """
        Carga los datos transformados en Redshift
        """
        print(">>> [L] Cargando datos en Redshift...")
        df_final.write \
            .format("jdbc") \
            .option("url", env['REDSHIFT_URL']) \
            .option("dbtable", f"{env['REDSHIFT_SCHEMA']}.{env['REDSHIFT_TABLE']}") \
            .option("user", env['REDSHIFT_USER']) \
            .option("password", env['REDSHIFT_PASSWORD']) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

        print(">>> [L] Datos cargados exitosamente")


if __name__ == "__main__":
    print("Corriendo script")
    etl = ETL_entrega_final()
    etl.run()

