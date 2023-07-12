# Este script está pensado para correr en Spark y hacer el proceso de ETL de Yahoo Finance

import requests
from datetime import datetime, timedelta
from os import environ as env

from pyspark.sql.functions import concat, col, lit, when, expr, to_date
from pyspark.sql.types import DecimalType

from commons import ETL_Spark

import yfinance as yf




class ETL_Entregable3(ETL_Spark):
    def __init__(self, job_name=None):
        super().__init__(job_name)
        self.process_date = datetime.now().strftime("%Y-%m-%d")

    def run(self):
        process_date = "2023-07-09"  # datetime.now().strftime("%Y-%m-%d")
        self.execute(process_date)

    def extract(self):
        """
        Extrae datos de la API
        """
        print(">>> [E] Extrayendo datos de la API...")
        hist = yf.Ticker('TSLA').history(period="1y")
        hist['Date'] = hist.index
        data = hist.reset_index(drop=True)
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
    etl = ETL_Entregable3()
    etl.run()
