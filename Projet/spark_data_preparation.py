from minio import Minio
import pandas as pd
import datetime
import json
from io import BytesIO
from minio.error import S3Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window, functions as F

"""""
    dans ce fichier on va faire de la datapreparations
    en spark on va lire le fichier dans la bucket minio
    enregistrer la dooonées en dataframe
    et reecrire dans une nouvelles bucket minIO
"""""

def get_minio_data():

    now = datetime.datetime.now()
    datetimeFormatted = now.strftime("%Y-%m-%d")
    
    object_name = f"donnees-capteur.{datetimeFormatted}.json"

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    try:
        response = client.get_object(
            "donnes-capteurs", object_name
        )
        return response.data.decode()

    # Read data from response.
    finally:
        response.close()
        response.release_conn()

def sendDataToMinio(data):
    spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

    df = spark.read.json("./sample.json")
    df.show()

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    found = client.bucket_exists("donnes-capteurs-spark")
    if not found:
        client.make_bucket("donnes-capteurs-spark")
    else:
        print("Bucket 'donnes-capteurs-spark' existe déjà")

    now = datetime.datetime.now()
    datetimeFormatted = now.strftime("%Y-%m-%d")
    object_name = f"donnees-capteur.{datetimeFormatted}.csv"

    isDataPerfect = True
    if not isDataPerfect:
        print("do some data cleaning idk")
        
    pandaDF = df.toPandas()
    pandaDF.to_csv(object_name, index=False)
    
    client.fput_object(
        "donnes-capteurs-spark", object_name, "./"+object_name,)
    
def sendDataToMinioViaPanda():
    pandaDF = pd.read_json("./sample.json")
    
    now = datetime.datetime.now()
    datetimeFormatted = now.strftime("%Y-%m-%d")
    object_name = f"donnees-capteur.{datetimeFormatted}.csv"

    csv_df = pandaDF.to_csv(object_name, index=False)

    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )

    found = client.bucket_exists("donnes-capteurs-spark")
    if not found:
        client.make_bucket("donnes-capteurs-spark")
    else:
        print("Bucket 'donnes-capteurs-spark' existe déjà")

    client.fput_object(
        "donnes-capteurs-spark", object_name, "./"+object_name,)

def main():
    data = get_minio_data()
    with open("sample.json", "w") as outfile:
        outfile.write(data)
   
    sendDataToMinioViaPanda()

    # sendDataToMinio(data)

if __name__ == "__main__":
    main()