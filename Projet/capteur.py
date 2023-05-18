import pandas as pd
import numpy as np
import random
import datetime
from kafka import *
from minio import Minio
from minio.error import S3Error
import datetime
import json

"""
Mini-Projet : Traitement de l'Intelligence Artificielle
Contexte : Allier les concepts entre l'IA, le Big Data et IoT

Squelette pour simuler un capteur qui est temporairement stocké sous la forme de Pandas
"""

"""
    Dans ce fichier capteur.py, vous devez compléter les méthodes pour générer les données brutes vers Pandas 
    et par la suite, les envoyer vers Kafka.
    ---
    Le premier devra prendre le contenue du topic donnees_capteurs pour le stocker dans un HDFS. (Fichier Kafka-topic.py)
    Le deuxième devra prendre le contenue du HDFS pour nettoyer les données que vous avez besoin avec SPARK, puis le stocker en HDFS.
    Le troisième programme est un entraînement d'un modèle du machine learning (Vous utiliserez TensorFlow, avec une Régression Linéaire) (Fichier train.py)
    Un Quatrième programme qui va prédire une valeur en fonction de votre projet. (Fichier predict.py)
"""
        # humidity = random.randrange(0,100)
        # pressure = random.randrange(980, 1100)
        # monoxydeCarbon = random.randrange(0, 1)
        # nicotine = random.randrange(0,1)
        # carbon = random.randrange(0,1)
        # temperature = random.randrange(0, 35)
        # now = datetime.datetime.now()
def set_air_quality(humidity, pressure, monoxydeCarbon, nicotine,carbon, temperature):
    air_quality = 0
    if(humidity > 40 and pressure <= 1080):
        air_quality = 1
    if(humidity > 80 and pressure > 1080):
        air_quality = 2
    if(nicotine == 1):
        air_quality = 3
    if(carbon == 1):
        air_quality = 4
    if(carbon == 1 and temperature >30):
        air_quality = 5
    if(monoxydeCarbon == 1):
        air_quality = 6
    
    return air_quality

def generate_dataFrame(col):
    """
    Cette méthode permet de générer un DataFrame Pandas pour alimenter vos data
    """
    df = pd.DataFrame(columns=col)
    add_data(df)
    return df

def add_data(df: pd.DataFrame):
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas
    """
    # temps_execution =
    current_timestamp = datetime.datetime.now()

    
    duration = 60 # Nombre de secondes pour la génération des données.

    i = 0
    while i < 1000:
        ## Dans cette boucle, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
        ## Chaque itération comportera 1 ligne à ajouter.
        humidity = random.randrange(0,100)
        pressure = random.randrange(980, 1100)
        monoxydeCarbon = random.randint(0, 1)
        nicotine = random.randint(0,1)
        carbon = random.randint(0,1)
        temperature = random.randrange(0, 35)
        now = datetime.datetime.now()
        # ajouter une ligne au DataFrame
        air_quality = set_air_quality(humidity, pressure, monoxydeCarbon, nicotine, carbon, temperature)
        df.loc[i] = [now, humidity, pressure, monoxydeCarbon, nicotine, carbon, temperature, air_quality]
        i += 1
    return df

def write_data_kafka(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Kafka.
    (Optionnel)
    """
    
    df_json = df.to_json()
    producer = KafkaProducer( bootstrap_servers=['127.0.0.1:9092'])
    producer.send('capteur', value=bytes(df_json, 'utf-8'))
    producer.close()

def write_data_minio(df: pd.DataFrame):
    """
    Cette méthode permet d'écrire le DataFrame vers Minio.
    (Obligatoire)
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    found = client.bucket_exists("donnes-capteurs")
    if not found:
        client.make_bucket("donnes-capteurs")
    else:
        print("Bucket 'donnes-capteurs' existe déjà")

    timestamp = datetime.datetime.now().strftime('%d-%m-%y')
    df.to_csv("donnes_capteurs_" + str(timestamp) + ".csv", encoding='utf-8', index=False)
    client.fput_object(
        "donnes-capteurs", "donnes_capteurs_" + str(timestamp) + ".csv",  "donnes_capteurs_" + str(timestamp) + ".csv")

if __name__ == "__main__":
    columns = ["timestamp", "humidity", "pressure","monoxyde_carbone", "nicotine", "carbone", "temperature","air_quality"]
    df = generate_dataFrame(columns)
    write_data_kafka(df)
    # write_data_minio(df)