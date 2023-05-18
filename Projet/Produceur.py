from kafka import KafkaProducer
import datetime
import numpy as np

def add_datatokafka():
    """
    Cette méthode permet d'ajouter de la donnée vers votre DataFrame Pandas

    Prenez la base de la fonction addData() dans le fichier capteur.py
    """

    # on va creer une boucle infinie, vous devez créer et ajouter des données dans Pandas à valeur aléatoire.
    # Chaque itération comportera 1 ligne à ajouter. ex : timestamp = datetime.timedelta(seconds=1)
    #  on va creer une liste avec les elements créees
    # on va crée le produceur kafka qui envoie la liste sur le topic qu'on a crée sur conduktor


if __name__ == "__main__":
    add_datatokafka()