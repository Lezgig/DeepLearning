from kafka import KafkaConsumer
import pandas as pd
import io
import json
from minio import Minio
from minio.error import S3Error
import datetime

# https://towardsdatascience.com/kafka-python-explained-in-10-lines-of-code-800e3e07dad1

def main():
    col = ["timestamp", "humidity", "pressure", "temperature","monoxyde_carbone", "nicotine", "carbone"]
    df = pd.DataFrame(columns=col)

    bootstrap_servers = ['localhost:9092']
    # Initialiser le client MinIO
    minioClient = Minio('localhost:9000',
                    access_key='minio',
                    secret_key='minio123',
                    secure=False)

    # Vérifier si le seau existe, sinon le créer
    if not minioClient.bucket_exists("mybucket"):
        minioClient.make_bucket("mybucket")

    # Initialiser le consommateur Kafka
    consumer = KafkaConsumer ("capteur", group_id ='group1',
                              bootstrap_servers = bootstrap_servers)


    # Boucle infinie pour lire les données Kafka
    for message in consumer:
        json_message = json.loads(message.value)
        
        print(json.dumps(json_message, indent=4, sort_keys=True))
    # Enregistrer les données dans le seau MinIO
        try:
            now = datetime.datetime.now()
            datetimeFormatted = now.strftime("%Y-%m-%d")
            # Définir le nom de l'objet
            object_name = f"donnees-capteur.{datetimeFormatted}.json"
            # Encodage des données en JSON
            data_json = json.dumps(json_message).encode('utf-8')

            # Enregistrement des données dans le bucket MinIO
            minioClient.put_object(
                "donnes-capteurs",
                object_name,
                io.BytesIO(data_json),
                len(data_json),
                content_type='application/json'
            )

        except S3Error as e:
            print("Error:", e)

if __name__ == "__main__":
  main()