import tensorflow as tf


if __name__ == "__main__":
    # Chargement du modèle
    new_model = tf.keras.models.load_model('saved_model/my_model')
    
    # Charger ici les données pour prédire une valeur du modèle entrainé

    # Lancement de la prédiction des données
    result = new_model.predict("Mettre les historiques ici à prédire")

    # Affichage du résultat
    print(result.shape)