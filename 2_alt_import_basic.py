# Script de démonstration avec Weaviate
# Mise à jour avec des corrections et des print() pour suivi

import weaviate
from weaviate.classes.data import DataObject

# Connexion au client local
client = weaviate.connect_to_local()

print("Connexion établie avec Weaviate.")

# Récupération des collections
reviews = client.collections.get("Review")
movies = client.collections.get("Movie")
print("Collections récupérées :")
print(f"- Reviews: {reviews}")
print(f"- Movies: {movies}")

# Ajout d'un objet à la collection Review
tgt_uuid = reviews.data.insert(properties={"username": "jphwang1"})
print(f"Nouvelle Review insérée avec UUID : {tgt_uuid}")

# Ajout d'un objet Movie avec une référence à la Review
movies.data.insert(
    properties={"title": "Modern Times"},
    references={"hasReview": tgt_uuid}
)
print(f"Film 'Modern Times' ajouté avec une référence à la Review : {tgt_uuid}")

# Insertion multiple d'objets dans Review
review_props = [{"username": f"user_{i}"} for i in range(5)]
reviews.data.insert_many(review_props)
print(f"5 Reviews ajoutées : {review_props}")

# Insertion multiple d'objets dans Movie avec des références
movie_objs = [
    DataObject(
        properties={"title": f"Movie {i}"},
        references={"hasReview": tgt_uuid},
    ) for i in range(5)
]
movies.data.insert_many(movie_objs)
print(f"5 Films ajoutés avec des références : {[obj.properties['title'] for obj in movie_objs]}")

# Insertion avec batch fixe
print("Ajout avec batch fixe...")
with client.batch.fixed_size(batch_size=100) as batch:
    batch.add_object(
        properties={"title": "Avatar"},
        collection="Movie",
    )
print("Film 'Avatar' ajouté dans un batch fixe.")

# Insertion avec batch dynamique
print("Ajout avec batch dynamique...")
with movies.batch.dynamic() as batch:
    for i in range(100):
        batch.add_object(
            properties={"title": "When Harry Met Sally"}
        )
        if batch.number_errors > 100:  # Si plus de 100 erreurs, on arrête
            print("Plus de 100 erreurs détectées, arrêt du batch.")
            break
print("Batch dynamique terminé.")

# Vérification des erreurs
if len(movies.batch.failed_objects) > 0 or len(movies.batch.failed_references) > 0:
    print("Des erreurs ont été détectées dans les batchs.")
    print(f"Objets échoués : {movies.batch.failed_objects}")
    print(f"Références échouées : {movies.batch.failed_references}")
else:
    print("Tous les objets et références ont été ajoutés avec succès.")

# Fermeture du client
client.close()
print("Connexion avec Weaviate fermée.")
