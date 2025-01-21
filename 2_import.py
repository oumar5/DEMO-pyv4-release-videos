import weaviate
import os
import pandas as pd
from datetime import datetime, timezone
from weaviate.util import generate_uuid5

# Connexion au client Weaviate
print("Connecting to Weaviate...")
client = weaviate.connect_to_local()
print("Connected.")

# Récupération des collections
movies = client.collections.get("Movie")
reviews = client.collections.get("Review")
print("Collections retrieved: Movie and Review.")

# Chargement des données depuis les fichiers JSON
print("Loading data...")
df = pd.read_json("data/movie_reviews_1990_2024_20_movies_info.json")
rdf = pd.read_json("data/movie_reviews_1990_2024_20_movie_reviews.json")
print(f"Loaded {len(df)} movies and {len(rdf)} reviews.")

# Mapping des IDs pour les références
ref_id_map = dict()
print("Adding reviews...")
with reviews.batch.fixed_size(batch_size=50, concurrent_requests=2) as batch:
    for i, row in rdf.iterrows():
        movie_id = row["id"]
        ref_ids = []
        for review in row["results"]:
            props = {
                "username": review["author_details"]["username"],
                "content": review["content"],
                "tmdb_id": review["id"],
            }
            review_uuid = generate_uuid5(row["id"])
            batch.add_object(
                properties=props,
                uuid=review_uuid
            )
            ref_ids.append(review_uuid)
        ref_id_map[movie_id] = ref_ids
        if i % 50 == 0:
            print(f"Processed {i} reviews...")

# Gestion des erreurs pour les reviews
if len(reviews.batch.failed_objects) > 0 or len(reviews.batch.failed_references) > 0:
    print("Failed review objects:", reviews.batch.failed_objects[:5])
    print("Failed review references:", reviews.batch.failed_references[:5])

# Ajout des films et des références aux reviews
print("Adding movies...")
with movies.batch.fixed_size(batch_size=50, concurrent_requests=2) as batch:
    for i, row in df.iterrows():
        cols = ["title", "tagline", "overview", "vote_average", "runtime", "imdb_id"]
        movie_id = row["id"]
        movie_uuid = generate_uuid5(row["id"])
        props = {c: row[c] for c in cols}
        props["release_date"] = datetime.strptime(row["release_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        # Ajout des références aux reviews
        if movie_id in ref_id_map.keys():
            refs = {"hasReview": ref_id_map[movie_id]}
        else:
            refs = None

        batch.add_object(
            properties=props,
            references=refs,
            uuid=movie_uuid
        )
        if i % 100 == 0:
            print(f"Processed {i} movies...")

# Gestion des erreurs pour les films
if len(movies.batch.failed_objects) > 0 or len(movies.batch.failed_references) > 0:
    print("Failed movie objects:", movies.batch.failed_objects[:5])
    print("Failed movie references:", movies.batch.failed_references[:5])

# Fermeture du client Weaviate
print("Closing connection to Weaviate...")
client.close()
print("Connection closed.")
