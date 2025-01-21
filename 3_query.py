import weaviate
import os
import weaviate.classes.query as wq

# Connexion à Weaviate
print("Connecting to Weaviate...")
client = weaviate.connect_to_local()

if client.is_ready():
    print("Connected.")
else:
    print("Weaviate is not ready. Exiting.")
    exit()

# Récupérer les collections
print("Retrieving collections...")
movies = client.collections.get("Movie")
reviews = client.collections.get("Review")
print(f"Collections retrieved: {movies.name} and {reviews.name}")

# Effectuer une recherche avec des propriétés de filtrage
print("Performing query...")
response = movies.query.near_text(
    query="holiday season",
    limit=4,
    return_references=[
        wq.QueryReference(link_on="hasReview", return_properties=["username", "content"])
    ],
    return_properties=["title", "tagline", "runtime"],
    filters=(
        wq.Filter.by_property("runtime").less_than(100) & 
        wq.Filter.by_property("runtime").greater_than(85)
    )
)

# Afficher les résultats
for o in response.objects:
    print("\n===== Movie =====")
    print(f"Title: {o.properties['title']}")
    print(f"Tagline: {o.properties['tagline']}")
    print(f"Runtime: {o.properties['runtime']}")
    print(f"UUID: {o.uuid}")
    if o.references["hasReview"].objects:
        review = o.references["hasReview"].objects[0]
        print(f"Sample review by: {review.properties['username']}")
        print(f"Review body: {review.properties['content']}")
    else:
        print("No reviews available for this movie.")

# Fermer la connexion
print("Closing connection to Weaviate...")
client.close()
print("Connection closed.")
