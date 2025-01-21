import weaviate
import weaviate.classes.config as wc

# Connexion au client Weaviate
print("Connecting to Weaviate...")
client = weaviate.connect_to_local()
print("Connected successfully.")

# Création de la classe Review
print("\nCreating the 'Review' class...")
reviews = client.collections.create(
    name="Review",
    properties=[
        wc.Property(name="username", data_type=wc.DataType.TEXT, skip_vectorization=True),
        wc.Property(name="content", data_type=wc.DataType.TEXT),  # Sera vectorisé
        wc.Property(name="tmdbId", data_type=wc.DataType.TEXT, skip_vectorization=True),  # camelCase
    ],
    vectorizer_config=wc.Configure.Vectorizer.text2vec_contextionary(),
    generative_config=wc.Configure.Generative.openai(),
    inverted_index_config=wc.Configure.inverted_index(
        index_property_length=True
    )
)
print("Review class created successfully.")

# Création de la classe Movie
print("\nCreating the 'Movie' class...")
movies = client.collections.create(
    name="Movie",
    properties=[
        wc.Property(name="title", data_type=wc.DataType.TEXT),  # Sera vectorisé
        wc.Property(name="tagline", data_type=wc.DataType.TEXT),  # Sera vectorisé
        wc.Property(name="overview", data_type=wc.DataType.TEXT),  # Sera vectorisé
        wc.Property(name="voteAverage", data_type=wc.DataType.NUMBER),  # camelCase
        wc.Property(name="releaseDate", data_type=wc.DataType.DATE),  # camelCase
        wc.Property(name="runtime", data_type=wc.DataType.INT),
        wc.Property(name="imdbId", data_type=wc.DataType.TEXT, skip_vectorization=True),  # camelCase
        wc.Property(name="tmdbId", data_type=wc.DataType.INT),  # camelCase
    ],
    vectorizer_config=wc.Configure.Vectorizer.text2vec_contextionary(),
    generative_config=wc.Configure.Generative.openai(),
    vector_index_config=wc.Configure.VectorIndex.hnsw(
        distance_metric=wc.VectorDistances.COSINE
    ),
    references=[
        wc.ReferenceProperty(name="hasReview", target_collection="Review")
    ]
)
print("Movie class created successfully.")

# Fermeture du client
print("\nClosing connection to Weaviate...")
client.close()
print("Connection closed.")
