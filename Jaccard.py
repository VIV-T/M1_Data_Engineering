# Fonction pour calculer la similarité de Jaccard
def jaccard_similarity(list1, list2):
    set1 = set(list1)
    set2 = set(list2)
    intersection = len(set1.intersection(set2))
    union = len(set1.union(set2))
    return intersection / union if union != 0 else 0


# Function to find the most similar movie
def find_most_similar_document(reference_doc_name, collection):
    # Get all the documents in the collection (movies)
    documents = list(collection.find({}, {"name": 1, "tropes": 1}))

    # Find the base document
    reference_doc = next(
        (doc for doc in documents if doc["name"] == reference_doc_name),
        None
    )
    if not reference_doc:
        raise ValueError(f"Base document '{reference_doc_name}' not found.")

    reference_tropes = reference_doc["tropes"]

    # Similarity calculation with all other tropes.
    similarities = []
    for doc in documents:
        if doc["name"] != reference_doc_name:
            similarity = jaccard_similarity(reference_tropes, doc["tropes"])
            similarities.append((doc["name"], similarity))

    # Sort by similarity (decreasing)
    similarities.sort(key=lambda x: x[1], reverse=True)

    # Return the most similar movie (document)
    most_similar_name, most_similar_score = similarities[0] if similarities else (None, 0)
    return most_similar_name, most_similar_score
