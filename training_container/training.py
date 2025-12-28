import json
import numpy as np
from sentence_transformers import SentenceTransformer
import faiss
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import torch

# --- 1. Charger la base de données de tropes ---
with open("tropes_db.json", "r", encoding="utf-8") as f:
    tropes_db = json.load(f)["tropes"]

# --- 2. Encoder les définitions de tropes avec Sentence-BERT ---
model_embedding = SentenceTransformer('all-MiniLM-L6-v2')
trope_definitions = [trope["definition"] for trope in tropes_db]
trope_embeddings = model_embedding.encode(trope_definitions)

# --- 3. Créer un index FAISS pour la recherche vectorielle ---
dimension = trope_embeddings.shape[1]
index = faiss.IndexFlatL2(dimension)
index.add(trope_embeddings)

# --- 4. Fonction pour segmenter le script ---
def segment_script(script_path, max_length=1000):
    with open(script_path, "r", encoding="utf-8") as f:
        script = f.read()
    # Découper par paragraphes ou scènes (marquées par "INT." ou "EXT.")
    segments = [p.strip() for p in script.split("\n\n") if p.strip()]
    return segments

# --- 5. Fonction pour récupérer les tropes pertinents ---
def retrieve_tropes(text, index, tropes_db, model_embedding, k=3):
    text_embedding = model_embedding.encode([text])
    distances, indices = index.search(text_embedding, k)
    return [tropes_db[i]["name"] for i in indices[0]]

# --- 6. Charger le modèle génératif (Mistral-7B) ---
model_name = "mistralai/Mistral-7B-Instruct-v0.2"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForCausalLM.from_pretrained(model_name, torch_dtype=torch.float16, device_map="auto")

# --- 7. Fonction pour générer la réponse avec le modèle ---
def generate_response(text, retrieved_tropes, is_global=False):
    if is_global:
        task = "Analyse ce script **dans son ensemble** et confirme la présence des tropes suivants."
    else:
        task = "Analyse ce **segment de script** et confirme la présence des tropes suivants."

    prompt = f"""
    {task}
    Texte : {text}

    Tropes potentiels : {', '.join(retrieved_tropes)}

    **Consignes** :
    1. Confirme si ces tropes sont présents.
    2. Si oui, explique pourquoi.
    3. Si non, propose d'autres tropes pertinents.
    4. Réponds en JSON : {{"tropes": ["trope1", "trope2"], "explications": "..."}}.
    """

    pipe = pipeline(
        "text-generation",
        model=model,
        tokenizer=tokenizer,
        max_new_tokens=256,
        temperature=0.3
    )
    response = pipe(prompt)[0]["generated_text"]
    return response

# --- 8. Pipeline hybride ---
def analyze_hybrid(script_path):
    with open(script_path, "r", encoding="utf-8") as f:
        script = f.read()

    # --- Analyse globale ---
    global_tropes = retrieve_tropes(script, index, tropes_db, model_embedding, k=5)
    global_response = generate_response(script, global_tropes, is_global=True)

    # --- Analyse locale (par segments) ---
    segments = segment_script(script_path)
    local_results = []
    for segment in segments:
        local_tropes = retrieve_tropes(segment, index, tropes_db, model_embedding, k=3)
        local_response = generate_response(segment, local_tropes, is_global=False)
        local_results.append({
            "segment": segment,
            "response": local_response
        })

    return {
        "global_analysis": global_response,
        "local_analysis": local_results
    }

# --- 9. Exécution ---
if __name__ == "__main__":
    script_path = "script.txt"
    results = analyze_hybrid(script_path)

    print("=== ANALYSE GLOBALE ===")
    print(results["global_analysis"])

    print("\n=== ANALYSE LOCALE ===")
    for i, result in enumerate(results["local_analysis"]):
        print(f"\n--- Segment {i+1} ---")
        print(f"Segment: {result['segment'][:200]}...")
        print(f"Réponse: {result['response']}")
