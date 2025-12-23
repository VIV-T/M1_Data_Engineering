import pandas as pd
from datasets import Dataset
from sklearn.model_selection import train_test_split
from transformers import LongformerTokenizer
import torch
from torch.utils.data import Dataset as TorchDataset

# --- Exemple de données annotées ---
data = {
    "scene_id": [1, 2, 3, 10],
    "scene_text": [
        "The hero discovers his power.",
        "The mentor explains the quest.",
        "A mysterious object appears in the forest.",
        "The hero triumphs over the villain."
    ],
    "local_tropes": [
        ["chosen_one"],  # Tropes locaux pour la scène 1
        ["mentor_archetype"],  # Tropes locaux pour la scène 2
        ["chekhovs_gun"],  # Tropes locaux pour la scène 3
        []  # Aucun trope local pour la scène 10
    ],
    "global_tropes": [
        {"arc_transformation": "start"},  # Tropes globaux pour la scène 1
        {"arc_transformation": "continue"},  # Tropes globaux pour la scène 2
        {"chekhovs_gun": "start", "arc_transformation": "continue"},  # Tropes globaux pour la scène 3
        {"arc_transformation": "end", "chekhovs_gun": "end"}  # Tropes globaux pour la scène 10
    ]
}

df = pd.DataFrame(data)

# --- Génération du contexte global ---
def generate_global_context(df):
    global_contexts = []
    active_tropes = {}  # Suivi des tropes actifs

    for _, row in df.iterrows():
        scene_id = row["scene_id"]
        global_tropes = row["global_tropes"]

        # Mise à jour des tropes actifs
        for trope, status in global_tropes.items():
            if status == "start":
                active_tropes[trope] = {"status": status, "scenes": [scene_id]}
            elif status == "continue":
                if trope in active_tropes:
                    active_tropes[trope]["scenes"].append(scene_id)
            elif status == "end":
                if trope in active_tropes:
                    active_tropes[trope]["scenes"].append(scene_id)
                    active_tropes[trope]["status"] = "end"

        # Sauvegarde du contexte global pour cette scène
        global_contexts.append(active_tropes.copy())

        # Nettoyage des tropes terminés
        for trope in list(active_tropes.keys()):
            if active_tropes[trope]["status"] == "end":
                del active_tropes[trope]

    df["global_tropes_context"] = global_contexts
    return df

df = generate_global_context(df)

# --- Conversion en Dataset Hugging Face ---
dataset = Dataset.from_pandas(df)

# --- Tokenization ---
tokenizer = LongformerTokenizer.from_pretrained("allenai/longformer-base-4096")

def tokenize_function(examples):
    return tokenizer(
        examples["scene_text"],
        padding="max_length",
        truncation=True,
        max_length=4096,
    )

tokenized_dataset = dataset.map(tokenize_function, batched=True)

# --- Préparation des labels ---
# Conversion des listes de tropes locaux/globaux en vecteurs binaires
all_local_tropes = sorted(list(set(sum(df["local_tropes"].tolist(), []))))
all_global_tropes = sorted(list(set([trope for d in df["global_tropes"] for trope in d.keys()])))

def prepare_labels(examples):
    local_labels = torch.zeros(len(examples["scene_text"]), len(all_local_tropes))
    global_labels = torch.zeros(len(examples["scene_text"]), len(all_global_tropes))

    for i, (local, global_tropes) in enumerate(zip(examples["local_tropes"], examples["global_tropes_context"])):
        for trope in local:
            local_labels[i, all_local_tropes.index(trope)] = 1
        for trope, info in global_tropes.items():
            global_labels[i, all_global_tropes.index(trope)] = 1

    return {"local_labels": local_labels, "global_labels": global_labels}

tokenized_dataset = tokenized_dataset.map(prepare_labels, batched=True)

# --- Split train/test ---
train_test_split = tokenized_dataset.train_test_split(test_size=0.2)
train_dataset = train_test_split["train"]
test_dataset = train_test_split["test"]
