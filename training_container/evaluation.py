from sklearn.metrics import f1_score
import numpy as np

def compute_metrics(eval_pred):
    local_logits, global_logits, local_labels, global_labels = eval_pred
    local_preds = (torch.sigmoid(torch.tensor(local_logits)) > 0.5).int().numpy()
    global_preds = (torch.sigmoid(torch.tensor(global_logits)) > 0.5).int().numpy()

    local_f1 = f1_score(local_labels, local_preds, average="micro")
    global_f1 = f1_score(global_labels, global_preds, average="micro")

    return {"local_f1": local_f1, "global_f1": global_f1}

trainer = CustomTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=test_dataset,
    compute_metrics=compute_metrics,
)

# --- Évaluation finale ---
results = trainer.evaluate()
print(results)
