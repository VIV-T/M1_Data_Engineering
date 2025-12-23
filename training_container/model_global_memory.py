from transformers import LongformerModel, LongformerPreTrainedModel
import torch.nn as nn

class GlobalContextLongformer(LongformerPreTrainedModel):
    def __init__(self, config, num_local_labels, num_global_labels):
        super().__init__(config)
        self.longformer = LongformerModel(config)
        self.local_classifier = nn.Linear(config.hidden_size, num_local_labels)
        self.global_classifier = nn.Linear(config.hidden_size, num_global_labels)
        self.global_memory = nn.LSTM(config.hidden_size, config.hidden_size, batch_first=True)

        self.init_weights()

    def forward(self, input_ids, attention_mask, global_context=None, labels=None):
        outputs = self.longformer(input_ids=input_ids, attention_mask=attention_mask)
        sequence_output = outputs.last_hidden_state

        # Classification des tropes locaux
        local_logits = self.local_classifier(sequence_output[:, 0, :])

        # Mise à jour de la mémoire globale
        if global_context is not None:
            _, (hidden, _) = self.global_memory(global_context.unsqueeze(0))
            global_features = hidden.squeeze(0)
        else:
            global_features = sequence_output[:, 0, :]

        # Classification des tropes globaux
        global_logits = self.global_classifier(global_features)

        loss = None
        if labels is not None:
            local_loss = nn.BCEWithLogitsLoss()(local_logits, labels["local_labels"].float())
            global_loss = nn.BCEWithLogitsLoss()(global_logits, labels["global_labels"].float())
            loss = local_loss + global_loss

        return {"loss": loss, "local_logits": local_logits, "global_logits": global_logits}

# --- Initialisation du modèle ---
model = GlobalContextLongformer(
    config=LongformerModel.from_pretrained("allenai/longformer-base-4096").config,
    num_local_labels=len(all_local_tropes),
    num_global_labels=len(all_global_tropes),
)
