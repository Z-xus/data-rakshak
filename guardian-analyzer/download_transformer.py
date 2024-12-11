from huggingface_hub import snapshot_download
from transformers import AutoTokenizer, AutoModelForTokenClassification
import torch

# Define the model path
transformers_model = "MichaelHuang/muril_base_cased_hindi_ner"

# Download the model and tokenizer
snapshot_download(repo_id=transformers_model)

# Ensure the model and tokenizer are correctly downloaded
AutoTokenizer.from_pretrained(transformers_model)
AutoModelForTokenClassification.from_pretrained(transformers_model)

# Example:


# model = AutoModelForTokenClassification.from_pretrained(
#     "MichaelHuang/muril_base_cased_hindi_ner"
# )
# tokenizer = AutoTokenizer.from_pretrained("google/muril-base-cased")
#
# # Define the labels dictionary
# labels_dict = {
#     0: "B-FESTIVAL",
#     1: "B-GAME",
#     2: "B-LANGUAGE",
#     3: "B-LITERATURE",
#     4: "B-LOCATION",
#     5: "B-MISC",
#     6: "B-NUMEX",
#     7: "B-ORGANIZATION",
#     8: "B-PERSON",
#     9: "B-RELIGION",
#     10: "B-TIMEX",
#     11: "I-FESTIVAL",
#     12: "I-GAME",
#     13: "I-LANGUAGE",
#     14: "I-LITERATURE",
#     15: "I-LOCATION",
#     16: "I-MISC",
#     17: "I-NUMEX",
#     18: "I-ORGANIZATION",
#     19: "I-PERSON",
#     20: "I-RELIGION",
#     21: "I-TIMEX",
#     22: "O",
# }
#
#
# def ner_predict(sentence, model, tokenizer, labels_dict):
#     # Tokenize the input sentence
#     inputs = tokenizer(
#         sentence, return_tensors="pt", padding=True, truncation=True, max_length=128
#     )
#
#     # Perform inference
#     with torch.no_grad():
#         outputs = model(**inputs)
#
#     # Get the predicted labels
#     predicted_labels = torch.argmax(outputs.logits, dim=2)
#
#     # Convert tokens and labels to lists
#     tokens = tokenizer.convert_ids_to_tokens(inputs["input_ids"][0])
#     labels = predicted_labels.squeeze().tolist()
#
#     # Map numeric labels to string labels
#     predicted_labels = [labels_dict[label] for label in labels]
#
#     # Combine tokens and labels
#     result = list(zip(tokens, predicted_labels))
#
#     return result
#
#
# test_sentence = "अकबर ईद पर टेनिस खेलता है"
# predictions = ner_predict(test_sentence, model, tokenizer, labels_dict)
#
# for token, label in predictions:
#     print(f"{token}: {label}")
