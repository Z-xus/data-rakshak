import json


def extract_entities_with_text(s, entities):
    """
    Process the input JSON entities to include the actual text from the given string.

    Args:
        s (str): The string to extract substrings from using start and end indices.
        entities (list): The list of entities containing start and end indices.

    Returns:
        list: The updated entities with the actual text included.
    """
    updated_entities = []
    for entity in entities:
        # Extract the substring using start and end indices
        start = entity.get("start")
        end = entity.get("end")
        if start is not None and end is not None:
            actual_text = s[start:end]
            # Include the text in the entity dictionary
            entity["text"] = actual_text
        updated_entities.append(entity)
    return updated_entities


# Example usage
if __name__ == "__main__":
    json_input = """[
  {
    "analysis_explanation": null,
    "end": 528,
    "entity_type": "EMAIL_ADDRESS",
    "recognition_metadata": {
      "recognizer_identifier": "EmailRecognizer_139463442380224",
      "recognizer_name": "EmailRecognizer"
    },
    "score": 1,
    "start": 501
  },
  {
    "analysis_explanation": null,
    "end": 528,
    "entity_type": "PERSON",
    "recognition_metadata": {
      "recognizer_identifier": "TransformersRecognizer_139459502856224",
      "recognizer_name": "TransformersRecognizer"
    },
    "score": 0.8569611310958862,
    "start": 499
  },
  {
    "analysis_explanation": null,
    "end": 511,
    "entity_type": "URL",
    "recognition_metadata": {
      "recognizer_identifier": "UrlRecognizer_139459713950320",
      "recognizer_name": "UrlRecognizer"
    },
    "score": 0.5,
    "start": 501
  },
  {
    "analysis_explanation": null,
    "end": 528,
    "entity_type": "URL",
    "recognition_metadata": {
      "recognizer_identifier": "UrlRecognizer_139459713950320",
      "recognizer_name": "UrlRecognizer"
    },
    "score": 0.5,
    "start": 517
  },
  {
    "analysis_explanation": null,
    "end": 561,
    "entity_type": "PHONE_NUMBER",
    "recognition_metadata": {
      "recognizer_identifier": "PhoneRecognizer_139463442375712",
      "recognizer_name": "PhoneRecognizer"
    },
    "score": 0.4,
    "start": 547
  },
  {
    "analysis_explanation": null,
    "end": 595,
    "entity_type": "PHONE_NUMBER",
    "recognition_metadata": {
      "recognizer_identifier": "PhoneRecognizer_139463442375712",
      "recognizer_name": "PhoneRecognizer"
    },
    "score": 0.4,
    "start": 584
  },
  {
    "analysis_explanation": null,
    "end": 745,
    "entity_type": "PHONE_NUMBER",
    "recognition_metadata": {
      "recognizer_identifier": "PhoneRecognizer_139463442375712",
      "recognizer_name": "PhoneRecognizer"
    },
    "score": 0.4,
    "start": 731
  }
]"""
    entities = json.loads(json_input)
    s = """Here’s the revised example in Hindi with numbers written in the Devanagari script:  ---  **से:** सार्वजनिक अभिलेख विभाग   **विषय:** आधिकारिक दस्तावेज़ सबमिशन    प्रिय **माइकल जॉनसन**,    आपका आवेदन प्राप्त करने के लिए धन्यवाद। आपके द्वारा दी गई जानकारी का विवरण नीचे दिया गया है:    - **पूरा नाम:** माइकल आरोन जॉनसन   - **जन्म तिथि:** २२ जुलाई १९८०   - **सामाजिक सुरक्षा संख्या:** ९८७-६५-४३२१   - **ड्राइविंग लाइसेंस नंबर:** डी१२३४५६७८   - **पता:** ४५६ लिबर्टी लेन, ऑस्टिन, TX ७३३०१   - **ईमेल पता:** michael.johnson@govmail.com   - **फोन नंबर:** (५१२) ५५५-१२३४    आपका केस आईडी **TX-२०२३-०४५६७८** है और इसे आगे की प्रक्रिया के लिए **स्वास्थ्य और सुरक्षा विभाग** को भेज दिया गया है। यदि आपको सहायता की आवश्यकता हो, तो कृपया हमसे **(८००) ५५५-६७८९** पर संपर्क करें।    सादर,   **एलिज़ाबेथ राइट**   केस प्रबंधक   सार्वजनिक अभिलेख विभाग    ---  यह उदाहरण संख्याओं और डेटा के लिए हिंदी अंकों का उपयोग करते हुए एक सटीक सरकारी पत्राचार का प्रतिनिधित्व करता है। इसे PII विश्लेषण उपकरणों के परीक्षण के लिए इस्तेमाल किया जा सकता है।"""  # Replace with the actual string
    result = extract_entities_with_text(s, entities)
    print(json.dumps(result, indent=4, ensure_ascii=False))
