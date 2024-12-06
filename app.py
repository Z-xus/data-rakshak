from flask import Flask, request, jsonify, Response
from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig
from presidio_image_redactor import ImageRedactorEngine
from presidio_image_redactor.entities import InvalidParamError

from PIL import Image
from io import BytesIO

app = Flask(__name__)

# Initialize engines
analyzer = AnalyzerEngine()
anonymizer = AnonymizerEngine()


@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        data = request.get_json()
        text = data.get("text")

        if not text:
            return jsonify({"error": "No text provided"}), 400

        # Analyze text
        analyzer_results = analyzer.analyze(text=text, language="en")

        return jsonify({"results": [result.to_dict() for result in analyzer_results]})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/process", methods=["POST"])
def process():
    try:
        data = request.get_json()
        text = data.get("text")
        action = data.get("action", "anonymize")  # 'anonymize' or 'redact'

        if not text:
            return jsonify({"error": "No text provided"}), 400

        # First analyze the text
        analyzer_results = analyzer.analyze(text=text, language="en")

        if action == "anonymize":
            # Anonymize the detected PII
            anonymized_result = anonymizer.anonymize(
                text=text, analyzer_results=analyzer_results
            )
            return jsonify(
                {"original_text": text, "processed_text": anonymized_result.text}
            )

        elif action == "redact":
            # Redact the detected PII
            operator_config = OperatorConfig("replace", {"new_value": "<REDACTED>"})
            redacted_result = anonymizer.anonymize(
                text=text,
                analyzer_results=analyzer_results,
                operators={"DEFAULT": operator_config},
            )
            return jsonify(
                {"original_text": text, "processed_text": redacted_result.text}
            )

        else:
            return jsonify({"error": "Invalid action specified"}), 400

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/process-image", methods=["POST"])
def process_image():
    """Process an uploaded image for redaction."""
    try:
        # Check for image data in request
        if not request.files or "image" not in request.files:
            raise InvalidParamError("Invalid parameter, please add image data")

        # Get image from request
        im = Image.open(request.files.get("image"))

        # Get fill color (default to 'contrast')
        color_fill = request.form.get("fill")

        # Initialize the redactor engine if not already done
        engine = ImageRedactorEngine()

        # Perform redaction
        redacted_image = engine.redact(im, color_fill, score_threshold=0.4)

        # Convert to byte array for response
        img_byte_arr = BytesIO()
        redacted_image.save(img_byte_arr, format=im.format or "PNG")
        img_byte_arr = img_byte_arr.getvalue()

        return Response(img_byte_arr, mimetype="application/octet-stream")

    except InvalidParamError as e:
        print(f"Invalid Parameter Error: {e}")
        return {"error": str(e)}, 400
    except Exception as e:
        print(f"Unhandled error: {e}")
        return {"error": "Internal Server Error"}, 500


if __name__ == "__main__":
    app.run(debug=True)
