"""REST API server for image redactor."""

import json  # Ensure JSON is imported

import base64
import logging
import os
from io import BytesIO

from flask import Flask, Response, jsonify, request
from PIL import Image
from presidio_image_redactor import ImageRedactorEngine
from presidio_image_redactor.entities import InvalidParamError
from presidio_image_redactor.entities.api_request_convertor import (
    color_fill_string_to_value,
    get_json_data,
    image_to_byte_array,
)

DEFAULT_PORT = "3000"

WELCOME_MESSAGE = r"""
 _______  _______  _______  _______ _________ ______  _________ _______
(  ____ )(  ____ )(  ____ \(  ____ \\__   __/(  __  \ \__   __/(  ___  )
| (    )|| (    )|| (    \/| (    \/   ) (   | (  \  )   ) (   | (   ) |
| (____)|| (____)|| (__    | (_____    | |   | |   ) |   | |   | |   | |
|  _____)|     __)|  __)   (_____  )   | |   | |   | |   | |   | |   | |
| (      | (\ (   | (            ) |   | |   | |   ) |   | |   | |   | |
| )      | ) \ \__| (____/\/\____) |___) (___| (__/  )___) (___| (___) |
|/       |/   \__/(_______/\_______)\_______/(______/ \_______/(_______)
"""


class Server:
    """Flask server for image redactor."""

    def __init__(self):
        self.logger = logging.getLogger("presidio-image-redactor")
        self.app = Flask(__name__)
        self.logger.info("Starting image redactor engine")
        self.engine = ImageRedactorEngine()
        self.logger.info(WELCOME_MESSAGE)

        @self.app.route("/health")
        def health() -> str:
            """Return basic health probe result."""
            return "Presidio Image Redactor service is up"

        @self.app.route("/redact", methods=["POST"])
        def redact():
            """Return a redacted image."""
            try:
                # Extract and parse parameters from `data`
                data = request.form.get("data")
                if not data:
                    raise InvalidParamError("Invalid parameter, 'data' is missing")

                print("Raw data:", data)

                try:
                    # Attempt to load JSON from 'data'
                    params = json.loads(data)
                    if not isinstance(params, dict):
                        raise InvalidParamError(
                            "Invalid JSON structure, expected a dictionary"
                        )
                except json.JSONDecodeError:
                    raise InvalidParamError("Invalid JSON in 'data' field")

                print("Params:", params)

                # Pass the params dictionary into the color_fill_string_to_value function
                color_fill = color_fill_string_to_value(
                    params
                )  # Passing the whole params dictionary here
                print("Color fill:", color_fill)
                analyzer_entities = params.get("analyzer_entities", [])
                apply_ocr = params.get("apply_ocr", False)

                # Handle image from `request.files`
                if "image" in request.files:
                    im = Image.open(request.files.get("image"))
                    redacted_image = self.engine.redact(
                        im, color_fill, entities=analyzer_entities
                    )
                    img_byte_arr = image_to_byte_array(redacted_image, im.format)
                    return Response(img_byte_arr, mimetype="application/octet-stream")
                else:
                    raise InvalidParamError("Invalid parameter, please add image data")
            except InvalidParamError as e:
                print(f"Invalid Parameter Error: {e}")
                return {"error": str(e)}, 400
            except Exception as e:
                print(f"Unhandled error: {e}")
                return {"error": "Internal Server Error"}, 500

        # @self.app.route("/redact", methods=["POST"])
        # def redact():
        #     """Return a redacted image."""
        #     # Extract and parse parameters from `data`
        #     data = request.form.get("data")
        #     if data:
        #         try:
        #             params = json.loads(data)  # Parse the JSON string into a dictionary
        #         except json.JSONDecodeError:
        #             raise InvalidParamError("Invalid JSON in 'data' field")
        #     else:
        #         raise InvalidParamError("Invalid parameter, 'data' is missing")
        #
        #     try:
        #         color_fill = color_fill_string_to_value(
        #             params.get("color_fill", "black")
        #         )
        #     except Exception as e:
        #         print(f"Error in color_fill_string_to_value: {e}")
        #         raise InvalidParamError("Invalid value for 'color_fill'")
        #
        #     analyzer_entities = params.get("analyzer_entities", [])
        #     apply_ocr = params.get("apply_ocr", False)
        #
        #     # Handle image from `request.files`
        #     if request.files and "image" in request.files:
        #         im = Image.open(request.files.get("image"))
        #         redacted_image = self.engine.redact(
        #             im, color_fill, entities=analyzer_entities
        #         )
        #         img_byte_arr = image_to_byte_array(redacted_image, im.format)
        #         return Response(img_byte_arr, mimetype="application/octet-stream")
        #     else:
        #         raise InvalidParamError("Invalid parameter, please add image data")

        # @self.app.route("/redact", methods=["POST"])
        # def redact():
        #     """Return a redacted image."""
        #     try:
        #         # Extract parameters from `data`
        #         params = request.form.get("data")
        #         if params:
        #             params = get_json_data(params)
        #         else:
        #             raise InvalidParamError("Invalid parameter, 'data' is missing")
        #
        #         print("heleow")
        #         color_fill = color_fill_string_to_value(
        #             params.get("color_fill", "black")
        #         )
        #         analyzer_entities = params.get("analyzer_entities")
        #         apply_ocr = params.get("apply_ocr", False)
        #
        #         # Handle image from `request.files`
        #         if request.files and "image" in request.files:
        #             im = Image.open(request.files.get("image"))
        #             redacted_image = self.engine.redact(
        #                 im, color_fill, entities=analyzer_entities
        #             )
        #             img_byte_arr = image_to_byte_array(redacted_image, im.format)
        #             return Response(img_byte_arr, mimetype="application/octet-stream")
        #         else:
        #             raise InvalidParamError("Invalid parameter, please add image data")
        #     except Exception as e:
        #         self.logger.error(f"Failed to redact image with error: {e}")
        #         print(e)
        #         return jsonify(error=str(e)), 422

        # @self.app.route("/redact", methods=["POST"])
        # def redact():
        #     """Return a redacted image."""
        #     params = get_json_data(request.form.get("data"))
        #     color_fill = color_fill_string_to_value(params)
        #     if request.get_json(silent=True) and "image" in request.json:
        #         im = Image.open(BytesIO(base64.b64decode(request.json.get("image"))))
        #         analyzer_entities = request.json.get("analyzer_entities")
        #         redacted_image = self.engine.redact(
        #             im, color_fill, entities=analyzer_entities
        #         )
        #         img_byte_arr = image_to_byte_array(redacted_image, im.format)
        #         return Response(
        #             base64.b64encode(img_byte_arr), mimetype="application/octet-stream"
        #         )
        #
        #     elif request.files and "image" in request.files:
        #         im = Image.open(request.files.get("image"))
        #         redacted_image = self.engine.redact(im, color_fill, score_threshold=0.4)
        #         img_byte_arr = image_to_byte_array(redacted_image, im.format)
        #         return Response(img_byte_arr, mimetype="application/octet-stream")
        #     else:
        #         raise InvalidParamError("Invalid parameter, please add image data")

        @self.app.errorhandler(InvalidParamError)
        def invalid_param(err):
            self.logger.warning(
                f"failed to redact image with validation error: {err.err_msg}"
            )
            return jsonify(error=err.err_msg), 422

        @self.app.errorhandler(Exception)
        def server_error(e):
            self.logger.error(f"A fatal error occurred during execution: {e}")
            return jsonify(error="Internal server error"), 500


if __name__ == "__main__":
    os.environ["TESSDATA_PREFIX"] = "/usr/share/tessdata/"
    port = int(os.environ.get("PORT", DEFAULT_PORT))
    server = Server()
    server.app.run(host="0.0.0.0", port=port, debug=True)
