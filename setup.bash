pip install poetry && poetry install --no-root --only=main -E server
cd presidio-image-redaction
poetry run python app.py
