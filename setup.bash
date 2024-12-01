# Install poetry depending on your system.
# https://python-poetry.org/docs/#installation

pip install poetry
cd presidio-image-redaction
poetry install --all-extras
poetry run python app.py
