"""Uninstall NLP models defined in the provided yaml file."""

import argparse
import logging
import os
import shutil
import yaml
import sys
from typing import Dict, Union, List

try:
    import spacy
    import stanza
    import transformers
    from huggingface_hub import snapshot_download
except ImportError as e:
    print(f"Error importing required libraries: {e}")
    print(
        "Please ensure spacy, stanza, transformers, and huggingface_hub are installed."
    )
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_installed_models(conf_file: str) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    """
    Read the configuration file and return a list of models.

    :param conf_file: Path to the yaml file containing the models
    :return: List of models from the configuration
    """
    try:
        nlp_configuration = yaml.safe_load(open(conf_file))

        if "nlp_engine_name" not in nlp_configuration:
            raise ValueError("NLP config file should contain an nlp_engine_name field")

        if "models" not in nlp_configuration:
            raise ValueError("NLP config file should contain a list of models")

        return nlp_configuration["models"]
    except Exception as e:
        logger.error(f"Error reading configuration file: {e}")
        return []


def list_installed_models(conf_file: str) -> None:
    """
    List all models defined in the configuration file.

    :param conf_file: Path to the yaml file containing the models
    """
    models = get_installed_models(conf_file)

    if not models:
        print("No models found in the configuration file.")
        return

    print("Installed Models:")
    for i, model in enumerate(models, 1):
        print(f"{i}. {model}")


def uninstall_model(engine_name: str, model_name: Union[str, Dict[str, str]]) -> None:
    """
    Uninstall a specific model based on its engine and name.

    :param engine_name: Name of the NLP engine (spacy, stanza, transformers)
    :param model_name: Name or configuration of the model to uninstall
    """
    try:
        if engine_name == "spacy":
            uninstall_spacy_model(model_name)
        elif engine_name == "stanza":
            uninstall_stanza_model(model_name)
        elif engine_name == "transformers":
            uninstall_transformers_model(model_name)
        else:
            logger.error(f"Unsupported NLP engine: {engine_name}")
    except Exception as e:
        logger.error(f"Error uninstalling model {model_name}: {e}")


def uninstall_spacy_model(model_name: str) -> None:
    """
    Uninstall a spaCy model.

    :param model_name: Name of the spaCy model to uninstall
    """
    try:
        import spacy

        # Attempt to remove the model directory
        model_path = spacy.util.get_model_path(model_name)
        if os.path.exists(model_path):
            shutil.rmtree(model_path)
            logger.info(f"Uninstalled spaCy model: {model_name}")
        else:
            logger.warning(f"spaCy model {model_name} not found")
    except Exception as e:
        logger.error(f"Failed to uninstall spaCy model {model_name}: {e}")


def uninstall_stanza_model(model_name: str) -> None:
    """
    Uninstall a Stanza model.

    :param model_name: Name of the Stanza model to uninstall
    """
    try:
        import stanza

        stanza_model_dir = os.path.join(
            stanza.resources.DEFAULT_RESOURCES_DIR, model_name
        )
        if os.path.exists(stanza_model_dir):
            shutil.rmtree(stanza_model_dir)
            logger.info(f"Uninstalled Stanza model: {model_name}")
        else:
            logger.warning(f"Stanza model {model_name} not found")
    except Exception as e:
        logger.error(f"Failed to uninstall Stanza model {model_name}: {e}")


def uninstall_transformers_model(model_name: Dict[str, str]) -> None:
    """
    Uninstall a Transformers model and associated spaCy model.

    :param model_name: Dictionary containing spaCy and Transformers model names
    """
    try:
        # Uninstall spaCy model
        if "spacy" in model_name:
            uninstall_spacy_model(model_name["spacy"])

        # Remove Transformers model from cache
        if "transformers" in model_name:
            transformers_model = model_name["transformers"]
            cache_dir = transformers.TRANSFORMERS_CACHE
            model_cache_path = os.path.join(
                cache_dir, transformers_model.replace("/", "--")
            )

            if os.path.exists(model_cache_path):
                shutil.rmtree(model_cache_path)
                logger.info(f"Uninstalled Transformers model: {transformers_model}")
            else:
                logger.warning(
                    f"Transformers model {transformers_model} not found in cache"
                )
    except Exception as e:
        logger.error(f"Failed to uninstall Transformers model: {e}")


def interactive_uninstall(conf_file: str) -> None:
    """
    Interactive model uninstallation with user selection.

    :param conf_file: Path to the yaml file containing the models
    """
    models = get_installed_models(conf_file)

    if not models:
        print("No models found in the configuration file.")
        return

    print("Select models to uninstall (comma-separated indices, or 'all'):")
    list_installed_models(conf_file)

    user_input = input("Enter your selection: ").strip()

    if user_input.lower() == "all":
        selected_models = models
    else:
        try:
            # Convert user input to indices
            indices = [int(x.strip()) - 1 for x in user_input.split(",")]
            selected_models = [models[i] for i in indices]
        except (ValueError, IndexError):
            print("Invalid selection. Please enter valid model indices.")
            return

    # Read the configuration to get the NLP engine name
    nlp_configuration = yaml.safe_load(open(conf_file))
    engine_name = nlp_configuration.get("nlp_engine_name")

    # Uninstall selected models
    for model in selected_models:
        uninstall_model(engine_name, model)

    print("Model uninstallation process completed.")


def main() -> None:
    """
    Main function to handle command-line arguments for model uninstallation.
    """
    parser = argparse.ArgumentParser(
        description="Uninstall NLP models from the configuration file"
    )
    parser.add_argument(
        "--conf_file",
        required=False,
        default="guardian_analyzer/conf/default.yaml",
        help="Location of nlp configuration yaml file. Default: conf/default.yaml",
    )
    parser.add_argument(
        "--list", action="store_true", help="List all models without uninstalling"
    )

    args = parser.parse_args()

    if args.list:
        list_installed_models(args.conf_file)
    else:
        interactive_uninstall(args.conf_file)


if __name__ == "__main__":
    main()
