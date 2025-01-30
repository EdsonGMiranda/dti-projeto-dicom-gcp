import os

import pkg_resources
from dotenv import load_dotenv


def load_env_variables(env='dev'):
    """
    Load environment variables from a .env file.

    Parameters:
    env (str): The environment name to load the .env file. If None or empty, it loads the default .env file.

    Returns:
    dict: A dictionary containing the environment variables.
    """

    PKG_NAME = __name__.split(".")[0]

    env_name = f".env.{env}" if env else ".env"

    env_path = pkg_resources.resource_filename(PKG_NAME, env_name)
    load_dotenv(env_path)

    return {**os.environ}
