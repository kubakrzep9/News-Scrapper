""" File to handle configuration. """

import os
from news_scanner.twitter_handle.twitter_handle import TwitterHandleConfig
from news_scanner.td_api.td_api_handle import TDAPIConfig
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent
SECRETS_PATH = BASE_DIR / ".env"
SECRETS_NOT_FOUND = "No .env file found at "


class Config:
    """ Class to handle configuration.

    Reads secrets from .env file located in root project directory.

    Ex News-Scrapper/.env file:
        WEBSITE_URL="<website_url>"
        TWITTER_API_KEY="<twitter_api_key>"
        TWITTER_API_SECRET_KEY="<twitter_api_secret_key>"
        TWITTER_ACCESS_TOKEN="<twitter_access_token>"
        TWITTER_ACCESS_TOKEN_SECRET="<twitter_access_token_secret>"
        TDA_API_KEY="<tda_api_key>"
        TDA_REDIRECT_URI="<tda_redirect_uri>"
    """
    # env variable names
    _WEBSITE_URL = "WEBSITE_URL"
    _TWITTER_API_KEY = "TWITTER_API_KEY"
    _TWITTER_API_SECRET_KEY = "TWITTER_API_SECRET_KEY"
    _TWITTER_ACCESS_TOKEN = "TWITTER_ACCESS_TOKEN"
    _TWITTER_ACCESS_TOKEN_SECRET = "TWITTER_ACCESS_TOKEN_SECRET"
    _TDA_API_KEY = "TDA_API_KEY"
    _TDA_REDIRECT_URI = "TDA_REDIRECT_URI"
    _TDA_TOKEN_PATH = "TDA_TOKEN_PATH"
    _SCRAPPER_API_KEY = "SCRAPPER_API_KEY"

    def __init__(self):
        """ Retrieves secrets and api keys from environment variables. """
        # check if .env exists
        if not SECRETS_PATH.is_file():
            raise ValueError(f"{SECRETS_NOT_FOUND} {str(SECRETS_PATH)}")
        # load env vars from .env
        load_dotenv()

        self.website_url = os.environ[self._WEBSITE_URL]
        self.twitter_config = TwitterHandleConfig(
            api_key=os.environ[self._TWITTER_API_KEY],
            api_secret_key=os.environ[self._TWITTER_API_SECRET_KEY],
            access_token=os.environ[self._TWITTER_ACCESS_TOKEN],
            access_token_secret=os.environ[self._TWITTER_ACCESS_TOKEN_SECRET]
        )
        self.tda_config = TDAPIConfig(
            api_key=os.environ[self._TDA_API_KEY],
            redirect_uri=os.environ[self._TDA_REDIRECT_URI],
            token_path=str(Path(__file__).parent.parent / "access_token.txt")
        )
        self.scrapper_api_key = os.environ[self._SCRAPPER_API_KEY]
