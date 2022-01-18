""" File to handle configuration. """

import os
from news_scanner.twitter_handle.twitter_handle import TwitterHandleConfig
from news_scanner.td_api.td_api import TDAPIConfig
import subprocess
from pathlib import Path

EXPORT_SECRETS_SCRIPT_PATH = Path(__file__).parent.parent / "export_secrets.sh"
SECRETS_SCRIPT_NOT_FOUND_ERROR = "Secrets script not found at:"


class Config:
    """ Class to handle configuration.

    This class reads secrets from environment variables set by a shell script.
    The shell script should contain values for all variables being exported.

    Note: May need to 'chmod +x <secrets_script path>'.

    Ex in file ../News-Scrapper/export_secrets.sh:
        export WEBSITE_URL="<website_url>"
        export TWITTER_API_KEY="<twitter_api_key>"
        export TWITTER_API_SECRET_KEY="<twitter_api_secret_key>"
        export TWITTER_ACCESS_TOKEN="<twitter_access_token>"
        export TWITTER_ACCESS_TOKEN_SECRET="<twitter_access_token_secret>"
        export TDA_API_KEY="<tda_api_key>"
        export TDA_REDIRECT_URI="<tda_redirect_uri>"
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

    def __init__(
        self,
        export_secrets_script_path: Path = EXPORT_SECRETS_SCRIPT_PATH
    ):
        """ Retrieves secrets and api keys from environment variables.

        Params:
            export_secrets_script_path: Path to script file exporting secrets.
        """
        _load_secrets(
            export_secrets_script_path=export_secrets_script_path
        )

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
        )


def _load_secrets(
    export_secrets_script_path: Path = EXPORT_SECRETS_SCRIPT_PATH
) -> None:
    """ Loads secrets from script into environment.

    Params:
        export_secrets_script_path: Path to script file exporting secrets.
    """
    if not export_secrets_script_path.is_file():
        raise ValueError(
            f"{SECRETS_SCRIPT_NOT_FOUND_ERROR} {export_secrets_script_path}"
        )
    subprocess.call(["sh", str(export_secrets_script_path)])
