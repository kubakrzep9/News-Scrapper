""" Validates functionality of 'Config' class. """

import os
from news_scanner.config import (
    Config,
    _load_secrets,
    SECRETS_SCRIPT_NOT_FOUND_ERROR
)
from unittest import mock
from pathlib import Path
import pytest

CONFIG_ENV_VARS = {
    Config._WEBSITE_URL: Config._WEBSITE_URL.lower(),
    Config._TWITTER_API_KEY: Config._TWITTER_API_KEY.lower(),
    Config._TWITTER_API_SECRET_KEY: Config._TWITTER_API_SECRET_KEY.lower(),
    Config._TWITTER_ACCESS_TOKEN: Config._TWITTER_ACCESS_TOKEN.lower(),
    Config._TWITTER_ACCESS_TOKEN_SECRET:
        Config._TWITTER_ACCESS_TOKEN_SECRET.lower(),
    Config._TDA_API_KEY: Config._TDA_API_KEY.lower(),
    Config._TDA_REDIRECT_URI: Config._TDA_REDIRECT_URI.lower(),
    Config._TDA_TOKEN_PATH: Config._TDA_TOKEN_PATH.lower()
}


def test_config_init():
    """ Ensures config object is properly set. """
    with mock.patch.dict(os.environ, CONFIG_ENV_VARS):
        config = Config()
        assert config.website_url == Config._WEBSITE_URL.lower()
        assert config.twitter_config.api_key == Config._TWITTER_API_KEY.lower()
        assert config.twitter_config.api_secret_key == \
               Config._TWITTER_API_SECRET_KEY.lower()
        assert config.twitter_config.access_token == \
               Config._TWITTER_ACCESS_TOKEN.lower()
        assert config.twitter_config.access_token_secret == \
               Config._TWITTER_ACCESS_TOKEN_SECRET.lower()


def test_load_secrets_no_script():
    """ Ensures a script exists to export secrets. """
    script_path = Path(__file__).parent / "export_secrets.sh"
    with pytest.raises(ValueError, match=rf".*{SECRETS_SCRIPT_NOT_FOUND_ERROR}.*" ):
        _load_secrets(
            export_secrets_script_path=script_path
        )
