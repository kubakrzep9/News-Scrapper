""" Validates functionality of 'Config' class. """

import os
from news_scanner.config import Config, SECRETS_NOT_FOUND
from unittest import mock
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


class MockLoadDotenv:
    """ Used to mock dotenv.load_dotenv. """
    def __call__(self):
        pass


class MockIsFileTrue:
    """ Used to mock pathlib.Path.is_file. """
    def __call__(self):
        return True


class MockIsFileFalse:
    """ Used to mock pathlib.Path.is_file. """
    def __call__(self):
        return False


# callable functions to mock functions
mock_load_dotenv = MockLoadDotenv
mock_is_file_true = MockIsFileTrue
mock_is_file_false = MockIsFileFalse


@mock.patch('pathlib.Path.is_file', new_callable=mock_is_file_true)
@mock.patch('dotenv.load_dotenv', new_callable=mock_load_dotenv)
def test_init(mock_func1, mock_func2):
    """ Ensures config object is properly set from .env file.

    Mocker is used to mock the functions used to extract env vars from a .env
    file. Instead, another mocker patches test env vars.

    Params:
        mock_func1: not used.
        mock_func2: not used.
    """
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


@mock.patch('pathlib.Path.is_file', new_callable=mock_is_file_false)
def test_init_no_secrets(mock_func):
    """ Ensures exception is raised if .env file is missing.

    Params:
        mock_func: not used.
    """
    with pytest.raises(ValueError, match=rf".*{SECRETS_NOT_FOUND}.*" ):
       config = Config()
