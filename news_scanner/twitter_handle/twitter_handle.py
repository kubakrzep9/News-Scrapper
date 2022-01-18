from typing import NamedTuple
from twython import Twython


class TwitterHandleConfig(NamedTuple):
    """ Secrets to access twitter API.

    Attrs:
        api_key: Twitter account api key.
        api_secret_key: Twitter account secret api key.
        access_token: Twitter account access token.
        access_token_secret: Twitter account access token secret.
    """
    api_key: str
    api_secret_key: str
    access_token: str
    access_token_secret: str


class TwitterHandle:
    def __init__(
        self,
        config: TwitterHandleConfig
    ):
        print("creating twitter handle")
        self.twitter = Twython(
            config.api_key,
            config.api_secret_key,
            config.access_token,
            config.access_token_secret
        )

    def make_post(self, message: str):
        print("message length:", len(message))
        if len(message) > 280:
            print("Too many characters in post")
        self.twitter.update_status(status=message)
