""" Proxy configuration constants. """

from selenium import webdriver

_HEADLESS_PROXY = "localhost:3128"
_PROXY = {
    "proxyType": "manual",
    "httpProxy": _HEADLESS_PROXY,
    "ftpProxy": _HEADLESS_PROXY,
    "sslProxy": _HEADLESS_PROXY,
    "noProxy": "",
}

PROXY_OPTIONS = webdriver.ChromeOptions()
PROXY_OPTIONS.add_argument("--headless")
PROXY_OPTIONS.add_argument("--ignore-certificate-errors")
PROXY_OPTIONS.set_capability("proxy", _PROXY)
