from selenium import webdriver

_HEADLESS_PROXY = "localhost:3128"
_PROXY = {
    "proxyType": "manual",
    "httpProxy": _HEADLESS_PROXY,
    "ftpProxy": _HEADLESS_PROXY,
    "sslProxy": _HEADLESS_PROXY,
    "noProxy": "",
}

proxy_options = webdriver.ChromeOptions()
proxy_options.add_argument("--headless")
proxy_options.add_argument("--ignore-certificate-errors")
proxy_options.set_capability("proxy", _PROXY)
