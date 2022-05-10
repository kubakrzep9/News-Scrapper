from setuptools import setup

setup(
    name="News Scrapper",
    version="0.0",
    description="Scrapes for relevant articles on new site.",
    packages=["news_scanner"],
    install_requires=[
        "tda-api", "pytest", "pandas", "bs4", "requests", "selenium",
        "httpx", "twython", "numpy", "python-dotenv", "nltk"
    ]
)
