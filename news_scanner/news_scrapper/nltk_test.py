import nltk
nltk.download('vader_lexicon')
from nltk.sentiment import SentimentIntensityAnalyzer

from pathlib import Path

if __name__ == "__main__":
    data_path = Path().cwd() / "data" / "Lawsuit.txt"
    with open(data_path, "r") as file:
        data = file.read()

    #print(data)
    sia = SentimentIntensityAnalyzer()
    scores = sia.polarity_scores(data)
    print(scores)