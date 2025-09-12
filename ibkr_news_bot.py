import os, sqlite3, logging, requests, ahocorasick
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# ---------------------------
#  ENV + CONFIG
# ---------------------------
load_dotenv()
API_KEY = os.getenv("BZ_API_KEY")  # put your key in .env as BZ_API_KEY
SYMBOLS = ["AAPL", "TSLA"]
KEYWORDS = ["the", "Tesla", "AI", "earnings"]


# ---------------------------
#  Benzinga Client
# ---------------------------
class BenzingaClient:
    BASE_URL = "https://api.benzinga.com/api/v2/news"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_news(self, symbols, limit=20):
        params = {
            "token": self.api_key,
            "symbols": ",".join(symbols),
            "limit": limit,
            "format": "json"
        }
        r = self.session.get(self.BASE_URL, params=params)
        r.raise_for_status()
        return r.json()  # list of articles

# ---------------------------
#  Keyword Matcher
# ---------------------------
class KeywordMatcher:
    def __init__(self, keywords):
        self.automaton = ahocorasick.Automaton()
        for kw in keywords:
            self.automaton.add_word(kw.lower(), kw)
        self.automaton.make_automaton()

    def match(self, text: str):
        return list({kw for _, kw in self.automaton.iter(text.lower())})

# ---------------------------
#  News Logger
# ---------------------------
class NewsLogger:
    def __init__(self, db_path="data/news.db", log_path="logs/bot.log"):
        os.makedirs("data", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY,
                created TEXT,
                title TEXT,
                url TEXT,
                matches TEXT
            )
        """)
        self.logger = logging.getLogger("bot")
        self.logger.setLevel(logging.INFO)
        handler = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=5)
        self.logger.addHandler(handler)

    def save(self, item, matches):
        self.conn.execute(
            "INSERT OR IGNORE INTO news(id,created,title,url,matches) VALUES(?,?,?,?,?)",
            (item["id"], item["created"], item["title"], item["url"], ",".join(matches))
        )
        self.conn.commit()
        self.logger.info(f"{item['created']} | {item['title']} | {matches}")

# ---------------------------
#  Main
# ---------------------------
def main():
    bz = BenzingaClient(API_KEY)
    matcher = KeywordMatcher(KEYWORDS)
    logger = NewsLogger()

    print("Fetching news…")
    news = bz.get_news(SYMBOLS, limit=25)

    for item in news:
        matches = matcher.match(item["title"])
        if matches:
            logger.save(item, matches)
            print(f"⚡ MATCH {matches}: {item['title']}")

if __name__ == "__main__":
    main()
