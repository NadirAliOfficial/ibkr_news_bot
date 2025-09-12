
import json, time, re, csv, html, argparse, sqlite3, sys, logging, threading
from pathlib import Path
from datetime import datetime, timezone
from websocket import WebSocketApp
from logging.handlers import RotatingFileHandler
import ssl, certifi

# ------------------------
# Setup
# ------------------------
DB_FILE = "bz_news.db"
LOG_FILE = "bz_bot.log"
DEFAULT_KEYWORDS = [
    "merger","acquisition","guidance","downgrade","upgrade",
    "investigation","halt","offering","partnership","contract"
]

def norm_text(s):
    s = html.unescape(s or "")
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]+"," ",s)
    return re.sub(r"\s+"," ",s).strip()

def setup_logger():
    lg = logging.getLogger("bzbot")
    lg.setLevel(logging.INFO)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    lg.addHandler(ch)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    lg.addHandler(fh)
    return lg

def load_watchlist(path):
    watch = {}
    if Path(path).exists():
        with open(path,"r",encoding="utf-8") as f:
            for r in csv.DictReader(f):
                sym = r["symbol"].strip().upper()
                nm = r["name"].strip()
                watch[sym] = nm
    else:
        for sym,name in [("AAPL","Apple Inc."),("MSFT","Microsoft"),("NVDA","NVIDIA")]:
            watch[sym]=name
    return watch

def load_keywords(path):
    if Path(path).exists():
        return [x.strip().lower() for x in open(path) if x.strip()]
    return DEFAULT_KEYWORDS

def ensure_db():
    c = sqlite3.connect(DB_FILE)
    c.execute("""
    CREATE TABLE IF NOT EXISTS headlines(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts_utc TEXT, symbol TEXT, headline TEXT, source TEXT
    );
    """)
    c.execute("""
    CREATE TABLE IF NOT EXISTS matches(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      headline_id INTEGER, keyword TEXT,
      FOREIGN KEY(headline_id) REFERENCES headlines(id)
    );
    """)
    c.commit()
    return c

# ------------------------
# Keyword matcher
# ------------------------
try:
    import ahocorasick
    AHO = True
except:
    AHO = False

class Matcher:
    def __init__(self, kws):
        self.kws = sorted(set(kws))
        if AHO:
            A = ahocorasick.Automaton()
            for k in self.kws: A.add_word(k,k)
            A.make_automaton()
            self.A=A
        else:
            self.r = re.compile("|".join(map(re.escape,self.kws)),re.I)
    def find(self,text):
        text = text.lower()
        if AHO:
            return {v for _,v in self.A.iter(text)}
        return {m.group(0).lower() for m in self.r.finditer(text)}

# ------------------------
# Main Bot
# ------------------------
class BZBot:
    def __init__(self, press_token, stock_token, watch, kws, log, db):
        self.press_token = press_token
        self.stock_token = stock_token
        self.watch = watch
        self.matcher = Matcher(kws)
        self.log = log
        self.db = db

    def start(self):
        ws = WebSocketApp(
            "wss://api.benzinga.com/api/v2/news",
            on_open=self.on_open,
            on_message=self.on_msg,
            on_error=lambda w,e:self.log.error(f"WS error: {e}"),
            on_close=lambda w,c,m:self.log.warning(f"WS closed: {c} {m}")
        )

        ssl_opts = {"cert_reqs": ssl.CERT_REQUIRED, "ca_certs": certifi.where()}

        t = threading.Thread(target=lambda: ws.run_forever(sslopt=ssl_opts), daemon=True)
        t.start()
        while True: time.sleep(1)

    def on_open(self, ws):
        self.log.info("WebSocket open → authenticating + subscribing")
        # Authenticate
        ws.send(json.dumps({"action": "auth", "token": self.press_token}))
        ws.send(json.dumps({"action": "auth", "token": self.stock_token}))

        # Then subscribe to channels (BZ expects channel names after auth)
        ws.send(json.dumps({"action": "subscribe", "channels": ["press-releases"]}))
        ws.send(json.dumps({"action": "subscribe", "channels": ["stock-news"]}))


    def on_msg(self,ws,msg):
        try:
            j=json.loads(msg)
            if j.get("type")!="news": return
            headline = j.get("title") or j.get("headline") or ""
            if not headline.strip(): return
            tickers = j.get("tickers") or []
            symset = {s for s in tickers if s in self.watch}

            # if no explicit symbols, try match by company names
            if not symset:
                hn = norm_text(headline)
                for s,nm in self.watch.items():
                    if norm_text(nm) in hn: symset.add(s)

            matches = self.matcher.find(headline)
            if matches:
                for sym in (symset or {None}):
                    cur = self.db.cursor()
                    cur.execute("INSERT INTO headlines(ts_utc,symbol,headline,source) VALUES(?,?,?,?)",
                        (datetime.now(timezone.utc).isoformat(), sym, headline, "BZ"))
                    hid = cur.lastrowid
                    for k in matches:
                        cur.execute("INSERT INTO matches(headline_id,keyword) VALUES(?,?)",(hid,k))
                    self.db.commit()
                self.log.info(f"[BZ][{','.join(symset) or 'GEN'}] {headline}")
        except Exception as e:
            self.log.error(f"on_msg error: {e}")

# ------------------------
# Entry
# ------------------------
def main():
    a=argparse.ArgumentParser()
    a.add_argument("--press",required=True,help="BZ Press Releases token")
    a.add_argument("--stock",required=True,help="BZ Stock News token")
    a.add_argument("--watchlist",default="watchlist.csv")
    a.add_argument("--keywords",default="keywords.txt")
    args=a.parse_args()

    log = setup_logger()
    watch = load_watchlist(args.watchlist)
    kws = load_keywords(args.keywords)
    db = ensure_db()
    bot = BZBot(args.press,args.stock,watch,kws,log,db)
    log.info(f"Loaded {len(watch)} symbols, {len(kws)} keywords")
    bot.start()

if __name__=="__main__":
    main()
