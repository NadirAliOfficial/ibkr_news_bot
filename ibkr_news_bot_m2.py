import os, time, json, math, sqlite3, logging, requests, ahocorasick, traceback
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone
from typing import List, Dict, Set, Optional

from dotenv import load_dotenv
from ib_insync import IB, Stock, Ticker, LimitOrder, MarketOrder, util, Contract, Order

# ---------------------------
#  ENV + CONFIG
# ---------------------------
load_dotenv()

# Benzinga
BZ_API_KEY = os.getenv("BZ_API_KEY")

# Watchlist / keywords (edit as needed)
SYMBOLS  = [s.strip().upper() for s in os.getenv("WATCHLIST", "AAPL,TSLA").split(",")]
KEYWORDS = [k.strip() for k in os.getenv("KEYWORDS", "bankruptcy,merger,upgrade,downgrade,earnings").split(",")]

# IBKR connection
IB_HOST      = os.getenv("IBKR_HOST", "127.0.0.1")
IB_PORT      = int(os.getenv("IBKR_PORT", "7497"))   # 7497 paper, 7496 live by default
IB_CLIENT_ID = int(os.getenv("IBKR_CLIENT_ID", "7")) # choose a free client id

# Strategy & Risk
TP_PCT              = float(os.getenv("TP_PCT", "0.7"))     # take-profit % from entry (e.g., 0.7%)
SL_PCT              = float(os.getenv("SL_PCT", "0.5"))     # stop-loss % from entry (e.g., 0.5%)
ORDER_SIZE_SHARES   = int(os.getenv("ORDER_SIZE_SHARES", "50"))
MAX_CONCURRENT_TRDS = int(os.getenv("MAX_CONCURRENT_TRADES", "3"))
ENABLE_ICEBERG      = os.getenv("ENABLE_ICEBERG", "false").lower() == "true"
ICEBERG_CHILD_QTY   = int(os.getenv("ICEBERG_CHILD_QTY", "10"))  # child slice size
GLOBAL_PNL_STOP     = float(os.getenv("GLOBAL_PNL_STOP", "-200.0"))  # close-all if PnL below this (USD)
KILL_SWITCH_FILE    = os.getenv("KILL_SWITCH_FILE", "KILL")

# Polling cadence
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))

# Paths
DB_PATH   = os.getenv("DB_PATH", "data/news.db")
LOG_PATH  = os.getenv("LOG_PATH", "logs/bot.log")

# ---------------------------
#  LOGGING
# ---------------------------
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)

logger = logging.getLogger("ibkr_news_bot")
logger.setLevel(logging.INFO)
_handler = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=5)
_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
_handler.setFormatter(_formatter)
logger.addHandler(_handler)
logger.addHandler(logging.StreamHandler())

def log_exc(msg: str, ex: Exception):
    logger.error(f"{msg}: {ex}\n{traceback.format_exc()}")

# ---------------------------
#  STORAGE (SQLite)
# ---------------------------
class Store:
    def __init__(self, db_path=DB_PATH):
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("""CREATE TABLE IF NOT EXISTS news (
            id INTEGER PRIMARY KEY,
            created TEXT, title TEXT, url TEXT,
            matches TEXT, tickers TEXT
        )""")
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                oid INTEGER PRIMARY KEY,
                parent_oid INTEGER,
                symbol TEXT,
                side TEXT,
                qty INTEGER,
                limit_price REAL,
                tp REAL,
                sl REAL,
                status TEXT,
                reason TEXT,
                created_ts TEXT
            )
        """)

        self.conn.execute("""CREATE TABLE IF NOT EXISTS fills (
            exec_id TEXT PRIMARY KEY,
            oid INTEGER, symbol TEXT,
            side TEXT, qty INTEGER, price REAL,
            ts TEXT
        )""")
        self.conn.execute("""CREATE TABLE IF NOT EXISTS events (
            ts TEXT, level TEXT, message TEXT
        )""")
        self.conn.commit()

    def save_news(self, item: dict, matches: List[str], tickers: List[str]):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO news(id,created,title,url,matches,tickers) VALUES(?,?,?,?,?,?)",
                (item["id"], item["created"], item["title"], item["url"],
                 ",".join(matches), ",".join(sorted(set(tickers))))
            )
            self.conn.commit()
        except Exception as e:
            log_exc("Failed to save_news", e)

    def save_order(self, oid: int, parent_oid: Optional[int], symbol: str, side: str,
                   qty: int, limit: float, tp: float, sl: float, status: str, reason: str):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO orders(oid,parent_oid,symbol,side,qty,limit_price,tp,sl,status,reason,created_ts) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (oid, parent_oid, symbol, side, qty, limit, tp, sl, status, reason, datetime.now(timezone.utc).isoformat())
            )

            self.conn.commit()
        except Exception as e:
            log_exc("Failed to save_order", e)

    def save_fill(self, exec_id: str, oid: int, symbol: str, side: str, qty: int, price: float):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO fills(exec_id,oid,symbol,side,qty,price,ts) VALUES(?,?,?,?,?,?,?)",
                (exec_id, oid, symbol, side, qty, price, datetime.now(timezone.utc).isoformat())
            )
            self.conn.commit()
        except Exception as e:
            log_exc("Failed to save_fill", e)

    def save_event(self, level: str, message: str):
        try:
            self.conn.execute("INSERT INTO events(ts,level,message) VALUES(?,?,?)",
                              (datetime.now(timezone.utc).isoformat(), level, message))
            self.conn.commit()
        except Exception as e:
            log_exc("Failed to save_event", e)

store = Store()

def evinfo(msg: str): store.save_event("INFO", msg); logger.info(msg)
def evwarn(msg: str): store.save_event("WARN", msg); logger.warning(msg)
def everr(msg: str):  store.save_event("ERROR", msg); logger.error(msg)

# ---------------------------
#  BENZINGA
# ---------------------------
class BenzingaClient:
    BASE_URL = "https://api.benzinga.com/api/v2/news"
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def get_news(self, symbols: List[str], limit=20) -> List[dict]:
        params = {
            "token": self.api_key,
            "symbols": ",".join(symbols),
            "limit": limit,
            "format": "json"
        }
        r = self.session.get(self.BASE_URL, params=params, timeout=20)
        r.raise_for_status()
        data = r.json()
        return data if isinstance(data, list) else []

# ---------------------------
#  KEYWORD MATCHER
# ---------------------------
class KeywordMatcher:
    def __init__(self, keywords: List[str]):
        self.automaton = ahocorasick.Automaton()
        for kw in keywords:
            kw = kw.strip()
            if kw:
                self.automaton.add_word(kw.lower(), kw)
        self.automaton.make_automaton()

    def match(self, text: str) -> List[str]:
        return list({kw for _, kw in self.automaton.iter((text or "").lower())})

# ---------------------------
#  IBKR & STRATEGY
# ---------------------------
class Trader:
    def __init__(self, ib: IB, max_concurrent: int):
        self.ib = ib
        self.max_concurrent = max_concurrent
        self.open_parent_oids: Set[int] = set()

        # Hook events
        self.ib.execDetailsEvent += self._on_exec
        self.ib.orderStatusEvent  += self._on_status
        self.ib.errorEvent        += self._on_error

    # ---- Helpers
    def _stk(self, symbol: str) -> Stock:
        # SMART routing, USD stocks; adjust as needed
        return Stock(symbol, "SMART", "USD", primaryExchange="NASDAQ")

    # Additional Data Subscription and For LIVE MODE
    
    # def _mid(self, symbol: str, retries=3) -> Optional[float]:
    #     c = self._stk(symbol)
    #     for _ in range(retries):
    #         try:
    #             ticker: Ticker = self.ib.reqTickers(c)[0]
    #             bid, ask = (ticker.bid or 0.0), (ticker.ask or 0.0)
    #             if bid > 0 and ask > 0:
    #                 return (bid + ask) / 2.0
    #         except Exception as e:
    #             log_exc(f"midprice fetch failed for {symbol}", e)
    #         time.sleep(0.5)
    #     return None

    
    # Mocked Midprice for TEST MODE (no market data subscription needed)

    def _mid(self, symbol: str, retries=1) -> Optional[float]:
        TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"
        if TEST_MODE:
            evwarn(f"TEST_MODE active → using fallback 100.00 for {symbol}")
            return 100.00

        c = self._stk(symbol)
        try:
            t = self.ib.reqTickers(c)[0]
            if t.bid and t.ask:
                return round((t.bid + t.ask) / 2, 2)
            elif t.last:
                return round(float(t.last), 2)
        except Exception as e:
            log_exc(f"midprice fetch failed for {symbol}", e)

        evwarn(f"No market data for {symbol}, using fallback 100.00")
        return 100.00

    def _bracket(self, action: str, qty: int, limit: float, tp: float, sl: float) -> List[Order]:
        """
        Returns [parent, takeProfitChild, stopLossChild]
        """
        parent = LimitOrder(action, qty, round(limit, 2))
        parent.transmit = False

        tp_action = "SELL" if action.upper() == "BUY" else "BUY"
        tp_order = LimitOrder(tp_action, qty, round(tp, 2))
        tp_order.parentId = None  # set later
        tp_order.transmit = False

        sl_order = Order()
        sl_order.orderType = "STP"  # stop
        sl_order.action = tp_action
        sl_order.totalQuantity = qty
        sl_order.auxPrice = round(sl, 2)
        sl_order.parentId = None
        sl_order.transmit = True  # final child transmits entire chain

        return [parent, tp_order, sl_order]

    def _can_trade_more(self) -> bool:
        return len(self.open_parent_oids) < self.max_concurrent

    # ---- Event Handlers
    def _on_exec(self, trade, fill):
        try:
            exec_id = fill.execId
            oid     = trade.order.orderId
            sym     = trade.contract.symbol
            side    = trade.order.action
            qty     = int(fill.execution.shares)
            price   = float(fill.execution.price)
            store.save_fill(exec_id, oid, sym, side, qty, price)
            evinfo(f"FILL {sym} {side} {qty}@{price} (oid {oid}, exec {exec_id})")
        except Exception as e:
            log_exc("on_exec handler failed", e)

    def _on_status(self, trade):
        try:
            oid = trade.order.orderId
            st  = trade.orderStatus.status
            sym = trade.contract.symbol

            # Track open parent IDs
            if oid in self.open_parent_oids and st in ("Filled","Cancelled","Inactive"):
                self.open_parent_oids.discard(oid)

            store.save_order(
                oid=oid, parent_oid=getattr(trade.order, "parentId", None),
                symbol=sym, side=trade.order.action, qty=int(trade.order.totalQuantity or 0),
                limit= float(getattr(trade.order, "lmtPrice", 0.0) or 0.0),
                tp=0.0, sl=0.0, status=st, reason="status_update"
            )
            evinfo(f"ORDER STATUS: oid={oid} sym={sym} status={st}")
        except Exception as e:
            log_exc("on_status handler failed", e)


    def _on_error(self, reqId, code, msg, contract):
        everr(f"IB ERROR reqId={reqId} code={code} msg={msg} contract={contract}")

    # ---- Core actions
    def place_bracket(self, symbol: str, side: str, qty: int, entry_limit: float,
                      tp_price: float, sl_price: float, reason: str) -> Optional[int]:
        """
        Returns parent orderId or None.
        """
        if not self._can_trade_more():
            evwarn(f"Max concurrent trades reached ({self.max_concurrent}); skip {symbol}")
            return None

        c = self._stk(symbol)
        parent, tp, sl = self._bracket(side.upper(), qty, entry_limit, tp_price, sl_price)

        try:
            trade = self.ib.placeOrder(c, parent)
            parent_id = trade.order.orderId
            self.open_parent_oids.add(parent_id)

            # Link children
            for child in (tp, sl):
                child.parentId = parent_id

            # Place children
            tp_trade = self.ib.placeOrder(c, tp)
            sl_trade = self.ib.placeOrder(c, sl)

            # Persist
            store.save_order(parent_id, None, symbol, side.upper(), qty, entry_limit, tp_price, sl_price, "Submitted", reason)
            store.save_order(tp_trade.order.orderId, parent_id, symbol, ("SELL" if side.upper()=="BUY" else "BUY"),
                             qty, tp_price, tp_price, sl_price, "Submitted", "tp_child")
            store.save_order(sl_trade.order.orderId, parent_id, symbol, ("SELL" if side.upper()=="BUY" else "BUY"),
                             qty, sl_price, tp_price, sl_price, "Submitted", "sl_child")

            evinfo(f"SUBMITTED BRACKET {symbol} {side} qty={qty} LMT={entry_limit} TP={tp_price} SL={sl_price} (parent {parent_id})")
            return parent_id
        except Exception as e:
            log_exc(f"place_bracket failed {symbol}", e)
            return None

    def iceberg(self, symbol: str, side: str, total_qty: int, limit: float, tp: float, sl: float, reason: str):
        """
        Slice total_qty into ICEBERG_CHILD_QTY chunks with short delays.
        """
        remaining = total_qty
        while remaining > 0:
            if os.path.exists(KILL_SWITCH_FILE):
                evwarn("Kill switch detected during iceberg; aborting.")
                return
            lot = min(ICEBERG_CHILD_QTY, remaining)
            self.place_bracket(symbol, side, lot, limit, tp, sl, reason + " (iceberg)")
            remaining -= lot
            time.sleep(0.7)  # brief spacing

# ---------------------------
#  GLOBAL P&L CHECK (simple)
# ---------------------------
def estimate_global_pnl_usd(ib: IB) -> float:
    """
    Lightweight PnL estimate using account summaries (realized + unrealized).
    For accuracy you can switch to reqPnL/reqPnLSingle per account if needed.
    """
    try:
        acc = ib.managedAccounts() or []
        if not acc:
            return 0.0
        acct = acc[0]
        # Pull account values
        vals = {v.tag: v.value for v in ib.accountSummary(acct)}
        # IB returns strings; fallback safely
        realized = float(vals.get("RealizedPnL", "0") or 0)
        unreal   = float(vals.get("UnrealizedPnL", "0") or 0)
        return realized + unreal
    except Exception as e:
        log_exc("estimate_global_pnl_usd failed", e)
        return 0.0

# ---------------------------
#  MAIN BOT
# ---------------------------
class Bot:
    def __init__(self):
        self.bz = BenzingaClient(BZ_API_KEY)
        self.matcher = KeywordMatcher(KEYWORDS)
        self.watch: Set[str] = set(SYMBOLS)
        self.ib = IB()

    def connect_ib(self):
        try:
            self.ib.connect(IB_HOST, IB_PORT, IB_CLIENT_ID, readonly=False)
            evinfo(f"Connected to IBKR @ {IB_HOST}:{IB_PORT} (clientId={IB_CLIENT_ID})")
        except Exception as e:
            log_exc("Failed to connect to IBKR", e)
            raise

    def run_once(self, trader: Trader):
        # 1) Kill switch?
        if os.path.exists(KILL_SWITCH_FILE):
            evwarn("Kill switch file present; skipping cycle.")
            return

        # 2) Global PnL safety
        pnl = estimate_global_pnl_usd(self.ib)
        if pnl <= GLOBAL_PNL_STOP:
            evwarn(f"Global PnL {pnl:.2f} <= stop {GLOBAL_PNL_STOP:.2f}. Closing all and halting.")
            # Close all positions (simple: market out)
            for pos in self.ib.positions():
                sym = pos.contract.symbol
                qty = int(pos.position)
                if qty != 0:
                    side = "SELL" if qty > 0 else "BUY"
                    try:
                        self.ib.placeOrder(Stock(sym, "SMART", "USD", primaryExchange="NASDAQ"),
                                           MarketOrder(side, abs(qty)))
                        evinfo(f"Close-all sent: {sym} {side} {abs(qty)} MKT")
                    except Exception as e:
                        log_exc("close-all failed", e)
            # Also cancel open orders
            try:
                for o in self.ib.reqAllOpenOrders():
                    self.ib.cancelOrder(o)
                evinfo("Cancelled all open orders.")
            except Exception as e:
                log_exc("cancelAll failed", e)
            # Stop further trading this cycle
            return

        # 3) Fetch news
        try:
            news = self.bz.get_news(list(self.watch), limit=30)
            # 👇 Inject a fake news item to test instantly
            news.append({
                "id": 999999,
                "created": datetime.now().isoformat(),
                "title": "Tesla Upgraded by Morgan Stanley to Overweight",
                "url": "https://test",
                "stocks": [{"symbol": "TSLA"}]
            })
        except Exception as e:
            log_exc("Benzinga get_news failed", e)
            return

        # 4) Filter by ticker tag, match by keywords, trade
        for item in news:
            try:
                stocks = {s.get("symbol") for s in item.get("stocks", []) if s.get("symbol")}
                related = stocks & self.watch
                if not related:
                    continue

                matches = self.matcher.match(item.get("title", "")) or self.matcher.match(item.get("description", ""))
                if not matches:
                    continue

                # Persist the signal
                store.save_news(item, matches, list(related))
                evinfo(f"MATCH {list(related)} {matches} | {item.get('title')}")
                # ---- Strategy (long-only):
                long_trigs = {"upgrade","beat","merger","record","guidance raise","acquisition"}

                text = (item.get("title","") + " " + item.get("description","")).lower()
                go_long = any(t in text for t in long_trigs)

                for sym in related:
                    mid = trader._mid(sym)
                    if not mid:
                        evwarn(f"No midprice for {sym}; skip")
                        continue

                    if not go_long:
                        evinfo(f"No long trigger keywords for {sym}; skip.")
                        continue

                    side = "BUY"
                    entry = mid
                    tp = entry * (1 + TP_PCT/100.0)
                    sl = entry * (1 - SL_PCT/100.0)

                    qty = ORDER_SIZE_SHARES
                    reason = f"news:{','.join(matches)}"
                    if ENABLE_ICEBERG and qty > ICEBERG_CHILD_QTY:
                        trader.iceberg(sym, side, qty, entry, tp, sl, reason)
                    else:
                        trader.place_bracket(sym, side, qty, entry, tp, sl, reason)


            except Exception as e:
                log_exc("cycle item failed", e)

    def main_loop(self):
        self.connect_ib()
        trader = Trader(self.ib, MAX_CONCURRENT_TRDS)
        evinfo(f"Starting loop. Watchlist={sorted(self.watch)} Keywords={KEYWORDS} TP={TP_PCT}% SL={SL_PCT}%")

        while True:
            try:
                self.run_once(trader)
            except Exception as e:
                log_exc("run_once crash", e)
            time.sleep(POLL_SECONDS)

# ---------------------------
#  ENTRY
# ---------------------------
def main():
    if not BZ_API_KEY:
        everr("Missing BZ_API_KEY in .env")
        return
    try:
        Bot().main_loop()
    except KeyboardInterrupt:
        evwarn("Interrupted by user.")
    except Exception as e:
        log_exc("Fatal error", e)

if __name__ == "__main__":
    main()
