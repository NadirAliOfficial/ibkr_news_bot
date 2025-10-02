import os, sys, csv, re, json, time, threading, sqlite3, logging, traceback, requests, ahocorasick, asyncio
from dataclasses import dataclass, asdict, field
from typing import List, Optional, Set, Callable
from datetime import datetime, time as dt_time, timezone
from zoneinfo import ZoneInfo
from logging.handlers import RotatingFileHandler
from websocket import WebSocketApp
import pandas as pd
import certifi
from ib_insync import IB, Stock, Ticker, LimitOrder, MarketOrder, Order

from PySide6.QtWidgets import (
    QApplication, QWidget, QMainWindow, QTabWidget, QFileDialog, QMessageBox,
    QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QLineEdit, QTextEdit,
    QListWidget, QListWidgetItem, QTableWidget, QTableWidgetItem,
    QSpinBox, QDoubleSpinBox, QCheckBox, QGroupBox, QHeaderView, QPushButton, QPlainTextEdit
)
from PySide6.QtCore import Qt, QDate, QObject, Signal

# -----------------------------
# Paths & constants
# -----------------------------
CONFIG_PATH = "config.json"
DB_PATH     = "data/news.db"
LOG_PATH    = "logs/bot.log"
EST         = ZoneInfo("America/New_York")

os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# -----------------------------
# Config models
# -----------------------------
@dataclass
class TimeWindow:
    start: str = "03:50"  # HH:MM (EST)
    end:   str = "10:00"  # HH:MM (EST)

@dataclass
class WatchlistCfg:
    name: str = "Watchlist"
    tickers: List[str] = field(default_factory=list)
    keywords: List[str] = field(default_factory=list)  # long-only

@dataclass
class OrderBand:
    min_price: float = 0.0
    max_price: float = 100000.0
    shares:    int   = 50

@dataclass
class RiskCfg:
    tp_pct: float = 0.7
    sl_pct: float = 0.5
    max_concurrent: int = 3
    iceberg: bool = False
    iceberg_child_qty: int = 10
    global_profit_usd: float = 999999.0   # disable by default
    global_loss_usd: float = -200.0
    kill_switch_file: str = "KILL"
    min_news_volume: int = 5000   # << adjustable in GUI

@dataclass
class EngineCfg:
    poll_seconds: int = 30
    test_mode: bool = True  # uses fallback mid=100 if no market data

@dataclass
class AppCfg:
    date_start: str = ""  # optional; not used by engine (kept for future)
    date_end:   str = ""
    window_am: TimeWindow = field(default_factory=lambda: TimeWindow("03:50", "10:00"))
    window_pm: TimeWindow = field(default_factory=lambda: TimeWindow("15:45", "21:00"))

    watchlists: List[WatchlistCfg] = field(default_factory=lambda: [
        WatchlistCfg("Watchlist 1", ["AAPL","TSLA"], ["upgrade","earnings","merger","record"]),
        WatchlistCfg("Watchlist 2", [], []),
        WatchlistCfg("Watchlist 3", [], []),
    ])
    order_bands: List[OrderBand] = field(default_factory=lambda: [OrderBand(0.3, 2.0, 1000)])

    risk: RiskCfg = field(default_factory=RiskCfg)
    engine: EngineCfg = field(default_factory=EngineCfg)
    benzinga_api_key: str = ""    # set in GUI
    ibkr_host: str = "127.0.0.1"
    ibkr_port: int = 7497
    ibkr_client_id: int = 7

# -----------------------------
# Helpers
# -----------------------------
def load_config(path: str = CONFIG_PATH) -> AppCfg:
    if not os.path.exists(path):
        return AppCfg()
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    cfg = AppCfg()
    for k, v in raw.items():
        setattr(cfg, k, v)

    # Reconstruct nested dataclasses
    cfg.window_am = TimeWindow(**cfg.window_am) if isinstance(cfg.window_am, dict) else cfg.window_am
    cfg.window_pm = TimeWindow(**cfg.window_pm) if isinstance(cfg.window_pm, dict) else cfg.window_pm
    cfg.risk      = RiskCfg(**cfg.risk) if isinstance(cfg.risk, dict) else cfg.risk
    cfg.engine    = EngineCfg(**cfg.engine) if isinstance(cfg.engine, dict) else cfg.engine

    wl_fixed = []
    for w in cfg.watchlists:
        wl_fixed.append(WatchlistCfg(**w) if isinstance(w, dict) else w)
    cfg.watchlists = wl_fixed

    bands_fixed = []
    for b in cfg.order_bands:
        bands_fixed.append(OrderBand(**b) if isinstance(b, dict) else b)
    cfg.order_bands = bands_fixed

    return cfg

def save_config(cfg: AppCfg, path: str = CONFIG_PATH):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(asdict(cfg), f, indent=2)

def sanitize_symbols(lines: List[str]) -> List[str]:
    toks = []
    for s in lines:
        s = s.strip().upper()
        if not s:
            continue
        if re.match(r"^[A-Z0-9\.\-]+$", s):
            toks.append(s)
    # dedupe keep order
    seen = set()
    out = []
    for t in toks:
        if t not in seen:
            out.append(t)
            seen.add(t)
    return out

def keywords_from_text(text: str) -> List[str]:
    if "," in text:
        return [k.strip() for k in text.split(",") if k.strip()]
    return [k.strip() for k in text.splitlines() if k.strip()]

def keywords_to_text(keys: List[str]) -> str:
    return ", ".join(keys)

def parse_hhmm(hhmm: str) -> dt_time:
    h, m = [int(x) for x in hhmm.split(":")]
    return dt_time(hour=h, minute=m, tzinfo=EST)

def now_in_any_window(am: TimeWindow, pm: TimeWindow) -> bool:
    # Compare by local EST time
    now = datetime.now(EST).time()
    s1, e1 = parse_hhmm(am.start), parse_hhmm(am.end)
    s2, e2 = parse_hhmm(pm.start), parse_hhmm(pm.end)
    def in_win(s, e):
        if s <= e:
            return s <= now <= e
        # if window crosses midnight (not typical here)
        return now >= s or now <= e
    return in_win(s1, e1) or in_win(s2, e2)

# Robust ISO8601 parse (BZ timestamps vary)
def parse_news_time(ts: str) -> datetime:
    if not ts:
        return datetime.now(timezone.utc).astimezone(EST)
    try:
        # Common formats: "2025-09-23T12:03:21Z" or with offset
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(EST)
    except Exception:
        try:
            return datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=EST)
        except Exception:
            return datetime.now(EST)

# -----------------------------
# Storage (SQLite)
# -----------------------------
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
                (item.get("id", 0), item.get("created", ""), item.get("title",""), item.get("url",""),
                 ",".join(matches), ",".join(sorted(set(tickers))))
            )
            self.conn.commit()
        except Exception as e:
            self.log("ERROR", f"save_news failed: {e}")

    def save_order(self, oid: int, parent_oid: Optional[int], symbol: str, side: str,
                   qty: int, limit: float, tp: float, sl: float, status: str, reason: str):
        try:
            self.conn.execute(
                "INSERT OR REPLACE INTO orders(oid,parent_oid,symbol,side,qty,limit_price,tp,sl,status,reason,created_ts) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (oid, parent_oid, symbol, side, qty, limit, tp, sl, status, reason, datetime.now(EST).isoformat())
            )
            self.conn.commit()
        except Exception as e:
            self.log("ERROR", f"save_order failed: {e}")

    def save_fill(self, exec_id: str, oid: int, symbol: str, side: str, qty: int, price: float):
        try:
            self.conn.execute(
                "INSERT OR IGNORE INTO fills(exec_id,oid,symbol,side,qty,price,ts) VALUES(?,?,?,?,?,?,?)",
                (exec_id, oid, symbol, side, qty, price, datetime.now(EST).isoformat())
            )
            self.conn.commit()
        except Exception as e:
            self.log("ERROR", f"save_fill failed: {e}")

    def log(self, level: str, message: str):
        try:
            self.conn.execute("INSERT INTO events(ts,level,message) VALUES(?,?,?)",
                              (datetime.now(EST).isoformat(), level, message))
            self.conn.commit()
        except:
            pass

# -----------------------------
# Logging
# -----------------------------
class GuiLogEmitter(QObject):
    message = Signal(str)

class GuiLogHandler(logging.Handler):
    def __init__(self, emitter: GuiLogEmitter):
        super().__init__()
        self.emitter = emitter
    def emit(self, record):
        try:
            msg = self.format(record)
            self.emitter.message.emit(msg)
        except Exception:
            pass

def build_logger(emitter: Optional[GuiLogEmitter] = None) -> logging.Logger:
    logger = logging.getLogger("ibkr_news_bot")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        fh = RotatingFileHandler(LOG_PATH, maxBytes=2_000_000, backupCount=5)
        fh.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(fh)
    if emitter and not any(isinstance(h, GuiLogHandler) for h in logger.handlers):
        gh = GuiLogHandler(emitter); gh.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(gh)
    return logger

# -----------------------------
# Benzinga REST (fallback if needed)
# -----------------------------
class BenzingaClient:
    BASE_URL = "https://api.benzinga.com/api/v2/news"
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
    def get_news(self, symbols: List[str], limit=30) -> List[dict]:
        if not self.api_key:
            return []
        params = {
            "token": self.api_key,
            "symbols": ",".join(symbols) if symbols else "",
            "limit": limit,
            "format": "json"
        }
        r = self.session.get(self.BASE_URL, params=params, timeout=20)
        if not r.ok:
            return []
        try:
            data = r.json()
            return data if isinstance(data, list) else []
        except:
            return []

# -----------------------------
# Keyword matcher
# -----------------------------
class KeywordMatcher:
    def __init__(self, keywords: List[str]):
        self.automaton = ahocorasick.Automaton()
        for kw in keywords:
            k = kw.strip().lower()
            if k:
                self.automaton.add_word(k, k)
        self.automaton.make_automaton()
    def match(self, text: str) -> List[str]:
        text = (text or "").lower()
        return list({kw for _, kw in self.automaton.iter(text)})

# -----------------------------
# Trader
# -----------------------------
class Trader:
    def __init__(self, ib: IB, store: Store, logger: logging.Logger, max_concurrent: int):
        self.ib = ib
        self.store = store
        self.log = logger
        self.max_concurrent = max_concurrent
        self.open_parent_oids: Set[int] = set()

        # Hook events
        self.ib.execDetailsEvent += self._on_exec
        self.ib.orderStatusEvent  += self._on_status
        self.ib.errorEvent        += self._on_error

    def _stk(self, symbol: str) -> Stock:
        return Stock(symbol, "SMART", "USD", primaryExchange="NASDAQ")

    def _mid(self, symbol: str, test_mode: bool) -> float:
        if test_mode:
            self.log.info(f"[TEST_MODE] Using fallback mid=100.00 for {symbol}")
            return 100.00
        try:
            t_list = self.ib.reqTickers(self._stk(symbol))
            if not t_list:
                raise RuntimeError("No ticker data")
            t: Ticker = t_list[0]
            if t.bid and t.ask:
                return round((t.bid + t.ask) / 2.0, 2)
            if t.last:
                return round(float(t.last), 2)
        except Exception as e:
            self.log.warning(f"midprice fetch failed for {symbol}: {e}")
        self.log.warning(f"No market data for {symbol}; using fallback mid=100.00")
        return 100.00

    def _bracket(self, qty: int, entry: float, tp: float, sl: float) -> List[Order]:
        parent = LimitOrder("BUY", qty, round(entry, 2))
        parent.transmit = False

        tp_order = LimitOrder("SELL", qty, round(tp, 2))
        tp_order.transmit = False

        sl_order = Order()
        sl_order.orderType = "STP"
        sl_order.action = "SELL"
        sl_order.totalQuantity = qty
        sl_order.auxPrice = round(sl, 2)
        sl_order.transmit = True  # final child transmits chain

        return [parent, tp_order, sl_order]

    def _can_trade(self) -> bool:
        return len(self.open_parent_oids) < self.max_concurrent

    def place_bracket(self, symbol: str, qty: int, entry: float, tp: float, sl: float, reason: str) -> Optional[int]:
        if not self._can_trade():
            self.log.warning(f"Max concurrent reached; skip {symbol}")
            return None
        c = self._stk(symbol)
        parent, tpo, slo = self._bracket(qty, entry, tp, sl)
        try:
            trade = self.ib.placeOrder(c, parent)
            pid = trade.order.orderId
            self.open_parent_oids.add(pid)

            tpo.parentId = pid
            slo.parentId = pid
            t1 = self.ib.placeOrder(c, tpo)
            t2 = self.ib.placeOrder(c, slo)

            self.store.save_order(pid, None, symbol, "BUY", qty, entry, tp, sl, "Submitted", reason)
            self.store.save_order(t1.order.orderId, pid, symbol, "SELL", qty, tp, tp, sl, "Submitted", "tp_child")
            self.store.save_order(t2.order.orderId, pid, symbol, "SELL", qty, sl, tp, sl, "Submitted", "sl_child")

            self.log.info(f"SUBMITTED BRACKET {symbol} BUY qty={qty} LMT={entry:.2f} TP={tp:.2f} SL={sl:.2f} (parent {pid})")
            return pid
        except Exception as e:
            self.log.error(f"place_bracket failed {symbol}: {e}")
            return None

    def iceberg(self, symbol: str, total_qty: int, entry: float, tp: float, sl: float, reason: str, child_qty: int = 10):
        remaining = total_qty
        while remaining > 0:
            lot = min(child_qty, remaining)
            self.place_bracket(symbol, lot, entry, tp, sl, reason + " (iceberg)")
            remaining -= lot
            time.sleep(0.7)

    # --- IB Events
    def _on_exec(self, trade, fill):
        try:
            exec_id = fill.execId
            oid     = trade.order.orderId
            sym     = trade.contract.symbol
            side    = trade.order.action
            qty     = int(fill.execution.shares)
            price   = float(fill.execution.price)
            self.store.save_fill(exec_id, oid, sym, side, qty, price)
            self.log.info(f"FILL {sym} {side} {qty}@{price} (oid {oid}, exec {exec_id})")
        except Exception as e:
            self.log.error(f"on_exec failed: {e}")

    def _on_status(self, trade):
        try:
            oid = trade.order.orderId
            st  = trade.orderStatus.status
            sym = trade.contract.symbol
            if oid in self.open_parent_oids and st in ("Filled","Cancelled","Inactive"):
                self.open_parent_oids.discard(oid)
            self.store.save_order(
                oid=oid, parent_oid=getattr(trade.order, "parentId", None),
                symbol=sym, side=trade.order.action, qty=int(trade.order.totalQuantity or 0),
                limit=float(getattr(trade.order, "lmtPrice", 0.0) or 0.0),
                tp=0.0, sl=0.0, status=st, reason="status_update"
            )
            self.log.info(f"ORDER STATUS oid={oid} sym={sym} status={st}")
        except Exception as e:
            self.log.error(f"on_status failed: {e}")

    def _on_error(self, reqId, code, msg, contract):
        self.log.error(f"IB ERROR reqId={reqId} code={code} msg={msg} contract={contract}")

# -----------------------------
# Benzinga WebSocket
# -----------------------------
class BZWebSocket:
    def __init__(self, token: str, callback, tickers=None):
        base = "wss://api.benzinga.com/api/v1/news/stream"   # ✅ correct endpoint
        params = f"?token={token}"
        if tickers:
            params += "&tickers=" + ",".join(tickers)
        self.url = base + params
        self.callback = callback
        self.ws = None

    def start(self):
        def _on_message(ws, msg):
            try:
                data = json.loads(msg)
                self.callback(data)
            except Exception as e:
                print("WS parse error:", e)

        def _on_error(ws, err):
            print("WS error:", err)

        def _on_close(ws, code, msg):
            print("WS closed:", code, msg)

        def _on_open(ws):
            print("✅ WS connected to Benzinga Analyst Insights stream")

        self.ws = WebSocketApp(
            self.url,
            on_message=_on_message,
            on_error=_on_error,
            on_close=_on_close,
            on_open=_on_open,
        )

        threading.Thread(
            target=lambda: self.ws.run_forever(sslopt={"ca_certs": certifi.where()}),
            daemon=True
        ).start()

# -----------------------------
# Engine Thread (WebSocket + Volume filter)
# -----------------------------
class Engine(threading.Thread):
    def __init__(self, cfg: AppCfg, log: logging.Logger, store: Store):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.log = log
        self.store = store
        self.running = threading.Event()
        self._ib = IB()
        self._trader: Optional[Trader] = None
        self.ws: Optional[BZWebSocket] = None
        self.matcher: Optional[KeywordMatcher] = None
        self.watch: Set[str] = set()

    def _estimate_global_pnl(self) -> float:
        try:
            accts = self._ib.managedAccounts() or []
            if not accts:
                return 0.0
            acct = accts[0]
            vals = {v.tag: v.value for v in self._ib.accountSummary(acct)}
            realized = float(vals.get("RealizedPnL", "0") or 0)
            unreal   = float(vals.get("UnrealizedPnL", "0") or 0)
            return realized + unreal
        except Exception as e:
            self.log.warning(f"P&L read failed: {e}")
            return 0.0

    def _qty_from_bands(self, price: float) -> int:
        for b in self.cfg.order_bands:
            if b.min_price <= price < b.max_price:
                return max(1, int(b.shares))
        return 50  # fallback

    def _get_first_minute_volume(self, symbol: str, news_time: datetime) -> int:
        """Fetch 1-min bar volume for the minute the news was released (RTH only)."""
        try:
            contract = Stock(symbol, "SMART", "USD", primaryExchange="NASDAQ")

            # Align to the minute of the news
            # Use EST (already converted)
            aligned = news_time.astimezone(EST).replace(second=0, microsecond=0)
            end = aligned + pd.Timedelta(minutes=1)

            bars = self._ib.reqHistoricalData(
                contract,
                endDateTime=end,          # IB expects local / timezone-aware
                durationStr="1 M",        # 1 Minute window
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=True
            )
            if not bars:
                return 0
            # We want the bar that covers [aligned, aligned+1min)
            return int(getattr(bars[0], "volume", 0) or 0)
        except Exception as e:
            self.log.error(f"Volume fetch failed for {symbol}: {e}")
            return 0

    def stop(self):
        self.running.clear()

    # ---- Core NEWS handler ----
    def _on_news(self, item: dict):
        """
        Handle incoming Benzinga Analyst Insight WebSocket message.
        """
        try:
            # Extract the payload
            content = item.get("data", {}).get("content", {})
            title   = content.get("analyst_insights", "") or content.get("title", "")
            sym     = content.get("security", {}).get("symbol", "")

            # 1. Check if ticker is valid & in watchlist
            if not sym or sym not in self.watch:
                return

            # 2. Check if keywords matched
            matches = self.matcher.match(title)
            if not matches:
                return

            # 3. Log the match
            self.log.info(f"WS MATCH {sym} {matches} | {title}")

            # 4. Check traded volume in first minute
            bars = self._ib.reqHistoricalData(
                Stock(sym, "SMART", "USD", primaryExchange="NASDAQ"),
                endDateTime="", 
                durationStr="1 m",   # last 1 minute
                barSizeSetting="1 min",
                whatToShow="TRADES",
                useRTH=True
            )
            if not bars or bars[-1].volume < self.cfg.risk.min_news_volume:
                self.log.info(f"Skip {sym}: volume {bars[-1].volume if bars else 0} < {self.cfg.risk.min_news_volume}")
                return

            # 5. Get mid price
            mid = self._trader._mid(sym, self.cfg.engine.test_mode)
            tp  = mid * (1 + self.cfg.risk.tp_pct/100.0)
            sl  = mid * (1 - self.cfg.risk.sl_pct/100.0)
            qty = self._qty_from_bands(mid)
            reason = f"news:{','.join(sorted(matches))}"

            # 6. Place order (normal or iceberg)
            if self.cfg.risk.iceberg and qty > self.cfg.risk.iceberg_child_qty:
                self._trader.iceberg(sym, qty, mid, tp, sl, reason, child_qty=self.cfg.risk.iceberg_child_qty)
            else:
                self._trader.place_bracket(sym, qty, mid, tp, sl, reason)

        except Exception as e:
            self.log.error(f"_on_news error: {e}")


    def run(self):
        # Fix asyncio loop in thread for ib_insync
        try:
            asyncio.set_event_loop(asyncio.new_event_loop())
        except Exception as e:
            self.log.error(f"Failed to set asyncio loop: {e}")
            return

        # Connect IBKR
        try:
            self._ib.connect(self.cfg.ibkr_host, self.cfg.ibkr_port, self.cfg.ibkr_client_id, readonly=False)
        except Exception as e:
            self.log.error(f"Failed to connect IBKR: {e}")
            return

        # Prepare keyword matcher & trader
        all_keywords = sorted({k.strip().lower()
                               for w in self.cfg.watchlists
                               for k in w.keywords if k.strip()})
        blacklist = {"downgrade","bankruptcy","miss","fraud","restatement","investigation"}
        all_keywords = [k for k in all_keywords if k not in blacklist]
        self.matcher = KeywordMatcher(all_keywords)

        self.watch = {s for w in self.cfg.watchlists for s in w.tickers}
        self._trader = Trader(self._ib, self.store, self.log, self.cfg.risk.max_concurrent)

        # Start WS
        if not self.cfg.benzinga_api_key:
            self.log.error("No Benzinga API key set. Please set it in Sessions tab.")
            return

        self.ws = BZWebSocket(self.cfg.benzinga_api_key, self._on_news, tickers=self.watch)
        self.ws.start()


        self.running.set()
        self.log.info(f"Engine started. Watchlist={sorted(self.watch)} Keywords={all_keywords} "
                      f"MinVol={self.cfg.risk.min_news_volume} TEST_MODE={'ON' if self.cfg.engine.test_mode else 'OFF'}")

        # Safety/housekeeping loop
        while self.running.is_set():
            try:
                # Kill switch / Session window
                if os.path.exists(self.cfg.risk.kill_switch_file):
                    self.log.warning("Kill switch present; sleeping.")
                    time.sleep(min(5, self.cfg.engine.poll_seconds))
                    continue

                if not now_in_any_window(self.cfg.window_am, self.cfg.window_pm):
                    # Still keep WS connected; just avoid trading by _on_news because of time window check here?
                    # We enforce session in _on_news via this guard:
                    time.sleep(min(15, self.cfg.engine.poll_seconds))
                    continue

                # P&L global guard
                pnl = self._estimate_global_pnl()
                if pnl <= self.cfg.risk.global_loss_usd or pnl >= self.cfg.risk.global_profit_usd:
                    self.log.warning(f"Global PnL cutoff hit ({pnl:.2f}); closing positions & canceling open orders.")
                    try:
                        for pos in self._ib.positions():
                            qty = int(pos.position)
                            if qty == 0: continue
                            sym = pos.contract.symbol
                            side = "SELL" if qty > 0 else "BUY"
                            self._ib.placeOrder(
                                Stock(sym, "SMART", "USD", primaryExchange="NASDAQ"),
                                MarketOrder(side, abs(qty))
                            )
                            self.log.info(f"Close-all sent: {sym} {side} {abs(qty)} MKT")
                        for o in self._ib.reqAllOpenOrders():
                            self._ib.cancelOrder(o)
                        self.log.info("Canceled all open orders.")
                    except Exception as e:
                        self.log.error(f"Close/cancel failed: {e}")
                    time.sleep(self.cfg.engine.poll_seconds)
                    continue

                time.sleep(self.cfg.engine.poll_seconds)

            except Exception as e:
                self.log.error(f"Engine loop error: {e}\n{traceback.format_exc()}")
                time.sleep(self.cfg.engine.poll_seconds)

        # Graceful shutdown
        try:
            if self._ib.isConnected():
                self._ib.disconnect()
        except:
            pass
        self.log.info("Engine stopped.")

# -----------------------------
# GUI Pages
# -----------------------------
class WatchlistPage(QWidget):
    def __init__(self, cfg: WatchlistCfg, parent=None):
        super().__init__(parent)
        self.cfg = cfg

        outer = QVBoxLayout(self)

        # Tickers
        box_t = QGroupBox(f"{cfg.name} — Tickers")
        tlay = QVBoxLayout(box_t)

        self.tickers_list = QListWidget()
        for t in cfg.tickers:
            QListWidgetItem(t, self.tickers_list)

        tbtns = QHBoxLayout()
        self.btn_add = QPushButton("Add")
        self.btn_del = QPushButton("Remove")
        self.btn_csv = QPushButton("Import CSV")
        tbtns.addWidget(self.btn_add); tbtns.addWidget(self.btn_del); tbtns.addStretch(1); tbtns.addWidget(self.btn_csv)

        tlay.addWidget(self.tickers_list)
        tlay.addLayout(tbtns)

        # Keywords (long-only)
        box_k = QGroupBox(f"{cfg.name} — Keywords")
        klay = QVBoxLayout(box_k)
        self.keywords_edit = QTextEdit()
        self.keywords_edit.setPlaceholderText(
          "One per line OR comma-separated (e.g., upgrade, merger, earnings)\n"
          "⚠️ Only long-side keywords are supported"
        )
        self.keywords_edit.setText(keywords_to_text(cfg.keywords))
        klay.addWidget(self.keywords_edit)

        outer.addWidget(box_t)
        outer.addWidget(box_k)

        # hooks
        self.btn_add.clicked.connect(self.add_symbol)
        self.btn_del.clicked.connect(self.del_symbol)
        self.btn_csv.clicked.connect(self.import_csv)

    def add_symbol(self):
        # quick inline add
        item = QListWidgetItem("NEW_TICKER", self.tickers_list)
        self.tickers_list.editItem(item)

    def del_symbol(self):
        for it in self.tickers_list.selectedItems():
            self.tickers_list.takeItem(self.tickers_list.row(it))

    def import_csv(self):
        pth, _ = QFileDialog.getOpenFileName(self, "Import CSV (first column = symbols)", "", "CSV Files (*.csv);;All Files (*)")
        if not pth: return
        syms = []
        try:
            with open(pth, "r", newline="", encoding="utf-8") as f:
                r = csv.reader(f)
                for row in r:
                    if not row: continue
                    syms.append(row[0])
        except Exception as e:
            QMessageBox.critical(self, "CSV error", f"Failed to read CSV:\n{e}")
            return
        syms = sanitize_symbols(syms)
        existing = [self.tickers_list.item(i).text() for i in range(self.tickers_list.count())]
        for s in syms:
            if s not in existing:
                QListWidgetItem(s, self.tickers_list)

    def export_cfg(self) -> WatchlistCfg:
        tickers = [self.tickers_list.item(i).text().strip().upper() for i in range(self.tickers_list.count())]
        tickers = sanitize_symbols(tickers)
        # Filter out short-side keywords defensively
        blacklist = {"downgrade","bankruptcy","miss","fraud","restatement","investigation"}
        keys = [k for k in keywords_from_text(self.keywords_edit.toPlainText())
                if k and k.strip().lower() not in blacklist]
        return WatchlistCfg(self.cfg.name, tickers, keys)

class OrderBandsPage(QWidget):
    def __init__(self, bands: List[OrderBand], parent=None):
        super().__init__(parent)
        self.table = QTableWidget(0, 3)
        self.table.setHorizontalHeaderLabels(["Min Price", "Max Price", "Shares"])
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)

        for b in bands:
            self._add_row(b)

        add_btn = QPushButton("Add Row")
        del_btn = QPushButton("Delete Selected")
        btns = QHBoxLayout()
        btns.addWidget(add_btn); btns.addWidget(del_btn); btns.addStretch(1)

        layout = QVBoxLayout(self)
        layout.addWidget(self.table)
        layout.addLayout(btns)

        add_btn.clicked.connect(lambda: self._add_row(OrderBand()))
        del_btn.clicked.connect(self._del_selected)

    def _add_row(self, band: OrderBand):
        r = self.table.rowCount()
        self.table.insertRow(r)
        self.table.setItem(r, 0, QTableWidgetItem(str(band.min_price)))
        self.table.setItem(r, 1, QTableWidgetItem(str(band.max_price)))
        self.table.setItem(r, 2, QTableWidgetItem(str(band.shares)))

    def _del_selected(self):
        rows = sorted({idx.row() for idx in self.table.selectedIndexes()}, reverse=True)
        for r in rows:
            self.table.removeRow(r)

    def export_bands(self) -> List[OrderBand]:
        bands = []
        for r in range(self.table.rowCount()):
            try:
                mn = float(self.table.item(r, 0).text())
                mx = float(self.table.item(r, 1).text())
                sh = int(float(self.table.item(r, 2).text()))
                if mx <= mn or sh <= 0: continue
                bands.append(OrderBand(mn, mx, sh))
            except:
                continue
        return bands

class SessionsPage(QWidget):
    def __init__(self, cfg: AppCfg, parent=None):
        super().__init__(parent)
        grid = QGridLayout(self)

        # AM/PM windows
        def time_row(row, label, tw: TimeWindow):
            grid.addWidget(QLabel(label + " start (EST)"), row, 0)
            s = QLineEdit(tw.start); grid.addWidget(s, row, 1)
            grid.addWidget(QLabel(label + " end (EST)"), row, 2)
            e = QLineEdit(tw.end); grid.addWidget(e, row, 3)
            return s, e

        self.am_s, self.am_e = time_row(0, "AM Window", cfg.window_am)
        self.pm_s, self.pm_e = time_row(1, "PM Window", cfg.window_pm)

        # BZ + IB + Engine
        grid.addWidget(QLabel("Benzinga API Key"), 2, 0)
        self.bz_key = QLineEdit(cfg.benzinga_api_key); grid.addWidget(self.bz_key, 2, 1, 1, 3)

        grid.addWidget(QLabel("IB Host"), 3, 0)
        self.ib_host = QLineEdit(cfg.ibkr_host); grid.addWidget(self.ib_host, 3, 1)
        grid.addWidget(QLabel("IB Port"), 3, 2)
        self.ib_port = QSpinBox(); self.ib_port.setRange(1, 65535); self.ib_port.setValue(cfg.ibkr_port); grid.addWidget(self.ib_port, 3, 3)

        grid.addWidget(QLabel("IB Client ID"), 4, 0)
        self.ib_cid = QSpinBox(); self.ib_cid.setRange(0, 999999); self.ib_cid.setValue(cfg.ibkr_client_id); grid.addWidget(self.ib_cid, 4, 1)

        grid.addWidget(QLabel("Poll seconds"), 4, 2)
        self.poll = QSpinBox(); self.poll.setRange(5, 3600); self.poll.setValue(cfg.engine.poll_seconds); grid.addWidget(self.poll, 4, 3)

        self.test_mode = QCheckBox("TEST_MODE (use fallback price when no market data)")
        self.test_mode.setChecked(cfg.engine.test_mode)
        grid.addWidget(self.test_mode, 5, 0, 1, 4)

    def export_sessions(self, cfg: AppCfg):
        cfg.window_am.start = self.am_s.text().strip() or cfg.window_am.start
        cfg.window_am.end   = self.am_e.text().strip() or cfg.window_am.end
        cfg.window_pm.start = self.pm_s.text().strip() or cfg.window_pm.start
        cfg.window_pm.end   = self.pm_e.text().strip() or cfg.window_pm.end

        cfg.benzinga_api_key = self.bz_key.text().strip()
        cfg.ibkr_host = self.ib_host.text().strip() or "127.0.0.1"
        cfg.ibkr_port = int(self.ib_port.value())
        cfg.ibkr_client_id = int(self.ib_cid.value())
        cfg.engine.poll_seconds = int(self.poll.value())
        cfg.engine.test_mode = self.test_mode.isChecked()
        return cfg

class RiskPage(QWidget):
    def __init__(self, risk: RiskCfg, parent=None):
        super().__init__(parent)
        grid = QGridLayout(self)

        grid.addWidget(QLabel("Take-profit %"), 0, 0)
        self.tp = QDoubleSpinBox(); self.tp.setRange(0.01, 50.0); self.tp.setDecimals(2); self.tp.setValue(risk.tp_pct)
        grid.addWidget(self.tp, 0, 1)

        grid.addWidget(QLabel("Stop-loss %"), 0, 2)
        self.sl = QDoubleSpinBox(); self.sl.setRange(0.01, 50.0); self.sl.setDecimals(2); self.sl.setValue(risk.sl_pct)
        grid.addWidget(self.sl, 0, 3)

        grid.addWidget(QLabel("Max concurrent trades"), 1, 0)
        self.maxc = QSpinBox(); self.maxc.setRange(1, 50); self.maxc.setValue(risk.max_concurrent)
        grid.addWidget(self.maxc, 1, 1)

        self.iceberg = QCheckBox("Iceberg ON"); self.iceberg.setChecked(risk.iceberg)
        grid.addWidget(self.iceberg, 1, 2)
        grid.addWidget(QLabel("Iceberg child qty"), 1, 3)
        self.child = QSpinBox(); self.child.setRange(1, 100000); self.child.setValue(risk.iceberg_child_qty)
        grid.addWidget(self.child, 1, 4)

        grid.addWidget(QLabel("Global Profit ($)"), 2, 0)
        self.gp = QDoubleSpinBox(); self.gp.setRange(-1e9, 1e9); self.gp.setDecimals(2); self.gp.setValue(risk.global_profit_usd)
        grid.addWidget(self.gp, 2, 1)
        grid.addWidget(QLabel("Global Loss ($)"), 2, 2)
        self.gl = QDoubleSpinBox(); self.gl.setRange(-1e9, 1e9); self.gl.setDecimals(2); self.gl.setValue(risk.global_loss_usd)
        grid.addWidget(self.gl, 2, 3)

        self.kill = QCheckBox("Kill-switch ON (create KILL file)")
        self.kill.setChecked(os.path.exists(risk.kill_switch_file))
        grid.addWidget(self.kill, 3, 0, 1, 3)
        grid.addWidget(QLabel("Kill-switch file path"), 3, 3)
        self.kill_path = QLineEdit(risk.kill_switch_file)
        grid.addWidget(self.kill_path, 3, 4)

        grid.addWidget(QLabel("Min 1-min Volume (Shares)"), 4, 0)
        self.min_vol = QSpinBox(); self.min_vol.setRange(0, 1_000_000); self.min_vol.setValue(risk.min_news_volume)
        grid.addWidget(self.min_vol, 4, 1)

    def export_risk(self, risk: RiskCfg):
        risk.tp_pct = float(self.tp.value())
        risk.sl_pct = float(self.sl.value())
        risk.max_concurrent = int(self.maxc.value())
        risk.iceberg = self.iceberg.isChecked()
        risk.iceberg_child_qty = int(self.child.value())
        risk.global_profit_usd = float(self.gp.value())
        risk.global_loss_usd = float(self.gl.value())
        risk.kill_switch_file = self.kill_path.text().strip() or "KILL"
        risk.min_news_volume = int(self.min_vol.value())

        # Toggle kill file safely
        try:
            if self.kill.isChecked():
                open(risk.kill_switch_file, "a").close()
            else:
                if os.path.exists(risk.kill_switch_file):
                    os.remove(risk.kill_switch_file)
        except Exception as e:
            QMessageBox.warning(self, "Kill-switch", f"Failed to toggle kill switch file:\n{e}")

        return risk

# -----------------------------
# Main Window
# -----------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("IBKR News Bot — Long-Only (WebSocket + Volume Filter)")
        self.resize(1200, 800)

        # Logging to GUI
        self.emitter = GuiLogEmitter()
        self.emitter.message.connect(self._append_log)
        self.logger = build_logger(self.emitter)
        self.store  = Store()

        self.cfg = load_config()

        # Tabs
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)

        # Sessions & Connection
        self.page_sessions = SessionsPage(self.cfg)
        self.tabs.addTab(self.page_sessions, "Sessions & Connection")

        # Watchlists (3)
        self.wpages: List[WatchlistPage] = []
        for w in self.cfg.watchlists:
            p = WatchlistPage(w, self)
            self.wpages.append(p)
            self.tabs.addTab(p, w.name)

        # Order bands
        self.page_bands = OrderBandsPage(self.cfg.order_bands, self)
        self.tabs.addTab(self.page_bands, "Custom Order Sizing")

        # Risk
        self.page_risk = RiskPage(self.cfg.risk, self)
        self.tabs.addTab(self.page_risk, "Risk & Safety")

        # Controls + Log
        ctrl = QHBoxLayout()
        self.btn_start = QPushButton("🟢 Start")
        self.btn_stop  = QPushButton("🔴 Stop")
        self.btn_save  = QPushButton("Save Config")
        self.btn_reload= QPushButton("Reload")
        ctrl.addWidget(self.btn_start); ctrl.addWidget(self.btn_stop)
        ctrl.addStretch(1)
        ctrl.addWidget(self.btn_save); ctrl.addWidget(self.btn_reload)

        self.logbox = QPlainTextEdit()
        self.logbox.setReadOnly(True)
        self.logbox.setStyleSheet("QPlainTextEdit { background-color: black; color: white; }")


        wrapper = QVBoxLayout()
        wrapper.addWidget(self.tabs)
        tmp = QWidget(); tmp.setLayout(ctrl)
        wrapper.addWidget(tmp)
        wrapper.addWidget(QLabel("Console"))
        wrapper.addWidget(self.logbox)

        cont = QWidget(); cont.setLayout(wrapper)
        self.setCentralWidget(cont)

        # Signals
        self.btn_save.clicked.connect(self._save_all)
        self.btn_reload.clicked.connect(self._reload)
        self.btn_start.clicked.connect(self._start_engine)
        self.btn_stop.clicked.connect(self._stop_engine)

        self.engine: Optional[Engine] = None
        self._ui_update_state(running=False)

        self.logger.info("App ready. Configure tabs, then press Start.")

    def _append_log(self, msg: str):
        self.logbox.appendPlainText(msg)
        self.store.log("INFO", msg)

    def _ui_update_state(self, running: bool):
        self.btn_start.setEnabled(not running)
        self.btn_stop.setEnabled(running)

    def _collect(self) -> AppCfg:
        # sessions
        self.cfg = self.page_sessions.export_sessions(self.cfg)
        # watchlists
        new_wls = []
        for page in self.wpages:
            new_wls.append(page.export_cfg())
        self.cfg.watchlists = new_wls
        # bands
        self.cfg.order_bands = self.page_bands.export_bands()
        # risk
        self.cfg.risk = self.page_risk.export_risk(self.cfg.risk)
        return self.cfg

    def _save_all(self):
        cfg = self._collect()
        save_config(cfg, CONFIG_PATH)
        QMessageBox.information(self, "Saved", f"Config saved to {CONFIG_PATH}")
        self.logger.info("Config saved.")

    def _reload(self):
        # Reload config from disk
        new_cfg = load_config(CONFIG_PATH)
        self.cfg = new_cfg

        # Refresh all UI pages with new config
        self.page_sessions = SessionsPage(self.cfg)
        self.tabs.removeTab(0)
        self.tabs.insertTab(0, self.page_sessions, "Sessions & Connection")

        for _ in list(range(len(self.wpages))):
            self.tabs.removeTab(1)
        self.wpages.clear()
        for w in self.cfg.watchlists:
            p = WatchlistPage(w, self)
            self.wpages.append(p)
            self.tabs.insertTab(1+len(self.wpages)-1, p, w.name)

        self.page_bands = OrderBandsPage(self.cfg.order_bands, self)
        self.tabs.insertTab(1+len(self.wpages), self.page_bands, "Custom Order Sizing")

        self.page_risk = RiskPage(self.cfg.risk, self)
        self.tabs.insertTab(2+len(self.wpages), self.page_risk, "Risk & Safety")

        self.logger.info("UI reloaded from config.json")

    def _start_engine(self):
        cfg = self._collect()
        save_config(cfg, CONFIG_PATH)

        if not cfg.benzinga_api_key:
            QMessageBox.warning(self, "Benzinga API Key", 
                                "Please enter your Benzinga API key in Sessions tab.")
            return
        
        ib = IB()

        try:
            # Quick probe to IBKR
            test_ib = IB()
            test_ib.connect(cfg.ibkr_host, cfg.ibkr_port, cfg.ibkr_client_id, readonly=False)
            test_ib.disconnect()
        except Exception as e:
            QMessageBox.warning(self, "IBKR Connection Error", 
                                f"Failed to connect to IBKR:\n{e}")
            return

        # Launch engine thread
        self.engine = Engine(cfg, self.logger, self.store)
        self.engine._ib = ib  # share the IB instance to reuse session
        self.engine.start()
        self._ui_update_state(running=True)
        self.logger.info("✅ Engine started.")

    def _stop_engine(self):
        if self.engine:
            self.engine.stop()
            self.engine.join(timeout=5)
            self.engine = None
        self._ui_update_state(running=False)
        self.logger.info("🛑 Engine stopped.")

# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    sys.exit(app.exec())
