
# 📰 IBKR News Bot

**Real-time news-driven engine for algorithmic trading on IBKR — powered by Benzinga Pro headlines.**  
Milestone 1: Core bot for ingesting market headlines, matching keywords, and logging results to SQLite + rotating logs.

---

## ⚡ Features

- 🔌 **Benzinga Pro Integration** — Fetches real-time market headlines via API
- 🧠 **Keyword Detection** — High-speed Aho-Corasick engine for matching terms
- 💾 **Persistent Logging** — Saves all matches into SQLite and rotating file logs
- 📦 **Modular Design** — Clean architecture ready for IBKR order routing

---

## 📂 Structure

```

ibkr\_news\_bot/
│
├── ibkr\_news\_bot.py     # main bot script
├── .env                  # API keys and config
├── data/news.db          # SQLite database (auto-created)
├── logs/bot.log           # Rotating log file (auto-created)
└── README.md

````

---

## ⚙️ Setup

### 1. Clone the Repository
```bash
git clone https://github.com/NadirAliOfficial/ibkr_news_bot.git
cd ibkr_news_bot
````

### 2. Create Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

*(Or manually)*:

```bash
pip install requests python-dotenv pyahocorasick
```

### 4. Add Your API Key

Create `.env`:

```
BZ_API_KEY=your_real_benzinga_api_key_here
```

---

## 🚀 Usage

```bash
python ibkr_news_bot.py
```

* Pulls latest Benzinga headlines for watchlist symbols (default: AAPL, TSLA)
* Detects configured keywords (default: bankruptcy, merger, upgrade, earnings)
* Logs matches into `data/news.db` and `logs/bot.log`

---

## 📌 Roadmap

### ✅ Milestone 1 (Current)

* Core bot engine
* Benzinga news ingestion
* Keyword matcher
* SQLite + file logging

### ⏳ Milestone 2

* Keyword-triggered orders on IBKR via `ib_insync`
* Bracket orders, risk controls, Telegram alerts

### ⏳ Milestone 3

* GUI (PySide6/Qt)
* Watchlists, keyword lists, live logs

### ⏳ Milestone 4

* VPS deployment
* Backtesting + forward testing
* Full documentation

---

## 🧠 Credits

* [Benzinga Pro API](https://www.benzinga.com/pro)
* [IBKR TWS API](https://interactivebrokers.github.io/)
* Developed by [@NadirAliOfficial](https://github.com/NadirAliOfficial)
<!-- updated: 2025-04-05-r01 -->
