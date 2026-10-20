import requests
import asyncio
import threading
import io
import os
import feedparser
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import mplfinance as mpf
import yfinance as yf
from datetime import datetime, timedelta
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes,
)
from config import BOT_TOKEN, CHAT_IDS, COINS, ALERT_THRESHOLD, CHECK_INTERVAL

# ── State ──────────────────────────────────────────────────
price_history: dict[str, list] = {}
last_signals: dict[str, dict] = {}
alerts_enabled = True
ALERT_THRESHOLD_VAR = ALERT_THRESHOLD
watchlist_alerts: dict[str, float] = {}
portfolio: dict[str, dict] = {}

YAHOO_MAP = {
    "bitcoin": "BTC-USD", "ethereum": "ETH-USD", "solana": "SOL-USD",
    "binancecoin": "BNB-USD", "ripple": "XRP-USD", "cardano": "ADA-USD",
    "dogecoin": "DOGE-USD", "polkadot": "DOT-USD", "avalanche-2": "AVAX-USD",
    "chainlink": "LINK-USD",
}
FOREX_TICKERS = {
    "XAU/USD": "GC=F", "XAG/USD": "SI=F",
    "USD/IDR": "IDR=X", "EUR/USD": "EURUSD=X",
    "GBP/USD": "GBPUSD=X", "USD/JPY": "JPY=X",
}

# API key Twelve Data, diambil dari Environment Variable Railway
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "demo")

# Mapping nama yang dipakai bot/command -> simbol Twelve Data
TWELVEDATA_MAP = {
    "xauusd": "XAU/USD", "xau": "XAU/USD", "gc=f": "XAU/USD",
    "xagusd": "XAG/USD", "xag": "XAG/USD", "si=f": "XAG/USD",
    "eurusd": "EUR/USD", "eur=x": "EUR/USD",
    "gbpusd": "GBP/USD", "gbp=x": "GBP/USD",
    "usdjpy": "USD/JPY", "jpy=x": "USD/JPY",
    "usdidr": "USD/IDR", "idr=x": "USD/IDR",
}

# Mapping interval bot -> interval Twelve Data
TWELVEDATA_INTERVAL_MAP = {
    "1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min",
    "1h": "1h", "60m": "1h", "2h": "2h", "4h": "4h",
    "1d": "1day", "1wk": "1week",
}
TWELVEDATA_PERIOD_SIZE = {
    "1d": 96, "5d": 200, "1mo": 200, "3mo": 200,
    "6mo": 200, "1y": 365, "60d": 200,
}

# Mapping coin_id (CoinGecko) -> simbol Binance, untuk data candlestick crypto
BINANCE_MAP = {
    "bitcoin": "BTCUSDT", "ethereum": "ETHUSDT", "solana": "SOLUSDT",
    "binancecoin": "BNBUSDT", "ripple": "XRPUSDT", "cardano": "ADAUSDT",
    "dogecoin": "DOGEUSDT", "polkadot": "DOTUSDT", "avalanche-2": "AVAXUSDT",
    "chainlink": "LINKUSDT",
}
TICKER_TO_COINGECKO = {v: k for k, v in YAHOO_MAP.items()}


# ══════════════════════════════════════════════════════════
# DATA SOURCES
# ══════════════════════════════════════════════════════════

_price_cache: dict = {"data": {}, "timestamp": 0, "coin_ids_key": ""}
_PRICE_CACHE_TTL = 30  # detik — hasil request CoinGecko dipakai ulang selama 30 detik
STRENGTH_THRESHOLD = 60  # ambang minimal strength% supaya sinyal dianggap layak dikirim


def get_crypto_prices(coin_ids: list) -> dict:
    import time
    ids = ",".join(sorted(coin_ids))
    now = time.time()

    # FIX: pakai cache kalau request terakhir untuk coin_ids yang sama masih < 30 detik
    # supaya tidak kena rate limit 429 dari CoinGecko akibat dipanggil terlalu sering
    if (
        _price_cache["coin_ids_key"] == ids
        and now - _price_cache["timestamp"] < _PRICE_CACHE_TTL
        and _price_cache["data"]
    ):
        return _price_cache["data"]

    url = (
        "https://api.coingecko.com/api/v3/simple/price"
        f"?ids={ids}&vs_currencies=usd,idr"
        "&include_24hr_change=true&include_24hr_vol=true&include_market_cap=true"
    )
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        _price_cache["data"] = data
        _price_cache["timestamp"] = now
        _price_cache["coin_ids_key"] = ids
        return data
    except Exception as e:
        print(f"[ERROR] get_crypto_prices: {e}")
        # FIX: kalau gagal (misal 429), tetap kembalikan cache lama kalau ada,
        # daripada kosong total
        if _price_cache["data"]:
            print("[INFO] get_crypto_prices: pakai cache lama karena request gagal")
            return _price_cache["data"]
        return {}


def get_ohlc_coingecko(coin_id: str, days: int = 30) -> list:
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/ohlc?vs_currency=usd&days={days}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[ERROR] get_ohlc: {e}")
        return []


# Mapping interval & period yang dipakai bot -> parameter Binance klines
BINANCE_INTERVAL_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "60m": "1h", "2h": "2h", "4h": "4h",
    "1d": "1d", "1wk": "1w",
}
BINANCE_PERIOD_LIMIT = {
    # period -> berapa banyak candle yang diminta (limit max Binance = 1000)
    "1d": 96, "5d": 200, "1mo": 200, "3mo": 200,
    "6mo": 200, "1y": 365, "60d": 200,
}


def get_ohlc_binance(symbol: str, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
    """Ambil data candlestick crypto dari Binance (gratis, tidak diblok dari server cloud)."""
    binance_interval = BINANCE_INTERVAL_MAP.get(interval, "1d")
    limit = BINANCE_PERIOD_LIMIT.get(period, 200)
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": symbol, "interval": binance_interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        raw = r.json()
        if not raw:
            return pd.DataFrame()
        df = pd.DataFrame(raw, columns=[
            "open_time", "Open", "High", "Low", "Close", "Volume",
            "close_time", "quote_vol", "trades", "taker_base", "taker_quote", "ignore",
        ])
        df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
        df.set_index("open_time", inplace=True)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(subset=["Close"], inplace=True)
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"[ERROR] get_ohlc_binance {symbol}: {e}")
        return pd.DataFrame()


def get_ohlc_yahoo(ticker: str, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False, auto_adjust=True)
        if df.empty:
            return pd.DataFrame()
        # FIX: flatten MultiIndex columns (yfinance >=0.2)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        else:
            df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col not in df.columns:
                df[col] = 0.0
        df.dropna(subset=["Close"], inplace=True)
        return df
    except Exception as e:
        print(f"[ERROR] get_ohlc_yahoo {ticker}: {e}")
        return pd.DataFrame()


def get_ohlc_twelvedata(symbol: str, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
    """Ambil data candlestick forex/XAU dari Twelve Data (tidak diblok dari server cloud)."""
    td_interval = TWELVEDATA_INTERVAL_MAP.get(interval, "1day")
    outputsize = TWELVEDATA_PERIOD_SIZE.get(period, 200)
    url = "https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": td_interval,
        "outputsize": outputsize,
        "apikey": TWELVEDATA_API_KEY,
        "order": "ASC",
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") == "error" or "values" not in data:
            print(f"[ERROR] get_ohlc_twelvedata {symbol}: {data.get('message', data)}")
            return pd.DataFrame()
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume",
        }, inplace=True)
        if "Volume" not in df.columns:
            df["Volume"] = 0.0
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df.dropna(subset=["Close"], inplace=True)
        return df[["Open", "High", "Low", "Close", "Volume"]]
    except Exception as e:
        print(f"[ERROR] get_ohlc_twelvedata {symbol}: {e}")
        return pd.DataFrame()


def get_ohlc_smart(ticker: str, period: str = "1mo", interval: str = "1d") -> pd.DataFrame:
    """Pilih sumber data otomatis: Binance untuk crypto, Twelve Data untuk forex/XAU, fallback Yahoo."""
    coin_id = TICKER_TO_COINGECKO.get(ticker)
    binance_symbol = BINANCE_MAP.get(coin_id) if coin_id else None
    if binance_symbol:
        df = get_ohlc_binance(binance_symbol, period=period, interval=interval)
        if not df.empty:
            return df
        return get_ohlc_yahoo(ticker, period=period, interval=interval)

    td_symbol = TWELVEDATA_MAP.get(ticker.lower())
    if td_symbol:
        df = get_ohlc_twelvedata(td_symbol, period=period, interval=interval)
        if not df.empty:
            return df
        return get_ohlc_yahoo(ticker, period=period, interval=interval)

    return get_ohlc_yahoo(ticker, period=period, interval=interval)


def get_forex_prices() -> dict:
    results = {}
    try:
        r = requests.get("https://api.frankfurter.app/latest?from=USD&to=IDR,EUR,GBP,JPY", timeout=10)
        if r.status_code == 200:
            rates = r.json().get("rates", {})
            results["USD/IDR"] = {"price": rates.get("IDR", 0)}
            results["EUR/USD"] = {"price": 1 / rates.get("EUR", 1)}
            results["GBP/USD"] = {"price": 1 / rates.get("GBP", 1)}
            results["USD/JPY"] = {"price": rates.get("JPY", 0)}
    except Exception as e:
        print(f"[ERROR] get_forex: {e}")
    try:
        xau_r = requests.get("https://metals.live/api/spot", timeout=10)
        if xau_r.status_code == 200:
            for m in xau_r.json():
                if m.get("metal") == "gold":
                    results["XAU/USD"] = {"price": m.get("price", 0)}
                if m.get("metal") == "silver":
                    results["XAG/USD"] = {"price": m.get("price", 0)}
    except Exception as e:
        print(f"[ERROR] get_xau: {e}")
    return results


def get_crypto_news(limit: int = 5) -> list:
    feeds = [
        "https://cointelegraph.com/rss",
        "https://coindesk.com/arc/outboundfeeds/rss/",
        "https://decrypt.co/feed",
    ]
    news = []
    for feed_url in feeds:
        try:
            # FIX: ambil raw content dulu dengan timeout, baru di-parse oleh feedparser
            # (feedparser.parse() langsung ke URL tidak punya timeout, bisa nge-hang lama)
            r = requests.get(feed_url, timeout=8, headers={"User-Agent": "Mozilla/5.0"})
            r.raise_for_status()
            feed = feedparser.parse(r.content)
            for entry in feed.entries[:3]:
                news.append({
                    "title": entry.get("title", ""),
                    "link": entry.get("link", ""),
                    "source": feed.feed.get("title", "") or feed_url,
                    "published": entry.get("published", ""),
                })
            if len(news) >= limit:
                break
        except Exception as e:
            print(f"[ERROR] news feed {feed_url}: {e}")
    return news[:limit]


# ══════════════════════════════════════════════════════════
# TECHNICAL INDICATORS
# ══════════════════════════════════════════════════════════

def compute_rsi(closes: list, period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def compute_ma(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    return round(sum(closes[-period:]) / period, 6)


def compute_ema(closes: list, period: int) -> float | None:
    if len(closes) < period:
        return None
    k = 2 / (period + 1)
    ema = closes[0]
    for p in closes[1:]:
        ema = p * k + ema * (1 - k)
    return round(ema, 6)


def compute_macd(closes: list) -> dict | None:
    if len(closes) < 26:
        return None
    k12 = 2 / (12 + 1)
    k26 = 2 / (26 + 1)
    ema12 = ema26 = closes[0]
    for p in closes:
        ema12 = p * k12 + ema12 * (1 - k12)
        ema26 = p * k26 + ema26 * (1 - k26)
    macd_line = ema12 - ema26

    macd_vals = []
    e12 = e26 = closes[0]
    for p in closes:
        e12 = p * k12 + e12 * (1 - k12)
        e26 = p * k26 + e26 * (1 - k26)
        macd_vals.append(e12 - e26)

    k9 = 2 / (9 + 1)
    signal = macd_vals[0]
    for v in macd_vals[1:]:
        signal = v * k9 + signal * (1 - k9)

    return {
        "macd": round(macd_line, 6),
        "signal": round(signal, 6),
        "histogram": round(macd_line - signal, 6),
    }


def compute_bollinger(closes: list, period: int = 20, std_dev: int = 2) -> dict | None:
    if len(closes) < period:
        return None
    subset = closes[-period:]
    ma = sum(subset) / period
    std = (sum((x - ma) ** 2 for x in subset) / period) ** 0.5
    return {
        "upper": round(ma + std_dev * std, 6),
        "middle": round(ma, 6),
        "lower": round(ma - std_dev * std, 6),
        "std": round(std, 6),
    }


def compute_support_resistance(closes: list) -> dict:
    if len(closes) < 10:
        return {}
    recent = closes[-20:]
    resistance = max(recent)
    support = min(recent)
    return {"support": round(support, 6), "resistance": round(resistance, 6)}


# ══════════════════════════════════════════════════════════
# INDIKATOR LANJUTAN (butuh OHLCV penuh, bukan cuma closes)
# ══════════════════════════════════════════════════════════

def compute_adx(df: pd.DataFrame, period: int = 14) -> dict | None:
    """Average Directional Index — mengukur kekuatan tren (bukan arahnya)."""
    if len(df) < period * 2:
        return None
    high, low, close = df["High"], df["Low"], df["Close"]

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)

    atr = tr.rolling(period).mean()
    # FIX: pakai np.nan (bukan pd.NA) + astype(float) supaya tidak crash di .rolling().mean()
    # saat data flat/sideways ekstrem menghasilkan pembagian 0/0
    atr_safe = atr.replace(0, np.nan)
    plus_di = (100 * (plus_dm.rolling(period).mean() / atr_safe)).astype(float)
    minus_di = (100 * (minus_dm.rolling(period).mean() / atr_safe)).astype(float)

    di_sum = (plus_di + minus_di).replace(0, np.nan)
    dx = (100 * (plus_di - minus_di).abs() / di_sum).astype(float)
    adx = dx.rolling(period).mean()

    if adx.empty or pd.isna(adx.iloc[-1]):
        return None
    return {
        "adx": round(float(adx.iloc[-1]), 1),
        "plus_di": round(float(plus_di.iloc[-1]), 1) if not pd.isna(plus_di.iloc[-1]) else 0.0,
        "minus_di": round(float(minus_di.iloc[-1]), 1) if not pd.isna(minus_di.iloc[-1]) else 0.0,
        "trending": bool(adx.iloc[-1] >= 25),
    }


def compute_stochastic(df: pd.DataFrame, k_period: int = 14, d_period: int = 3) -> dict | None:
    """Stochastic Oscillator %K / %D."""
    if len(df) < k_period + d_period:
        return None
    low_min = df["Low"].rolling(k_period).min()
    high_max = df["High"].rolling(k_period).max()
    denom = (high_max - low_min).replace(0, np.nan)
    k = (100 * (df["Close"] - low_min) / denom).astype(float)
    d = k.rolling(d_period).mean()
    if pd.isna(k.iloc[-1]) or pd.isna(d.iloc[-1]):
        return None
    return {"k": round(float(k.iloc[-1]), 1), "d": round(float(d.iloc[-1]), 1)}


def compute_cci(df: pd.DataFrame, period: int = 20) -> float | None:
    """Commodity Channel Index."""
    if len(df) < period:
        return None
    tp = (df["High"] + df["Low"] + df["Close"]) / 3
    sma = tp.rolling(period).mean()
    mad = tp.rolling(period).apply(lambda x: (x - x.mean()).abs().mean(), raw=False)
    denom = (0.015 * mad).replace(0, np.nan)
    cci = ((tp - sma) / denom).astype(float)
    if cci.empty or pd.isna(cci.iloc[-1]):
        return None
    return round(float(cci.iloc[-1]), 1)


def compute_williams_r(df: pd.DataFrame, period: int = 14) -> float | None:
    """Williams %R."""
    if len(df) < period:
        return None
    high_max = df["High"].rolling(period).max()
    low_min = df["Low"].rolling(period).min()
    denom = (high_max - low_min).replace(0, pd.NA)
    wr = -100 * (high_max - df["Close"]) / denom
    if wr.empty or pd.isna(wr.iloc[-1]):
        return None
    return round(float(wr.iloc[-1]), 1)


def compute_vwap(df: pd.DataFrame) -> float | None:
    """Volume Weighted Average Price (dihitung dari seluruh data yang tersedia di df)."""
    if df.empty or "Volume" not in df.columns:
        return None
    typical_price = (df["High"] + df["Low"] + df["Close"]) / 3
    vol = df["Volume"]
    if vol.sum() == 0:
        # Kalau tidak ada data volume (misal forex), VWAP tidak bisa dihitung secara wajar
        return None
    vwap = (typical_price * vol).cumsum() / vol.cumsum()
    val = vwap.iloc[-1]
    return round(float(val), 6) if not pd.isna(val) else None


def detect_candle_pattern(df: pd.DataFrame) -> str:
    """Klasifikasi candle terakhir secara sederhana: strong_bull / strong_bear / doji / netral."""
    if len(df) < 2:
        return "n/a"
    last = df.iloc[-1]
    o, h, l, c = last["Open"], last["High"], last["Low"], last["Close"]
    body = abs(c - o)
    rng = h - l
    if rng <= 0:
        return "n/a"
    body_ratio = body / rng
    if body_ratio >= 0.6 and c > o:
        return "strong_bull"
    if body_ratio >= 0.6 and c < o:
        return "strong_bear"
    if body_ratio <= 0.1:
        return "doji"
    return "bull" if c > o else "bear"


def check_volume_confirmation(df: pd.DataFrame, period: int = 20) -> bool | None:
    """True kalau volume candle terakhir di atas rata-rata (mengindikasikan minat pasar yang nyata)."""
    if "Volume" not in df.columns or df["Volume"].sum() == 0:
        return None  # data tidak punya volume (misal forex dari Twelve Data)
    if len(df) < period:
        return None
    avg_vol = df["Volume"].iloc[-period:-1].mean()
    last_vol = df["Volume"].iloc[-1]
    if avg_vol == 0:
        return None
    return bool(last_vol > avg_vol)


def detect_trend_simple(closes: list) -> str:
    """Klasifikasi tren sederhana berbasis slope beberapa candle terakhir: UP / DOWN / SIDEWAYS."""
    if len(closes) < 10:
        return "SIDEWAYS"
    recent = closes[-10:]
    start, end = recent[0], recent[-1]
    if start == 0:
        return "SIDEWAYS"
    pct_change = (end - start) / start * 100
    if pct_change >= 0.3:
        return "UP"
    if pct_change <= -0.3:
        return "DOWN"
    return "SIDEWAYS"


def find_swing_high_low(df: pd.DataFrame, lookback: int = 50) -> dict | None:
    """Cari swing high & swing low dalam N candle terakhir, untuk dasar perhitungan Fibonacci."""
    if df is None or df.empty or len(df) < 10:
        return None
    window = df.iloc[-lookback:] if len(df) >= lookback else df
    swing_high = float(window["High"].max())
    swing_low = float(window["Low"].min())
    high_idx = window["High"].idxmax()
    low_idx = window["Low"].idxmin()
    # Arah swing ditentukan dari mana yang terjadi lebih dulu: low->high (uptrend) atau high->low (downtrend)
    direction = "up" if low_idx < high_idx else "down"
    return {"high": swing_high, "low": swing_low, "direction": direction}


def compute_fibonacci_levels(df: pd.DataFrame, lookback: int = 50) -> dict | None:
    """Hitung level Fibonacci retracement (38.2/50/61.8%) dan extension (127.2/161.8%)
    berbasis swing high-low terakhir."""
    swing = find_swing_high_low(df, lookback)
    if not swing:
        return None
    high, low, direction = swing["high"], swing["low"], swing["direction"]
    diff = high - low
    if diff <= 0:
        return None

    if direction == "up":
        # Uptrend: retracement dihitung turun dari high, extension naik melampaui high
        retracement = {
            "23.6%": high - diff * 0.236,
            "38.2%": high - diff * 0.382,
            "50.0%": high - diff * 0.5,
            "61.8%": high - diff * 0.618,
            "78.6%": high - diff * 0.786,
        }
        extension = {
            "127.2%": high + diff * 0.272,
            "161.8%": high + diff * 0.618,
        }
    else:
        # Downtrend: retracement dihitung naik dari low, extension turun melampaui low
        retracement = {
            "23.6%": low + diff * 0.236,
            "38.2%": low + diff * 0.382,
            "50.0%": low + diff * 0.5,
            "61.8%": low + diff * 0.618,
            "78.6%": low + diff * 0.786,
        }
        extension = {
            "127.2%": low - diff * 0.272,
            "161.8%": low - diff * 0.618,
        }

    return {
        "swing_high": high, "swing_low": low, "direction": direction,
        "retracement": retracement, "extension": extension,
    }


def nearest_fib_level(price: float, fib: dict) -> tuple[str, float] | None:
    """Cari level Fibonacci (retracement/extension) yang paling dekat dengan harga saat ini."""
    if not fib:
        return None
    all_levels = {**fib["retracement"], **fib["extension"]}
    nearest_label = min(all_levels, key=lambda lbl: abs(all_levels[lbl] - price))
    return nearest_label, all_levels[nearest_label]


def is_trading_session_active() -> bool:
    """Cek apakah sesi trading aktif (sesi global 24/5 dianggap selalu aktif untuk crypto;
    untuk forex disederhanakan: True di luar weekend, dalam UTC)."""
    now_utc = datetime.utcnow()
    # Senin=0 ... Minggu=6. Forex tutup dari Jumat malam s.d. Minggu malam (disederhanakan: weekend = tutup)
    return now_utc.weekday() < 5


def full_technical_analysis(df: pd.DataFrame, htf_df: pd.DataFrame | None = None) -> dict:
    """Gabungan semua indikator menjadi satu paket analisis lengkap, mirip referensi sinyal trading."""
    if df is None or df.empty or "Close" not in df.columns:
        return {}
    closes = df["Close"].dropna().tolist()
    if len(closes) < 20:
        return {}

    current_price = closes[-1]
    rsi = compute_rsi(closes)
    htf_rsi = None
    if htf_df is not None and not htf_df.empty and "Close" in htf_df.columns:
        htf_closes = htf_df["Close"].dropna().tolist()
        if len(htf_closes) >= 15:
            htf_rsi = compute_rsi(htf_closes)

    adx = compute_adx(df)
    macd = compute_macd(closes)
    stoch = compute_stochastic(df)
    cci = compute_cci(df)
    wr = compute_williams_r(df)
    vwap = compute_vwap(df)
    candle = detect_candle_pattern(df)
    vol_confirm = check_volume_confirmation(df)
    bb = compute_bollinger(closes)
    sr = compute_support_resistance(closes)
    fib = compute_fibonacci_levels(df)
    trend_ltf = detect_trend_simple(closes)
    trend_htf = detect_trend_simple(htf_df["Close"].dropna().tolist()) if (htf_df is not None and not htf_df.empty and "Close" in htf_df.columns) else "SIDEWAYS"
    session_active = is_trading_session_active()

    return {
        "price": current_price, "rsi": rsi, "htf_rsi": htf_rsi, "adx": adx,
        "macd": macd, "stoch": stoch, "cci": cci, "wr": wr, "vwap": vwap,
        "candle": candle, "vol_confirm": vol_confirm, "bb": bb, "sr": sr, "fib": fib,
        "trend_ltf": trend_ltf, "trend_htf": trend_htf, "session_active": session_active,
    }


def score_signal_strength(analysis: dict) -> tuple[str, int]:
    """Hitung skor bullish/bearish dari paket analisis lengkap. Return (side, strength_percent)."""
    bull, bear, total = 0, 0, 0

    rsi = analysis.get("rsi")
    if rsi is not None:
        total += 1
        if rsi >= 55: bull += 1
        elif rsi <= 45: bear += 1

    adx = analysis.get("adx")
    if adx:
        total += 1
        if adx["trending"] and adx["plus_di"] > adx["minus_di"]: bull += 1
        elif adx["trending"] and adx["minus_di"] > adx["plus_di"]: bear += 1

    macd = analysis.get("macd")
    if macd:
        total += 1
        if macd["histogram"] > 0: bull += 1
        else: bear += 1

    stoch = analysis.get("stoch")
    if stoch:
        total += 1
        if stoch["k"] > stoch["d"]: bull += 1
        else: bear += 1

    cci = analysis.get("cci")
    if cci is not None:
        total += 1
        if cci > 0: bull += 1
        else: bear += 1

    vwap = analysis.get("vwap")
    if vwap:
        total += 1
        if analysis["price"] > vwap: bull += 1
        else: bear += 1

    fib = analysis.get("fib")
    if fib:
        price = analysis["price"]
        retr_618 = fib["retracement"].get("61.8%")
        retr_50 = fib["retracement"].get("50.0%")
        if retr_618 is not None and retr_50 is not None:
            total += 1
            # Golden zone (50%-61.8% retracement) adalah area pantulan klasik dalam analisis Fibonacci:
            # di swing uptrend, harga yang retrace ke zona ini & masih bertahan = sinyal lanjutan bullish;
            # di swing downtrend, sebaliknya = sinyal lanjutan bearish
            zone_low, zone_high = min(retr_618, retr_50), max(retr_618, retr_50)
            in_golden_zone = zone_low * 0.995 <= price <= zone_high * 1.005
            if in_golden_zone and fib["direction"] == "up":
                bull += 1
            elif in_golden_zone and fib["direction"] == "down":
                bear += 1

    candle = analysis.get("candle", "")
    if candle in ("strong_bull", "bull"):
        total += 1
        bull += 1
    elif candle in ("strong_bear", "bear"):
        total += 1
        bear += 1

    if analysis.get("trend_htf") == "UP":
        total += 1
        bull += 1
    elif analysis.get("trend_htf") == "DOWN":
        total += 1
        bear += 1

    if total == 0:
        return "neutral", 0
    side = "bull" if bull >= bear else "bear"
    strength_pct = round(max(bull, bear) / total * 100)
    return side, strength_pct


def build_trade_levels(analysis: dict, side: str, rr_ratio: float = 2.5) -> dict | None:
    """Hitung Entry, Stop Loss, dan Take Profit berdasar Bollinger Bands & support/resistance,
    dengan rasio risk:reward yang ditentukan."""
    price = analysis.get("price")
    bb = analysis.get("bb")
    sr = analysis.get("sr")
    if price is None:
        return None

    # SL diletakkan di sisi yang berlawanan dengan arah sinyal, mengacu ke BB / support-resistance
    if side == "bull":
        sl_candidate = None
        if bb: sl_candidate = bb["lower"]
        if sr and sr.get("support"): sl_candidate = max(sl_candidate or 0, sr["support"]) if sl_candidate else sr["support"]
        sl = sl_candidate if sl_candidate and sl_candidate < price else price * 0.994
        risk = price - sl
        tp = price + risk * rr_ratio
    else:
        sl_candidate = None
        if bb: sl_candidate = bb["upper"]
        if sr and sr.get("resistance"): sl_candidate = min(sl_candidate or float("inf"), sr["resistance"]) if sl_candidate else sr["resistance"]
        sl = sl_candidate if sl_candidate and sl_candidate > price else price * 1.006
        risk = sl - price
        tp = price - risk * rr_ratio

    if risk <= 0:
        return None

    pips_tp = abs(tp - price)
    pips_sl = abs(price - sl)
    return {
        "entry": round(price, 6), "tp": round(tp, 6), "sl": round(sl, 6),
        "pips_tp": round(pips_tp, 6), "pips_sl": round(pips_sl, 6),
        "rr_text": f"1:{rr_ratio:g}",
    }


def build_signal_narrative(analysis: dict, side: str) -> str:
    """Bikin narasi singkat alasan sinyal, mirip format 'HTF trend UP · ADX +DI>22 · ...'."""
    parts = []
    if analysis.get("trend_htf") in ("UP", "DOWN"):
        parts.append(f"HTF trend {analysis['trend_htf']}")
    adx = analysis.get("adx")
    if adx and adx["trending"]:
        leading_di = "+DI" if adx["plus_di"] > adx["minus_di"] else "-DI"
        parts.append(f"ADX {leading_di}>{max(adx['plus_di'], adx['minus_di']):.0f}")
    rsi = analysis.get("rsi")
    if rsi is not None:
        if side == "bull" and rsi > 50:
            parts.append("RSI rising")
        elif side == "bear" and rsi < 50:
            parts.append("RSI falling")
    macd = analysis.get("macd")
    if macd:
        if side == "bull" and macd["histogram"] > 0:
            parts.append("MACD bull xover+expand")
        elif side == "bear" and macd["histogram"] < 0:
            parts.append("MACD bear xover+expand")
    fib = analysis.get("fib")
    if fib:
        retr_618 = fib["retracement"].get("61.8%")
        retr_50 = fib["retracement"].get("50.0%")
        if retr_618 is not None and retr_50 is not None:
            price = analysis["price"]
            zone_low, zone_high = min(retr_618, retr_50), max(retr_618, retr_50)
            in_golden_zone = zone_low * 0.995 <= price <= zone_high * 1.005
            if in_golden_zone and (
                (fib["direction"] == "up" and side == "bull") or
                (fib["direction"] == "down" and side == "bear")
            ):
                parts.append("Price in Fib golden zone (50-61.8%)")
    bb = analysis.get("bb")
    if bb:
        if analysis["price"] >= bb["upper"]:
            parts.append("Price at BB upper")
        elif analysis["price"] <= bb["lower"]:
            parts.append("Price at BB lower")
    return " · ".join(parts) if parts else "Konfirmasi multi-indikator"


def format_full_signal_message(symbol_label: str, analysis: dict) -> str | None:
    """Format pesan sinyal lengkap, mirip tampilan referensi (Entry/TP/SL + semua indikator)."""
    if not analysis:
        return None
    side, strength = score_signal_strength(analysis)
    if strength < STRENGTH_THRESHOLD:
        return None  # sinyal terlalu lemah, tidak perlu dikirim

    levels = build_trade_levels(analysis, side)
    if not levels:
        return None

    arrow = "▲" if side == "bull" else "▼"
    label = "BUY" if side == "bull" else "SELL"
    emoji = "🟢" if side == "bull" else "🔴"

    rsi = analysis.get("rsi")
    htf_rsi = analysis.get("htf_rsi")
    adx = analysis.get("adx")
    macd = analysis.get("macd")
    stoch = analysis.get("stoch")
    cci = analysis.get("cci")
    wr = analysis.get("wr")
    vwap = analysis.get("vwap")
    price = analysis.get("price")
    candle = analysis.get("candle", "n/a")
    vol_confirm = analysis.get("vol_confirm")
    trend_ltf = analysis.get("trend_ltf", "SIDEWAYS")
    trend_htf = analysis.get("trend_htf", "SIDEWAYS")
    session_active = analysis.get("session_active", True)

    trend_icon = {"UP": "⬆️", "DOWN": "⬇️", "SIDEWAYS": "↔️"}
    vol_icon = "✅" if vol_confirm else ("❌" if vol_confirm is False else "⚪")
    vwap_above_icon = "✅" if (vwap and price and price > vwap) else ("❌" if vwap else "⚪")

    lines = [
        f"{emoji} *SINYAL {label} {arrow}*",
        f"{symbol_label} · RR {levels['rr_text']}",
        "━━━━━━━━━━━━━━━",
        f"💰 Entry: `{format_usd(levels['entry'])}`",
        f"🎯 TP: `{format_usd(levels['tp'])}` (+{levels['pips_tp']:.2f} pips)",
        f"🛑 SL: `{format_usd(levels['sl'])}` (-{levels['pips_sl']:.2f} pips)",
        "━━━━━━━━━━━━━━━",
        "📊 *Indikator*",
        f"• Strength: *{strength}%*",
        f"• RSI(14): `{rsi}`" + (f" | HTF RSI: `{htf_rsi}`" if htf_rsi is not None else ""),
    ]
    if adx:
        lines.append(f"• ADX: `{adx['adx']}` ({'✅ Trending' if adx['trending'] else '⚪ Ranging'})")
    if macd:
        lines.append(f"• MACD hist: `{macd['histogram']:+.4f}`")
    if stoch:
        lines.append(f"• Stoch K/D: `{stoch['k']}/{stoch['d']}`")
    if cci is not None:
        lines.append(f"• CCI: `{cci}`" + (f" | W%R: `{wr}`" if wr is not None else ""))
    if vwap:
        lines.append(f"• VWAP: `{format_usd(vwap)}` (Above {vwap_above_icon})")
    fib = analysis.get("fib")
    if fib:
        nearest = nearest_fib_level(price, fib)
        if nearest:
            label, level_price = nearest
            fib_type = "Ext" if "%" in label and float(label.rstrip("%")) > 100 else "Retr"
            lines.append(f"• Fibo {fib_type}: `{label}` → `{format_usd(level_price)}`")
    lines.append(f"• Candle: `{candle}`")
    lines.append(f"• Volume: {vol_icon} " + ("Confirmed" if vol_confirm else ("Lemah" if vol_confirm is False else "N/A")))
    lines.append("")
    lines.append(f"{trend_icon.get(trend_ltf,'↔️')} Trend LTF: {trend_ltf}")
    lines.append(f"{trend_icon.get(trend_htf,'↔️')} Trend HTF: {trend_htf}")
    lines.append(f"🕐 Session: {'✅ Active' if session_active else '⚪ Quiet'}")
    lines.append("")
    lines.append(f"💡 _{build_signal_narrative(analysis, side)}_")
    lines.append("")
    lines.append("⚠️ _Analisis teknikal otomatis, bukan jaminan profit. Selalu gunakan money management._")
    return "\n".join(lines)


def multi_timeframe_analysis(ticker: str) -> dict:
    # FIX: 4h diganti 2h, tambah fallback
    timeframes = {
        "1 Jam":  ("7d",  "1h"),
        "4 Jam":  ("60d", "2h"),
        "1 Hari": ("3mo", "1d"),
    }
    results = {}
    for label, (period, interval) in timeframes.items():
        df = get_ohlc_smart(ticker, period=period, interval=interval)
        if df.empty or "Close" not in df.columns or len(df) < 5:
            fallback = {
                "1 Jam":  ("5d",  "60m"),
                "4 Jam":  ("1mo", "1d"),
                "1 Hari": ("6mo", "1wk"),
            }
            fp, fi = fallback.get(label, ("1mo", "1d"))
            df = get_ohlc_smart(ticker, period=fp, interval=fi)
            if df.empty or len(df) < 5:
                results[label] = None
                continue
        closes = df["Close"].dropna().tolist()
        if len(closes) < 5:
            results[label] = None
            continue
        rsi = compute_rsi(closes)
        macd = compute_macd(closes)
        ma7 = compute_ma(closes, 7)
        ma25 = compute_ma(closes, 25)
        bb = compute_bollinger(closes)
        sr = compute_support_resistance(closes)
        results[label] = {
            "rsi": rsi, "macd": macd, "ma7": ma7, "ma25": ma25,
            "bb": bb, "sr": sr, "last_price": closes[-1] if closes else None,
        }
    return results


# ══════════════════════════════════════════════════════════
# SIGNAL DETECTION
# ══════════════════════════════════════════════════════════

def detect_signals(symbol: str, closes: list, current_price: float) -> list[dict]:
    signals = []
    prev = last_signals.setdefault(symbol, {})

    rsi  = compute_rsi(closes)
    ma7  = compute_ma(closes, 7)
    ma25 = compute_ma(closes, 25)
    macd = compute_macd(closes)
    bb   = compute_bollinger(closes)

    bull_score = bear_score = 0

    if rsi is not None:
        prev_zone = prev.get("rsi_zone", "neutral")
        zone = "overbought" if rsi >= 70 else ("oversold" if rsi <= 30 else "neutral")
        if zone == "overbought" and prev_zone != "overbought":
            bear_score += 2
            signals.append({"type": "RSI Overbought 🔴", "detail": f"RSI: `{rsi}` — Potensi JUAL", "strength": "KUAT", "side": "bear"})
        elif zone == "oversold" and prev_zone != "oversold":
            bull_score += 2
            signals.append({"type": "RSI Oversold 🟢", "detail": f"RSI: `{rsi}` — Potensi BELI", "strength": "KUAT", "side": "bull"})
        prev["rsi_zone"] = zone

    if macd:
        prev_hist = prev.get("macd_hist", 0)
        curr_hist = macd["histogram"]
        if prev_hist <= 0 and curr_hist > 0:
            bull_score += 2
            signals.append({"type": "MACD Golden Cross 🚀", "detail": f"MACD: `{macd['macd']}` potong Signal ke atas\n💡 Sinyal BELI", "strength": "KUAT", "side": "bull"})
        elif prev_hist >= 0 and curr_hist < 0:
            bear_score += 2
            signals.append({"type": "MACD Death Cross 💀", "detail": f"MACD: `{macd['macd']}` potong Signal ke bawah\n⚠️ Sinyal JUAL", "strength": "KUAT", "side": "bear"})
        prev["macd_hist"] = curr_hist

    if bb:
        prev_zone = prev.get("bb_zone", "mid")
        if current_price >= bb["upper"] and prev_zone != "upper":
            bear_score += 1
            signals.append({"type": "BB Upper Touch 📈", "detail": f"Harga sentuh BB atas: `{format_usd(bb['upper'])}`\n⚠️ Resistance", "strength": "SEDANG", "side": "bear"})
            prev["bb_zone"] = "upper"
        elif current_price <= bb["lower"] and prev_zone != "lower":
            bull_score += 1
            signals.append({"type": "BB Lower Touch 📉", "detail": f"Harga sentuh BB bawah: `{format_usd(bb['lower'])}`\n💡 Support / Bouncing", "strength": "SEDANG", "side": "bull"})
            prev["bb_zone"] = "lower"
        else:
            prev["bb_zone"] = "mid"

    if ma7 and ma25:
        prev_cross = prev.get("ma_cross")
        curr_cross = "bull" if ma7 > ma25 else "bear"
        if prev_cross and curr_cross != prev_cross:
            if curr_cross == "bull":
                bull_score += 2
                signals.append({"type": "MA Golden Cross 🌟", "detail": f"MA7 > MA25\nMA7: `{format_usd(ma7)}` | MA25: `{format_usd(ma25)}`\n💡 Tren Bullish", "strength": "KUAT", "side": "bull"})
            else:
                bear_score += 2
                signals.append({"type": "MA Death Cross ☠️", "detail": f"MA7 < MA25\n⚠️ Tren Bearish", "strength": "KUAT", "side": "bear"})
        prev["ma_cross"] = curr_cross

    if len(closes) >= 2 and closes[-2] > 0:
        chg = ((current_price - closes[-2]) / closes[-2]) * 100
        if abs(chg) >= 1.0:
            arrow = "🔺" if chg > 0 else "🔻"
            signals.append({"type": f"Price Spike {arrow}", "detail": f"Spike: `{chg:+.2f}%`\n`{format_usd(closes[-2])}` → `{format_usd(current_price)}`", "strength": "INFO", "side": "bull" if chg > 0 else "bear"})

    total = bull_score + bear_score
    for s in signals:
        if s["side"] == "bull":
            s["confirm"] = f"✅ Konfirmasi: {bull_score}/{total if total else 1} indikator bullish"
        else:
            s["confirm"] = f"⚠️ Konfirmasi: {bear_score}/{total if total else 1} indikator bearish"

    last_signals[symbol] = prev
    return signals


# ══════════════════════════════════════════════════════════
# CHART GENERATOR
# ══════════════════════════════════════════════════════════

def generate_chart(ticker: str, name: str, period: str = "1mo", interval: str = "1d") -> io.BytesIO | None:
    try:
        df = get_ohlc_smart(ticker, period=period, interval=interval)
        if df.empty:
            print(f"[WARN] Chart {ticker}: DataFrame kosong")
            return None
        if len(df) < 5:
            print(f"[WARN] Chart {ticker}: Data terlalu sedikit ({len(df)} rows)")
            return None
        if "Close" not in df.columns or df["Close"].isna().all():
            print(f"[WARN] Chart {ticker}: Kolom Close tidak valid")
            return None

        df.index = pd.to_datetime(df.index)
        for col in ["Open", "High", "Low", "Close", "Volume"]:
            if col not in df.columns:
                df[col] = 0.0
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        closes = df["Close"].tolist()
        ma7  = [compute_ma(closes[:i+1], 7)  for i in range(len(closes))]
        ma25 = [compute_ma(closes[:i+1], 25) for i in range(len(closes))]

        bb_upper = bb_lower = [None] * len(closes)
        if len(closes) >= 20:
            bb_upper = [None] * 19
            bb_lower = [None] * 19
            for i in range(19, len(closes)):
                bb = compute_bollinger(closes[:i+1])
                if bb:
                    bb_upper.append(bb["upper"])
                    bb_lower.append(bb["lower"])
                else:
                    bb_upper.append(None)
                    bb_lower.append(None)

        rsi_vals = [None] * len(closes)
        for i in range(14, len(closes)):
            rsi_vals[i] = compute_rsi(closes[:i+1])

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8),
                                        gridspec_kw={"height_ratios": [3, 1]},
                                        facecolor="#1a1a2e")
        ax1.set_facecolor("#1a1a2e")
        ax2.set_facecolor("#1a1a2e")

        dates = [d.strftime("%d/%m") for d in df.index]

        for i, (_, row) in enumerate(df.iterrows()):
            o, h, l, c = row["Open"], row["High"], row["Low"], row["Close"]
            color = "#26a69a" if c >= o else "#ef5350"
            ax1.plot([i, i], [l, h], color=color, linewidth=0.8)
            ax1.add_patch(mpatches.FancyBboxPatch(
                (i - 0.3, min(o, c)), 0.6, abs(c - o),
                boxstyle="square,pad=0", facecolor=color, edgecolor=color
            ))

        valid_x7  = [i for i, v in enumerate(ma7)  if v is not None]
        valid_x25 = [i for i, v in enumerate(ma25) if v is not None]
        valid_bb_u = [(i, v) for i, v in enumerate(bb_upper) if v is not None]
        valid_bb_l = [(i, v) for i, v in enumerate(bb_lower) if v is not None]

        if valid_x7:
            ax1.plot(valid_x7, [ma7[i] for i in valid_x7], color="#f39c12", linewidth=1.2, label="MA7")
        if valid_x25:
            ax1.plot(valid_x25, [ma25[i] for i in valid_x25], color="#3498db", linewidth=1.2, label="MA25")
        if valid_bb_u and valid_bb_l:
            ax1.plot([v[0] for v in valid_bb_u], [v[1] for v in valid_bb_u], color="#9b59b6", linewidth=0.8, linestyle="--", label="BB")
            ax1.plot([v[0] for v in valid_bb_l], [v[1] for v in valid_bb_l], color="#9b59b6", linewidth=0.8, linestyle="--")

        valid_rsi = [(i, v) for i, v in enumerate(rsi_vals) if v is not None]
        if valid_rsi:
            ax2.plot([v[0] for v in valid_rsi], [v[1] for v in valid_rsi], color="#e74c3c", linewidth=1.2)
            ax2.axhline(70, color="#ef5350", linewidth=0.6, linestyle="--")
            ax2.axhline(30, color="#26a69a", linewidth=0.6, linestyle="--")
            ax2.fill_between([v[0] for v in valid_rsi], 70, [v[1] for v in valid_rsi],
                             where=[v[1] >= 70 for v in valid_rsi], alpha=0.3, color="#ef5350")
            ax2.fill_between([v[0] for v in valid_rsi], 30, [v[1] for v in valid_rsi],
                             where=[v[1] <= 30 for v in valid_rsi], alpha=0.3, color="#26a69a")
        ax2.set_ylim(0, 100)
        ax2.set_ylabel("RSI", color="white", fontsize=9)

        tick_step = max(1, len(dates) // 8)
        tick_x = list(range(0, len(dates), tick_step))
        ax1.set_xticks(tick_x)
        ax1.set_xticklabels([dates[i] for i in tick_x], color="white", fontsize=8)
        ax2.set_xticks(tick_x)
        ax2.set_xticklabels([dates[i] for i in tick_x], color="white", fontsize=8)

        for ax in [ax1, ax2]:
            ax.tick_params(colors="white")
            ax.spines["bottom"].set_color("#444")
            ax.spines["top"].set_color("#444")
            ax.spines["left"].set_color("#444")
            ax.spines["right"].set_color("#444")
            ax.yaxis.label.set_color("white")

        ax1.tick_params(axis="y", colors="white")
        ax1.set_title(f"{name} — {period} ({interval})", color="white", fontsize=13, fontweight="bold", pad=10)
        ax1.legend(loc="upper left", facecolor="#2d2d44", labelcolor="white", fontsize=8)
        ax1.grid(color="#2d2d44", linewidth=0.5)
        ax2.grid(color="#2d2d44", linewidth=0.5)

        plt.tight_layout(pad=1.5)
        buf = io.BytesIO()
        plt.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor="#1a1a2e")
        plt.close(fig)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"[ERROR] generate_chart {ticker}: {e}")
        return None


# ══════════════════════════════════════════════════════════
# FORMATTERS
# ══════════════════════════════════════════════════════════

def escape_markdown_v2(text: str) -> str:
    """Escape karakter spesial MarkdownV2 supaya Telegram tidak menolak pesan."""
    special_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special_chars else c for c in text)


def format_idr(n: float) -> str:
    return f"Rp {n:,.0f}"

def format_usd(n: float) -> str:
    return f"${n:,.2f}" if n >= 1 else f"${n:.6f}"

def change_emoji(pct: float) -> str:
    if pct >= 5:  return "🚀"
    if pct >= 2:  return "📈"
    if pct > 0:   return "🟢"
    if pct >= -2: return "🔴"
    if pct >= -5: return "📉"
    return "💥"

def rsi_label(rsi) -> str:
    if rsi is None: return "⚪ N/A"
    if rsi >= 70:   return f"🔴 {rsi} Overbought"
    if rsi <= 30:   return f"🟢 {rsi} Oversold"
    return f"🟡 {rsi} Netral"

def macd_label(macd) -> str:
    if not macd: return "⚪ N/A"
    return f"🟢 Bullish" if macd["histogram"] > 0 else "🔴 Bearish"


# ══════════════════════════════════════════════════════════
# ALERT ENGINE
# ══════════════════════════════════════════════════════════

_last_signal_alert: dict = {}  # {symbol: {"side": "bull"/"bear", "timestamp": datetime}}
SIGNAL_ALERT_COOLDOWN_MINUTES = 30  # jangan kirim sinyal sama berulang dalam waktu < 30 menit


async def check_full_signal_alert(app: Application, name: str, fallback_price: float | None, display_name: str | None = None):
    """Cek sinyal teknikal lengkap (ADX/Stoch/CCI/VWAP/Fibonacci) untuk satu coin/forex,
    dan kirim alert otomatis kalau sinyal kuat & belum dikirim baru-baru ini (cooldown)."""
    label = display_name or name.upper()
    try:
        ticker = resolve_ticker(name)
        ltf_df = get_ohlc_smart(ticker, period="5d", interval="1h")
        if ltf_df.empty or len(ltf_df) < 20:
            return  # data tidak cukup, skip diam-diam (sudah ada error log di get_ohlc_smart kalau gagal)
        htf_df = get_ohlc_smart(ticker, period="3mo", interval="1d")

        analysis = full_technical_analysis(ltf_df, htf_df)
        if not analysis:
            return
        side, strength = score_signal_strength(analysis)
        if strength < STRENGTH_THRESHOLD:
            return

        # FIX: cooldown — jangan kirim sinyal arah yang sama berulang-ulang tiap 5 menit
        now = datetime.now()
        last = _last_signal_alert.get(name)
        if last and last["side"] == side and (now - last["timestamp"]).total_seconds() < SIGNAL_ALERT_COOLDOWN_MINUTES * 60:
            return

        msg = format_full_signal_message(label, analysis)
        if not msg:
            return

        _last_signal_alert[name] = {"side": side, "timestamp": now}
        for chat_id in CHAT_IDS:
            try:
                await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
            except Exception as e:
                print(f"[ERROR] send full signal alert {name}: {e}")
    except Exception as e:
        print(f"[ERROR] check_full_signal_alert {name}: {e}")


async def check_and_alert(app: Application):
    if not alerts_enabled:
        return

    data = get_crypto_prices(COINS)
    for coin_id, info in data.items():
        price  = info.get("usd", 0)
        change = info.get("usd_24h_change", 0)
        history = price_history.setdefault(coin_id, [])
        history.append(price)
        if len(history) > 300:
            history.pop(0)

        # FIX: alert otomatis sekarang pakai analisis teknikal LENGKAP (ADX, Stoch, CCI,
        # VWAP, Fibonacci, Entry/TP/SL) yang sama dengan command /sinyal — bukan lagi
        # detect_signals() versi sederhana yang lama.
        await check_full_signal_alert(app, coin_id, price)

        if abs(change) >= ALERT_THRESHOLD_VAR:
            arrow = "🔺" if change > 0 else "🔻"
            msg = (
                f"{change_emoji(change)} *ALERT: {coin_id.upper()}*\n"
                f"{arrow} 24j: `{change:+.2f}%`\n"
                f"💵 `{format_usd(price)}`\n"
                f"🕐 {datetime.now().strftime('%d/%m %H:%M')}"
            )
            for chat_id in CHAT_IDS:
                try:
                    await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                except Exception as e:
                    print(f"[ERROR] alert: {e}")

    # Forex spike
    forex = get_forex_prices()
    for symbol, info in forex.items():
        price = info.get("price", 0)
        if not price:
            continue
        h = price_history.setdefault(symbol, [])
        h.append(price)
        if len(h) > 300:
            h.pop(0)
        if len(h) >= 2 and h[-2] > 0:
            chg = ((price - h[-2]) / h[-2]) * 100
            if abs(chg) >= 0.3:
                arrow = "🔺" if chg > 0 else "🔻"
                msg = (
                    f"⚡ *SPIKE {symbol}*\n"
                    f"{arrow} `{chg:+.3f}%`\n"
                    f"💵 `{format_usd(price)}`\n"
                    f"🕐 {datetime.now().strftime('%d/%m %H:%M')}"
                )
                for chat_id in CHAT_IDS:
                    try:
                        await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    except Exception as e:
                        print(f"[ERROR] forex alert: {e}")

    # Cek sinyal teknikal lengkap untuk XAU/USD juga (selain crypto)
    await check_full_signal_alert(app, "xauusd", None, display_name="XAU/USD")

    # Cek price targets
    for coin, target in list(watchlist_alerts.items()):
        price = data.get(coin, {}).get("usd", 0)
        if price and price >= target:
            msg = (
                f"🎯 *TARGET TERCAPAI!*\n"
                f"*{coin.upper()}* menyentuh `{format_usd(price)}`\n"
                f"Target: `{format_usd(target)}`\n"
                f"🕐 {datetime.now().strftime('%d/%m %H:%M')}"
            )
            for chat_id in CHAT_IDS:
                try:
                    await app.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                except Exception as e:
                    print(f"[ERROR] target alert: {e}")
            del watchlist_alerts[coin]


# ══════════════════════════════════════════════════════════
# MENU TAMBAHAN
# ══════════════════════════════════════════════════════════

async def cmd_summary(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    data = get_crypto_prices(COINS)
    forex = get_forex_prices()
    best  = max(data.items(), key=lambda x: x[1].get("usd_24h_change", 0))
    worst = min(data.items(), key=lambda x: x[1].get("usd_24h_change", 0))
    lines = [
        f"📋 *RINGKASAN HARIAN*\n{datetime.now().strftime('%d %B %Y %H:%M')}\n━━━━━━━━━━━━━━━",
        f"🏆 Top Gainer: *{best[0].upper()}* `{best[1].get('usd_24h_change',0):+.2f}%`",
        f"💥 Top Loser:  *{worst[0].upper()}* `{worst[1].get('usd_24h_change',0):+.2f}%`\n",
        "*Semua Coin:*",
    ]
    for coin, info in data.items():
        chg = info.get("usd_24h_change", 0)
        lines.append(f"{change_emoji(chg)} {coin.upper()}: `{format_usd(info.get('usd',0))}` (`{chg:+.2f}%`)")
    lines.append("\n*Forex & Komoditas:*")
    for sym, info in forex.items():
        lines.append(f"💱 {sym}: `{format_usd(info.get('price',0))}`")
    lines.append("\n⚠️ _Bukan saran investasi._")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_porto(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if args and args[0] == "add" and len(args) >= 4:
        coin = args[1].lower()
        portfolio[coin] = {"qty": float(args[2]), "buy_price": float(args[3])}
        await update.message.reply_text(
            f"✅ *{coin.upper()}* x{args[2]} @ `${float(args[3]):,.2f}` ditambahkan.", parse_mode="Markdown"
        )
        return
    if args and args[0] == "del" and len(args) >= 2:
        portfolio.pop(args[1].lower(), None)
        await update.message.reply_text(f"🗑️ {args[1].upper()} dihapus dari portfolio.")
        return
    if not portfolio:
        await update.message.reply_text(
            "📭 Portfolio kosong.\nTambah: `/porto add bitcoin 0.5 45000`", parse_mode="Markdown"
        )
        return
    data = get_crypto_prices(list(portfolio.keys()))
    lines = ["💼 *PORTFOLIO TRACKER*\n━━━━━━━━━━━━━━━\n"]
    total_invest = total_now = 0
    for coin, pos in portfolio.items():
        current = data.get(coin, {}).get("usd", 0)
        qty, buy = pos["qty"], pos["buy_price"]
        invest = qty * buy
        now_val = qty * current
        pnl = now_val - invest
        pnl_pct = ((current - buy) / buy * 100) if buy > 0 else 0
        total_invest += invest
        total_now += now_val
        arrow = "🟢" if pnl >= 0 else "🔴"
        lines.append(
            f"{arrow} *{coin.upper()}* x{qty}\n"
            f"   Beli: `${buy:,.2f}` → Kini: `{format_usd(current)}`\n"
            f"   P&L: `{'+' if pnl>=0 else ''}{format_usd(pnl)}` (`{pnl_pct:+.2f}%`)\n"
        )
    total_pnl = total_now - total_invest
    total_pct = ((total_pnl / total_invest) * 100) if total_invest > 0 else 0
    lines.append("━━━━━━━━━━━━━━━")
    lines.append(f"{'📈' if total_pnl>=0 else '📉'} *Total P&L: {'+' if total_pnl>=0 else ''}{format_usd(total_pnl)} ({total_pct:+.2f}%)*")
    lines.append(f"💰 Modal: `{format_usd(total_invest)}` → Nilai: `{format_usd(total_now)}`")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")


async def cmd_target(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text(
            "🎯 *Price Target Alert*\n\n"
            "`/target bitcoin 70000` — alert jika BTC ≥ $70k\n"
            "`/target list` — lihat semua target\n"
            "`/target del bitcoin` — hapus target",
            parse_mode="Markdown"
        )
        return
    if args[0] == "list":
        if not watchlist_alerts:
            await update.message.reply_text("📭 Belum ada target.")
            return
        lines = ["🎯 *Target Price Alert:*\n"] + [f"  • {c.upper()}: `${t:,.2f}`" for c, t in watchlist_alerts.items()]
        await update.message.reply_text("\n".join(lines), parse_mode="Markdown")
        return
    if args[0] == "del" and len(args) >= 2:
        watchlist_alerts.pop(args[1].lower(), None)
        await update.message.reply_text(f"🗑️ Target {args[1].upper()} dihapus.")
        return
    if len(args) >= 2:
        try:
            coin, target = args[0].lower(), float(args[1])
            watchlist_alerts[coin] = target
            await update.message.reply_text(
                f"✅ Target: *{coin.upper()}* = `${target:,.2f}`\nBot notif otomatis saat tercapai!",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Format: `/target bitcoin 70000`", parse_mode="Markdown")


async def cmd_fng(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=7", timeout=10)
        data = r.json()["data"]
        today = data[0]
        val = int(today["value"])
        label = today["value_classification"]
        if val >= 75:   mood = "🤑"
        elif val >= 55: mood = "😊"
        elif val >= 45: mood = "😐"
        elif val >= 25: mood = "😨"
        else:           mood = "😱"
        bar = "█" * (val // 10) + "░" * (10 - val // 10)
        history = "\n".join(
            f"  `{d['timestamp'][:10]}`: `{d['value']}` — {d['value_classification']}"
            for d in data[1:6]
        )
        msg = (
            f"{mood} *FEAR & GREED INDEX*\n━━━━━━━━━━━━━━━\n"
            f"Hari ini: *{val}/100 — {label}*\n"
            f"`[{bar}]`\n\n"
            f"📅 *5 Hari Terakhir:*\n{history}\n\n"
            f"💡 _<25=Extreme Fear | >75=Extreme Greed_"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as e:
        await update.message.reply_text(f"❌ Gagal ambil F&G Index: {e}")


# ══════════════════════════════════════════════════════════
# COMMANDS
# ══════════════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    kb = [
        [InlineKeyboardButton("📊 Crypto",      callback_data="prices"),
         InlineKeyboardButton("💱 Forex/XAU",   callback_data="forex")],
        [InlineKeyboardButton("📈 Analisis",     callback_data="analyze_menu"),
         InlineKeyboardButton("🕯️ Chart",        callback_data="chart_menu")],
        [InlineKeyboardButton("🎯 Sinyal Lengkap", callback_data="signal_menu"),
         InlineKeyboardButton("🌐 Multi TF",     callback_data="mtf_menu")],
        [InlineKeyboardButton("📰 Berita",       callback_data="news"),
         InlineKeyboardButton("💼 Portfolio",    callback_data="porto")],
        [InlineKeyboardButton("😱 Fear&Greed",   callback_data="fng"),
         InlineKeyboardButton("📋 Ringkasan",    callback_data="summary")],
        [InlineKeyboardButton("🎯 Target Price", callback_data="target_list"),
         InlineKeyboardButton("🔔 Alert ON/OFF", callback_data="toggle_alert")],
        [InlineKeyboardButton("ℹ️ Bantuan",      callback_data="help")],
    ]
    await update.message.reply_text(
        "👋 *KriptoMeter Bot* 📡\n\n"
        "Fitur:\n"
        "• Harga crypto, XAU/USD & forex real-time\n"
        "• Sinyal teknikal otomatis (RSI, MACD, BB, MA)\n"
        "• Chart candlestick + indikator\n"
        "• Analisis multi timeframe (1j, 2j, 1h)\n"
        "• Berita crypto terbaru\n"
        "• Portfolio tracker & price target alert\n\n"
        "Pilih menu:",
        reply_markup=InlineKeyboardMarkup(kb),
        parse_mode="Markdown",
    )


async def cmd_harga(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await send_prices(update.message.reply_text)


async def send_prices(reply_fn):
    data = get_crypto_prices(COINS)
    if not data:
        await reply_fn("❌ Gagal mengambil data.")
        return
    lines = [f"📊 *Harga Crypto* — {datetime.now().strftime('%d/%m %H:%M')}\n"]
    for coin_id, info in data.items():
        usd = info.get("usd", 0)
        idr = info.get("idr", 0)
        chg = info.get("usd_24h_change", 0)
        vol = info.get("usd_24h_vol", 0)
        lines.append(
            f"{change_emoji(chg)} *{coin_id.upper()}*\n"
            f"   💵 {format_usd(usd)}  🇮🇩 {format_idr(idr)}\n"
            f"   24j: `{chg:+.2f}%`  Vol: `${vol/1e6:.1f}M`\n"
        )
    await reply_fn("\n".join(lines), parse_mode="Markdown")


async def cmd_forex(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await send_forex(update.message.reply_text)


async def send_forex(reply_fn):
    data = get_forex_prices()
    if not data:
        await reply_fn("❌ Gagal mengambil data forex.")
        return
    icons = {"XAU/USD": "🥇", "XAG/USD": "🥈", "USD/IDR": "🇮🇩",
             "EUR/USD": "🇪🇺", "GBP/USD": "🇬🇧", "USD/JPY": "🇯🇵"}
    lines = [f"💱 *Forex & Komoditas* — {datetime.now().strftime('%d/%m %H:%M')}\n"]
    for symbol, info in data.items():
        price = info.get("price", 0)
        if not price:
            continue
        icon = icons.get(symbol, "💱")
        lines.append(f"{icon} *{symbol}*: `{format_usd(price)}`")
    await reply_fn("\n".join(lines), parse_mode="Markdown")


async def cmd_analisis(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    coin = ctx.args[0].lower() if ctx.args else "bitcoin"
    await send_analysis(update.message.reply_text, coin)


async def send_analysis(reply_fn, coin: str):
    await reply_fn(f"🔍 Menganalisis *{coin.upper()}*...", parse_mode="Markdown")
    ohlc = get_ohlc_coingecko(coin, days=30)
    if not ohlc:
        await reply_fn("❌ Data tidak tersedia.")
        return
    closes = [c[4] for c in ohlc]
    price = closes[-1]
    rsi   = compute_rsi(closes)
    ma7   = compute_ma(closes, 7)
    ma25  = compute_ma(closes, 25)
    macd  = compute_macd(closes)
    bb    = compute_bollinger(closes)
    sr    = compute_support_resistance(closes)

    trend = "📈 Bullish" if (ma7 and ma25 and ma7 > ma25) else "📉 Bearish"
    bb_sig = ""
    if bb:
        if price >= bb["upper"]:   bb_sig = "🔴 Di atas BB (overbought)"
        elif price <= bb["lower"]: bb_sig = "🟢 Di bawah BB (oversold)"
        else:                      bb_sig = "🟡 Dalam range normal"

    msg = (
        f"📊 *Analisis {coin.upper()}*\n"
        f"━━━━━━━━━━━━━━━\n"
        f"💵 Harga: `{format_usd(price)}`\n\n"
        f"*MA*\n"
        f"  MA7: `{format_usd(ma7) if ma7 else 'N/A'}` | MA25: `{format_usd(ma25) if ma25 else 'N/A'}`\n"
        f"  Tren: {trend}\n\n"
        f"*RSI (14)*: {rsi_label(rsi)}\n\n"
        f"*MACD*: {macd_label(macd)}\n"
        + (f"  `{macd['macd']}` | Signal: `{macd['signal']}` | Hist: `{macd['histogram']}`\n\n" if macd else "\n") +
        f"*Bollinger Bands*\n"
        + (f"  Upper: `{format_usd(bb['upper'])}` | Lower: `{format_usd(bb['lower'])}`\n  {bb_sig}\n\n" if bb else "  N/A\n\n") +
        f"*Support & Resistance*\n"
        + (f"  🟢 Support: `{format_usd(sr['support'])}`\n  🔴 Resistance: `{format_usd(sr['resistance'])}`\n\n" if sr else "  N/A\n\n") +
        f"⚠️ _Bukan saran investasi. DYOR!_"
    )
    await reply_fn(msg, parse_mode="Markdown")


def resolve_ticker(name: str) -> str:
    """Konversi nama yang diketik user (misal 'bitcoin', 'xauusd') jadi ticker internal (misal 'BTC-USD', 'GC=F')."""
    name = name.lower()
    ticker = YAHOO_MAP.get(name)
    if ticker:
        return ticker
    fx_map = {"xauusd": "GC=F", "xagusd": "SI=F", "eurusd": "EURUSD=X",
              "gbpusd": "GBPUSD=X", "usdjpy": "JPY=X", "usdidr": "IDR=X"}
    return fx_map.get(name, name.upper() + "-USD")


async def cmd_chart(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    args = ctx.args
    if not args:
        await update.message.reply_text("Contoh: `/chart bitcoin` atau `/chart XAUUSD`", parse_mode="Markdown")
        return
    name = args[0].lower()
    period = args[1] if len(args) > 1 else "1mo"
    interval = args[2] if len(args) > 2 else "1d"
    await send_chart(update.message, name, period, interval)


async def send_chart(message, name: str, period: str = "1mo", interval: str = "1d"):
    ticker = resolve_ticker(name)

    await message.reply_text(f"📊 Membuat chart *{name.upper()}* ({period}, {interval})...", parse_mode="Markdown")
    buf = generate_chart(ticker, name.upper(), period, interval)
    if buf:
        caption = f"📈 *{name.upper()}* — {period} ({interval})\nMA7 🟡 | MA25 🔵 | BB 🟣 | RSI bawah"
        await message.reply_photo(photo=buf, caption=caption, parse_mode="Markdown")
    else:
        await message.reply_text("❌ Gagal membuat chart. Coba lagi atau cek nama ticker.")


async def cmd_sinyal(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    name = ctx.args[0].lower() if ctx.args else "bitcoin"
    await send_full_signal(update.message.reply_text, name)


async def send_full_signal(reply_fn, name: str):
    """Command /sinyal — analisis teknikal lengkap (ADX, Stoch, CCI, VWAP, dll) + Entry/TP/SL."""
    await reply_fn(f"🔍 Menganalisis *{name.upper()}* secara lengkap...", parse_mode="Markdown")
    ticker = resolve_ticker(name)

    # LTF = timeframe utama untuk entry, HTF = timeframe lebih besar untuk konfirmasi tren
    ltf_df = get_ohlc_smart(ticker, period="5d", interval="1h")
    if ltf_df.empty or len(ltf_df) < 20:
        ltf_df = get_ohlc_smart(ticker, period="1mo", interval="1d")
    htf_df = get_ohlc_smart(ticker, period="3mo", interval="1d")

    if ltf_df.empty or len(ltf_df) < 20:
        await reply_fn("❌ Data tidak cukup untuk analisis lengkap. Coba coin/simbol lain.")
        return

    analysis = full_technical_analysis(ltf_df, htf_df)
    if not analysis:
        await reply_fn("❌ Gagal menghitung indikator. Coba lagi.")
        return

    msg = format_full_signal_message(name.upper(), analysis)
    if not msg:
        side, strength = score_signal_strength(analysis)
        await reply_fn(
            f"⚪ *{name.upper()}*: sinyal belum cukup kuat saat ini (strength {strength}%).\n"
            f"Tunggu konfirmasi lebih lanjut atau cek pair lain.",
            parse_mode="Markdown",
        )
        return
    await reply_fn(msg, parse_mode="Markdown")


async def cmd_mtf(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    coin = ctx.args[0].lower() if ctx.args else "bitcoin"
    await send_mtf(update.message.reply_text, coin)


async def send_mtf(reply_fn, coin: str):
    await reply_fn(f"🌐 Analisis multi timeframe *{coin.upper()}*...", parse_mode="Markdown")
    ticker = YAHOO_MAP.get(coin)
    if not ticker:
        fx_map = {"xauusd": "GC=F", "eurusd": "EURUSD=X", "gbpusd": "GBPUSD=X"}
        ticker = fx_map.get(coin, coin.upper() + "-USD")

    results = multi_timeframe_analysis(ticker)
    lines = [f"🌐 *Multi Timeframe: {coin.upper()}*\n━━━━━━━━━━━━━━━\n"]
    for tf, data in results.items():
        if not data:
            lines.append(f"*{tf}*: ⚪ Data tidak tersedia\n")
            continue
        rsi  = data["rsi"]
        macd = data["macd"]
        ma7  = data["ma7"]
        ma25 = data["ma25"]
        trend = "📈 Bull" if (ma7 and ma25 and ma7 > ma25) else "📉 Bear"
        lines.append(
            f"*⏱ {tf}*\n"
            f"  RSI: {rsi_label(rsi)}\n"
            f"  MACD: {macd_label(macd)}\n"
            f"  Tren: {trend}\n"
        )

    bull = sum(1 for d in results.values() if d and d["ma7"] and d["ma25"] and d["ma7"] > d["ma25"])
    bear = len(results) - bull
    if bull > bear:
        kesimpulan = f"✅ *Mayoritas BULLISH* ({bull}/{len(results)} TF)"
    elif bear > bull:
        kesimpulan = f"⚠️ *Mayoritas BEARISH* ({bear}/{len(results)} TF)"
    else:
        kesimpulan = "🟡 *Mixed Signal* — tunggu konfirmasi"

    lines.append(f"\n📌 Kesimpulan: {kesimpulan}")
    lines.append("\n⚠️ _Bukan saran investasi. DYOR!_")
    await reply_fn("\n".join(lines), parse_mode="Markdown")


async def cmd_news(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await send_news(update.message.reply_text)


async def send_news(reply_fn):
    await reply_fn("📰 Mengambil berita terbaru...")
    try:
        # FIX: get_crypto_news() adalah fungsi blocking (network I/O),
        # jalankan di thread terpisah supaya bot tidak freeze menunggu RSS feed
        loop = asyncio.get_event_loop()
        news = await loop.run_in_executor(None, get_crypto_news, 5)
    except Exception as e:
        print(f"[ERROR] send_news: {e}")
        await reply_fn(f"❌ Gagal mengambil berita: {e}")
        return
    if not news:
        await reply_fn("❌ Gagal mengambil berita. Semua sumber RSS tidak merespons, coba lagi nanti.")
        return
    lines = [f"📰 *Berita Crypto Terbaru*\n━━━━━━━━━━━━━━━\n"]
    for i, n in enumerate(news, 1):
        title = escape_markdown_v2(n["title"])
        source = escape_markdown_v2(n["source"])
        link = n["link"].replace(")", "\\)")  # escape ')' di URL agar tidak merusak format link
        lines.append(f"{i}\\. [{title}]({link})\n   _— {source}_\n")
    await reply_fn("\n".join(lines), parse_mode="MarkdownV2", disable_web_page_preview=True)


async def cmd_setalert(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global ALERT_THRESHOLD_VAR
    if ctx.args:
        try:
            ALERT_THRESHOLD_VAR = float(ctx.args[0])
            await update.message.reply_text(f"✅ Threshold: `{ALERT_THRESHOLD_VAR}%`", parse_mode="Markdown")
        except ValueError:
            await update.message.reply_text("❌ Contoh: `/setalert 5`", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"ℹ️ Threshold: `{ALERT_THRESHOLD_VAR}%`", parse_mode="Markdown")


async def cmd_toggle(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global alerts_enabled
    alerts_enabled = not alerts_enabled
    await update.message.reply_text(
        f"Alert: *{'🔔 AKTIF' if alerts_enabled else '🔕 NONAKTIF'}*", parse_mode="Markdown"
    )


async def cmd_tambah(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Contoh: `/tambahcoin solana`", parse_mode="Markdown")
        return
    coin = ctx.args[0].lower()
    if coin not in COINS:
        COINS.append(coin)
        await update.message.reply_text(f"✅ *{coin.upper()}* ditambahkan!", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"ℹ️ {coin.upper()} sudah ada.")


async def cmd_hapus(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not ctx.args:
        await update.message.reply_text("Contoh: `/hapuscoin solana`", parse_mode="Markdown")
        return
    coin = ctx.args[0].lower()
    if coin in COINS:
        COINS.remove(coin)
        await update.message.reply_text(f"🗑️ *{coin.upper()}* dihapus.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"ℹ️ {coin.upper()} tidak ada.")


async def cmd_daftar(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    coin_list = "\n".join(f"  • {c.upper()}" for c in COINS)
    await update.message.reply_text(f"📋 *Coin dipantau:*\n{coin_list}", parse_mode="Markdown")


async def handle_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    global alerts_enabled
    q = update.callback_query
    await q.answer()
    d = q.data

    if d == "prices":
        await send_prices(q.message.reply_text)
    elif d == "forex":
        await send_forex(q.message.reply_text)
    elif d == "news":
        await send_news(q.message.reply_text)
    elif d == "analyze_menu":
        kb = [[InlineKeyboardButton(c.upper(), callback_data=f"analyze_{c}")] for c in COINS]
        await q.message.reply_text("Pilih coin:", reply_markup=InlineKeyboardMarkup(kb))
    elif d.startswith("analyze_"):
        await send_analysis(q.message.reply_text, d.replace("analyze_", ""))
    elif d == "signal_menu":
        kb = [[InlineKeyboardButton(c.upper(), callback_data=f"signal_{c}")] for c in COINS]
        kb.append([InlineKeyboardButton("🥇 XAU/USD", callback_data="signal_xauusd")])
        await q.message.reply_text("Pilih untuk sinyal lengkap:", reply_markup=InlineKeyboardMarkup(kb))
    elif d.startswith("signal_"):
        await send_full_signal(q.message.reply_text, d.replace("signal_", ""))
    elif d == "chart_menu":
        kb = [[InlineKeyboardButton(c.upper(), callback_data=f"chart_{c}")] for c in COINS]
        kb.append([InlineKeyboardButton("🥇 XAU/USD", callback_data="chart_xauusd")])
        await q.message.reply_text("Pilih chart:", reply_markup=InlineKeyboardMarkup(kb))
    elif d.startswith("chart_"):
        name = d.replace("chart_", "")
        await send_chart(q.message, name)
    elif d == "mtf_menu":
        kb = [[InlineKeyboardButton(c.upper(), callback_data=f"mtf_{c}")] for c in COINS]
        await q.message.reply_text("Pilih coin multi TF:", reply_markup=InlineKeyboardMarkup(kb))
    elif d.startswith("mtf_"):
        await send_mtf(q.message.reply_text, d.replace("mtf_", ""))
    elif d == "toggle_alert":
        alerts_enabled = not alerts_enabled
        status = "🔔 AKTIF" if alerts_enabled else "🔕 NONAKTIF"
        await q.message.reply_text(f"Alert: *{status}*", parse_mode="Markdown")
    elif d == "porto":
        ctx.args = []
        await cmd_porto(update, ctx)
    elif d == "fng":
        await cmd_fng(update, ctx)
    elif d == "summary":
        ctx.args = []
        await cmd_summary(update, ctx)
    elif d == "target_list":
        if not watchlist_alerts:
            await q.message.reply_text(
                "📭 Belum ada price target.\nGunakan: `/target bitcoin 70000`", parse_mode="Markdown"
            )
        else:
            lines = ["🎯 *Target Price Alert:*\n"] + [f"  • {c.upper()}: `${t:,.2f}`" for c, t in watchlist_alerts.items()]
            await q.message.reply_text("\n".join(lines), parse_mode="Markdown")
    elif d == "help":
        await q.message.reply_text(
            "*📖 Perintah:*\n\n"
            "/start — menu utama\n"
            "/harga — harga crypto\n"
            "/forex — harga XAU & forex\n"
            "/analisis bitcoin — analisis teknikal\n"
            "/sinyal bitcoin — analisis lengkap + Entry/TP/SL\n"
            "/chart bitcoin — chart candlestick\n"
            "/chart bitcoin 3mo 4h — chart custom\n"
            "/mtf bitcoin — multi timeframe\n"
            "/news — berita crypto terbaru\n"
            "/setalert 5 — threshold alert\n"
            "/toggle — aktif/nonaktif alert\n"
            "/tambahcoin solana — tambah coin\n"
            "/hapuscoin solana — hapus coin\n"
            "/daftar — daftar coin\n"
            "/summary — ringkasan harian\n"
            "/porto — portfolio tracker\n"
            "/target — price target alert\n"
            "/fng — fear & greed index\n\n"
            "*Chart periods:* 1d 5d 1mo 3mo 6mo 1y\n"
            "*Chart intervals:* 1m 5m 15m 1h 4h 1d",
            parse_mode="Markdown",
        )


# ══════════════════════════════════════════════════════════
# SCHEDULER
# ══════════════════════════════════════════════════════════

def start_scheduler(app: Application):
    def run():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def main_loop():
            print("⏳ Warming up...")
            for _ in range(5):
                data = get_crypto_prices(COINS)
                for coin_id, info in data.items():
                    h = price_history.setdefault(coin_id, [])
                    h.append(info.get("usd", 0))
                await asyncio.sleep(15)
            print("✅ Warmup selesai, monitoring aktif!")

            while True:
                try:
                    await check_and_alert(app)
                except Exception as e:
                    print(f"[ERROR] scheduler: {e}")
                await asyncio.sleep(CHECK_INTERVAL * 60)

        loop.run_until_complete(main_loop())

    threading.Thread(target=run, daemon=True).start()


# ══════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════

def main():
    print("🤖 KriptoMeter Bot starting...")
    print(f"📊 Memantau: {', '.join(c.upper() for c in COINS)}")
    print(f"⏱️  Interval: {CHECK_INTERVAL} menit | Threshold: {ALERT_THRESHOLD}%")

    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start",      cmd_start))
    app.add_handler(CommandHandler("harga",      cmd_harga))
    app.add_handler(CommandHandler("forex",      cmd_forex))
    app.add_handler(CommandHandler("analisis",   cmd_analisis))
    app.add_handler(CommandHandler("sinyal",     cmd_sinyal))
    app.add_handler(CommandHandler("chart",      cmd_chart))
    app.add_handler(CommandHandler("mtf",        cmd_mtf))
    app.add_handler(CommandHandler("news",       cmd_news))
    app.add_handler(CommandHandler("setalert",   cmd_setalert))
    app.add_handler(CommandHandler("toggle",     cmd_toggle))
    app.add_handler(CommandHandler("tambahcoin", cmd_tambah))
    app.add_handler(CommandHandler("hapuscoin",  cmd_hapus))
    app.add_handler(CommandHandler("daftar",     cmd_daftar))
    app.add_handler(CommandHandler("summary",    cmd_summary))
    app.add_handler(CommandHandler("porto",      cmd_porto))
    app.add_handler(CommandHandler("target",     cmd_target))
    app.add_handler(CommandHandler("fng",        cmd_fng))
    app.add_handler(CallbackQueryHandler(handle_callback))

    start_scheduler(app)
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()