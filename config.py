import os

# ── Bot Token (dari @BotFather) ────────────────────────────
BOT_TOKEN: str = os.environ["BOT_TOKEN"]

# ── Chat IDs penerima alert ────────────────────────────────
# Format di Railway: "123456789" atau "123456789,987654321"
_raw_ids = os.environ.get("CHAT_IDS", "")
CHAT_IDS: list[int] = [int(x.strip()) for x in _raw_ids.split(",") if x.strip()]

# ── Coin yang dipantau ─────────────────────────────────────
# Format di Railway: "bitcoin,ethereum,solana"
_raw_coins = os.environ.get("COINS", "bitcoin,ethereum,solana,binancecoin")
COINS: list[str] = [c.strip().lower() for c in _raw_coins.split(",") if c.strip()]

# ── Alert threshold (persen) ───────────────────────────────
ALERT_THRESHOLD: float = float(os.environ.get("ALERT_THRESHOLD", "5.0"))

# ── Interval pengecekan harga (menit) ─────────────────────
CHECK_INTERVAL: int = int(os.environ.get("CHECK_INTERVAL", "5"))
