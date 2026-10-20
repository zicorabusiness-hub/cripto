# 🤖 Crypto Monitor Bot

Bot Telegram untuk memantau harga crypto secara real-time dengan alert otomatis dan analisis teknikal.

## Fitur

- 📊 Harga real-time (USD & IDR) via CoinGecko
- 🔔 Alert otomatis saat harga naik/turun melebihi threshold
- 📈 Analisis teknikal: RSI 14, MA7, MA25
- ➕ Tambah/hapus coin tanpa restart
- 🆓 Gratis — CoinGecko tidak butuh API key

## Deploy ke Railway

### 1. Fork / clone repo ini ke GitHub

```bash
git clone https://github.com/USERNAME/crypto-bot.git
cd crypto-bot
```

### 2. Buat bot Telegram

1. Buka Telegram, cari `@BotFather`
2. Ketik `/newbot` dan ikuti instruksi
3. Salin **TOKEN** yang diberikan

### 3. Dapatkan Chat ID

1. Kirim pesan apa saja ke bot kamu
2. Buka: `https://api.telegram.org/bot<TOKEN>/getUpdates`
3. Salin nilai `"id"` di dalam `"chat"`

### 4. Deploy di Railway

1. Buka [railway.app](https://railway.app) → **New Project** → **Deploy from GitHub repo**
2. Pilih repo ini
3. Masuk ke tab **Variables**, tambahkan:

| Variable | Contoh | Wajib |
|----------|--------|-------|
| `BOT_TOKEN` | `7123456789:AAF...` | ✅ |
| `CHAT_IDS` | `123456789` | ✅ |
| `COINS` | `bitcoin,ethereum,solana,binancecoin` | ❌ |
| `ALERT_THRESHOLD` | `5` | ❌ |
| `CHECK_INTERVAL` | `5` | ❌ |

5. Klik **Deploy** — selesai!

## Perintah Bot

| Perintah | Fungsi |
|----------|--------|
| `/start` | Menu utama |
| `/harga` | Harga semua coin saat ini |
| `/analisis bitcoin` | Analisis RSI & MA untuk coin |
| `/setalert 5` | Set alert di perubahan ≥ 5% |
| `/toggle` | Aktif/nonaktifkan alert |
| `/tambahcoin solana` | Tambah coin ke pantauan |
| `/hapuscoin solana` | Hapus coin dari pantauan |
| `/daftar` | Lihat semua coin yang dipantau |

## Konfigurasi via Environment Variables

Semua konfigurasi diset lewat env vars di Railway — tidak perlu edit kode.

- `COINS`: ID coin dari CoinGecko, pisahkan dengan koma. Cek ID di [coingecko.com](https://www.coingecko.com)
- `ALERT_THRESHOLD`: Persentase minimum untuk trigger alert (default: `5`)
- `CHECK_INTERVAL`: Frekuensi pengecekan dalam menit (default: `5`)
- `CHAT_IDS`: Bisa lebih dari satu, pisahkan koma: `123456,789012`

## Catatan

- CoinGecko gratis punya rate limit ~30 req/menit. Jika memantau banyak coin, naikkan `CHECK_INTERVAL` ke 10–15.
- Bot ini bukan saran investasi. DYOR.
