# -*- coding: utf-8 -*-
import asyncio
import time
import requests
import random
import math
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

try:
    from config import TOKEN, CHAT_ID, BASE, PROXY_LIST
except:
    print("config.py not found. Using defaults.")
    TOKEN = "YOUR_BOT_TOKEN"
    CHAT_ID = "YOUR_CHAT_ID"
    BASE = "https://fapi.binance.com"
    PROXY_LIST = []

session = requests.Session()

def rotate_proxy():
    if PROXY_LIST:
        proxy = random.choice(PROXY_LIST)
        session.proxies.update({"http": proxy, "https": proxy})
        print(f"PROXY: {proxy}")
        return True
    return False

if PROXY_LIST:
    rotate_proxy()

def safe_request(url, params=None, retries=3):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=10)
            if r.status_code in [403, 429]:
                print(f"Status {r.status_code}. Rotating proxy.")
                rotate_proxy()
                continue
            return r.json()
        except Exception as e:
            print(f"Request error: {e}. Rotating proxy.")
            rotate_proxy()
            time.sleep(1)
    return None

settings = {
    "pump": {"price": 1.0, "time": 5, "volume": 3.0, "oi": 0.5, "active": False},
    "dump": {"pump_before": 10.0, "time": 20, "rsi": 80.0, "active": False},
    "vol": {"tf": "3m", "candles": 10, "max_old_volume": 700000, "min_new_volume": 2000000, "active": False}
}

waiting_for = {}
scanner_running = True

def get_symbols():
    data = safe_request(BASE + "/fapi/v1/ticker/24hr")
    if not data:
        return []
    coins = [x["symbol"] for x in data if x["symbol"].endswith("USDT")]
    print(f"Loaded coins: {len(coins)}")
    return coins

def get_klines(symbol, interval, limit):
    return safe_request(BASE + "/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit}) or []

def get_orderbook(symbol):
    data = safe_request(BASE + "/fapi/v1/depth", {"symbol": symbol, "limit": 20})
    if not data:
        return None
    bids = [[float(x[0]), float(x[1])] for x in data.get("bids", [])[:20]]
    asks = [[float(x[0]), float(x[1])] for x in data.get("asks", [])[:20]]
    spread = (asks[0][0] - bids[0][0]) / bids[0][0] * 100 if bids and asks else 0
    return {"spread": round(spread, 3), "bids": bids, "asks": asks}

def get_funding_rate(symbol):
    data = safe_request(BASE + "/fapi/v1/premiumIndex", {"symbol": symbol})
    return float(data.get("lastFundingRate", 0)) * 100 if data else 0

def get_open_interest(symbol):
    data = safe_request(BASE + "/fapi/v1/openInterest", {"symbol": symbol})
    if not data:
        return {"current": 0, "change_1h": 0}
    oi_current = float(data.get("openInterest", 0))
    hist = safe_request(BASE + "/fapi/v1/openInterestHist", {"symbol": symbol, "period": "1h", "limit": 2})
    if hist and len(hist) >= 2:
        oi_old = float(hist[-2]["sumOpenInterest"])
        oi_change = ((oi_current - oi_old) / oi_old) * 100 if oi_old else 0
    else:
        oi_change = 0
    return {"current": round(oi_current, 2), "change_1h": round(oi_change, 2)}

def get_price_change(symbol, lookback=5):
    klines = get_klines(symbol, "1m", lookback + 1)
    if len(klines) < 2:
        return 0
    old = float(klines[0][4])
    new = float(klines[-1][4])
    return ((new - old) / old) * 100

def calculate_rsi(symbol, period=14):
    klines = get_klines(symbol, "5m", period + 1)
    if len(klines) < period + 1:
        return 50
    gains, losses = 0, 0
    for i in range(1, len(klines)):
        change = float(klines[i][4]) - float(klines[i-1][4])
        if change > 0:
            gains += change
        else:
            losses += abs(change)
    if losses == 0:
        return 100
    rs = gains / losses
    return round(100 - (100 / (1 + rs)), 2)

# ========== ВСЕ ФУНКЦИИ ПРОВЕРКИ С УЧЁТОМ USDT ==========

def check_volume(symbol):
    try:
        s = settings["vol"]
        candles = get_klines(symbol, s["tf"], s["candles"] + 1)
        if len(candles) < s["candles"] + 1:
            return None
        
        # Получаем объёмы в USDT
        old_volumes = []
        for c in candles[:-1]:
            close_price = float(c[4])
            vol_coin = float(c[5])
            vol_usdt = close_price * vol_coin
            old_volumes.append(vol_usdt)
        
        avg_old = sum(old_volumes) / len(old_volumes)
        
        # Текущая свеча
        close_price_new = float(candles[-1][4])
        vol_coin_new = float(candles[-1][5])
        new_volume = close_price_new * vol_coin_new
        
        if avg_old <= s["max_old_volume"] and new_volume >= s["min_new_volume"]:
            return {
                "symbol": symbol,
                "tf": s["tf"],
                "old": round(avg_old),
                "new": round(new_volume),
                "ratio": round(new_volume / avg_old, 2) if avg_old > 0 else 0
            }
        return None
    except:
        return None

def check_pump(symbol):
    try:
        s = settings["pump"]
        klines = get_klines(symbol, "1m", s["time"] + 1)
        if len(klines) < 2:
            return None
        
        # Изменение цены
        price_old = float(klines[0][4])
        price_new = float(klines[-1][4])
        price_change = ((price_new - price_old) / price_old) * 100
        
        if price_change < s["price"]:
            return None
        
        # Объём в USDT (средний и текущий)
        old_volumes = []
        for c in klines[:-1]:
            close = float(c[4])
            vol_coin = float(c[5])
            vol_usdt = close * vol_coin
            old_volumes.append(vol_usdt)
        
        avg_old = sum(old_volumes) / len(old_volumes)
        
        close_new = float(klines[-1][4])
        vol_coin_new = float(klines[-1][5])
        new_vol_usdt = close_new * vol_coin_new
        
        vol_ratio = new_vol_usdt / avg_old if avg_old > 0 else 1
        
        if vol_ratio < s["volume"]:
            return None
        
        # OI
        oi = get_open_interest(symbol)
        if abs(oi["change_1h"]) < s["oi"]:
            return None
        
        return {
            "symbol": symbol,
            "price_change": round(price_change, 2),
            "vol_ratio": round(vol_ratio, 2),
            "oi_change": oi["change_1h"]
        }
    except:
        return None

def check_dump(symbol):
    try:
        s = settings["dump"]
        klines = get_klines(symbol, "1m", s["time"] + 1)
        if len(klines) < 2:
            return None
        
        # Изменение цены
        price_old = float(klines[0][4])
        price_new = float(klines[-1][4])
        price_change = ((price_new - price_old) / price_old) * 100
        
        if price_change > -s["pump_before"]:
            return None
        
        # RSI
        rsi = calculate_rsi(symbol)
        if rsi < s["rsi"]:
            return None
        
        return {
            "symbol": symbol,
            "price_change": round(price_change, 2),
            "rsi": rsi,
            "time_min": s["time"]
        }
    except:
        return None

# ========== МЕНЮ ==========

def main_menu():
    status = "RUNNING" if scanner_running else "STOPPED"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"SCANNER: {status}", callback_data="toggle_scanner")],
        [InlineKeyboardButton("STATUS", callback_data="status")],
        [InlineKeyboardButton("PUMP", callback_data="pump_menu")],
        [InlineKeyboardButton("DUMP", callback_data="dump_menu")],
        [InlineKeyboardButton("VOL", callback_data="vol_menu")]
    ])

def pump_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("PRICE %", callback_data="pump_price")],
        [InlineKeyboardButton("TIME MIN", callback_data="pump_time")],
        [InlineKeyboardButton("VOLUME X", callback_data="pump_volume")],
        [InlineKeyboardButton("OI %", callback_data="pump_oi")],
        [InlineKeyboardButton("START", callback_data="pump_start")],
        [InlineKeyboardButton("STOP", callback_data="pump_stop")],
        [InlineKeyboardButton("BACK", callback_data="main")]
    ])

def dump_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("PUMP BEFORE %", callback_data="dump_pump_before")],
        [InlineKeyboardButton("TIME MIN", callback_data="dump_time")],
        [InlineKeyboardButton("RSI", callback_data="dump_rsi")],
        [InlineKeyboardButton("START", callback_data="dump_start")],
        [InlineKeyboardButton("STOP", callback_data="dump_stop")],
        [InlineKeyboardButton("BACK", callback_data="main")]
    ])

def vol_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("TIMEFRAME", callback_data="vol_tf")],
        [InlineKeyboardButton("CANDLES", callback_data="vol_candles")],
        [InlineKeyboardButton("MAX OLD VOL", callback_data="vol_max_old_volume")],
        [InlineKeyboardButton("MIN NEW VOL", callback_data="vol_min_new_volume")],
        [InlineKeyboardButton("START", callback_data="vol_start")],
        [InlineKeyboardButton("STOP", callback_data="vol_stop")],
        [InlineKeyboardButton("BACK", callback_data="main")]
    ])

def status_text():
    total = len(get_symbols())
    return f"""
SCREENER STATUS
================
SCANNER: {'RUNNING' if scanner_running else 'STOPPED'}

PUMP
  Price: {settings['pump']['price']}%
  Time: {settings['pump']['time']} min
  Volume: {settings['pump']['volume']}x
  OI: {settings['pump']['oi']}%
  Active: {settings['pump']['active']}

DUMP
  Drop: {settings['dump']['pump_before']}%
  Time: {settings['dump']['time']} min
  RSI: {settings['dump']['rsi']}
  Active: {settings['dump']['active']}

VOLUME
  TF: {settings['vol']['tf']}
  Candles: {settings['vol']['candles']}
  Max Vol: {settings['vol']['max_old_volume']}
  Min Vol: {settings['vol']['min_new_volume']}
  Active: {settings['vol']['active']}

COINS: {total}
INTERVAL: 30 sec
"""

# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("SCREENER MENU", reply_markup=main_menu())

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scanner_running
    query = update.callback_query
    await query.answer()
    data = query.data

    if data == "main":
        await query.edit_message_text("SCREENER MENU", reply_markup=main_menu())
    elif data == "toggle_scanner":
        scanner_running = not scanner_running
        await query.edit_message_text(f"SCANNER: {'RUNNING' if scanner_running else 'STOPPED'}", reply_markup=main_menu())
    elif data == "status":
        await query.edit_message_text(status_text(), reply_markup=main_menu())
    elif data == "pump_menu":
        await query.edit_message_text("PUMP SETTINGS", reply_markup=pump_menu())
    elif data == "dump_menu":
        await query.edit_message_text("DUMP SETTINGS", reply_markup=dump_menu())
    elif data == "vol_menu":
        await query.edit_message_text("VOL SETTINGS", reply_markup=vol_menu())
    elif data.endswith("_start"):
        mode = data.split("_")[0]
        settings[mode]["active"] = True
        await query.edit_message_text(f"{mode.upper()} STARTED", reply_markup=main_menu())
    elif data.endswith("_stop"):
        mode = data.split("_")[0]
        settings[mode]["active"] = False
        await query.edit_message_text(f"{mode.upper()} STOPPED", reply_markup=main_menu())
    elif "_" in data:
        mode, key = data.split("_", 1)
        waiting_for[query.message.chat_id] = (mode, key)
        await query.message.reply_text("SEND VALUE:")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id not in waiting_for:
        return
    mode, key = waiting_for[chat_id]
    try:
        if key == "tf":
            value = update.message.text.strip()
            if value not in ["1m", "3m", "5m"]:
                await update.message.reply_text("ONLY: 1m, 3m, 5m")
                return
        else:
            value = float(update.message.text)
            if key in ["candles", "time"]:
                value = int(value)
        settings[mode][key] = value
        del waiting_for[chat_id]
        await update.message.reply_text("SAVED")
    except:
        await update.message.reply_text("ERROR")

# ========== СКАНЕР ==========

async def scanner(app):
    while True:
        if scanner_running:
            try:
                symbols = get_symbols()
                print(f"Scanning {len(symbols)} coins...")
                for symbol in symbols:
                    if settings["vol"]["active"]:
                        signal = check_volume(symbol)
                        if signal:
                            await app.bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"""
VOLUME SPIKE
Coin: {signal['symbol']}
TF: {signal['tf']}
Old Vol: {signal['old']:,} USDT
New Vol: {signal['new']:,} USDT
Growth: {signal['ratio']}x
"""
                            )
                            await asyncio.sleep(1)
                    if settings["pump"]["active"]:
                        signal = check_pump(symbol)
                        if signal:
                            await app.bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"""
PUMP SIGNAL
Coin: {signal['symbol']}
Price: {signal['price_change']}%
Volume: {signal['vol_ratio']}x
OI Change: {signal['oi_change']}%
"""
                            )
                            await asyncio.sleep(1)
                    if settings["dump"]["active"]:
                        signal = check_dump(symbol)
                        if signal:
                            await app.bot.send_message(
                                chat_id=CHAT_ID,
                                text=f"""
DUMP SIGNAL
Coin: {signal['symbol']}
Drop: {signal['price_change']}%
RSI: {signal['rsi']}
Time: {signal['time_min']} min
"""
                            )
                            await asyncio.sleep(1)
                    await asyncio.sleep(0.05)
            except Exception as e:
                print("SCAN ERROR", e)
        await asyncio.sleep(30)

async def post_init(app):
    asyncio.create_task(scanner(app))

# ========== ЗАПУСК ==========

app = Application.builder().token(TOKEN).post_init(post_init).build()
app.add_handler(CommandHandler("start", start))
app.add_handler(CallbackQueryHandler(button))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

print("BOT STARTED (ALL VOLUMES IN USDT)")
app.run_polling()