# -*- coding: utf-8 -*-
import asyncio
import json
import time
import random
import math
import traceback
from datetime import datetime
import aiohttp
import websockets
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

settings = {
    "pump": {
        "price": 1.0,
        "time": 5,
        "volume": 2000000,
        "oi": 5.0,
        "active": False
    },
    "dump": {
        "price": 10.0,
        "time": 20,
        "active": False
    },
    "vol": {
        "tf": "3m",
        "candles": 10,
        "max_old_volume": 700000,
        "min_new_volume": 2000000,
        "active": False
    }
}

waiting_for = {}
scanner_running = True
orderbook_cache = {}
symbols_cache = {"data": [], "time": 0}
data_cache = {"oi": {}, "funding": {}, "klines": {}, "ticker": {}}
rsi_cache = {}
ws_cache = {}
liquidation_cache = {}
CACHE_TTL = 30
CACHE_TTL_OB = 5
last_signal_time = {}

def load_settings():
    global settings
    try:
        with open("settings.json", "r") as f:
            loaded = json.load(f)
            for mode in settings:
                if mode in loaded:
                    for key in loaded[mode]:
                        if key in settings[mode]:
                            settings[mode][key] = loaded[mode][key]
        print("✅ Settings loaded")
    except:
        print("No saved settings")

def save_settings():
    try:
        with open("settings.json", "w") as f:
            json.dump(settings, f, indent=4)
    except:
        print("Failed to save settings")

load_settings()

# ========== ПРОКСИ - ВСЕГДА РАБОТАЮТ ==========
def rotate_proxy():
    if not PROXY_LIST:
        return None
    # Просто берем случайный прокси из списка
    # НИКАКИХ dead_proxies - прокси ВСЕГДА РАБОТАЮТ
    proxy = random.choice(PROXY_LIST)
    print(f"🔑 Using proxy: {proxy[:40]}...")
    return proxy

async def safe_request(session, url, params=None, retries=3):
    for attempt in range(retries):
        # НОВЫЙ ПРОКСИ НА КАЖДУЮ ПОПЫТКУ!
        proxy = rotate_proxy()
        
        try:
            if proxy:
                async with session.get(url, params=params, timeout=15, proxy=proxy) as response:
                    if response.status in [403, 429, 451]:
                        print(f"⚠️ Status {response.status} for {url}")
                        await asyncio.sleep(1)
                        continue
                    if response.status != 200:
                        text = await response.text()
                        print(f"⚠️ Status {response.status} for {url} - {text[:100]}")
                        await asyncio.sleep(1)
                        continue
                    return await response.json()
            else:
                async with session.get(url, params=params, timeout=15) as response:
                    if response.status != 200:
                        text = await response.text()
                        print(f"⚠️ Status {response.status} for {url} - {text[:100]}")
                        await asyncio.sleep(1)
                        continue
                    return await response.json()
        except asyncio.TimeoutError:
            print(f"⏰ Timeout on attempt {attempt+1}, retrying...")
            await asyncio.sleep(1)
            continue
        except Exception as e:
            print(f"⚠️ Request error: {str(e)[:50]}")
            await asyncio.sleep(1)
            continue
    
    print(f"❌ All retries failed for {url}")
    return None

async def get_symbols(session):
    now = time.time()
    if symbols_cache["data"] and now - symbols_cache["time"] < 1800:
        return symbols_cache["data"]
    print("🔄 Loading symbols...")
    data = await safe_request(session, BASE + "/fapi/v1/ticker/24hr")
    if not data:
        print("❌ Failed to load symbols")
        return symbols_cache["data"] or []
    coins = [x for x in data if x["symbol"].endswith("USDT")]
    coins.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
    top_coins = [x["symbol"] for x in coins[:150]]
    symbols_cache["data"] = top_coins
    symbols_cache["time"] = now
    print(f"✅ Loaded top {len(top_coins)} coins")
    return top_coins

async def get_klines(session, symbol, interval, limit):
    key = f"{symbol}_{interval}_{limit}"
    now = time.time()
    if key in data_cache["klines"] and now - data_cache["klines"][key]["time"] < 60:
        return data_cache["klines"][key]["data"]
    
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    data = await safe_request(session, BASE + "/fapi/v1/klines", params)
    
    if not data:
        print(f"⚠️ No klines data for {symbol} {interval} {limit}")
        data = []
    
    data_cache["klines"][key] = {"data": data, "time": now}
    return data

async def get_funding_rate(session, symbol):
    now = time.time()
    if symbol in data_cache["funding"] and now - data_cache["funding"][symbol]["time"] < CACHE_TTL:
        return data_cache["funding"][symbol]["data"]
    data = await safe_request(session, BASE + "/fapi/v1/premiumIndex", {"symbol": symbol})
    result = float(data.get("lastFundingRate", 0)) * 100 if data else None
    data_cache["funding"][symbol] = {"data": result, "time": now}
    return result

async def get_open_interest(session, symbol, interval_minutes=5):
    now = time.time()
    cache_key = f"{symbol}_{interval_minutes}"
    if cache_key in data_cache["oi"] and now - data_cache["oi"][cache_key]["time"] < CACHE_TTL:
        return data_cache["oi"][cache_key]["data"]
    
    data = await safe_request(session, BASE + "/fapi/v1/openInterest", {"symbol": symbol})
    if not data:
        result = None
    else:
        oi_current = float(data.get("openInterest", 0))
        
        period_map = {
            5: "5m",
            15: "15m",
            30: "30m",
            60: "1h"
        }
        period = period_map.get(interval_minutes, "5m")
        
        hist = await safe_request(
            session,
            BASE + "/futures/data/openInterestHist",
            {
                "symbol": symbol,
                "period": period,
                "limit": 2
            }
        )
        if hist and len(hist) >= 2:
            oi_old = float(hist[-2]["sumOpenInterest"])
            oi_change = ((oi_current - oi_old) / oi_old) * 100 if oi_old else 0
        else:
            oi_change = 0
        
        ticker = await get_ticker(session, symbol)
        price = ticker["price"] if ticker else 0
        oi_usdt = oi_current * price
        result = {"current": round(oi_usdt, 2), "change": round(oi_change, 2)}
    
    data_cache["oi"][cache_key] = {"data": result, "time": now}
    return result

async def get_spot_price(session, symbol):
    data = await safe_request(
        session,
        "https://api.binance.com/api/v3/ticker/price",
        {"symbol": symbol}
    )
    return float(data["price"]) if data else 0

async def get_ticker(session, symbol):
    now = time.time()
    if symbol in data_cache["ticker"] and now - data_cache["ticker"][symbol]["time"] < CACHE_TTL:
        return data_cache["ticker"][symbol]["data"]
    data = await safe_request(session, BASE + "/fapi/v1/ticker/24hr", {"symbol": symbol})
    if data:
        result = {
            "price": float(data.get("lastPrice", 0)),
            "volume": float(data.get("quoteVolume", 0))
        }
        data_cache["ticker"][symbol] = {"data": result, "time": now}
        return result
    return None

def calculate_rsi_from_klines(klines, period=14):
    if len(klines) < period + 1:
        return 50
    gains, losses = 0, 0
    for i in range(1, period + 1):
        change = float(klines[i][4]) - float(klines[i-1][4])
        if change > 0:
            gains += change
        else:
            losses += abs(change)
    if losses == 0:
        return 100
    rs = gains / losses
    return round(100 - (100 / (1 + rs)), 2)

async def get_rsi(session, symbol, interval, period=14):
    key = f"{symbol}_{interval}_{period}"
    if key in rsi_cache and time.time() - rsi_cache[key]["time"] < 60:
        return rsi_cache[key]["data"]
    klines = await get_klines(session, symbol, interval, period + 1)
    rsi = calculate_rsi_from_klines(klines, period)
    rsi_cache[key] = {"data": rsi, "time": time.time()}
    return rsi

async def websocket_global():
    url = "wss://fstream.binance.com/ws/!bookTicker@arr"
    while True:
        try:
            async with websockets.connect(url) as ws:
                print("✅ Global WebSocket connected")
                async for msg in ws:
                    data = json.loads(msg)
                    if isinstance(data, list):
                        for item in data:
                            symbol = item.get("s")
                            if symbol and symbol in symbols_cache["data"]:
                                ws_cache[symbol] = {
                                    "time": time.time(),
                                    "bid": float(item.get("b", 0)),
                                    "ask": float(item.get("a", 0)),
                                    "bid_qty": float(item.get("B", 0)),
                                    "ask_qty": float(item.get("A", 0))
                                }
        except Exception as e:
            print(f"❌ Global WS error: {e}")
            await asyncio.sleep(5)

async def websocket_liquidations():
    url = "wss://fstream.binance.com/ws/!forceOrder@arr"

    while True:
        try:
            async with websockets.connect(url) as ws:
                print("✅ Liquidation WS connected")

                async for msg in ws:
                    data = json.loads(msg)
                    payload = data.get("data", data)

                    if "o" not in payload:
                        continue

                    order = payload["o"]

                    symbol = order["s"]
                    side = order["S"]
                    price = float(order["ap"])
                    qty = float(order["q"])

                    usdt = price * qty

                    if symbol not in liquidation_cache:
                        liquidation_cache[symbol] = []

                    liquidation_cache[symbol].append({
                        "time": time.time(),
                        "side": side,
                        "usdt": usdt
                    })

                    liquidation_cache[symbol] = [
                        x for x in liquidation_cache[symbol]
                        if time.time() - x["time"] < 1200
                    ]

        except Exception as e:
            print("Liquidation WS:", e)
            await asyncio.sleep(5)

def get_liquidations(symbol):
    if symbol not in liquidation_cache:
        return 0, 0

    long_liq = 0
    short_liq = 0

    for x in liquidation_cache[symbol]:
        if x["side"] == "SELL":
            long_liq += x["usdt"]
        elif x["side"] == "BUY":
            short_liq += x["usdt"]

    return round(long_liq, 2), round(short_liq, 2)

def analyze_ws_data(symbol):
    entry = ws_cache.get(symbol)
    if not entry:
        return None

    bid = entry["bid"]
    ask = entry["ask"]

    bid_vol = bid * entry["bid_qty"]
    ask_vol = ask * entry["ask_qty"]
    delta = bid_vol - ask_vol

    total = bid_vol + ask_vol

    if total == 0:
        imbalance = 50
    else:
        imbalance = bid_vol / total * 100

    ratio = bid_vol / max(ask_vol, 1)

    return {
        "bid": bid,
        "ask": ask,
        "spread": round((ask - bid) / bid * 100, 4),
        "bid_vol": round(bid_vol, 2),
        "ask_vol": round(ask_vol, 2),
        "imbalance": round(imbalance, 1),
        "ratio": round(ratio, 2),
        "delta": round(delta, 2)
    }

async def analyze_orderbook(session, symbol):
    try:
        if symbol in orderbook_cache and time.time() - orderbook_cache[symbol]["time"] < CACHE_TTL_OB:
            return orderbook_cache[symbol]["data"]
        data = await safe_request(session, BASE + "/fapi/v1/depth", {"symbol": symbol, "limit": 50})
        if not data or "bids" not in data or "asks" not in data:
            return None
        bids = [[float(x[0]), float(x[1])] for x in data.get("bids", [])[:50]]
        asks = [[float(x[0]), float(x[1])] for x in data.get("asks", [])[:50]]
        if not bids or not asks:
            return None
        current_price = bids[0][0]
        buy_vol = sum([p[0] * p[1] for p in bids])
        sell_vol = sum([p[0] * p[1] for p in asks])
        total_vol = buy_vol + sell_vol
        buy_pct = (buy_vol / total_vol * 100) if total_vol > 0 else 50
        sell_pct = 100 - buy_pct
        price_range = (asks[-1][0] - bids[0][0]) / bids[0][0] * 100
        buy_density = buy_vol / price_range if price_range > 0 else 0
        sell_density = sell_vol / price_range if price_range > 0 else 0
        avg_vol = total_vol / len(bids + asks)
        big_bids = [p for p in bids if p[0] * p[1] > avg_vol * 1.5]
        big_asks = [p for p in asks if p[0] * p[1] > avg_vol * 1.5]
        cluster_buy_usdt = sum([p[0] * p[1] for p in big_bids])
        cluster_sell_usdt = sum([p[0] * p[1] for p in big_asks])
        large_orders = [p for p in bids + asks if p[0] * p[1] > 50000]
        support = None
        resistance = None
        for bid in sorted(bids, key=lambda x: x[0]*x[1], reverse=True):
            if abs(bid[0] - current_price) / current_price <= 0.02:
                support = bid[0]
                break
        for ask in sorted(asks, key=lambda x: x[0]*x[1], reverse=True):
            if abs(ask[0] - current_price) / current_price <= 0.02:
                resistance = ask[0]
                break
        result = {
            "price": current_price,
            "buy_vol": round(buy_vol, 2),
            "sell_vol": round(sell_vol, 2),
            "buy_pct": round(buy_pct, 2),
            "sell_pct": round(sell_pct, 2),
            "buy_density": round(buy_density, 2),
            "sell_density": round(sell_density, 2),
            "cluster_buy_usdt": round(cluster_buy_usdt, 2),
            "cluster_sell_usdt": round(cluster_sell_usdt, 2),
            "large_orders": len(large_orders),
            "support": support,
            "resistance": resistance
        }
        orderbook_cache[symbol] = {"time": time.time(), "data": result}
        return result
    except Exception as e:
        print(f"OrderBook error {symbol}: {e}")
        return None

def can_send_signal(symbol, mode):
    key = f"{symbol}_{mode}"
    now = time.time()
    if key in last_signal_time:
        if now - last_signal_time[key] < 60:
            return False
    last_signal_time[key] = now
    return True

# ========== СКАНЕР ==========
async def scanner(app):
    async with aiohttp.ClientSession() as session:
        print("✅ Бот запущен и мониторит рынок...")
        scan_count = 0
        while True:
            if scanner_running:
                try:
                    symbols = await get_symbols(session)
                    scan_count += 1
                    print(f"🔄 Scan #{scan_count} - checking {len(symbols)} coins...")
                    
                    for symbol in symbols:
                        s_pump = settings["pump"]
                        s_dump = settings["dump"]
                        s_vol = settings["vol"]
                        
                        # ===== PUMP =====
                        if s_pump["active"]:
                            klines = await get_klines(session, symbol, "1m", s_pump["time"] + 1)
                            
                            if len(klines) >= s_pump["time"] + 1:
                                price_old = float(klines[0][4])
                                price_new = float(klines[-1][4])
                                price_change = ((price_new - price_old) / price_old) * 100
                                new_vol = float(klines[-1][7])
                                oi = await get_open_interest(session, symbol, s_pump["time"])
                                
                                price_ok = price_change >= s_pump["price"]
                                vol_ok = new_vol >= s_pump["volume"]
                                oi_ok = oi and oi["change"] >= s_pump["oi"]
                                
                                if price_ok and vol_ok and oi_ok:
                                    print(f"✅ PUMP conditions met for {symbol}!")
                                    if can_send_signal(symbol, "pump"):
                                        await send_full_signal(session, app, symbol, "PUMP", {
                                            "price_change": price_change,
                                            "price_ok": price_ok,
                                            "vol_ok": vol_ok,
                                            "oi_ok": oi_ok,
                                            "new_vol": new_vol,
                                            "oi_change": oi["change"] if oi else 0,
                                            "klines": klines,
                                            "oi_data": oi,
                                            "time_minutes": s_pump["time"]
                                        })
                        
                        # ===== DUMP =====
                        if s_dump["active"]:
                            klines = await get_klines(session, symbol, "1m", s_dump["time"] + 1)
                            
                            if len(klines) >= s_dump["time"] + 1:
                                price_old = float(klines[0][4])
                                price_new = float(klines[-1][4])
                                price_change = ((price_new - price_old) / price_old) * 100
                                
                                if price_change >= s_dump["price"]:
                                    print(f"✅ DUMP conditions met for {symbol}!")
                                    if can_send_signal(symbol, "dump"):
                                        await send_full_signal(session, app, symbol, "DUMP", {
                                            "price_change": price_change,
                                            "klines": klines,
                                            "time_minutes": s_dump["time"]
                                        })
                        
                        # ===== VOLUME =====
                        if s_vol["active"]:
                            klines = await get_klines(session, symbol, s_vol["tf"], s_vol["candles"] + 1)
                            if len(klines) >= s_vol["candles"] + 1:
                                all_old_ok = True
                                old_vols = []
                                for c in klines[:-1]:
                                    vol = float(c[7])
                                    old_vols.append(vol)
                                    if vol > s_vol["max_old_volume"]:
                                        all_old_ok = False
                                new_vol = float(klines[-1][7])
                                if all_old_ok and new_vol >= s_vol["min_new_volume"]:
                                    print(f"✅ VOLUME conditions met for {symbol}!")
                                    if can_send_signal(symbol, "vol"):
                                        await send_full_signal(session, app, symbol, "VOLUME", {
                                            "new_vol": new_vol,
                                            "old_vols": old_vols,
                                            "klines": klines
                                        })
                        
                        await asyncio.sleep(0.01)
                except Exception as e:
                    print(f"❌ SCAN ERROR: {e}")
                    traceback.print_exc()
            await asyncio.sleep(30)

# ========== ОТПРАВКА СИГНАЛА ==========
async def send_full_signal(session, app, symbol, mode, data):
    try:
        print(f"📡 Sending {mode} signal for {symbol}")
        ticker = await get_ticker(session, symbol)
        if not ticker:
            print(f"❌ No ticker data for {symbol}")
            return
        futures_price = ticker["price"]
        spot_price = await get_spot_price(session, symbol)
        
        rsi_1m = await get_rsi(session, symbol, "1m")
        rsi_5m = await get_rsi(session, symbol, "5m")
        rsi_15m = await get_rsi(session, symbol, "15m")
        rsi_1h = await get_rsi(session, symbol, "1h")
        
        funding = await get_funding_rate(session, symbol)
        long_liq, short_liq = get_liquidations(symbol)
        
        ob = await analyze_orderbook(session, symbol)
        ws = analyze_ws_data(symbol)
        
        emoji = "🚀" if mode == "PUMP" else "🔥" if mode == "DUMP" else "📦"
        
        msg = f"{emoji} {mode} SIGNAL\n\n📊 {symbol}\n"
        
        if mode == "PUMP":
            price_change = data.get("price_change", 0)
            new_vol = data.get("new_vol", 0)
            oi_change = data.get("oi_change", 0)
            time_minutes = data.get("time_minutes", settings['pump']['time'])
            msg += f"""
✔ Price: +{price_change:.2f}% (≥ {settings['pump']['price']}%)
✔ Volume: {new_vol:,.0f} USDT (≥ {settings['pump']['volume']:,} USDT)
✔ OI: +{oi_change:.2f}% (≥ {settings['pump']['oi']}%)
✔ Period: {time_minutes} min (1m candles)
"""
        elif mode == "DUMP":
            price_change = data.get("price_change", 0)
            time_minutes = data.get("time_minutes", settings['dump']['time'])
            msg += f"""
✔ Price Growth: +{price_change:.2f}% (≥ {settings['dump']['price']}%)
✔ Period: {time_minutes} min (1m candles)
"""
        elif mode == "VOLUME":
            old_vols = data.get("old_vols", [])
            new_vol = data.get("new_vol", 0)
            msg += f"""
✔ All old candles ≤ {settings['vol']['max_old_volume']:,} USDT
✔ New candle: {new_vol:,.0f} USDT (≥ {settings['vol']['min_new_volume']:,} USDT)
✔ TF: {settings['vol']['tf']} | Candles: {settings['vol']['candles']}
"""
        
        msg += "\n" + "-" * 40 + "\n"
        
        msg += f"""
💲 Spot: ${spot_price:.4f}
💲 Futures: ${futures_price:.4f}

📈 RSI:
  1m: {rsi_1m:.1f} | 5m: {rsi_5m:.1f}
  15m: {rsi_15m:.1f} | 1h: {rsi_1h:.1f}
"""
        
        if mode == "PUMP":
            oi_data = data.get("oi_data")
            if oi_data:
                msg += f"\n📊 OI: {oi_data['current']:,.0f} USDT (Change: {oi_data['change']:.2f}%)"
            else:
                msg += "\n📊 OI: N/A"
        else:
            oi = await get_open_interest(session, symbol, 5)
            if oi:
                msg += f"\n📊 OI: {oi['current']:,.0f} USDT (Change: {oi['change']:.2f}%)"
            else:
                msg += "\n📊 OI: N/A"
        
        msg += f"\n💰 Funding: {funding:.4f}%" if funding is not None else "\n💰 Funding: N/A"
        
        msg += f"""
💥 Liquidations (20 min)
  LONG: {long_liq:,.0f} USDT
  SHORT: {short_liq:,.0f} USDT
"""
        
        if ob:
            buy_density = ob.get("buy_density", 0)
            sell_density = ob.get("sell_density", 0)
            cluster_buy = ob.get("cluster_buy_usdt", 0)
            cluster_sell = ob.get("cluster_sell_usdt", 0)
            large_orders = ob.get("large_orders", 0)
            support = ob.get("support")
            resistance = ob.get("resistance")
            
            msg += f"""
📚 Order Book:
  Buyers: {ob['buy_pct']:.1f}% | Sellers: {ob['sell_pct']:.1f}%
  Buy Density: {buy_density:,.0f} USDT
  Sell Density: {sell_density:,.0f} USDT
  Buy Clusters: {cluster_buy:,.0f} USDT
  Sell Clusters: {cluster_sell:,.0f} USDT
  Large Orders: {large_orders}
  Support: ${support:.4f}""" if support else """
  Support: N/A"""
            msg += f"""
  Resistance: ${resistance:.4f}""" if resistance else """
  Resistance: N/A"""
        
        if ws:
            ratio = ws["ratio"]

            if ratio >= 4:
                pressure = "EXTREME BUY"
            elif ratio >= 2.5:
                pressure = "STRONG BUY"
            elif ratio >= 1.4:
                pressure = "BUY"
            elif ratio <= 0.25:
                pressure = "EXTREME SELL"
            elif ratio <= 0.50:
                pressure = "STRONG SELL"
            elif ratio <= 0.75:
                pressure = "SELL"
            else:
                pressure = "NEUTRAL"

            msg += f"""
⚡ Live OrderBook

Spread: {ws['spread']:.4f} %

Bid Volume: {ws['bid_vol']:,.0f} USDT
Ask Volume: {ws['ask_vol']:,.0f} USDT

Imbalance: {ws['imbalance']:.1f} %

OrderBook Pressure: {pressure}
"""
        
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
        ])
        await app.bot.send_message(chat_id=CHAT_ID, text=msg, reply_markup=keyboard)
        print(f"✅ Signal {mode} for {symbol} sent successfully!")
    except Exception as e:
        print(f"❌ Send signal error {symbol}: {e}")
        traceback.print_exc()

# ========== МЕНЮ ==========
def main_menu():
    status = "RUNNING ✅" if scanner_running else "STOPPED ❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"⚡ СКАНЕР: {status}", callback_data="toggle_scanner")],
        [InlineKeyboardButton("📊 СТАТУС", callback_data="status")],
        [InlineKeyboardButton("🔧 PUMP", callback_data="pump_menu")],
        [InlineKeyboardButton("🔧 DUMP", callback_data="dump_menu")],
        [InlineKeyboardButton("🔧 VOL", callback_data="vol_menu")]
    ])

def pump_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("PRICE %", callback_data="pump_price")],
        [InlineKeyboardButton("TIME MIN", callback_data="pump_time")],
        [InlineKeyboardButton("VOLUME USDT", callback_data="pump_volume")],
        [InlineKeyboardButton("OI %", callback_data="pump_oi")],
        [InlineKeyboardButton("▶️ START", callback_data="pump_start")],
        [InlineKeyboardButton("⏹ STOP", callback_data="pump_stop")],
        [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
    ])

def dump_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("PRICE %", callback_data="dump_price")],
        [InlineKeyboardButton("TIME MIN", callback_data="dump_time")],
        [InlineKeyboardButton("▶️ START", callback_data="dump_start")],
        [InlineKeyboardButton("⏹ STOP", callback_data="dump_stop")],
        [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
    ])

def vol_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("TIMEFRAME", callback_data="vol_tf")],
        [InlineKeyboardButton("CANDLES", callback_data="vol_candles")],
        [InlineKeyboardButton("MAX OLD VOL", callback_data="vol_max_old_volume")],
        [InlineKeyboardButton("MIN NEW VOL", callback_data="vol_min_new_volume")],
        [InlineKeyboardButton("▶️ START", callback_data="vol_start")],
        [InlineKeyboardButton("⏹ STOP", callback_data="vol_stop")],
        [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
    ])

def status_text():
    total = len(symbols_cache["data"]) if symbols_cache["data"] else 0
    status = "RUNNING ✅" if scanner_running else "STOPPED ❌"
    return f"""
📊 СТАТУС СКАНЕРА
==================
⚡ СКАНЕР: {status}

🔧 PUMP
  Цена: {settings['pump']['price']}%
  Время: {settings['pump']['time']} мин
  Объём: {settings['pump']['volume']:,} USDT
  OI: {settings['pump']['oi']}%
  Активен: {'✅' if settings['pump']['active'] else '❌'}

🔧 DUMP
  Цена: {settings['dump']['price']}%
  Время: {settings['dump']['time']} мин
  Активен: {'✅' if settings['dump']['active'] else '❌'}

🔧 VOLUME
  TF: {settings['vol']['tf']}
  Свечей: {settings['vol']['candles']}
  Max Vol: {settings['vol']['max_old_volume']:,} USDT
  Min Vol: {settings['vol']['min_new_volume']:,} USDT
  Активен: {'✅' if settings['vol']['active'] else '❌'}

📊 МОНЕТ: {total}
⏱ ИНТЕРВАЛ: 30 сек
"""

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("📊 ГЛАВНОЕ МЕНЮ", reply_markup=main_menu())

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scanner_running
    query = update.callback_query
    await query.answer()
    data = query.data
    
    if data == "main":
        await query.edit_message_text("📊 ГЛАВНОЕ МЕНЮ", reply_markup=main_menu())
    elif data == "toggle_scanner":
        scanner_running = not scanner_running
        status = "RUNNING ✅" if scanner_running else "STOPPED ❌"
        await query.edit_message_text(f"⚡ СКАНЕР: {status}", reply_markup=main_menu())
    elif data == "status":
        await query.edit_message_text(status_text(), reply_markup=main_menu())
    elif data == "pump_menu":
        await query.edit_message_text("🔧 PUMP НАСТРОЙКИ", reply_markup=pump_menu())
    elif data == "dump_menu":
        await query.edit_message_text("🔧 DUMP НАСТРОЙКИ", reply_markup=dump_menu())
    elif data == "vol_menu":
        await query.edit_message_text("🔧 VOLUME НАСТРОЙКИ", reply_markup=vol_menu())
    elif data.endswith("_start"):
        mode = data.split("_")[0]
        settings[mode]["active"] = True
        save_settings()
        await query.edit_message_text(f"✅ {mode.upper()} ЗАПУЩЕН", reply_markup=main_menu())
    elif data.endswith("_stop"):
        mode = data.split("_")[0]
        settings[mode]["active"] = False
        save_settings()
        await query.edit_message_text(f"❌ {mode.upper()} ОСТАНОВЛЕН", reply_markup=main_menu())
    elif "_" in data:
        mode, key = data.split("_", 1)
        waiting_for[query.message.chat_id] = (mode, key)
        await query.edit_message_text(f"📝 ВВЕДИТЕ ЗНАЧЕНИЕ ДЛЯ {key.upper()}:")
    else:
        await query.edit_message_text("❌ НЕИЗВЕСТНАЯ КОМАНДА", reply_markup=main_menu())

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    if chat_id not in waiting
