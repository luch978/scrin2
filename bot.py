# -*- coding: utf-8 -*-
import asyncio
import json
import time
import random
import math
import traceback
import sys
import sqlite3
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, Dict, List
import aiohttp
import websockets
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, MessageHandler, ContextTypes, filters

try:
    from config import TOKEN, CHAT_ID, EXCHANGES, PROXY_LIST
except:
    print("config.py not found. Using defaults.")
    TOKEN = "YOUR_BOT_TOKEN"
    CHAT_ID = "YOUR_CHAT_ID"
    EXCHANGES = {
        "binance": {"base": "https://fapi.binance.com", "name": "Binance"},
        "mexc": {"base": "https://api.mexc.com", "name": "MEXC"}
    }
    PROXY_LIST = []

# ========== БАЗА ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    
    # Таблица пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        subscription_end TEXT,
        created_at TEXT,
        is_admin INTEGER DEFAULT 0
    )''')
    
    # Таблица настроек пользователей
    c.execute('''CREATE TABLE IF NOT EXISTS user_settings (
        user_id INTEGER,
        exchange TEXT DEFAULT 'binance',
        PRIMARY KEY (user_id)
    )''')
    
    # Добавляем админа (создателя)
    c.execute('''INSERT OR IGNORE INTO users (user_id, username, is_admin, created_at)
                 VALUES (?, ?, 1, ?)''', (int(CHAT_ID), "admin", datetime.now().isoformat()))
    
    conn.commit()
    conn.close()

init_db()

# ========== ФУНКЦИИ БАЗЫ ДАННЫХ ==========
def get_user(user_id):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()
    conn.close()
    return user

def add_user(user_id, username, first_name):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute('''INSERT OR IGNORE INTO users (user_id, username, first_name, created_at)
                 VALUES (?, ?, ?, ?)''', (user_id, username, first_name, datetime.now().isoformat()))
    # По умолчанию даем 3 дня бесплатно
    c.execute('''UPDATE users SET subscription_end = ? 
                 WHERE user_id = ? AND subscription_end IS NULL''', 
              ((datetime.now() + timedelta(days=3)).isoformat(), user_id))
    conn.commit()
    conn.close()

def has_subscription(user_id):
    user = get_user(user_id)
    if not user:
        return False
    # user[3] - subscription_end
    if user[3] is None:
        return False
    end_date = datetime.fromisoformat(user[3])
    return end_date > datetime.now()

def get_exchange(user_id):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT exchange FROM user_settings WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else "binance"

def set_exchange(user_id, exchange):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute('''INSERT OR REPLACE INTO user_settings (user_id, exchange) VALUES (?, ?)''', 
              (user_id, exchange))
    conn.commit()
    conn.close()

def set_subscription(user_id, days):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    new_date = (datetime.now() + timedelta(days=days)).isoformat()
    c.execute("UPDATE users SET subscription_end = ? WHERE user_id = ?", (new_date, user_id))
    conn.commit()
    conn.close()

def remove_subscription(user_id):
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("UPDATE users SET subscription_end = NULL WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()

def get_all_users():
    conn = sqlite3.connect("users.db")
    c = conn.cursor()
    c.execute("SELECT user_id, username, first_name, subscription_end FROM users WHERE is_admin = 0")
    users = c.fetchall()
    conn.close()
    return users

def is_admin(user_id):
    user = get_user(user_id)
    return user and user[5] == 1  # is_admin

# ========== НАСТРОЙКИ ==========
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
current_exchange = "binance"  # По умолчанию

# Кэши
orderbook_cache = {}
symbols_cache = {"data": [], "time": 0}
data_cache = {"oi": {}, "funding": {}, "klines": {}, "ticker": {}}
rsi_cache = {}
ws_cache = {}
liquidation_cache = {}
dead_proxies = set()
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

def rotate_proxy():
    if not PROXY_LIST:
        return None
    available = [p for p in PROXY_LIST if p not in dead_proxies]
    if not available:
        print("⚠️ All proxies dead")
        return None
    proxy = random.choice(available)
    return proxy

async def safe_request(session, url, params=None, retries=3):
    proxy = rotate_proxy()
    for attempt in range(retries):
        try:
            async with session.get(url, params=params, timeout=10, proxy=proxy) as response:
                if response.status in [403, 429]:
                    print(f"⚠️ Status {response.status} for {url}")
                    if proxy:
                        dead_proxies.add(proxy)
                    await asyncio.sleep(1)
                    continue
                if response.status != 200:
                    text = await response.text()
                    print(f"⚠️ Status {response.status} for {url} - {text[:200]}")
                    return None
                return await response.json()
        except Exception as e:
            print(f"⚠️ REQUEST FAILED: {url} - {str(e)}")
            if proxy:
                dead_proxies.add(proxy)
            await asyncio.sleep(1)
    return None

def get_exchange_config(exchange_name):
    return EXCHANGES.get(exchange_name, EXCHANGES["binance"])

async def get_symbols(session, exchange="binance"):
    now = time.time()
    key = f"symbols_{exchange}"
    if symbols_cache.get(key) and now - symbols_cache.get(f"{key}_time", 0) < 1800:
        return symbols_cache[key]
    
    config = get_exchange_config(exchange)
    base = config["base"]
    
    print(f"🔄 Loading symbols from {exchange}...")
    
    # Разные эндпоинты для разных бирж
    if exchange == "binance":
        data = await safe_request(session, base + "/fapi/v1/ticker/24hr")
        if not data:
            return []
        coins = [x for x in data if x["symbol"].endswith("USDT")]
        coins.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        top_coins = [x["symbol"] for x in coins[:150]]
    elif exchange == "mexc":
        data = await safe_request(session, base + "/api/v3/ticker/24hr")
        if not data:
            return []
        coins = [x for x in data if x["symbol"].endswith("USDT")]
        coins.sort(key=lambda x: float(x["quoteVolume"]), reverse=True)
        top_coins = [x["symbol"] for x in coins[:150]]
    else:
        return []
    
    symbols_cache[key] = top_coins
    symbols_cache[f"{key}_time"] = now
    print(f"✅ Loaded top {len(top_coins)} coins from {exchange}")
    return top_coins

async def get_klines(session, symbol, interval, limit, exchange="binance"):
    config = get_exchange_config(exchange)
    base = config["base"]
    key = f"klines_{exchange}_{symbol}_{interval}_{limit}"
    now = time.time()
    
    if key in data_cache and now - data_cache[key]["time"] < 60:
        return data_cache[key]["data"]
    
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    
    if exchange == "binance":
        url = base + "/fapi/v1/klines"
    elif exchange == "mexc":
        url = base + "/api/v3/klines"
    else:
        return []
    
    data = await safe_request(session, url, params)
    if not data:
        print(f"⚠️ No klines data for {symbol} {interval} {limit} on {exchange}")
        data = []
    
    data_cache[key] = {"data": data, "time": now}
    return data

async def get_ticker(session, symbol, exchange="binance"):
    config = get_exchange_config(exchange)
    base = config["base"]
    now = time.time()
    key = f"ticker_{exchange}_{symbol}"
    
    if key in data_cache and now - data_cache[key]["time"] < CACHE_TTL:
        return data_cache[key]["data"]
    
    if exchange == "binance":
        url = base + "/fapi/v1/ticker/24hr"
    elif exchange == "mexc":
        url = base + "/api/v3/ticker/24hr"
    else:
        return None
    
    data = await safe_request(session, url, {"symbol": symbol})
    if data:
        result = {
            "price": float(data.get("lastPrice", 0)),
            "volume": float(data.get("quoteVolume", 0))
        }
        data_cache[key] = {"data": result, "time": now}
        return result
    return None

# ========== АДМИН-КОМАНДЫ ==========
async def admin_add_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Нет прав!")
        return
    
    try:
        args = context.args
        if len(args) < 2:
            await update.message.reply_text("📝 Использование: /add_subscription <user_id> <days>\nПример: /add_subscription 123456789 30")
            return
        
        target_user = int(args[0])
        days = int(args[1])
        
        set_subscription(target_user, days)
        await update.message.reply_text(f"✅ Пользователю {target_user} добавлена подписка на {days} дней")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_remove_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Нет прав!")
        return
    
    try:
        args = context.args
        if len(args) < 1:
            await update.message.reply_text("📝 Использование: /remove_subscription <user_id>")
            return
        
        target_user = int(args[0])
        remove_subscription(target_user)
        await update.message.reply_text(f"✅ Подписка пользователя {target_user} удалена")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_list_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Нет прав!")
        return
    
    users = get_all_users()
    if not users:
        await update.message.reply_text("📊 Нет пользователей")
        return
    
    msg = "📊 СПИСОК ПОЛЬЗОВАТЕЛЕЙ\n\n"
    for u in users:
        user_id, username, first_name, sub_end = u
        status = "✅" if sub_end and datetime.fromisoformat(sub_end) > datetime.now() else "❌"
        end_str = sub_end[:10] if sub_end else "Нет"
        msg += f"ID: {user_id}\nИмя: {first_name or username or 'Без имени'}\nСтатус: {status}\nДо: {end_str}\n\n"
    
    # Отправка по частям, если сообщение длинное
    if len(msg) > 4000:
        for i in range(0, len(msg), 4000):
            await update.message.reply_text(msg[i:i+4000])
    else:
        await update.message.reply_text(msg)

async def admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if not is_admin(user_id):
        await update.message.reply_text("❌ Нет прав!")
        return
    
    args = context.args
    if not args:
        await update.message.reply_text("📝 Использование: /broadcast <сообщение>")
        return
    
    message = " ".join(args)
    users = get_all_users()
    sent = 0
    
    for u in users:
        try:
            await context.bot.send_message(chat_id=u[0], text=f"📢 АНОНС:\n\n{message}")
            sent += 1
            await asyncio.sleep(0.1)
        except:
            pass
    
    await update.message.reply_text(f"✅ Рассылка отправлена {sent} пользователям")

# ========== МЕНЮ ==========
def main_menu(user_id):
    status = "RUNNING ✅" if scanner_running else "STOPPED ❌"
    exchange = get_exchange(user_id)
    exchange_name = EXCHANGES.get(exchange, {}).get("name", "Binance")
    sub_active = has_subscription(user_id)
    sub_status = "✅" if sub_active else "❌"
    
    keyboard = [
        [InlineKeyboardButton(f"⚡ СКАНЕР: {status}", callback_data="toggle_scanner")],
        [InlineKeyboardButton(f"🏦 БИРЖА: {exchange_name}", callback_data="exchange_menu")],
        [InlineKeyboardButton(f"💎 ПОДПИСКА: {sub_status}", callback_data="subscription_info")],
        [InlineKeyboardButton("📊 СТАТУС", callback_data="status")],
        [InlineKeyboardButton("🔧 PUMP", callback_data="pump_menu")],
        [InlineKeyboardButton("🔧 DUMP", callback_data="dump_menu")],
        [InlineKeyboardButton("🔧 VOL", callback_data="vol_menu")]
    ]
    
    # Если админ - добавляем админ-панель
    if is_admin(user_id):
        keyboard.append([InlineKeyboardButton("👑 АДМИН-ПАНЕЛЬ", callback_data="admin_panel")])
    
    return InlineKeyboardMarkup(keyboard)

def exchange_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟡 Binance", callback_data="set_exchange_binance")],
        [InlineKeyboardButton("🟣 MEXC", callback_data="set_exchange_mexc")],
        [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
    ])

def admin_panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📋 Список пользователей", callback_data="admin_list")],
        [InlineKeyboardButton("📢 Рассылка", callback_data="admin_broadcast")],
        [InlineKeyboardButton("📊 МЕНЮ", callback_data="main")]
    ])

# ========== ОБРАБОТЧИКИ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    username = update.message.from_user.username or "Без username"
    first_name = update.message.from_user.first_name or "Без имени"
    
    add_user(user_id, username, first_name)
    await update.message.reply_text("📊 ГЛАВНОЕ МЕНЮ", reply_markup=main_menu(user_id))

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global scanner_running, current_exchange
    query = update.callback_query
    await query.answer()
    data = query.data
    user_id = query.from_user.id
    
    if data == "main":
        await query.edit_message_text("📊 ГЛАВНОЕ МЕНЮ", reply_markup=main_menu(user_id))
    
    elif data == "exchange_menu":
        await query.edit_message_text("🏦 ВЫБЕРИТЕ БИРЖУ", reply_markup=exchange_menu())
    
    elif data.startswith("set_exchange_"):
        exchange = data.replace("set_exchange_", "")
        if exchange in EXCHANGES:
            set_exchange(user_id, exchange)
            await query.edit_message_text(f"✅ Биржа изменена на {EXCHANGES[exchange]['name']}", 
                                          reply_markup=main_menu(user_id))
    
    elif data == "subscription_info":
        sub_active = has_subscription(user_id)
        user = get_user(user_id)
        if sub_active and user:
            end_date = datetime.fromisoformat(user[3])
            days_left = (end_date - datetime.now()).days
            msg = f"💎 ПОДПИСКА\n\nСтатус: ✅ АКТИВНА\nДней осталось: {days_left}\nДо: {end_date.strftime('%d.%m.%Y')}"
        else:
            msg = "💎 ПОДПИСКА\n\nСтатус: ❌ НЕ АКТИВНА\n\nДля активации обратитесь к @admin"
        await query.edit_message_text(msg, reply_markup=main_menu(user_id))
    
    elif data == "admin_panel":
        if is_admin(user_id):
            await query.edit_message_text("👑 АДМИН-ПАНЕЛЬ", reply_markup=admin_panel())
    
    elif data == "admin_list":
        if is_admin(user_id):
            users = get_all_users()
            if not users:
                await query.edit_message_text("📊 Нет пользователей", reply_markup=admin_panel())
                return
            msg = "📊 ПОЛЬЗОВАТЕЛИ\n\n"
            for u in users:
                sub = "✅" if u[3] and datetime.fromisoformat(u[3]) > datetime.now() else "❌"
                msg += f"ID: {u[0]}\nИмя: {u[1] or u[2] or 'Без имени'}\nСтатус: {sub}\n\n"
            await query.edit_message_text(msg[:4000], reply_markup=admin_panel())
    
    elif data == "admin_broadcast":
        if is_admin(user_id):
            waiting_for[user_id] = ("broadcast",)
            await query.edit_message_text("📝 ВВЕДИТЕ ТЕКСТ ДЛЯ РАССЫЛКИ:", reply_markup=admin_panel())
    
    elif data == "toggle_scanner":
        scanner_running = not scanner_running
        await query.edit_message_text("📊 ГЛАВНОЕ МЕНЮ", reply_markup=main_menu(user_id))
    
    elif data == "status":
        await query.edit_message_text(status_text(user_id), reply_markup=main_menu(user_id))
    
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
        await query.edit_message_text(f"✅ {mode.upper()} ЗАПУЩЕН", reply_markup=main_menu(user_id))
    
    elif data.endswith("_stop"):
        mode = data.split("_")[0]
        settings[mode]["active"] = False
        save_settings()
        await query.edit_message_text(f"❌ {mode.upper()} ОСТАНОВЛЕН", reply_markup=main_menu(user_id))
    
    elif "_" in data:
        mode, key = data.split("_", 1)
        waiting_for[query.message.chat_id] = (mode, key)
        await query.edit_message_text(f"📝 ВВЕДИТЕ ЗНАЧЕНИЕ ДЛЯ {key.upper()}:")

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    user_id = update.message.from_user.id
    
    if chat_id not in waiting_for:
        return
    
    waiting = waiting_for[chat_id]
    
    # Обработка рассылки
    if waiting == ("broadcast",):
        if not is_admin(user_id):
            return
        message = update.message.text
        users = get_all_users()
        sent = 0
        for u in users:
            try:
                await context.bot.send_message(chat_id=u[0], text=f"📢 АНОНС:\n\n{message}")
                sent += 1
                await asyncio.sleep(0.1)
            except:
                pass
        await update.message.reply_text(f"✅ Рассылка отправлена {sent} пользователям")
        del waiting_for[chat_id]
        return
    
    # Обработка настроек
    mode, key = waiting
    try:
        if key == "tf":
            value = update.message.text.strip()
            if value not in ["1m", "3m", "5m"]:
                await update.message.reply_text("❌ ТОЛЬКО: 1m, 3m, 5m")
                return
        else:
            value = float(update.message.text.replace(",", "."))
            if key in ["candles", "time"]:
                value = int(value)
        
        settings[mode][key] = value
        del waiting_for[chat_id]
        save_settings()
        
        menu_map = {"pump": pump_menu, "dump": dump_menu, "vol": vol_menu}
        menu_text = {"pump": "🔧 PUMP НАСТРОЙКИ", "dump": "🔧 DUMP НАСТРОЙКИ", "vol": "🔧 VOLUME НАСТРОЙКИ"}
        
        await update.message.reply_text(
            f"✅ СОХРАНЕНО: {key.upper()} = {value}\n\n{menu_text[mode]}",
            reply_markup=menu_map[mode]()
        )
    except Exception as e:
        print(f"Text handler error: {e}")
        await update.message.reply_text("❌ ОШИБКА ВВОДА")

# ========== ОСТАЛЬНЫЕ ФУНКЦИИ ==========
def status_text(user_id):
    total = len(symbols_cache.get("data", [])) if symbols_cache.get("data") else 0
    status = "RUNNING ✅" if scanner_running else "STOPPED ❌"
    exchange = get_exchange(user_id)
    exchange_name = EXCHANGES.get(exchange, {}).get("name", "Binance")
    
    return f"""
📊 СТАТУС СКАНЕРА
==================
⚡ СКАНЕР: {status}
🏦 БИРЖА: {exchange_name}

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
"""

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

# ========== ЗАПУСК ==========
app = Application.builder().token(TOKEN).build()

# Команды
app.add_handler(CommandHandler("start", start))
app.add_handler(CommandHandler("add_subscription", admin_add_subscription))
app.add_handler(CommandHandler("remove_subscription", admin_remove_subscription))
app.add_handler(CommandHandler("list_users", admin_list_users))
app.add_handler(CommandHandler("broadcast", admin_broadcast))

app.add_handler(CallbackQueryHandler(button))
app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))

print("🚀 BOT STARTED WITH MULTI-USER SUPPORT!")
app.run_polling()