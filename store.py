import os
import asyncio
import base64
import qrcode
import sqlite3
import threading
import time
import queue
import logging
import hashlib
import functools
from io import BytesIO
from datetime import datetime
from telethon.errors import AuthKeyUnregisteredError
from flask import (
    Flask, session, request, render_template_string,
    jsonify, g, redirect, url_for, send_from_directory, render_template 
)
from telethon import TelegramClient, events
import json
from flask_socketio import SocketIO, emit
from telethon.events import MessageDeleted


import os
import uuid
import json
from werkzeug.utils import secure_filename
from flask import send_from_directory


import secrets
import math
import pandas as pd
import tempfile
import pdfplumber
sqlite3.register_adapter(datetime, lambda d: d.strftime("%Y-%m-%d %H:%M:%S"))






# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = 'supersecretkey_for_demo'
app.config['DATABASE'] = 'app.db'
app.json.ensure_ascii = False
# ПЕРЕНЕСТИ СЮДА (строки, которые сейчас перед шаблонами)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', ping_timeout=120, ping_interval=25) 


# Настройка папки для хранения фото
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# НОВЫЙ МАРШРУТ: Отдаем загруженные файлы по ссылке
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

def notify_clients(data_type="general"):
    socketio.emit('db_updated', {'type': data_type})
# ---------- Глобальные структуры ----------
background_tasks = {}      # session_id -> thread
user_clients = {}          # session_id -> (thread, client, user_id)
qr_sessions = {}           # session_id -> данные для QR
listener_locks = {}        # блокировки для каждого session_id
user_loops = {}            # session_id -> asyncio event loop


import asyncio
import logging

# --- ПОДАВЛЕНИЕ СИСТЕМНЫХ ОШИБОК TELETHON ПРИ ЗАКРЫТИИ ПОТОКОВ ---
def custom_exception_handler(loop, context):
    exception = context.get("exception")
    msg_str = str(exception) if exception else str(context.get("message", ""))
    
    # 1. Игнорируем ошибки закрытого цикла
    if isinstance(exception, (RuntimeError, GeneratorExit)) or "Task was destroyed" in msg_str or "Event loop is closed" in msg_str:
        return
    
    # 2. Игнорируем внутренний баг Telethon при отмене задач (AttributeError)
    if isinstance(exception, AttributeError) and "'NoneType' object has no attribute" in msg_str and ("recv" in msg_str or "disconnect" in msg_str):
        return
        
    # Для остальных, реальных ошибок используем стандартный обработчик
    loop.default_exception_handler(context)

# Применяем этот обработчик ко всем новым потокам
# (Вам нужно добавить `loop.set_exception_handler(custom_exception_handler)` 
# в функцию, где вы создаете новый цикл `loop = asyncio.new_event_loop()`)

import functools
from flask import session, redirect, url_for
from datetime import datetime, timedelta, timezone
def login_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
            
        # --- НОВАЯ СТРОГАЯ ПРОВЕРКА СЕССИИ ---
        user_id = session['user_id']
        db = get_db()
        user = db.execute("SELECT session_token FROM users WHERE id = ?", (user_id,)).fetchone()
        
        # Если в базе есть токен, а в текущем браузере он не совпадает — разлогиниваем
        if user and 'session_token' in user.keys() and user['session_token']:
            if session.get('session_token') != user['session_token']:
                session.clear()
                return redirect(url_for('login_page'))
        # --------------------------------------
        
        return f(*args, **kwargs)
    return wrapped

def ensure_event_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

# ---------- Работа с БД ----------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        # 1. Добавляем timeout=20. Если база занята, поток подождет 20 секунд, а не упадет сразу
        db = g._database = sqlite3.connect(app.config['DATABASE'], timeout=20)
        
        # 2. Включаем режим WAL для параллельной работы
        db.execute("PRAGMA journal_mode=WAL;")
        
        # 3. Синхронизация (опционально для ускорения записи)
        db.execute("PRAGMA synchronous=NORMAL;")
        
        db.row_factory = sqlite3.Row
    return db

def bot_interaction_scheduler():
    """Фоновый поток для автоматической отправки команд ботам по расписанию"""
    while True:
        time.sleep(60)  # Проверяем каждую минуту
        try:
            with app.app_context():
                db = get_db()
                active_interactions = db.execute("SELECT * FROM interaction_bots WHERE status = 'active'").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for interaction in active_interactions:
                    # Оборачиваем КАЖДОГО бота в свой try..except, чтобы ошибка в одном не ломала других
                    try:
                        userbot_id = interaction['userbot_id']
                        
                        if not is_parsing_allowed(userbot_id):
                            continue 
                            
                        last_run = interaction['last_run']
                        if last_run:
                            try:
                                last_run_dt = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                last_run_dt = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S')
                                
                            diff_minutes = (now - last_run_dt).total_seconds() / 60
                            if diff_minutes < interaction['interval_minutes']:
                                continue  # Время еще не пришло
                        
                        if userbot_id in user_clients:
                            _, client, _ = user_clients[userbot_id]
                            loop = user_loops.get(userbot_id)
                            
                            if loop and loop.is_running() and client.is_connected():
                                commands = json.loads(interaction['commands'])
                                bot_username = interaction['bot_username']
                                
                                logger.info(f"🔄 Запуск команд для бота {bot_username}...")
                                
                                for cmd in commands:
                                    # Асинхронная обертка для гарантированной отправки и получения ошибок
                                    async def send_cmd(target, text):
                                        # get_entity заставляет Telethon "вспомнить" бота перед отправкой
                                        entity = await client.get_entity(target)
                                        await client.send_message(entity, text)
                                    
                                    try:
                                        # Запускаем и ЖДЕМ результат максимум 15 секунд
                                        future = asyncio.run_coroutine_threadsafe(send_cmd(bot_username, cmd), loop)
                                        future.result(timeout=15) 
                                        logger.info(f"✅ Успешно отправлена команда '{cmd}' боту {bot_username}")
                                    except Exception as e:
                                        logger.error(f"❌ Ошибка отправки '{cmd}' боту {bot_username}: {e}")
                                        
                                    time.sleep(2) # Небольшая пауза между командами
                                
                                # Обновляем время последнего запуска
                                db.execute("UPDATE interaction_bots SET last_run = ? WHERE id = ?", (now, interaction['id']))
                                db.commit()
                                
                                if 'notify_clients' in globals():
                                    notify_clients()
                                    
                    except Exception as bot_e:
                        logger.error(f"Сбой в логике расписания для бота {interaction['bot_username']}: {bot_e}")
                             
        except Exception as e:
            logger.error(f"Глобальная ошибка в планировщике ботов: {e}")





# --- Маршруты для Товаров ---
@app.before_request
def check_session_token():
    # Пропускаем проверку для страниц логина и логаута
    if request.endpoint in ['login_page', 'static', 'logout']:
        return
        
    if 'user_id' in session:
        try:
            db = get_db()
            user = db.execute("SELECT session_token FROM users WHERE id = ?", (session['user_id'],)).fetchone()
            
            if user and 'session_token' in user.keys() and user['session_token']:
                if session.get('session_token') != user['session_token']:
                    session.clear()
                    
                    # Если запрос пришел от фонового скрипта (fetch/ajax)
                    if request.path.startswith('/api/'):
                        return jsonify({'error': 'Сеанс завершен'}), 401
                        
                    # Если это обычная загрузка страницы
                    return redirect(url_for('login_page'))
        except Exception:
            pass

import google.generativeai as genai
import json
from flask import request, jsonify



@app.route('/api/autofill', methods=['POST'])
def autofill():
    data = request.json
    product_name = data.get('name', '')

    if not product_name:
        return jsonify({"error": "No product name"}), 400

    prompt = f"""
Ты - старший эксперт по заполнению карточек товаров для интернет-магазина.
В магазин поступают самые разные товары: смартфоны, ноутбуки, планшеты, наушники, аксессуары.
Твоя задача: проанализировать название: "{product_name}" и максимально точно заполнить характеристики.

ВАЖНОЕ УСЛОВИЕ: ПЕРЕД ЗАПОЛНЕНИЕМ ТЫ ОБЯЗАН ИСПОЛЬЗОВАТЬ ПОИСК В ИНТЕРНЕТЕ для проверки спецификаций устройства. КАТЕГОРИЧЕСКИ ЗАПРЕЩАЕТСЯ придумывать, генерировать случайные цифры или угадывать данные (особенно вес, артикулы, камеры и батарею).

ОБЩИЕ ПРАВИЛА:

Идентификация Apple: Если в названии есть "13", "14 Pro", "15", "16 Pro Max", "17 Pro" и т.д. без бренда — это "Apple", модель "iPhone [номер]". AirPods, MacBook, iPad - тоже Apple.

Идентификация Samsung: "S23", "S24", "A54", "Fold" — бренд "Samsung", модель "Galaxy [название]".

Пустые поля: Если для товара характеристика не применима (например, диагональ экрана для наушников) или в интернете не удалось найти точных, достоверных данных, ОБЯЗАТЕЛЬНО оставляй поле пустым: "". Никаких галлюцинаций.

ИНСТРУКЦИЯ ПО ЗАПОЛНЕНИЮ КАЖДОГО ПОЛЯ (СТРОГИЕ ФОРМАТЫ):

"brand": Производитель товара (например: Apple, Samsung, Xiaomi, Dyson).

"country_sim": Тип SIM-карты (для смартфонов). Если в названии "(2Sim)", "Dual", "ZA/A", "CH/A" -> пиши "Dual SIM". Если "(eSim)", "LL/A" -> пиши "eSIM only". Если это версия для физической + виртуальной карты, пиши "nano-SIM + eSIM".

"weight": Вес товара СТРОГО в килограммах. Формат: ноль или число, точка, три цифры. Например: "0.350", "1.200", "0.050". Опирайся только на реальные данные. Если не нашел — оставь "".

"color": Цвет товара СТРОГО НА АНГЛИЙСКОМ ЯЗЫКЕ (как в оригинальном названии). Не переводи на русский (например: пиши "Black", "White", "Natural Titanium").

"storage": Объем встроенной памяти. СТРОГИЙ ФОРМАТ: Цифра + пробел + "GB" или "TB" (например: "256 GB", "512 GB", "1 TB"). ВАЖНО: Если в названии указан слитный формат (например, "12/256GB" или "8/128"), то БОЛЬШЕЕ число — это встроенная память (storage).

"ram": Объем оперативной памяти. СТРОГИЙ ФОРМАТ: Цифра + пробел + "GB" (например: "8 GB", "16 GB"). ВАЖНО: 1) Если в названии указан формат "12/256GB", то МЕНЬШЕЕ число — это оперативная память (ram). 2) ИСКЛЮЧЕНИЕ: Apple не указывает RAM на официальном сайте. Для заполнения RAM у iPhone и iPad ОБЯЗАТЕЛЬНО ищи данные на авторитетных ресурсах (например, GSMArena) и заполняй это поле. Не оставляй его пустым для Apple.

"display": Характеристики экрана из официальных источников. СТРОГИЙ ФОРМАТ: [Диагональ]" [Тип матрицы] ([Особенности]), [Частота] Гц. Например: "6.9" OLED (Super Retina XDR), 120 Гц" или "6.8" Dynamic AMOLED 2X, 120 Гц".

"processor": Точное название чипа. СТРОГИЙ ФОРМАТ: Производитель + Модель. Например: "Apple A19 Pro", "Qualcomm Snapdragon 8 Gen 3".

"camera": Характеристики основного блока камер. СТРОГИЙ ФОРМАТ: Мегапиксели через плюс. Например: "48 МП + 48 МП + 12 МП" или "50 МП + 12 МП".

"front_camera": Характеристики фронтальной камеры. Формат: "12 МП" или "12 МП + 3D сенсор".

"video": Максимальное разрешение и частота кадров основной камеры. Например: "4K@60fps", "8K@30fps".

"connectivity": Беспроводные сети и модули (через запятую). Например: "Wi-Fi 7, Bluetooth 5.3, 5G, NFC".

"battery": Автономность. СТРОГИЙ ФОРМАТ: "до [число] часов работы" (например: "до 22 часов работы", "до 29 часов работы"). Если измеряется только в мАч (как у Android), пиши "5000 мАч".

"os": Операционная система "из коробки" для этой модели (например: "iOS 18", "Android 14").

"biometrics": Способы разблокировки. Например: "Face ID" (для Apple) или "Сканер отпечатка пальца (в экране), Распознавание лица" (для Android).

"charging": Поддерживаемые технологии зарядки. СТРОГИЙ ФОРМАТ: перечисление через запятую. Например: "MagSafe, быстрая зарядка, беспроводная зарядка".

"warranty": По умолчанию всегда "12 месяцев".

"model_no": АРТИКУЛ. Ищи реальный артикул.

ДЛЯ IPHONE: найди в интернете корень оригинального артикула (Part Number) из 5 символов, начинающийся на "M" для этой конфигурации (например: MY0L4, MU793).
ДЛЯ ОСТАЛЬНЫХ: пиши официальный заводской код, если нашел. Если не нашел - оставляй пустым "". Не выдумывай.

"description": Напиши привлекательное продающее описание товара на 2-3 предложения, выделяя его главные преимущества для покупателя на основе его реальных характеристик.

Верни результат СТРОГО в формате JSON, без приветствий, без пояснений, без markdown.
Ключи должны идти строго в таком порядке (чтобы ты сначала собрал характеристики, а артикул и описание выдал в конце):
{{
"brand": "",
"country_sim": "",
"weight": "",
"color": "",
"storage": "",
"ram": "",
"display": "",
"processor": "",
"camera": "",
"front_camera": "",
"video": "",
"connectivity": "",
"battery": "",
"os": "",
"biometrics": "",
"charging": "",
"warranty": "12 месяцев",
"model_no": "",
"description": ""
}}
    """

    try:
        # Используем gemini-1.5-flash (самая быстрая и дешевая для таких задач)
        model = genai.GenerativeModel('gemini-3.1-flash-lite-preview')
        response = model.generate_content(prompt)
        
        # Убираем возможную разметку ```json ... ``` которую иногда выдает ИИ
        result_text = response.text.replace('```json', '').replace('```', '').strip()
        
        parsed_data = json.loads(result_text)
        return jsonify(parsed_data)
        
    except Exception as e:
        print("Ошибка Gemini:", str(e))
        return jsonify({"error": str(e)}), 500



@app.route('/api/products/<int:prod_id>/messages')
@login_required
def get_product_messages(prod_id):
    user_id = session['user_id']
    db = get_db()
    
    # 1. Получаем синонимы товара
    prod = db.execute("SELECT synonyms FROM products WHERE id = ? AND user_id = ?", (prod_id, user_id)).fetchone()
    if not prod or not prod['synonyms']:
        return jsonify({'proposed': [], 'confirmed': []})
    
    # --- УМНАЯ ОЧИСТКА (превращает строку в набор слов) ---
    # --- УЛУЧШЕННАЯ ОЧИСТКА ---
    import re
    def clean_for_search(text):
        if not text: return set()
        
        # ДОБАВЛЯЕМ '*' (звездочку) в список удаляемых символов
        # Также добавили '.', если вдруг поставщик пишет "17.512"
        t = re.sub(r'[()*.[\]{},;!|/+\-]', ' ', text)
        
        # Убираем лишние пробелы и неразрывные пробелы
        t = re.sub(r'\s+', ' ', t.replace('\xa0', ' ')).strip().lower()
        
        # Решаем проблему кириллицы/латиницы
        t = t.translate(str.maketrans('асеокрх', 'aceokpx'))
        
        # Возвращаем набор чистых слов
        return set(t.split())

    # Подготовка синонимов (разбиваем на сеты слов)
    synonyms_as_word_sets = []
    for s in prod['synonyms'].split(','):
        if s.strip():
            words = clean_for_search(s)
            if words:
                synonyms_as_word_sets.append(words)

    # --- ИСПРАВЛЕНИЕ: Получаем все последние сообщения (all_msgs) ---
    # Без этого блока была ошибка "all_msgs is not defined"
    all_msgs_rows = db.execute("""
        SELECT m.id, m.text, m.date, m.chat_id, m.sender_name, m.chat_title, m.type, tc.custom_name
        FROM messages m
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE m.user_id = ? 
        ORDER BY m.date DESC LIMIT 1000
    """, (user_id,)).fetchall()
    all_msgs = [dict(r) for r in all_msgs_rows]

    # --- ИСПРАВЛЕНИЕ: Собираем актуальные отпечатки (latest_fingerprints) ---
    # Без этого блока была ошибка "latest_fingerprints is not defined"
    latest_fingerprints = {}
    for msg in all_msgs:
        if not msg['text']: continue
        lines = msg['text'].split('\n')
        for i, line in enumerate(lines):
            if not line.strip(): continue
            fp = get_fingerprint(msg['chat_id'], msg['sender_name'], line)
            if fp not in latest_fingerprints or msg['date'] > latest_fingerprints[fp]['date']:
                # Ищем цену в текущей строке или на 1-2 строки ниже
                price_val = extract_price(line)
                if not price_val and i + 1 < len(lines):
                    price_val = extract_price(lines[i+1])
                if not price_val and i + 2 < len(lines):
                    price_val = extract_price(lines[i+2])

                latest_fingerprints[fp] = {
                    'text': line,
                    'price': price_val,
                    'date': msg['date'],
                    'message_id': msg['id'],
                    'line_index': i
                }

    # 2. Обработка ПОДТВЕРЖДЕННЫХ
    confirmed_rows = db.execute("""
        SELECT pm.id as pm_id, pm.group_id, pm.message_id, m.chat_title, m.chat_id, m.sender_name, 
               pm.extracted_price, m.text, m.date, pm.line_index, tc.custom_name, m.type
        FROM product_messages pm
        JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE pm.product_id = ? AND pm.status = 'confirmed'
    """, (prod_id,)).fetchall()
    
    confirmed_raw = []
    confirmed_fingerprints = set()
    has_updates = False 

    for r in confirmed_rows:
        d = dict(r)
        if d['line_index'] != -1 and d['text']:
            lines = d['text'].split('\n')
            if 0 <= d['line_index'] < len(lines):
                orig_line = lines[d['line_index']]
                fp = get_fingerprint(d['chat_id'], d['sender_name'], orig_line)
                confirmed_fingerprints.add(fp)
                
                latest = latest_fingerprints.get(fp)
                if latest and latest['date'] > d['date']:
                    d['text'] = latest['text']
                    d['extracted_price'] = latest['price']
                    d['date'] = latest['date'] 
                    current_id = d.get('pm_id')
                    try:
                        db.execute("UPDATE product_messages SET message_id=?, line_index=?, extracted_price=? WHERE id=?",
                                   (latest['message_id'], latest['line_index'], latest['price'], current_id))
                        has_updates = True
                    except: pass
                else:
                    d['text'] = orig_line
        confirmed_raw.append(d)
        
    if has_updates:
        db.commit()
        notify_clients()
        
    grouped_confirmed = {}
    for d in confirmed_raw:
        g_id = d['group_id'] if d['group_id'] else d['pm_id']
        if g_id not in grouped_confirmed or d['date'] > grouped_confirmed[g_id]['date']:
            grouped_confirmed[g_id] = d
    
    confirmed = sorted(list(grouped_confirmed.values()), key=lambda x: x['date'], reverse=True)

    # === НОВОЕ: Собираем все строки, которые уже привязаны к ДРУГИМИ товарам ===
    occupied_rows = db.execute(
        "SELECT message_id, line_index FROM product_messages WHERE status = 'confirmed' AND product_id != ?", 
        (prod_id,)
    ).fetchall()
    
    # Создаем удобный список занятых строк, чтобы быстро их проверять
    occupied = {(r['message_id'], r['line_index']) for r in occupied_rows}
    # ===========================================================================
    
    # 3. Обработка ПРЕДЛОЖЕННЫХ (склеивание дубликатов и поиск по словам)
    # 3. Обработка ПРЕДЛОЖЕННЫХ (склеивание дубликатов и поиск по словам)
    proposed_list = []
    seen_fps = set()
    
    for row in all_msgs:
        lines = row['text'].split('\n') if row['text'] else []
        for i, line in enumerate(lines):
            if not line.strip(): continue
            
            # --- НОВОЕ СТРОГОЕ ПРАВИЛО ---
            # Если эта строка уже кем-то занята - мы просто игнорируем ее
            # и даже не показываем в списке "Предложенные"
            if (row['id'], i) in occupied or (row['id'], -1) in occupied:
                continue
            # -----------------------------

            fp = get_fingerprint(row['chat_id'], row['sender_name'], line)
            
            if fp in seen_fps or fp in confirmed_fingerprints:
                continue
            
            # --- ЛОГИКА ЖЕСТКОГО ПОИСКА ПО СЛОВАМ ---
            # --- ЛОГИКА ЖЕСТКОГО ПОИСКА ПО СЛОВАМ ---
            line_words = clean_for_search(line)
            is_match = False
            for syn_set in synonyms_as_word_sets:
                # Если ВСЕ слова из синонима есть в строке поставщика
                if syn_set.issubset(line_words):
                    # ЖЕСТКОЕ СРАВНЕНИЕ: Отсекаем старшие/другие модели, если их нет в синониме
                    if "pro" not in syn_set and "pro" in line_words: continue
                    if "max" not in syn_set and "max" in line_words: continue
                    if "plus" not in syn_set and "plus" in line_words: continue
                    if "ultra" not in syn_set and "ultra" in line_words: continue
                    if "mini" not in syn_set and "mini" in line_words: continue
                    if "fe" not in syn_set and "fe" in line_words: continue
                    
                    is_match = True
                    break
            
            if is_match:
                seen_fps.add(fp)
                
                # Ищем цену в текущей строке или на 1-2 строки ниже
                s_price = extract_price(line)
                if not s_price and i + 1 < len(lines):
                    s_price = extract_price(lines[i+1])
                if not s_price and i + 2 < len(lines):
                    s_price = extract_price(lines[i+2])
                
                proposed_list.append({
                    'message_id': row['id'],
                    'line_index': i,
                    'chat_title': row['chat_title'],
                    'sender_name': row['sender_name'],
                    'custom_name': row.get('custom_name'),
                    'type': row['type'],
                    'match_line': line,
                    'suggested_price': s_price,
                    'date': row['date']
                })
    
    proposed_list.sort(key=lambda x: x['date'], reverse=True)
    return jsonify({'proposed': proposed_list, 'confirmed': confirmed})

import re

def parse_number(s):
    clean = re.sub(r'[^\d]', '', s)
    return float(clean) if clean else 0.0

def extract_price(line):
    if not line: return None
    # Добавил K/R для обработки "KR" как на вашем скриншоте
    line_clean = re.sub(r'[₽$€рRkKкК]', '', line) 
    number_pattern = r'(\d[\d\s.,]*)'
    all_numbers = list(re.finditer(number_pattern, line_clean))
    
    if not all_numbers: return None
    
    for match in reversed(all_numbers):
        number_str = match.group(1).strip()
        try:
            price_value = parse_number(number_str)
            if price_value < 3000: continue
        except: continue
        
        end_pos = match.end()
        text_after = line_clean[end_pos:].strip().lower()
        if re.match(r'^[-]?\s*(шт|шт\.|штук|pcs|pc|item|ед|единиц)', text_after): continue
        
        text_before = line_clean[:match.start()].strip()
        if text_before.endswith('-'): return price_value
        if end_pos >= len(line_clean) or line_clean[end_pos] in '()': return price_value
        if end_pos == len(line_clean) or not line_clean[end_pos].isdigit(): return price_value
        
    for match in reversed(all_numbers):
        number_str = match.group(1).strip()
        try:
            price_value = parse_number(number_str)
            if price_value >= 1500:
                end_pos = match.end()
                text_after = line_clean[end_pos:].strip().lower()
                if not re.match(r'^[-]?\s*(шт|шт\.|штук|pcs|pc|item|ед|единиц)', text_after):
                    return price_value
        except: continue
    return None

def get_fingerprint(chat_id, sender_name, line):
    """Создает 'слепок' строки, удаляя цены для сравнения товаров"""
    # Заменяем все числа от 1000 и выше (с точками и пробелами) на тег [PRICE]
    norm_line = re.sub(r'\b\d{1,3}(?:[ .,]\d{3})+\b|\b\d{4,}\b', '[PRICE]', line.lower())
    norm_line = re.sub(r'[₽$€рRkKкК]', '', norm_line).strip()
    return f"{chat_id}_{sender_name}_{norm_line}"







@app.route('/api/product_messages/confirm', methods=['POST'])
@login_required
def confirm_product_message():
    data = request.get_json()
    db = get_db()
    try:
        line_idx = data.get('line_index', -1)
        price = data.get('price')
        if price == '': 
            price = None
            
        # --- НОВАЯ СТРОГАЯ ПРОВЕРКА: Занята ли уже эта строка другим товаром? ---
        if line_idx != -1:
            existing = db.execute("""
                SELECT product_id FROM product_messages 
                WHERE message_id = ? AND line_index = ?
            """, (data['message_id'], line_idx)).fetchone()
            
            if existing and existing['product_id'] != data['product_id']:
                return jsonify({'error': 'Эта строка уже привязана к другому товару!'}), 400
        # ------------------------------------------------------------------------

        db.execute("""
            INSERT INTO product_messages (product_id, message_id, line_index, status, extracted_price) 
            VALUES (?, ?, ?, 'confirmed', ?)
            ON CONFLICT(product_id, message_id, line_index) DO UPDATE SET status='confirmed', extracted_price=excluded.extracted_price
        """, (data['product_id'], data['message_id'], line_idx, price))
        db.commit()
        notify_clients()
         
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка привязки: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/product_messages/detach', methods=['POST'])
@login_required
def detach_product_message():
    data = request.get_json()
    db = get_db()
    # Ищем, состоит ли сообщение в группе. Если да — удаляем всю группу
    pm = db.execute("SELECT id, group_id FROM product_messages WHERE product_id=? AND message_id=?", 
                    (data['product_id'], data['message_id'])).fetchone()
    if pm:
        if pm['group_id']:
            db.execute("DELETE FROM product_messages WHERE group_id = ?", (pm['group_id'],))
        else:
            db.execute("DELETE FROM product_messages WHERE id = ?", (pm['id'],))
        db.commit()
        notify_clients()
         
    return jsonify({'success': True})

@app.route('/api/product_messages/update_price', methods=['POST'])
@login_required
def update_product_price():
    data = request.get_json()
    db = get_db()
    db.execute("UPDATE product_messages SET extracted_price = ? WHERE id = ?", 
               (data['price'], data['pm_id']))
    db.commit()
    notify_clients()
     
    return jsonify({'success': True})

def clean_for_search(text):
    if not text: return set()
    t = re.sub(r'[()*.[\]{},;!|/+\-]', ' ', text)
    t = re.sub(r'\s+', ' ', t.replace('\xa0', ' ')).strip().lower()
    t = t.translate(str.maketrans('асеокрх', 'aceokpx'))
    return set(t.split())

@app.route('/api/products', methods=['GET', 'POST'])
@login_required
def manage_products():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        query = """
            SELECT p.*, 
                   (SELECT COUNT(*) FROM product_messages pm 
                    JOIN messages m ON pm.message_id = m.id 
                    WHERE pm.product_id = p.id AND pm.status = 'confirmed') as confirmed_count
            FROM products p
            WHERE p.user_id = ?
        """
        products = db.execute(query, (user_id,)).fetchall()
        
        # Достаем все сообщения для поиска совпадений
        all_msgs = db.execute("SELECT text FROM messages WHERE user_id = ? ORDER BY date DESC LIMIT 1000", (user_id,)).fetchall()
        
        result = []
        for p in products:
            p_dict = dict(p)
            p_dict['proposed_count'] = 0
            
            # Если нет подтвержденных, ищем предложенные
            if p_dict['confirmed_count'] == 0 and p_dict['synonyms']:
                synonyms_as_word_sets = []
                for s in p_dict['synonyms'].split(','):
                    if s.strip():
                        words = clean_for_search(s)
                        if words:
                            synonyms_as_word_sets.append(words)
                
                has_proposed = False
                if synonyms_as_word_sets:
                    for row in all_msgs:
                        if has_proposed: break
                        lines = row['text'].split('\n') if row['text'] else []
                        for line in lines:
                            if not line.strip(): continue
                            line_words = clean_for_search(line)
                            for syn_set in synonyms_as_word_sets:
                                if syn_set.issubset(line_words):
                                    # ЖЕСТКОЕ СРАВНЕНИЕ: Отсекаем старшие/другие модели
                                    if "pro" not in syn_set and "pro" in line_words: continue
                                    if "max" not in syn_set and "max" in line_words: continue
                                    if "plus" not in syn_set and "plus" in line_words: continue
                                    if "ultra" not in syn_set and "ultra" in line_words: continue
                                    if "mini" not in syn_set and "mini" in line_words: continue
                                    if "fe" not in syn_set and "fe" in line_words: continue

                                    has_proposed = True
                                    break
                            if has_proposed: break
                            
                p_dict['proposed_count'] = 1 if has_proposed else 0
            
            # Также проверяем статус 'pending' в базе (для товаров из Excel и чужих API)
            if p_dict['proposed_count'] == 0:
                pending_count = db.execute("SELECT COUNT(*) FROM product_messages WHERE product_id = ? AND status = 'pending'", (p_dict['id'],)).fetchone()[0]
                if pending_count > 0:
                    p_dict['proposed_count'] = 1
                    
            result.append(p_dict)
            
        return jsonify(result)
        
    # POST
    # POST
    # POST
    # POST
    # Так как мы передаем файлы, используем request.form, а не get_json()
    folder_id = request.form.get('folder_id')
    
    # 1. Обработка синонимов (теперь они приходят из формы)
    try:
        syns = json.loads(request.form.get('synonyms', '[]'))
    except:
        syns = []
    synonyms_str = ", ".join([s.strip() for s in syns if s.strip()])
    
    # 2. ВСТАВЛЯЕМ ВАШ НОВЫЙ КОД ДЛЯ ФОТО ЗДЕСЬ:
    files = request.files.getlist('photos')
    saved_names = []
    for file in files:
        if file and file.filename:
            # Генерируем уникальное имя, чтобы не было конфликтов
            ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
            filename = f"{uuid.uuid4().hex}.{ext}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            saved_names.append(filename)

    # Сохраняем в БД просто как строку через запятую: "id1.jpg, id2.jpg"
    photo_url_str = ",".join(saved_names) 

    # 3. Сохраняем все в базу данных
    # 3. Сохраняем все в базу данных
    db.execute("""
        INSERT INTO products 
        (user_id, name, synonyms, price, folder_id, photo_url, brand, country, weight, model_number, is_on_request, color, storage, ram, warranty, description, specs) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, 
        request.form.get('name'), 
        synonyms_str, 
        float(request.form.get('price', 0.0)), 
        folder_id,
        
        photo_url_str,
        
        request.form.get('brand'),
        request.form.get('country'),
        request.form.get('weight'),
        request.form.get('model_number'),
        1 if request.form.get('is_on_request') == '1' else 0,
        
        # 🔽 ДОБАВЛЯЕМ ЭТИ СТРОКИ СЮДА 🔽
        request.form.get('color'),
        request.form.get('storage'),
        request.form.get('ram'),
        request.form.get('warranty'),
        request.form.get('description'),
        request.form.get('specs')
    ))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/products/<int:prod_id>', methods=['DELETE'])
@login_required
def delete_product(prod_id):
    db = get_db()
    db.execute("DELETE FROM products WHERE id=? AND user_id=?", (prod_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/reports/confirmed')
@login_required
def get_all_confirmed():
    user_id = session['user_id']
    db = get_db()
    
    query = """
        SELECT pm.id as binding_id, pm.group_id, p.id as product_id, p.name as product_name, pm.extracted_price, m.text, 
               m.chat_id, m.chat_title, m.sender_name, m.date, pm.line_index, pm.message_id, pm.is_actual,
               m.telegram_message_id, m.type, ec.sheet_url, tc.chat_title as tc_chat_title, tc.custom_name
        FROM product_messages pm
        JOIN products p ON pm.product_id = p.id
        JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        LEFT JOIN excel_configs ec ON m.chat_id = ec.chat_id AND m.type LIKE 'excel%'
        WHERE p.user_id = ?
        AND pm.status = 'confirmed'
    """
    rows = db.execute(query, (user_id,)).fetchall()
    
    # Собираем актуальные версии строк (с добавленными telegram_message_id и type)
    all_msgs = db.execute("SELECT id, text, chat_id, sender_name, date, telegram_message_id, type FROM messages WHERE user_id = ? AND (is_blocked IS NULL OR is_blocked = 0) AND (is_delayed IS NULL OR is_delayed = 0) ORDER BY date DESC", (user_id,)).fetchall()
    latest_fingerprints = {}
    for row in all_msgs:
        if not row['text']: continue
        lines = row['text'].split('\n')
        for i, line in enumerate(lines):
            if not line.strip(): continue
            fp = get_fingerprint(row['chat_id'], row['sender_name'], line)
            if fp not in latest_fingerprints:
                price_val = extract_price(line)
                if not price_val and i + 1 < len(lines):
                    price_val = extract_price(lines[i+1])
                if not price_val and i + 2 < len(lines):
                    price_val = extract_price(lines[i+2])

                latest_fingerprints[fp] = {
                    'message_id': row['id'],
                    'line_index': i,
                    'text': line,
                    'date': row['date'],
                    'price': price_val,
                    'telegram_message_id': row['telegram_message_id'],
                    'type': row['type']
                }


    result_raw = []
    has_updates = False 
    for r in rows:
        d = dict(r)
        if d['line_index'] != -1 and d['text']:
            lines = d['text'].split('\n')
            if 0 <= d['line_index'] < len(lines):
                orig_line = lines[d['line_index']]
                fp = get_fingerprint(d['chat_id'], d['sender_name'], orig_line)
                
                latest = latest_fingerprints.get(fp)
                if latest and latest['date'] > d['date']:
                    d['text'] = latest['text']
                    d['extracted_price'] = latest['price']
                    d['date'] = latest['date']
                    d['telegram_message_id'] = latest['telegram_message_id']
                    d['type'] = latest['type']
                    
                    current_id = d.get('pm_id') or d.get('id')

                    try:
                        db.execute("UPDATE product_messages SET message_id=?, line_index=?, extracted_price=? WHERE id=?",
                                   (latest['message_id'], latest['line_index'], latest['price'], current_id))
                        has_updates = True
                    except sqlite3.IntegrityError:
                        if current_id:
                            db.execute("DELETE FROM product_messages WHERE id=?", (current_id,))
                            has_updates = True
                else:
                    d['text'] = orig_line
        result_raw.append(d)
        
    if has_updates:
        db.commit()
        notify_clients()

    grouped_reports = {}
    for d in result_raw:
        g_id = d['group_id'] if d['group_id'] else d['binding_id']
        if g_id not in grouped_reports or d['date'] > grouped_reports[g_id]['date']:
            grouped_reports[g_id] = d
            
    final_result = list(grouped_reports.values())
    final_result.sort(key=lambda x: x['date'], reverse=True)
    return jsonify(final_result)


# --- Маршруты для Взаимодействия с ботами ---
@app.route('/api/interaction_bots', methods=['GET', 'POST'])
@login_required
def manage_interaction_bots():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        bots = db.execute("SELECT * FROM interaction_bots WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(b) for b in bots])
    
    # POST
    data = request.get_json()
    bot_username = data.get('bot_username').strip()
    custom_name = data.get('custom_name')
    userbot_id = data.get('userbot_id')
    
    # 1. Находим запущенного юзербота (он нужен, чтобы узнать числовой ID бота)
    client_to_use = None
    client_loop_to_use = None
    
    if userbot_id in user_clients:
        _, client_to_use, _ = user_clients[userbot_id]
        client_loop_to_use = user_loops.get(userbot_id)
    else:
        active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
        if active_sessions:
            sid = active_sessions[0]
            _, client_to_use, _ = user_clients[sid]
            client_loop_to_use = user_loops.get(sid)

    # 2. Превращаем текстовый @username в ЧИСЛОВОЙ ID для парсера
    numeric_chat_id = None
    if client_to_use and client_loop_to_use:
        async def get_bot_id():
            try:
                entity = await client_to_use.get_entity(bot_username)
                return entity.id
            except Exception as e:
                logging.error(f"Не удалось получить ID для {bot_username}: {e}")
                return None
        
        try:
            future = asyncio.run_coroutine_threadsafe(get_bot_id(), client_loop_to_use)
            numeric_chat_id = future.result(timeout=10)
        except Exception:
            pass

    # Если юзербот выключен, мы не сможем получить ID — выдаем ошибку
    if not numeric_chat_id:
        return jsonify({'error': 'Не удалось определить ID бота. Убедитесь, что юзербот включен и юзернейм указан верно (например, @MyBot).'}), 400

    # 3. Сохраняем автоматизацию (здесь оставляем текст, чтобы бот знал, кому писать)
    db.execute("""INSERT INTO interaction_bots 
                  (user_id, userbot_id, bot_username, custom_name, commands, interval_minutes, status) 
                  VALUES (?, ?, ?, ?, ?, ?, 'active')""",
               (user_id, userbot_id, bot_username, custom_name,
                json.dumps(data.get('commands', [])), data.get('interval_minutes', 60)))
    
    # 4. АВТОМАТИЧЕСКОЕ ДОБАВЛЕНИЕ В ПАРСЕР (здесь используем ЧИСЛОВОЙ ID!)
    try:
        db.execute("INSERT INTO tracked_chats (user_id, chat_id, chat_title, custom_name) VALUES (?, ?, ?, ?)",
                   (user_id, numeric_chat_id, bot_username, custom_name))
    except sqlite3.IntegrityError:
        pass # Уже добавлен

    db.commit()
    notify_clients()

    # 5. ПАРСИНГ ИСТОРИИ (также по числовому ID)
    # 5. ПАРСИНГ ИСТОРИИ 
    chat_title_for_parser = custom_name if custom_name else bot_username

    return jsonify({'success': True})

@app.route('/api/interaction_bots/<int:id>', methods=['DELETE'])
@login_required
def delete_interaction_bot(id):
    db = get_db()
    user_id = session['user_id']
    
    # 1. Находим бота перед удалением
    bot = db.execute("SELECT bot_username FROM interaction_bots WHERE id=? AND user_id=?", (id, user_id)).fetchone()
    if bot:
        bot_un = bot['bot_username']
        # 2. Ищем его в отслеживаемых чатах и удаляем все его настройки и сохраненные сообщения
        tc_list = db.execute("SELECT id, chat_id FROM tracked_chats WHERE user_id=? AND (chat_title=? OR custom_name=? OR chat_title=?)", 
                             (user_id, bot_un, bot_un, bot_un.replace('@', ''))).fetchall()
        for tc in tc_list:
            real_chat_id = tc['chat_id']
            db.execute("DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE chat_id=? AND user_id=?)", (real_chat_id, user_id))
            db.execute("DELETE FROM messages WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            db.execute("DELETE FROM excel_configs WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            db.execute("DELETE FROM tracked_chats WHERE id=?", (tc['id'],))
            
    db.execute("DELETE FROM interaction_bots WHERE id=? AND user_id=?", (id, user_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    if not os.path.exists('sessions'):
        os.makedirs('sessions')
    with app.app_context():
        db = get_db()
        db.execute('''
        CREATE TABLE IF NOT EXISTS pub_markups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pub_id INTEGER NOT NULL,
                    folder_id INTEGER DEFAULT 0,
                    markup_type TEXT DEFAULT 'percent',
                    markup_value REAL DEFAULT 0,
                    rounding INTEGER DEFAULT 100,
                    FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE,
                    UNIQUE(pub_id, folder_id)
                )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            is_active INTEGER DEFAULT 0,
            interval_min INTEGER DEFAULT 60,
            chat_id TEXT,
            message_id TEXT,
            userbot_id INTEGER,
            template TEXT,
            allowed_items TEXT -- Здесь будет храниться JSON с выбранными галочками (как в API)
        )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS api_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT,
            url TEXT,
            token TEXT,
            interval_min INTEGER DEFAULT 60,
            is_active INTEGER DEFAULT 1
        )
    ''')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'user'
            );
            CREATE TABLE IF NOT EXISTS user_telegram_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                api_id TEXT,
                api_hash TEXT,
                session_file TEXT,
                status TEXT DEFAULT 'inactive',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                telegram_message_id INTEGER UNIQUE,
                type TEXT,
                text TEXT,
                date TIMESTAMP,
                chat_id INTEGER,
                chat_title TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT NOT NULL,
                parent_id INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES folders(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saved_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                folder_id INTEGER,
                saved_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
                UNIQUE(message_id, folder_id)
            );
            CREATE TABLE IF NOT EXISTS tracked_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                synonyms TEXT,
                price REAL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            
            CREATE TABLE IF NOT EXISTS interaction_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                userbot_id INTEGER NOT NULL,
                bot_username TEXT NOT NULL,
                commands TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL DEFAULT 60,
                last_run TIMESTAMP,
                status TEXT DEFAULT 'active',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(userbot_id) REFERENCES user_telegram_sessions(id) ON DELETE CASCADE
            );
        ''')
        # Добавляем новые колонки в существующие таблицы (игнорируем ошибку, если они уже есть)
        # 1. Безопасное добавление колонок
        columns_to_add = [
            ("products", "photo_url TEXT DEFAULT NULL"),
            ("products", "brand TEXT DEFAULT NULL"),
            ("products", "country TEXT DEFAULT NULL"),
            ("products", "weight TEXT DEFAULT NULL"),
            ("products", "model_number TEXT DEFAULT NULL"),
            ("products", "is_on_request INTEGER DEFAULT 0"),
            ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
           ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
            ("products", "description TEXT DEFAULT NULL"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
            ("products", "description TEXT DEFAULT NULL"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("product_messages", "line_index INTEGER DEFAULT -1"),
            ("product_messages", "group_id INTEGER"),
            ("product_messages", "line_index INTEGER DEFAULT -1"),
            ("product_messages", "group_id INTEGER"),
            ("product_messages", "is_actual INTEGER DEFAULT 1"), 
            ("messages", "is_blocked INTEGER DEFAULT 0"),
            ("tracked_chats", "custom_name TEXT"),
            ("interaction_bots", "custom_name TEXT"),
            ("messages", "sender_name TEXT"),
            ("products", "folder_id INTEGER"),
            ("user_telegram_sessions", "account_name TEXT"),
            ("user_telegram_sessions", "time_start TEXT DEFAULT '00:00'"), 
            ("user_telegram_sessions", "time_end TEXT DEFAULT '23:59'"),   
            ("user_telegram_sessions", "schedule_enabled INTEGER DEFAULT 0"),
            # --- НОВЫЕ КОЛОНКИ ДЛЯ API КЛИЕНТОВ ---
            ("api_clients", "time_start TEXT DEFAULT '00:00'"),
            ("api_clients", "time_end TEXT DEFAULT '23:59'"),
            ("api_clients", "schedule_enabled INTEGER DEFAULT 0"),
            ("api_clients", "access_rules TEXT"),
            ("api_clients", "allowed_folders TEXT DEFAULT 'all'"), # <--- ДОБАВИТЬ ЭТО
            ("api_clients", "allowed_chats TEXT DEFAULT 'all'"),
            ("api_clients", "publish_chat_id INTEGER"),
            ("api_clients", "publish_message_id INTEGER"),
            ("api_clients", "publish_enabled INTEGER DEFAULT 0"),
            ("api_clients", "userbot_id INTEGER"),
            ("api_clients", "publish_template TEXT"),
            ("publications", "user_id INTEGER"),
            ("api_clients", "folders TEXT"),
            ("api_clients", "publish_interval INTEGER DEFAULT 60"),
            ("users", "session_token TEXT"),
            ("excel_configs", "is_grouped INTEGER DEFAULT 0"),
            ("publications", "markup_type TEXT DEFAULT 'percent'"), # <--- НОВОЕ
            ("publications", "markup_value REAL DEFAULT 0"),        # <--- НОВОЕ
            ("publications", "rounding INTEGER DEFAULT 100"),
            ("excel_configs", "sku_col INTEGER DEFAULT -1"),
            ("excel_configs", "source_type TEXT DEFAULT 'file'"),       # 'file' или 'google_sheet'
            ("excel_configs", "sheet_url TEXT"),                        # Ссылка на таблицу
            ("excel_configs", "parse_interval INTEGER DEFAULT 60")
        ]
        for table, col in columns_to_add:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col};")
            except sqlite3.OperationalError:
                pass # Если колонка уже есть, просто идем к следующей

        # 2. Обновление таблицы привязок (чтобы разные строки одного сообщения не затирали друг друга)
        # 2. Обновление таблицы привязок

        # --- ТАБЛИЦА ДЛЯ НАСТРОЕК EXCEL (С ПОДДЕРЖКОЙ ЛИСТОВ) ---
        db.executescript('''
            CREATE TABLE IF NOT EXISTS excel_configs_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                sheet_name TEXT NOT NULL,
                name_col INTEGER NOT NULL,
                name_row_offset INTEGER NOT NULL,
                price_col INTEGER NOT NULL,
                price_row_offset INTEGER NOT NULL,
                block_step INTEGER NOT NULL,
                start_row INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, sheet_name)
            );
        ''')


        # Безопасный перенос старых данных (если они были) и переименование
        try:
            db.executescript('''
                INSERT OR IGNORE INTO excel_configs_new (id, user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row)
                SELECT id, user_id, chat_id, 'Лист1', name_col, name_row_offset, price_col, price_row_offset, block_step, start_row FROM excel_configs;
                DROP TABLE excel_configs;
            ''')
        except Exception:
            pass
        try:
            db.executescript('ALTER TABLE excel_configs_new RENAME TO excel_configs;')
        except Exception:
            pass



        try:
            # --- ТАБЛИЦА ДЛЯ НАСТРОЕК EXCEL ---
            db.executescript('''
                CREATE TABLE IF NOT EXISTS excel_missing_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                sheet_name TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, sheet_name)
            );
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS excel_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    name_col INTEGER NOT NULL,
                    name_row_offset INTEGER NOT NULL,
                    price_col INTEGER NOT NULL,
                    price_row_offset INTEGER NOT NULL,
                    block_step INTEGER NOT NULL,
                    start_row INTEGER NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(chat_id)
                );
            ''')
            # --- ТАБЛИЦЫ ДЛЯ API И НАЦЕНОК ---
            db.executescript('''
                CREATE TABLE IF NOT EXISTS api_clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS api_markups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL,
                    folder_id INTEGER DEFAULT 0, -- 0 означает "Для всех остальных папок"
                    markup_type TEXT DEFAULT 'percent', -- 'percent' или 'fixed'
                    markup_value REAL DEFAULT 0,
                    rounding INTEGER DEFAULT 100, -- 100, 500, 1000 и тд.
                    FOREIGN KEY(client_id) REFERENCES api_clients(id) ON DELETE CASCADE,
                    UNIQUE(client_id, folder_id)
                );
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS pm_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    line_index INTEGER DEFAULT -1,
                    group_id INTEGER,
                    status TEXT DEFAULT 'proposed',
                    extracted_price REAL,
                    is_actual INTEGER DEFAULT 1, -- <--- ДОБАВЛЕНА ЭТА СТРОКА
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
                    FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                    UNIQUE(product_id, message_id, line_index)
                );
                -- Переносим старые данные (добавляем 1 по умолчанию для is_actual)
                INSERT OR IGNORE INTO pm_new (id, product_id, message_id, line_index, group_id, status, extracted_price, is_actual) 
                SELECT id, product_id, message_id, line_index, group_id, status, extracted_price, 1 FROM product_messages;
                
                -- Заменяем старую таблицу на новую
                DROP TABLE IF EXISTS product_messages;
                ALTER TABLE pm_new RENAME TO product_messages;
            ''')
        except Exception as e:
            pass


        # --- ИСПРАВЛЕНИЕ БАГА "5 СООБЩЕНИЙ" (Снятие глобального UNIQUE) ---
        try:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS messages_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    telegram_message_id INTEGER,
                    type TEXT,
                    text TEXT,
                    date TIMESTAMP,
                    chat_id INTEGER,
                    chat_title TEXT,
                    is_blocked INTEGER DEFAULT 0,
                    sender_name TEXT,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(chat_id, telegram_message_id) -- Теперь уникальность только внутри одного чата!
                );
                
                -- Переносим все старые сообщения в новую правильную таблицу
                INSERT OR IGNORE INTO messages_new (id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name)
                SELECT id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name FROM messages;
                
                -- Удаляем сломанную таблицу и ставим на её место новую
                DROP TABLE messages;
                ALTER TABLE messages_new RENAME TO messages;
            ''')
        except Exception as e:
            logger.error(f"Ошибка миграции сообщений: {e}")

        # 3. Создание учетки администратора по умолчанию
        cursor = db.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            pwd_hash = hashlib.sha256('admin'.encode()).hexdigest()
            db.execute("INSERT INTO users (login, password_hash, role) VALUES (?, ?, ?)",
                       ('admin', pwd_hash, 'admin'))
            db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (1, 'По умолчанию', NULL)")
            
        db.commit()
        notify_clients()

init_db()


@app.route('/api/products/<int:prod_id>/link_folder', methods=['POST'])
@login_required
def link_product_folder(prod_id):
    data = request.get_json()
    folder_id = data.get('folder_id') # Может быть None (отвязка)
    db = get_db()
    db.execute("UPDATE products SET folder_id=? WHERE id=? AND user_id=?", (folder_id, prod_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/products/<int:prod_id>', methods=['PUT'])
@login_required
def edit_product(prod_id):
    db = get_db()
    
    # 1. Синонимы (из FormData в JSON)
    try:
        syns = json.loads(request.form.get('synonyms', '[]'))
    except:
        syns = []
    synonyms_str = ", ".join([s.strip() for s in syns if s.strip()])

    # 2. Существующие фото, которые не были удалены
    try:
        existing_photos = json.loads(request.form.get('existing_photos', '[]'))
    except:
        existing_photos = []

    # 3. Сохраняем новые фото, если они были добавлены
    files = request.files.getlist('photos')
    saved_names = []
    for file in files:
        if file and file.filename:
            ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
            filename = f"{uuid.uuid4().hex}.{ext}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            saved_names.append(filename)

    # 4. Склеиваем старые фото и новые загруженные
    all_photos = existing_photos + saved_names
    photo_url_str = ",".join(all_photos)

    # 5. Обновляем товар в базе
    db.execute("""
        UPDATE products 
        SET name=?, synonyms=?, photo_url=?, brand=?, country=?, weight=?, model_number=?, is_on_request=?, folder_id=?,
            color=?, storage=?, ram=?, warranty=?, description=?, specs=?
        WHERE id=? AND user_id=?
    """, (
        request.form.get('name'), 
        synonyms_str, 
        photo_url_str, 
        request.form.get('brand'),
        request.form.get('country'),
        request.form.get('weight'),
        request.form.get('model_number'),
        1 if request.form.get('is_on_request') == '1' else 0,
        request.form.get('folder_id'), 
        
        # 🔽 ДОБАВЛЯЕМ ПРИЕМ ПОЛЕЙ
        request.form.get('color'),
        request.form.get('storage'),
        request.form.get('ram'),
        request.form.get('warranty'),
        request.form.get('description'),
        request.form.get('specs'), # Предполагается, что с фронта specs придет как JSON-строка

        prod_id, 
        session['user_id']
    ))
    db.commit()
    notify_clients()
    return jsonify({'success': True})





@app.route('/api/access_tree')
@login_required
def get_access_tree():
    user_id = session['user_id']
    db = get_db()
    folders = [dict(f) for f in db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()]
    
    # Гениальный запрос: вытаскиваем товары сразу с их подтвержденными поставщиками
    prods_raw = db.execute("""
        SELECT p.id as product_id, p.name as product_name, p.folder_id,
               m.chat_id, tc.custom_name, tc.chat_title
        FROM products p
        LEFT JOIN product_messages pm ON p.id = pm.product_id AND pm.status = 'confirmed'
        LEFT JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE p.user_id = ?
        GROUP BY p.id, m.chat_id
        ORDER BY p.name
    """, (user_id,)).fetchall()
    
    products = {}
    for row in prods_raw:
        pid = row['product_id']
        if pid not in products:
            products[pid] = {'id': pid, 'name': row['product_name'], 'folder_id': row['folder_id'], 'chats': []}
        if row['chat_id']:
            c_name = row['custom_name'] if row['custom_name'] else (row['chat_title'] or 'Неизвестный поставщик')
            if not any(c['chat_id'] == row['chat_id'] for c in products[pid]['chats']):
                products[pid]['chats'].append({'chat_id': row['chat_id'], 'name': c_name})
            
    return jsonify({'folders': folders, 'products': list(products.values())})



def fetch_google_sheet_as_df(sheet_url):
    """
    Превращает обычную ссылку на Google Таблицу в прямую ссылку на скачивание XLSX
    и сразу читает её через pandas.
    Пример: https://docs.google.com/spreadsheets/d/1ABC123.../edit#gid=0
    """
    try:
        if "/edit" in sheet_url:
            # Заменяем концовку ссылки на команду экспорта
            export_url = sheet_url.split('/edit')[0] + '/export?format=xlsx'
        else:
            export_url = sheet_url
            
        # Читаем напрямую из интернета в DataFrame
        df = pd.read_excel(export_url)
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке Google Таблицы: {e}")
        return None

@app.route('/api/excel/google_sheet/preview', methods=['POST'])
@login_required
def preview_google_sheet_route():
    data = request.json
    sheet_url = data.get('sheet_url')
    try:
        import requests
        from io import BytesIO
        from openpyxl import load_workbook

        if "/edit" in sheet_url:
            export_url = sheet_url.split('/edit')[0] + '/export?format=xlsx'
        else:
            export_url = sheet_url
            
        resp = requests.get(export_url, timeout=30)
        if resp.status_code != 200:
            return jsonify({'error': 'Не удалось скачать таблицу'}), 400

        # Загружаем книгу ПРАВИЛЬНО (не в режиме read_only)
        xls_file = BytesIO(resp.content)
        wb = load_workbook(xls_file, data_only=True) # data_only=True читает значения, а не формулы
        
        sheets_data = {}
        # Оставляем только видимые листы
        visible_sheets = [s.title for s in wb.worksheets if s.sheet_state == 'visible']
        
        for sn in visible_sheets:
            ws = wb[sn]
            rows_list = []
            count = 0
            
            # Итерируемся по строкам напрямую через openpyxl
            for row in ws.iter_rows(values_only=True):
                # Проверяем, не скрыта ли строка
                row_idx = count + 1
                if row_idx in ws.row_dimensions and ws.row_dimensions[row_idx].hidden:
                    count += 1
                    continue
                
                # Очищаем данные от None
                clean_row = [str(cell) if cell is not None else "" for cell in row]
                rows_list.append(clean_row)
                count += 1
                
                # Ограничиваем превью 15 видимыми строками
                if len(rows_list) >= 15:
                    break
            
            sheets_data[sn] = rows_list
            
        return jsonify({'success': True, 'sheets_data': sheets_data})
    except Exception as e:
        logger.error(f"GS Preview Error: {e}")
        return jsonify({'error': str(e)}), 500



import pdfplumber

@app.route('/api/pdf/preview', methods=['POST'])
@login_required
def preview_pdf_route():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    
    file = request.files['file']
    try:
        with pdfplumber.open(file) as pdf:
            if not pdf.pages:
                return jsonify({'error': 'PDF пустой'}), 400
            
            # Извлекаем таблицу с первой страницы
            table = pdf.pages[0].extract_table()
            if not table:
                return jsonify({'error': 'Не удалось найти чёткую таблицу в PDF (возможно, нет линий сетки).'}), 400
            
            # Очищаем таблицу от пустых значений (None) и переносов строк
            cleaned_table = []
            for row in table:
                cleaned_table.append([str(cell).replace('\n', ' ') if cell is not None else "" for cell in row])
                
        # Возвращаем в формате "1 лист" (т.к. у PDF нет листов как в Excel)
        return jsonify({'success': True, 'sheets_data': {'Страница 1': cleaned_table[:30]}}) # берем первые 30 строк для превью
    except Exception as e:
        logger.error(f"PDF Preview Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pdf/save_config', methods=['POST'])
@login_required
def save_pdf_config():
    data = request.json
    db = get_db()
    try:
        # Сохраняем в ту же таблицу, но с меткой source_type = 'pdf'
        db.execute("""
            INSERT INTO excel_configs 
            (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col, source_type, parse_interval) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pdf', ?)
            ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
            name_col=excluded.name_col, price_col=excluded.price_col, 
            block_step=excluded.block_step, start_row=excluded.start_row,
            is_grouped=excluded.is_grouped, sku_col=excluded.sku_col, source_type='pdf'
        """, (
            session['user_id'], data['chat_id'], data['sheet_name'], 
            data['name_col'], 0, data['price_col'], data.get('price_row_offset', 0), 
            data.get('block_step', 1), data['start_row'], data.get('is_grouped', 0), 
            data.get('sku_col', -1), data.get('parse_interval', 60)
        ))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel/google_sheet/save_config', methods=['POST'])
@login_required
def save_gs_config():
    data = request.json
    chat_id = data.get('chat_id')
    if not chat_id:
        return jsonify({'error': 'Ошибка: Выбор чата поставщика обязателен!'}), 400
    db = get_db()
    try:
        db.execute("""
            INSERT INTO excel_configs 
            (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col, source_type, sheet_url, parse_interval) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'google_sheet', ?, ?)
            ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
            name_col=excluded.name_col, name_row_offset=excluded.name_row_offset, price_col=excluded.price_col, 
            price_row_offset=excluded.price_row_offset, block_step=excluded.block_step, start_row=excluded.start_row,
            is_grouped=excluded.is_grouped, sku_col=excluded.sku_col, source_type=excluded.source_type, 
            sheet_url=excluded.sheet_url, parse_interval=excluded.parse_interval
        """, (
            session['user_id'], data['chat_id'], data['sheet_name'], 
            data['name_col'], 0, data['price_col'], data.get('price_row_offset', 0), 
            data.get('block_step', 1), data['start_row'], data.get('is_grouped', 0), 
            data.get('sku_col', -1), data['sheet_url'], data['parse_interval']
        ))
        
        # Удаляем из missing_sheets (убираем плашку "Не настроен лист")
        cid_str = str(data['chat_id'])
        core_id = cid_str[4:] if cid_str.startswith('-100') else (cid_str[1:] if cid_str.startswith('-') else cid_str)
        possible_ids = [core_id, f"-{core_id}", f"-100{core_id}"]
        for pid in possible_ids:
            if data['sheet_name'] == '*':
                db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND user_id = ?", (pid, session['user_id']))
            else:
                db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND sheet_name = ? AND user_id = ?", (pid, data['sheet_name'], session['user_id']))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига Google Sheet: {e}")
        return jsonify({'error': str(e)}), 500


def format_price_list(template, products):
    """Вставляет товары в шаблон между маркерами START_PRICE_LIST и END_PRICE_LIST."""
    lines = []
    for p in products:
        lines.append(f"• {p['name']} — {p['price']} руб.")
    price_list = "\n".join(lines)
    
    if "<!-- START_PRICE_LIST -->" in template and "<!-- END_PRICE_LIST -->" in template:
        # Заменяем только часть между маркерами
        parts = template.split("<!-- START_PRICE_LIST -->")
        before = parts[0]
        after = parts[1].split("<!-- END_PRICE_LIST -->")[1]
        return before + "<!-- START_PRICE_LIST -->\n" + price_list + "\n<!-- END_PRICE_LIST -->" + after
    else:
        # Иначе заменяем всё сообщение
        return price_list

# Словарь в памяти для отслеживания времени последней публикации
# Словарь в памяти для отслеживания времени последней публикации
last_publish_times = {}

def publish_scheduler():
    """Фоновый поток для публикации каталогов в Telegram."""
    time.sleep(10)
    with app.app_context():
        while True:
            try:
                db = get_db()
                # Берем все активные публикации
                pubs = db.execute("SELECT * FROM publications WHERE is_active = 1").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for pub in pubs:
                    pub_id = pub['id']
                    interval_minutes = pub['interval_min'] or 60
                    
                    # 1. ПРОВЕРКА ИНТЕРВАЛА
                    last_run = last_publish_times.get(pub_id)
                    if last_run:
                        diff_minutes = (now - last_run).total_seconds() / 60
                        if diff_minutes < interval_minutes:
                            continue  # Время публикации еще не пришло
                    
                    # 2. ПОЛУЧЕНИЕ ЦЕН И ТОВАРОВ
                    userbot_id = pub['userbot_id']
                    if userbot_id not in user_clients: continue
                    
                    # Извлекаем user_id из юзербота
                    _, tg_client, user_id = user_clients[userbot_id] 
                    
                    allowed_items = {}
                    try: allowed_items = json.loads(pub['allowed_items']) 
                    except: pass
                    
                    if not allowed_items: continue
                    
                    # ---- НОВОЕ: ДОСТАЕМ НАЦЕНКИ ----
                    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM pub_markups WHERE pub_id=?", (pub_id,)).fetchall()
                    markups = {}
                    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
                    for m in markups_raw:
                        if m['folder_id'] == 0: default_markup = dict(m)
                        else: markups[m['folder_id']] = dict(m)
                    # --------------------------------
                    
                    final_products = []
                    
                    # Перебираем товары и разрешенных поставщиков из JSON
                    # Перебираем товары и разрешенных поставщиков из JSON
                    for p_id_str, allowed_suppliers in allowed_items.items():
                        if not allowed_suppliers: 
                            continue
                        
                        # --- НОВАЯ ЛОГИКА ДЛЯ "ЦЕНА ПО ЗАПРОСУ" ---
                        if '0' in allowed_suppliers or 0 in allowed_suppliers:
                            # У товара нет цен, достаем из базы только его название
                            p_info = db.execute("SELECT name FROM products WHERE id = ?", (int(p_id_str),)).fetchone()
                            if p_info:
                                final_products.append({
                                    'name': p_info['name'], 
                                    'price': 'Цена по запросу'
                                })
                            continue  # Пропускаем запросы цен, идем к следующему товару
                        # -----------------------------------------

                        placeholders = ','.join(['?'] * len(allowed_suppliers))
                        query_params = [int(p_id_str)] + allowed_suppliers
                        
                        # ВАЖНО: Добавлено p.folder_id в SELECT
                        price_row = db.execute(f"""
                            SELECT pm.extracted_price, p.name, p.folder_id 
                            FROM product_messages pm 
                            JOIN messages m ON pm.message_id = m.id 
                            JOIN products p ON p.id = pm.product_id
                            WHERE pm.product_id = ? 
                              AND pm.status = 'confirmed' 
                              AND pm.is_actual = 1 
                              AND m.chat_id IN ({placeholders})
                            ORDER BY m.date DESC LIMIT 1
                        """, query_params).fetchone()
                        
                        if price_row and price_row['extracted_price'] is not None:
                            # -------- ПРИМЕНЯЕМ НАЦЕНКУ ПО ПАПКАМ ---------
                            base_price = float(price_row['extracted_price'])
                            f_id = price_row['folder_id'] or 0
                            mk = markups.get(f_id, default_markup)
                            
                            if mk['markup_type'] == 'percent':
                                final_price = base_price * (1 + mk['markup_value'] / 100)
                            else:
                                final_price = base_price + mk['markup_value']
                                
                            rnd = mk['rounding']
                            if rnd > 0:
                                final_price = math.ceil(final_price / rnd) * rnd

                            final_products.append({
                                'name': price_row['name'], 
                                'price': int(final_price)
                            })
                    
                    if not final_products: continue

                    template = pub['template'] or "🔥 Актуальные цены:\n\n{prices}\n\nДля заказа пишите менеджеру!"
                    
                    # 1. Красиво собираем товары в текстовый список
                    prices_lines = []
                    for p in final_products:
                        if p['price'] == 'Цена по запросу':
                            prices_lines.append(f"▪️ {p['name']} — {p['price']}")
                        else:
                            prices_lines.append(f"▪️ {p['name']} — {p['price']} ₽")
                            
                    prices_text = "\n".join(prices_lines)
                    
                    # 2. Подставляем список вместо метки {prices}
                    if "{prices}" in template:
                        message_text = template.replace("{prices}", prices_text)
                    else:
                        # Если вы случайно забыли написать {prices} в шаблоне, 
                        # бот просто аккуратно приклеит прайс-лист в самый низ сообщения
                        message_text = f"{template}\n\n{prices_text}"

                    loop = user_loops.get(userbot_id)
                    if not loop or not loop.is_running(): continue

                    chat_id = pub['chat_id']
                    message_id = pub['message_id']
                    
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    async def update_or_create(cid, mid, txt, p_id):
                        try:
                            cid = int(cid)
                        except ValueError:
                            pass

                        # --- ЛОГИКА РАЗДЕЛЕНИЯ ТЕКСТА ---
                        parts = []
                        max_len = 4000 # Оставляем небольшой запас до 4096
                        temp_txt = txt
                        
                        while len(temp_txt) > max_len:
                            # Ищем последний перенос строки в пределах лимита, чтобы не рвать строку пополам
                            split_idx = temp_txt.rfind('\n', 0, max_len)
                            if split_idx == -1:
                                split_idx = max_len # Если нет переносов, режем жестко
                                
                            parts.append(temp_txt[:split_idx])
                            temp_txt = temp_txt[split_idx:].strip()
                            
                        if temp_txt:
                            parts.append(temp_txt)
                        # --------------------------------

                        if mid:
                            try:
                                # ВАЖНЫЙ НЮАНС ПРИ РЕДАКТИРОВАНИИ: 
                                # Telegram не позволяет превратить 1 старое сообщение в 2 новых.
                                # Поэтому при редактировании обновится только первая часть, а остаток обрежется.
                                await tg_client.edit_message(cid, int(mid), parts[0])
                                if len(parts) > 1:
                                    logger.warning(f"Прайс слишком большой для обновления одного сообщения (ID {mid}). Хвост обрезан.")
                            except Exception as e:
                                logger.error(f"Ошибка редактирования: {e}")
                        else:
                            try:
                                # В режиме "отправки нового" просто шлем все куски друг за другом
                                first_msg = None
                                for part in parts:
                                    msg = await tg_client.send_message(cid, part)
                                    if not first_msg:
                                        first_msg = msg # Запоминаем только самое первое сообщение

                                # Сохраняем в базу ID первого сообщения (если захотите его потом удалять/редактировать)
                                if first_msg:
                                    with app.app_context():
                                        local_db = get_db()
                                        local_db.execute("UPDATE publications SET message_id = ? WHERE id = ?", (str(first_msg.id), p_id))
                                        local_db.commit()
                            except Exception as e:
                                logger.error(f"Ошибка отправки разделенного сообщения: {e}")
                                
                    try:
                        asyncio.run_coroutine_threadsafe(update_or_create(chat_id, message_id, message_text, pub_id), loop)
                        last_publish_times[pub_id] = now
                    except Exception as e:
                        logger.error(f"Ошибка передачи в цикл публикации: {e}")

            except Exception as e:
                logging.error(f"Ошибка в планировщике публикации: {e}")
            time.sleep(60)


def get_catalog_for_client(client_id):
    """Возвращает каталог для указанного клиента (как в /api/v1/catalog)."""
    db = get_db()
    client = db.execute("SELECT * FROM api_clients WHERE id=?", (client_id,)).fetchone()
    if not client:
        return None

    user_id = client['user_id']
    
    if client['schedule_enabled']:
        from datetime import timedelta
        now = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
        start_time = datetime.strptime(client['time_start'], '%H:%M').time()
        end_time = datetime.strptime(client['time_end'], '%H:%M').time()
        
        is_active = start_time <= now <= end_time if start_time <= end_time else (now >= start_time or now <= end_time)
        if not is_active: return {'categories': [], 'products': []}
        
    user_id = client['user_id']
    client_id = client['id']
    
    allowed_folders = client['allowed_folders'] if 'allowed_folders' in client.keys() else 'all'
    allowed_chats = client['allowed_chats'] if 'allowed_chats' in client.keys() else 'all'

    folders_list = allowed_folders.split(',') if allowed_folders not in ('all', '', None) else allowed_folders
    chats_list = allowed_chats.split(',') if allowed_chats not in ('all', '', None) else allowed_chats

    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM api_markups WHERE client_id=?", (client_id,)).fetchall()
    markups = {}
    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
    for m in markups_raw:
        if m['folder_id'] == 0: default_markup = dict(m)
        else: markups[m['folder_id']] = dict(m)
            
    products_raw = db.execute("""
        SELECT p.id, p.name, p.folder_id, p.price as manual_price, p.photo_url, p.brand, p.country, p.weight, p.model_number, p.is_on_request,
               p.color, p.storage, p.ram, p.warranty, p.description, p.specs,
               (SELECT pm.extracted_price FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as parsed_price,
               (SELECT pm.is_actual FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as is_actual,
               (SELECT m.chat_id FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as latest_chat_id
        FROM products p
        WHERE p.user_id = ?
    """, (user_id,)).fetchall()
    
    products = []
    active_folder_ids = set()
    
    try:
        access_rules = json.loads(client['access_rules']) if 'access_rules' in client.keys() and client['access_rules'] else {}
    except:
        access_rules = {}
        
    for p in products_raw:
        if not access_rules: continue
        
        pid_str = str(p['id'])
        if pid_str not in access_rules: continue
        
        allowed_chats = access_rules[pid_str]
        chat_id_str = str(p['latest_chat_id'])
        
        if chat_id_str != 'None':
            if allowed_chats != ['all'] and chat_id_str not in allowed_chats: 
                continue
        
        base_price = float(p['parsed_price'] if (p['parsed_price'] is not None and p['is_actual'] == 1) else (p['manual_price'] or 0))
        
        if base_price > 0 and not p['is_on_request']:
            mk = markups.get(p['folder_id'], default_markup)
            if mk['markup_type'] == 'percent':
                final_price = base_price * (1 + mk['markup_value'] / 100)
            else:
                final_price = base_price + mk['markup_value']
                
            rnd = mk['rounding']
            if rnd > 0:
                final_price = math.ceil(final_price / rnd) * rnd
            final_price = int(final_price)
        else:
            final_price = "По запросу"
            
        photo_list = []
        if p['photo_url']:
            for url in p['photo_url'].split(','):
                url = url.strip()
                if url:
                    if url.startswith('http'):
                        photo_list.append(url)
                    else:
                        photo_list.append(f"https://engine.astoredirect.ru/uploads/{url}")

        # Единый, правильный парсинг specs
        specs_data = {}
        if p['specs']:
            try:
                specs_data = json.loads(p['specs'])
            except Exception:
                pass

        products.append({
            'id': p['id'],
            'name': p['name'],
            'category_id': p['folder_id'],
            'price': final_price,
            'price2': "По запросу" ,
            'photo_url': photo_list, 
            'brand': p['brand'],
            'country': p['country'],
            'weight': p['weight'],
            'model_number': p['model_number'],
            'is_on_request': bool(p['is_on_request']),
            
            # Все системные поля и объект характеристик
            'color': p['color'],
            'storage': p['storage'],
            'ram': p['ram'],
            'warranty': p['warranty'],
            'description': p['description'],
            'specs': specs_data
        })
        
        if p['folder_id']:
            active_folder_ids.add(p['folder_id'])
            
    folders_raw = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    
    parent_map = {f['id']: f['parent_id'] for f in folders_raw}
    folders_to_keep = set(active_folder_ids)
    
    for fid in active_folder_ids:
        current = fid
        while current in parent_map and parent_map[current] is not None:
            current = parent_map[current]
            folders_to_keep.add(current)
            
    categories = [{'id': f['id'], 'name': f['name'], 'parent_id': f['parent_id']} 
                  for f in folders_raw if f['id'] in folders_to_keep]
        
    return {
        'categories': categories,
        'products': products
    }

def google_sheets_scheduler():
    """Фоновый поток для парсинга Гугл Таблиц по расписанию с учетом скрытых строк"""
    import requests
    from io import BytesIO
    from openpyxl import load_workbook
    
    time.sleep(5)
    last_parsed = {}
    
    with app.app_context():
        while True:
            try:
                db = get_db()
                # Берем все конфиги для Google Таблиц
                configs = db.execute("SELECT * FROM excel_configs WHERE source_type = 'google_sheet'").fetchall()
                current_time = time.time()
                
                # Группируем по URL, чтобы не скачивать один и тот же файл многократно
                url_configs = {}
                for c in configs:
                    url = c['sheet_url']
                    if url not in url_configs: url_configs[url] = []
                    url_configs[url].append(c)

                for url, conf_list in url_configs.items():
                    # Проверяем интервал по первому конфигу в группе
                    first_conf = conf_list[0]
                    cid = first_conf['id']
                    interval_sec = int(first_conf['parse_interval'] or 60) * 60
                    
                    if cid not in last_parsed or (current_time - last_parsed[cid]) >= interval_sec:
                        logger.info(f"Парсинг Гугл Таблицы {url} по расписанию...")
                        
                        try:
                            # Формируем ссылку на экспорт
                            export_url = url.split('/edit')[0] + '/export?format=xlsx' if "/edit" in url else url
                            resp = requests.get(export_url, timeout=30)
                            if resp.status_code != 200:
                                logger.error(f"Не удалось скачать таблицу: {resp.status_code}")
                                continue
                            
                            # Загружаем книгу через openpyxl (БЕЗ read_only, чтобы видеть скрытые строки)
                            xls_file = BytesIO(resp.content)
                            wb = load_workbook(xls_file, data_only=True)
                        except Exception as e:
                            logger.error(f"Ошибка загрузки книги: {e}")
                            continue

                        # Определяем видимые листы
                        visible_sheets = [s.title for s in wb.worksheets if s.sheet_state == 'visible']

                        for config in conf_list:
                            c_dict = dict(config) # Конвертируем Row в dict
                            sheet_target = c_dict['sheet_name']
                            user_id = c_dict['user_id']
                            chat_id = c_dict['chat_id']
                            
                            # Пропускаем, если конкретный лист скрыт
                            if sheet_target != '*' and sheet_target not in visible_sheets:
                                continue 
                                
                            sheets_to_parse = visible_sheets if sheet_target == '*' else [sheet_target]
                            
                            parsed_lines = []
                            for sn in sheets_to_parse:
                                if sn not in wb.sheetnames: continue
                                ws = wb[sn]
                                
                                # Превращаем данные листа в список строк для удобства
                                # Индексация в all_rows будет с 0, а в Excel (row_dimensions) с 1
                                all_rows = list(ws.iter_rows(values_only=True))
                                
                                step = int(c_dict.get('block_step', 1))
                                start = int(c_dict.get('start_row', 0))
                                n_col = int(c_dict.get('name_col', 0))
                                p_col = int(c_dict.get('price_col', 1))
                                n_off = int(c_dict.get('name_row_offset', 0))
                                p_off = int(c_dict.get('price_row_offset', 0))
                                is_grouped = c_dict.get('is_grouped', 0)
                                sku_col = c_dict.get('sku_col', -1)

                                if is_grouped == 1:
                                    # --- ЛОГИКА ДЛЯ СГРУППИРОВАННЫХ ТАБЛИЦ ---
                                    current_group = ""
                                    for i in range(start, len(all_rows)):
                                        # ПРОВЕРКА: Скрыта ли строка? (openpyxl индексирует с 1)
                                        if i + 1 in ws.row_dimensions and ws.row_dimensions[i + 1].hidden:
                                            continue
                                            
                                        row = all_rows[i]
                                        col_n_val = str(row[n_col]).strip() if n_col < len(row) and row[n_col] is not None else ""
                                        col_sku_val = str(row[sku_col]).strip() if sku_col != -1 and sku_col < len(row) and row[sku_col] is not None else ""
                                        col_p_val = str(row[p_col]).strip() if p_col < len(row) and row[p_col] is not None else ""
                                        
                                        has_price = (col_p_val and col_p_val.lower() != 'none' and col_p_val.lower() != 'nan')
                                        
                                        # Проверка цены
                                        extracted = extract_price(col_p_val) if has_price else None
                                        if extracted is not None and extracted <= 0: continue

                                        if n_col == sku_col:
                                            if col_n_val and not has_price: current_group = col_n_val
                                            elif col_n_val and has_price and current_group:
                                                parsed_lines.append(f"{current_group} {col_n_val} - {col_p_val}")
                                        else:
                                            if col_n_val and col_n_val.lower() != 'none' and not has_price: 
                                                current_group = col_n_val
                                            if col_sku_val and has_price and current_group:
                                                parsed_lines.append(f"{current_group} {col_sku_val} - {col_p_val}")
                                else:
                                    # --- ЛОГИКА ДЛЯ ОБЫЧНЫХ ТАБЛИЦ ---
                                    for i in range(start, len(all_rows), step):
                                        # Проверка на скрытость основной строки
                                        if i + 1 in ws.row_dimensions and ws.row_dimensions[i + 1].hidden:
                                            continue
                                            
                                        name_idx = i + n_off
                                        price_idx = i + p_off
                                        
                                        if name_idx < len(all_rows) and price_idx < len(all_rows):
                                            # Проверяем на скрытость строки со смещением
                                            if (name_idx + 1 in ws.row_dimensions and ws.row_dimensions[name_idx + 1].hidden) or \
                                               (price_idx + 1 in ws.row_dimensions and ws.row_dimensions[price_idx + 1].hidden):
                                                continue
                                                
                                            row_n = all_rows[name_idx]
                                            row_p = all_rows[price_idx]
                                            
                                            n_val = str(row_n[n_col]).strip() if n_col < len(row_n) and row_n[n_col] is not None else ""
                                            p_val = str(row_p[p_col]).strip() if p_col < len(row_p) and row_p[p_col] is not None else ""
                                            
                                            if n_val and p_val and n_val.lower() != 'none' and p_val.lower() != 'none':
                                                extracted = extract_price(p_val)
                                                if extracted is not None and extracted > 0:
                                                    parsed_lines.append(f"{n_val} - {p_val}")

                            if parsed_lines:
                                msg_text = "📊 [GOOGLE ТАБЛИЦА]:\n" + "\n".join(parsed_lines)
                                tc = db.execute("SELECT custom_name, chat_title FROM tracked_chats WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)).fetchone()
                                chat_title = tc['custom_name'] if tc and tc['custom_name'] else (tc['chat_title'] if tc else str(chat_id))
                                
                                from datetime import timedelta
                                now_str = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                                msg_type = f"excel_{c_dict['id']}" 
                                
                                # Сохраняем сообщение в базу
                                cursor = db.execute(
                                    "INSERT INTO messages (user_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (user_id, msg_type, msg_text, now_str, chat_id, chat_title, f"Лист: {sheet_target}")
                                )
                                new_msg_id = cursor.lastrowid
                                
                                # --- МИГРАЦИЯ ПРИВЯЗОК ---
                                old_bindings = db.execute("""
                                    SELECT pm.id, pm.line_index, p.synonyms
                                    FROM product_messages pm
                                    JOIN products p ON pm.product_id = p.id
                                    JOIN messages m ON pm.message_id = m.id
                                    WHERE m.chat_id = ? AND m.user_id = ? AND m.id != ? AND m.type = ?
                                """, (chat_id, user_id, new_msg_id, msg_type)).fetchall()

                                if old_bindings:
                                    lines = msg_text.split('\n')
                                    for b in old_bindings:
                                        synonyms = [s.strip().lower() for s in (b['synonyms'] or "").split(',') if s.strip()]
                                        match_found = False
                                        new_price = None
                                        new_line_idx = 0
                                        
                                        for i, line in enumerate(lines):
                                            if any(syn in line.lower() for syn in synonyms):
                                                match_found = True
                                                new_price = extract_price(line)
                                                new_line_idx = i
                                                break
                                        
                                        if match_found and new_price and new_price > 0:
                                            db.execute("UPDATE product_messages SET message_id = ?, line_index = ?, extracted_price = ?, is_actual = 1 WHERE id = ?", (new_msg_id, new_line_idx, new_price, b['id']))
                                        else:
                                            db.execute("UPDATE product_messages SET is_actual = 0 WHERE id = ?", (b['id'],))
                                            
                                # Чистим старые сообщения этого листа
                                db.execute("""
                                    DELETE FROM messages 
                                    WHERE chat_id = ? AND user_id = ? AND id != ? AND type = ? 
                                    AND id NOT IN (SELECT message_id FROM product_messages)
                                """, (chat_id, user_id, new_msg_id, msg_type))
                                
                                # --- АВТО-ПРИВЯЗКА НОВЫХ ---
                                all_prods = db.execute("SELECT id, synonyms FROM products WHERE user_id=?", (user_id,)).fetchall()
                                for idx, line in enumerate(parsed_lines):
                                    ext_p = extract_price(line)
                                    if not ext_p or ext_p <= 0: continue
                                    line_l = line.lower()
                                    for prod in all_prods:
                                        syns = [s.strip().lower() for s in (prod['synonyms'] or '').split(',') if s.strip()]
                                        if syns and any(syn in line_l for syn in syns):
                                            exists = db.execute("SELECT id FROM product_messages WHERE product_id=? AND message_id=?", (prod['id'], new_msg_id)).fetchone()
                                            if not exists:
                                                db.execute("INSERT INTO product_messages (product_id, message_id, extracted_price, status, is_actual, line_index) VALUES (?, ?, ?, 'pending', 1, ?)", (prod['id'], new_msg_id, ext_p, idx + 1))
                                            break
                                            
                                db.commit()
                                notify_clients()
                                logger.info(f"✅ Успешно: Лист {sheet_target} из {url}")
                        
                        # Запоминаем время последнего парсинга
                        for config in conf_list:
                            last_parsed[config['id']] = current_time

            except Exception as e:
                logger.error(f"Глобальная ошибка планировщика GS: {e}", exc_info=True)
                
            time.sleep(60) # Проверка раз в минуту

# Запускаем поток вместе с сервером
threading.Thread(target=google_sheets_scheduler, daemon=True).start()



@app.route('/api/messages/<int:msg_id>/block', methods=['POST'])
@login_required
def block_message(msg_id):
    db = get_db()
    # Устанавливаем флаг блокировки для сообщения
    db.execute("UPDATE messages SET is_blocked = 1 WHERE id = ? AND user_id = ?", (msg_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/product_messages/merge', methods=['POST'])
@login_required
def merge_product_messages():
    data = request.get_json()
    pm_ids = data.get('pm_ids', [])
    if len(pm_ids) < 2:
        return jsonify({'error': 'Выберите минимум 2 сообщения'}), 400
    
    db = get_db()
    # Берем минимальный ID как главный номер группы
    group_id = min(map(int, pm_ids))
    placeholders = ','.join('?' for _ in pm_ids)
    
    db.execute(f"UPDATE product_messages SET group_id = ? WHERE id IN ({placeholders})", [group_id] + pm_ids)
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/products/<int:source_id>/merge', methods=['POST'])
@login_required
def merge_product(source_id):
    target_id = request.get_json()['target_id']
    user_id = session['user_id']
    db = get_db()
    
    # Получаем синонимы обоих товаров
    src = db.execute("SELECT synonyms FROM products WHERE id=? AND user_id=?", (source_id, user_id)).fetchone()
    tgt = db.execute("SELECT synonyms FROM products WHERE id=? AND user_id=?", (target_id, user_id)).fetchone()
    
    if src and tgt:
        src_syns = [s.strip() for s in src['synonyms'].split(',') if s.strip()]
        tgt_syns = [s.strip() for s in tgt['synonyms'].split(',') if s.strip()]
        combined = list(set(src_syns + tgt_syns)) # Объединяем без дубликатов
        
        # Обновляем синонимы целевого товара
        db.execute("UPDATE products SET synonyms=? WHERE id=?", (', '.join(combined), target_id))
        
        # Переносим привязанные сообщения (игнорируем ошибку, если такое сообщение уже привязано)
        try:
            db.execute("UPDATE OR IGNORE product_messages SET product_id=? WHERE product_id=?", (target_id, source_id))
        except:
            pass
            
        # Удаляем старый товар
        db.execute("DELETE FROM products WHERE id=?", (source_id,))
        db.commit()
        notify_clients()
    return jsonify({'success': True})

import re
@app.route('/api/profile/change_password', methods=['POST'])
@login_required
def change_password():
    user_id = session['user_id']
    data = request.get_json()
    
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not old_password or not new_password:
        return jsonify({'error': 'Заполните все поля'}), 400

    db = get_db()
    
    # Хэшируем введённый старый пароль и проверяем его
    old_pwd_hash = hashlib.sha256(old_password.encode()).hexdigest()
    user = db.execute("SELECT id FROM users WHERE id = ? AND password_hash = ?", (user_id, old_pwd_hash)).fetchone()

    if not user:
        return jsonify({'error': 'Неверный старый пароль'}), 400

    # Если всё верно, хэшируем новый пароль и записываем в БД 
    new_pwd_hash = hashlib.sha256(new_password.encode()).hexdigest()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_pwd_hash, user_id))
    db.commit()

    return jsonify({'success': True})


async def fetch_chat_history(client, target_entity, chat_title, user_id, db_chat_id=None):
    """Парсинг старых сообщений с подробной статистикой"""
    if db_chat_id is None:
        db_chat_id = getattr(target_entity, 'id', target_entity)

    try:
        with app.app_context():
            db = get_db()
            added = 0
            duplicates = 0
            no_text = 0
            errors = 0
            
            async for message in client.iter_messages(target_entity): 
                # Пропускаем пустые (например, просто фото без подписи)
                if (not message.text and not message.document):
                    no_text += 1
                    continue
                
                try:
                    sender = await message.get_sender()
                    sender_name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip() or "Unknown"
                    msg_type = 'channel' if message.is_channel else 'group' if message.is_group else 'private'
                    
                    parsed_text = await parse_excel_message(client, message, db_chat_id, user_id)
                    if not parsed_text:
                        no_text += 1
                        continue

                    # ИСПРАВЛЕНИЕ: Форматируем дату вручную, чтобы не было DeprecationWarning
                    from datetime import timedelta
                    msg_date = (message.date + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

                    db.execute(
                        'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        (user_id, message.id, msg_type, parsed_text, msg_date, db_chat_id, chat_title, sender_name)
                    )
                    db.commit()
                    added += 1
                    
                    if added % 50 == 0:
                        
                        notify_clients()
                        
                except sqlite3.IntegrityError:
                    duplicates += 1  # Сообщение уже есть в базе
                except Exception as msg_e:
                    errors += 1
                    logger.warning(f"Ошибка в сообщении {message.id}: {msg_e}")
                    
            db.commit()
            notify_clients()
            
            # ВЫВОДИМ ПОДРОБНЫЙ ОТЧЕТ В КОНСОЛЬ
            logger.info(f"Парсинг [{chat_title}]: Добавлено {added} | Уже были в базе: {duplicates} | Без текста/фото: {no_text} | Ошибок: {errors}")
            
    except Exception as e:
        logger.error(f"Глобальная ошибка парсинга истории для {chat_title}: {e}")

@app.route('/api/api_clients/<int:client_id>/publish', methods=['POST'])
@login_required
def update_publish_settings(client_id):
    data = request.json
    db = get_db()
    # Проверяем, что клиент принадлежит текущему пользователю
    client = db.execute("SELECT id FROM api_clients WHERE id=? AND user_id=?", 
                        (client_id, session['user_id'])).fetchone()
    if not client:
        return jsonify({'error': 'Клиент не найден'}), 404

    db.execute("""
        UPDATE api_clients 
        SET publish_enabled=?, publish_chat_id=?, userbot_id=?, publish_template=?, folders=?, publish_interval=?
        WHERE id=?
    """, (data.get('publish_enabled', 0), data.get('publish_chat_id'), data.get('userbot_id'),
          data.get('publish_template'), data.get('folders'), data.get('publish_interval', 60), client_id))
    db.commit()
    return jsonify({'success': True})

# ---------- Декораторы авторизации ----------
def login_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        return f(*args, **kwargs)
    return wrapped

def admin_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        user_id = session['user_id']
        db = get_db()
        cursor = db.execute("SELECT role FROM users WHERE id = ?", (user_id,))
        row = cursor.fetchone()
        if not row or row['role'] != 'admin':
            return "Access denied", 403
        return f(*args, **kwargs)
    return wrapped

# ---------- Фоновый слушатель сообщений ----------
def stop_message_listener(session_id):
    client_info = user_clients.get(session_id)
    if client_info:
        _, client, _ = client_info
        loop = user_loops.get(session_id)
        if loop and loop.is_running():
            async def safe_disconnect():
                try:
                    await client.disconnect()
                except:
                    pass
            try:
                asyncio.run_coroutine_threadsafe(safe_disconnect(), loop)
            except:
                pass

    # ВАЖНО: Мы БОЛЬШЕ НЕ удаляем user_clients.pop() здесь!
    # Это сделает сам фоновый поток перед своей смертью.

    try:
        with app.app_context():
            db = get_db()
            db.execute("UPDATE user_telegram_sessions SET status = 'inactive' WHERE id = ?", (session_id,))
            db.commit()
            if 'notify_clients' in globals(): notify_clients()
    except Exception as e:
        logging.error(f"Ошибка БД при остановке сессии {session_id}: {e}")

@app.route('/logo.svg')
def serve_logo():
    # Отдаем файл logo.svg прямо из корня проекта
    return send_from_directory(os.getcwd(), 'logo.svg')

# Было: @app.route('/api/excel/configs/chat/<int:chat_id>', methods=['DELETE'])
@app.route('/api/excel/configs/chat/<chat_id>', methods=['DELETE'])
def delete_excel_chat_configs(chat_id):
    try:
        chat_id = int(chat_id)
        user_id = session.get('user_id')
        db = get_db()
        
        # 1. Удаляем привязки к товарам
        db.execute("""
            DELETE FROM product_messages 
            WHERE message_id IN (SELECT id FROM messages WHERE chat_id = ? AND user_id = ? AND type LIKE 'excel%')
        """, (chat_id, user_id))
        
        # 2. Удаляем сами сообщения (прайс-листы)
        db.execute("DELETE FROM messages WHERE chat_id = ? AND user_id = ? AND type LIKE 'excel%'", (chat_id, user_id))
        
        # 3. Удаляем конфигурацию парсера
        db.execute("DELETE FROM excel_configs WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Ошибка при удалении файла: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/publications/<int:pub_id>/markups', methods=['GET', 'POST'])
@login_required
def manage_pub_markups(pub_id):
    db = get_db()
    if request.method == 'GET':
        markups = db.execute("""
            SELECT pm.*, f.name as folder_name 
            FROM pub_markups pm 
            LEFT JOIN folders f ON pm.folder_id = f.id 
            WHERE pm.pub_id=?
        """, (pub_id,)).fetchall()
        return jsonify([dict(m) for m in markups])
    
    data = request.json
    folder_id = data.get('folder_id') or 0
    db.execute("""
        INSERT INTO pub_markups (pub_id, folder_id, markup_type, markup_value, rounding) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(pub_id, folder_id) DO UPDATE SET 
        markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
    """, (pub_id, folder_id, data['markup_type'], data['markup_value'], data['rounding']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/pub_markups/<int:markup_id>', methods=['DELETE'])
@login_required
def delete_pub_markup(markup_id):
    db = get_db()
    db.execute("DELETE FROM pub_markups WHERE id=? AND folder_id != 0", (markup_id,))
    db.commit()
    return jsonify({'success': True})


def start_message_listener(session_id, user_id, api_id, api_hash):
    with listener_locks.setdefault(session_id, threading.Lock()):
        if session_id in background_tasks:
            logging.info(f"Слушатель для сессии {session_id} уже запущен")
            return

        def listener():
            loop = asyncio.new_event_loop()
         
            asyncio.set_event_loop(loop)
            loop.set_exception_handler(custom_exception_handler)
            session_file = f'sessions/user_{user_id}_{session_id}'
            client = TelegramClient(session_file, int(api_id), api_hash)
            
            user_clients[session_id] = (threading.current_thread(), client, user_id)
            user_loops[session_id] = loop

           

   
            # --- НОВЫЙ БЛОК: СЛУШАЕМ УДАЛЕНИЯ ---
            @client.on(MessageDeleted)
            async def deleted_handler(event):
                with app.app_context():
                    db = get_db()
                    try:
                        for msg_id in event.deleted_ids:
                            # 1. Отвязываем товары
                            db.execute("""
                                UPDATE product_messages 
                                SET is_actual = 0 
                                WHERE message_id IN (SELECT id FROM messages WHERE telegram_message_id = ?)
                            """, (msg_id,))
                            
                            # 2. ФИЗИЧЕСКИ удаляем сообщение из базы
                            db.execute("""
                                DELETE FROM messages WHERE telegram_message_id = ?
                            """, (msg_id,))
                            
                        db.commit()
                    except sqlite3.OperationalError as e:
                        logger.warning(f"База занята, не удалось обновить удаленные сообщения: {e}")
                    finally:
                        notify_clients()

            @client.on(events.MessageEdited)
            async def edit_handler(event):
                message = event.message
                if (not message.text and not message.document):
                    return
                
                chat = await event.get_chat()
                
                # --- ИГНОРИРУЕМ ИСХОДЯЩИЕ ТОЛЬКО ДЛЯ БОТОВ ---
                if getattr(message, 'out', False) and event.is_private and getattr(chat, 'bot', False):
                    return
                # ---------------------------------------------
                
                chat_id = chat.id
                
              
                # --- УМНАЯ ИЗОЛЯЦИЯ ПАРСИНГА БОТОВ (ФИНАЛ) ---
                sender = await event.get_sender()
                is_bot = getattr(sender, 'bot', False)
                
                if is_bot:
                    bot_un = getattr(sender, 'username', '')
                    if not bot_un:
                        try:
                            entity = await client.get_entity(chat_id)
                            bot_un = getattr(entity, 'username', '')
                        except: pass
                        
                    bot_un_clean = bot_un.lower().replace('@', '').strip() if bot_un else ''
                    
                    # Генерируем все возможные варианты того, как бот мог быть записан в базу
                    bot_un_at = f"@{bot_un_clean}"
                    bot_link_1 = f"https://t.me/{bot_un_clean}"
                    bot_link_2 = f"t.me/{bot_un_clean}"
                    
                    print(f"\n[БОТ ДЕБАГ] 🤖 Пришло сообщение от бота: '{bot_un_clean}'. Chat ID: {chat_id}")
                    
                    with app.app_context():
                        local_db = get_db()
                        # Ищем любое из 4-х совпадений: ник, @ник, t.me/ник или https://t.me/ник
                        check_bot = local_db.execute(
                            "SELECT id FROM interaction_bots WHERE user_id = ? AND (LOWER(bot_username) IN (?, ?, ?, ?))",
                            (user_id, bot_un_clean, bot_un_at, bot_link_1, bot_link_2)
                        ).fetchone()
                        
                        if not check_bot:
                            print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот '{bot_un_clean}' не привязан к аккаунту {user_id}! Игнорируем.")
                            return
                        else:
                            print(f"[БОТ ДЕБАГ] ✅ БОТ ОДОБРЕН: '{bot_un_clean}' найден в базе. Идем дальше...")
                            
                            check_tracked = local_db.execute(
                                "SELECT id FROM tracked_chats WHERE chat_id = ? AND user_id = ?",
                                (chat_id, user_id)
                            ).fetchone()
                            
                            if not check_tracked:
                                print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот одобрен, но не добавлен в источники (tracked_chats)!")
                            else:
                                print(f"[БОТ ДЕБАГ] ✅ ПОЛНЫЙ УСПЕХ: Начинаем парсить!")
                # -------------------------------------------------------------
                
                # --- МАГИЯ EXCEL ---
                parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                if not parsed_text:
                    return

                with app.app_context():
                    db = get_db()
                    # 1. Обновляем текст в базе
                    db.execute("UPDATE messages SET text = ? WHERE telegram_message_id = ? AND chat_id = ? AND user_id = ?", 
                               (parsed_text, message.id, chat_id, user_id))
        
                    bindings = db.execute("""
                        SELECT pm.id, pm.line_index, p.synonyms 
                        FROM product_messages pm 
                        JOIN products p ON pm.product_id = p.id 
                        JOIN messages m ON pm.message_id = m.id
                        WHERE m.telegram_message_id = ? AND m.chat_id = ? AND m.user_id = ?
                    """, (message.id, chat_id, user_id)).fetchall()
        
                    lines = parsed_text.split('\n') if parsed_text else []

                    for b in bindings:
                        line_idx = b['line_index']
                        synonyms = [s.strip().lower() for s in b['synonyms'].split(',') if s.strip()]
                        
                        is_actual = 0
                        new_price = None
                        
                        # Если привязка идет ко всему сообщению (line_index = -1)
                        # Если привязка идет ко всему сообщению (line_index = -1)
                        if line_idx == -1:
                            text_lower = parsed_text.lower()
                            if any(syn in text_lower for syn in synonyms):
                                is_actual = 1
                                new_price = extract_price(parsed_text)
                                
                        # Если привязка идет к конкретной строке
                        else:
                            if 0 <= line_idx < len(lines):
                                edited_line = lines[line_idx]
                                text_lower = edited_line.lower()
                                
                                # Проверяем, остался ли товар в этой строке
                                if any(syn in text_lower for syn in synonyms):
                                    is_actual = 1
                                    # ДОСТАЕМ НОВУЮ ЦЕНУ ИЗ ОТРЕДАКТИРОВАННОЙ СТРОКИ
                                    new_price = extract_price(edited_line)
                        
                        # Обновляем и статус актуальности, и НОВУЮ ЦЕНУ
                        db.execute("""
                            UPDATE product_messages 
                            SET is_actual = ?, extracted_price = ? 
                            WHERE id = ?
                        """, (is_actual, new_price, b['id']))
            
                    db.commit()
                    notify_clients()

            # --------------------------------------------------
            # --------------------------------------------------

            

            @client.on(events.NewMessage)
            async def handler(event):
                message = event.message
                
                if (not message.text and not message.document):
                    return

                chat = await event.get_chat()

                # --- ИГНОРИРУЕМ ИСХОДЯЩИЕ ТОЛЬКО ДЛЯ БОТОВ ---
                if getattr(message, 'out', False) and event.is_private and getattr(chat, 'bot', False):
                    return
                # ---------------------------------------------

                allowed_now = is_parsing_allowed(session_id)
                is_delayed = 0 if allowed_now else 1
                
                chat_id = chat.id

                # Проверка: отслеживаем ли мы этот чат?
                with app.app_context():
                    db = get_db()
                    cursor = db.execute(
                        "SELECT COUNT(*) FROM tracked_chats WHERE user_id = ? AND chat_id = ?",
                        (user_id, chat_id)
                    )
                    count = cursor.fetchone()[0]
                    
                    # СТРОГОЕ ПРАВИЛО: Если чат не добавлен в список - игнорируем сообщение!
                    if count == 0:
                        return

                # Определяем тип и название чата
                if event.is_private:
                    first = getattr(chat, 'first_name', '')
                    last = getattr(chat, 'last_name', '')
                    chat_title = f"{first} {last}".strip() or "Unknown"
                    msg_type = 'private'
                elif event.is_group:
                    chat_title = getattr(chat, 'title', 'Untitled Group')
                    msg_type = 'group'
                elif event.is_channel:
                    chat_title = getattr(chat, 'title', 'Untitled Channel')
                    msg_type = 'channel'
                else:
                    chat_title = "Unknown"
                    msg_type = 'unknown'

                # --- УМНАЯ ИЗОЛЯЦИЯ ПАРСИНГА БОТОВ (ФИНАЛ) ---
                sender = await event.get_sender()
                is_bot = getattr(sender, 'bot', False)
                sender_name = getattr(sender, 'username', '')
                if not sender_name:
                    sender_name = getattr(sender, 'first_name', 'Бот')
                    
                if is_bot:
                    bot_un = getattr(sender, 'username', '')
                    if not bot_un:
                        try:
                            entity = await client.get_entity(chat_id)
                            bot_un = getattr(entity, 'username', '')
                        except: pass
                        
                    bot_un_clean = bot_un.lower().replace('@', '').strip() if bot_un else ''
                    
                    # Генерируем все возможные варианты того, как бот мог быть записан в базу
                    bot_un_at = f"@{bot_un_clean}"
                    bot_link_1 = f"https://t.me/{bot_un_clean}"
                    bot_link_2 = f"t.me/{bot_un_clean}"
                    
                    print(f"\n[БОТ ДЕБАГ] 🤖 Пришло сообщение от бота: '{bot_un_clean}'. Chat ID: {chat_id}")
                    
                    with app.app_context():
                        local_db = get_db()
                        # Ищем любое из 4-х совпадений: ник, @ник, t.me/ник или https://t.me/ник
                        check_bot = local_db.execute(
                            "SELECT id FROM interaction_bots WHERE user_id = ? AND (LOWER(bot_username) IN (?, ?, ?, ?))",
                            (user_id, bot_un_clean, bot_un_at, bot_link_1, bot_link_2)
                        ).fetchone()
                        
                        if not check_bot:
                            print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот '{bot_un_clean}' не привязан к аккаунту {user_id}! Игнорируем.")
                            return
                        else:
                            print(f"[БОТ ДЕБАГ] ✅ БОТ ОДОБРЕН: '{bot_un_clean}' найден в базе. Идем дальше...")
                            
                            check_tracked = local_db.execute(
                                "SELECT id FROM tracked_chats WHERE chat_id = ? AND user_id = ?",
                                (chat_id, user_id)
                            ).fetchone()
                            
                            if not check_tracked:
                                print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот одобрен, но не добавлен в источники (tracked_chats)!")
                                return
                            else:
                                print(f"[БОТ ДЕБАГ] ✅ ПОЛНЫЙ УСПЕХ: Начинаем парсить!")
                # -------------------------------------------------------------

                parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                
                # Если после парсинга текста нет (например, пустой Excel или не настроен), прерываем
                if not parsed_text:
                    return

                with app.app_context():
                    db = get_db()
                    try:
                        # 1. Записываем новое сообщение
                        from datetime import timedelta
                        msg_date = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                        cursor = db.execute(
                            'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name, is_delayed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            (user_id, message.id, msg_type, parsed_text, msg_date, chat_id, chat_title, sender_name, is_delayed)
                        )
                        db.commit()

                        # ТАМОЖНЯ: Если время нерабочее - выходим (не обновляем интерфейс мгновенно)
                        if not allowed_now:
                            return

                        # Оповещаем веб-интерфейс о новом сообщении
                        notify_clients()
                        
                        # Сохраняем имя чата, если оно ранее не было сохранено
                        if chat_title:
                            db.execute(
                                "UPDATE tracked_chats SET chat_title = ? WHERE user_id = ? AND chat_id = ? AND chat_title IS NULL",
                                (chat_title, user_id, chat_id)
                            )
                            db.commit()
                            notify_clients()
                            
                    except sqlite3.IntegrityError:
                        pass

            async def main():
                await client.connect()
            
                # --- ОБРАБОТКА СЛЕТЕВШЕЙ АВТОРИЗАЦИИ ---
                if not await client.is_user_authorized():
                    logging.error(f"Сессия {session_id}: Клиент не авторизован!")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                
                    with app.app_context():
                        db = get_db()
                        # Устанавливаем статус unauthorized
                        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                        db.commit()
                        if 'notify_clients' in globals(): notify_clients()
                    return
                # ---------------------------------------

                me = await client.get_me()
            
                # --- Получаем @username или Имя юзербота ---
                account_name = getattr(me, 'username', None)
                if account_name:
                    account_name = f"@{account_name}"
                else:
                    account_name = getattr(me, 'first_name', f"ID: {me.id}")
                # --------------------------------------------------
            
                logging.info(f"Сессия {session_id}: слушатель запущен для {me.first_name}")
            
                with app.app_context():
                    db = get_db()
                    try:
                        db.execute(
                            "UPDATE user_telegram_sessions SET status = 'active', session_file = ?, account_name = ? WHERE id = ?",
                            (session_file, account_name, session_id)
                        )
                    except sqlite3.OperationalError:
                        db.execute(
                            "UPDATE user_telegram_sessions SET status = 'active', session_file = ? WHERE id = ?",
                            (session_file, session_id)
                        )
                    db.commit()
                    if 'notify_clients' in globals(): notify_clients()
                
                # Блокирующий вызов: клиент слушает события до отключения
                # Блокирующий вызов: клиент слушает события до отключения
                try:
                    await client.run_until_disconnected()
                except AuthKeyUnregisteredError:
                    logging.error(f"Сессия {session_id}: Ключ авторизации отозван сервером Telegram (слетела сессия).")
                    with app.app_context():
                        db = get_db()
                        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                        db.commit()
                        if 'notify_clients' in globals(): notify_clients()


            # --- ЗАПУСК И БЕЗОПАСНАЯ ОЧИСТКА В ЦИКЛЕ ---
            try:
                loop.run_until_complete(main())
            except Exception as e:
                logging.exception(f"Сессия {session_id}: ошибка в слушателе - {e}")
            finally:
                # 1. Корректное отключение от серверов Telegram
                try:
                    if client and client.is_connected():
                        loop.run_until_complete(client.disconnect())
                except Exception:
                    pass
            
                # 2. Очищаем глобальные словари ВНУТРИ родного потока.
                # Это полностью исключает панику сборщика мусора (GC) в Flask!
                user_clients.pop(session_id, None)
                user_loops.pop(session_id, None)
                background_tasks.pop(session_id, None)

                # 3. Отменяем оставшиеся фоновые задачи Telethon и закрываем цикл
                try:
                    tasks = asyncio.all_tasks(loop)
                    for t in tasks:
                        t.cancel()
                
                    if tasks:
                        # Даем задачам шанс чисто закрыться (игнорируя ошибки отмены)
                        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                
                    loop.close()
                except Exception:
                    pass
                
                # 4. === ЗАЩИТА СТАТУСА: Сбрасываем статус на "выключен", если поток упал ===
                try:
                    with app.app_context():
                        local_db = get_db()
                        local_db.execute("UPDATE user_telegram_sessions SET status = 'inactive' WHERE id = ?", (session_id,))
                        local_db.commit()
                        if 'notify_clients' in globals(): 
                            notify_clients()
                except Exception as e:
                    logging.error(f"Не удалось сбросить статус сессии {session_id} в БД: {e}")
                # ===========================================================================

                logging.info(f"Сессия {session_id}: слушатель полностью остановлен")

    # --- СОЗДАНИЕ И ЗАПУСК ПОТОКА ---
    # Даем потоку имя для удобства дебага
    thread = threading.Thread(target=listener, daemon=True, name=f"UserbotThread-{session_id}")
    thread.start()
    background_tasks[session_id] = thread
    logging.info(f"Поток слушателя для сессии {session_id} создан")

# ---------- Маршруты Flask ----------
@app.route('/')
@login_required
def index():
    return render_template('store.html')

@app.route('/api/profile/logout_others', methods=['POST'])
@login_required
def logout_others():
    user_id = session['user_id']
    new_token = secrets.token_hex(16)
    
    db = get_db()
    db.execute("UPDATE users SET session_token = ? WHERE id = ?", (new_token, user_id))
    db.commit()
    
    session['session_token'] = new_token
    
    # --- НОВАЯ СТРОКА: Рассылаем сигнал всем подключенным клиентам ---
    socketio.emit('force_logout')
    
    return jsonify({'success': True})

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        login = request.form.get('login')
        password = request.form.get('password')
        pwd_hash = hashlib.sha256(password.encode()).hexdigest()
        
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE login = ? AND password_hash = ?", (login, pwd_hash)).fetchone()
        
        if user:
            # --- ГЕНЕРАЦИЯ И СОХРАНЕНИЕ ТОКЕНА ---
            token = user['session_token'] if 'session_token' in user.keys() else None
            if not token:
                token = secrets.token_hex(16)
                try:
                    db.execute("UPDATE users SET session_token = ? WHERE id = ?", (token, user['id']))
                    db.commit()
                except sqlite3.OperationalError:
                    pass # На случай, если колонка еще не создалась
            
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['login'] = user['login']
            session['session_token'] = token # Обязательно записываем в браузер
            # -------------------------------------
            
            return redirect(url_for('index'))
        return render_template_string(LOGIN_TEMPLATE, error="Неверный логин или пароль")
    return render_template_string(LOGIN_TEMPLATE)

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))

@app.route('/api/publish_tree_data', methods=['GET'])
@login_required
def get_publish_tree_data():
    db = get_db()
    user_id = session.get('user_id')
    
    folders = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    products = db.execute("SELECT id, name, folder_id FROM products WHERE user_id=?", (user_id,)).fetchall()
    
    # Получаем поставщиков, у которых есть актуальные цены на товары
    # ВАЖНО: Я использую m.chat_id и m.chat_title. Если в вашей базе таблица messages 
    # использует sender_id или channel_name, просто замените эти слова в запросе ниже!
    suppliers = db.execute("""
        SELECT pm.product_id, m.chat_id as supplier_id, m.chat_title as supplier_name, pm.extracted_price
        FROM product_messages pm
        JOIN messages m ON pm.message_id = m.id
        WHERE pm.status = 'confirmed' AND pm.is_actual = 1 AND m.user_id = ?
        GROUP BY pm.product_id, m.chat_id
    """, (user_id,)).fetchall()
    
    return jsonify({
        'folders': [dict(f) for f in folders],
        'products': [dict(p) for p in products],
        'suppliers': [dict(s) for s in suppliers]
    })

@app.route('/api/tracked_chats/<int:chat_id>', methods=['PUT'])
@login_required
def update_tracked_chat(chat_id):
    data = request.get_json()
    custom_name = data.get('custom_name')
    user_id = session['user_id']
    db = get_db()
    # Проверяем, принадлежит ли чат пользователю
    cur = db.execute("SELECT id FROM tracked_chats WHERE id = ? AND user_id = ?", (chat_id, user_id))
    if not cur.fetchone():
        return jsonify({'error': 'Not found'}), 404
    # Разрешаем пустую строку, сохраняем как NULL
    if custom_name == '':
        custom_name = None
    db.execute("UPDATE tracked_chats SET custom_name = ? WHERE id = ?", (custom_name, chat_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/get_qr', methods=['POST'])
@login_required
def get_qr():
    ensure_event_loop()
    data = request.get_json()
    api_id = data.get('api_id')
    api_hash = data.get('api_hash')
    session_id = data.get('session_id') # <--- ВОТ ЗДЕСЬ ПРИНИМАЕМ ID СТАРОЙ СЕССИИ
    user_id = session['user_id']

    if not api_id or not api_hash:
        return jsonify({'success': False, 'error': 'Missing fields'}), 400

    db = get_db()
    
    # --- ИСПРАВЛЕННАЯ ЛОГИКА ---
    if session_id:
        # Если передали ID старой сессии, просто обновляем её статус
        db.execute("UPDATE user_telegram_sessions SET status = 'pending' WHERE id = ? AND user_id = ?", (session_id, user_id))
    else:
        # Иначе создаем новую
        cursor = db.execute(
            "INSERT INTO user_telegram_sessions (user_id, api_id, api_hash, status) VALUES (?, ?, ?, 'pending')",
            (user_id, api_id, api_hash)
        )
        session_id = cursor.lastrowid
    # ---------------------------
        
    db.commit()
    if 'notify_clients' in globals(): notify_clients()
    
    session_file = f'sessions/user_{user_id}_{session_id}'
    q = queue.Queue()

    def qr_worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(session_file, int(api_id), api_hash)

        async def _run():
            await client.connect()
            if await client.is_user_authorized():
                await client.disconnect()
                q.put({'already_authorized': True})
                return

            try:
                qr_login = await client.qr_login()
                q.put({'qr_url': qr_login.url})
                
                await qr_login.wait(timeout=300)
                await client.disconnect()
                
                with app.app_context():
                    local_db = get_db()
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'active' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
                logging.info(f"Сессия {session_id}: статус обновлён на active")
                start_message_listener(session_id, user_id, api_id, api_hash)

            except asyncio.TimeoutError:
                logging.error(f"Сессия {session_id}: таймаут ожидания QR")
                await client.disconnect()
                with app.app_context():
                    local_db = get_db()
                    # Если ошибка, возвращаем статус unauthorized вместо удаления
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
            except Exception as e:
                logging.exception(f"Сессия {session_id}: ошибка при ожидании QR")
                await client.disconnect()
                with app.app_context():
                    local_db = get_db()
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
            finally:
                qr_sessions.pop(session_id, None)

        loop.run_until_complete(_run())
        loop.close()

    thread = threading.Thread(target=qr_worker, daemon=True)
    thread.start()

    try:
        res = q.get(timeout=15)
    except queue.Empty:
        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': False, 'error': 'Таймаут генерации QR'}), 500

    if res.get('already_authorized'):
        start_message_listener(session_id, user_id, api_id, api_hash)
        db.execute("UPDATE user_telegram_sessions SET status = 'active' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': True, 'already_authorized': True})

    qr_url = res.get('qr_url')
    if not qr_url:
        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': False, 'error': 'Не удалось сгенерировать QR'}), 500

    qr_sessions[session_id] = True 

    img = qrcode.make(qr_url)
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.getvalue()).decode()

    return jsonify({'success': True, 'qr': img_base64, 'session_id': session_id})

@app.route('/api/check_login')
@login_required
def check_login():
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'Missing session_id'}), 400
    session_id = int(session_id)

    db = get_db()
    row = db.execute("SELECT status FROM user_telegram_sessions WHERE id = ?", (session_id,)).fetchone()
    if row and row['status'] == 'active':
        return jsonify({'success': True})
    return jsonify({'success': False})

@app.route('/api/check_session')
@login_required
def check_session():
    user_id = session['user_id']
    active = any(uid == user_id for _, _, uid in user_clients.values())
    return jsonify({'authorized': active})

import requests

# --- РОУТЫ ДЛЯ НАСТРОЙКИ API-ПАРСЕРА ---
@app.route('/api/api_sources', methods=['GET', 'POST'])
@login_required
def handle_api_sources():
    db = get_db()
    user_id = session.get('user_id')
    
    if request.method == 'GET':
        sources = db.execute("SELECT * FROM api_sources WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(s) for s in sources])
    else:
        data = request.json
        db.execute("""
            INSERT INTO api_sources (user_id, name, url, token, interval_min)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, data.get('name'), data.get('url'), data.get('token'), data.get('interval_min', 60)))
        db.commit()
        return jsonify({'success': True})

@app.route('/api/api_sources/<int:source_id>', methods=['DELETE'])
def delete_api_source(source_id):
    try:
        db = get_db()
        user_id = session.get('user_id')
        dummy_chat_id = f"api_src_{source_id}"
        
        # 1. Удаляем привязки цен к товарам
        db.execute("DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE chat_id=? AND user_id=?)", (dummy_chat_id, user_id))
        
        # 2. Удаляем сами сохраненные прайсы/сообщения
        db.execute("DELETE FROM messages WHERE chat_id=? AND user_id=?", (dummy_chat_id, user_id))
        
        # 3. Удаляем само подключение API
        db.execute("DELETE FROM api_sources WHERE id=? AND user_id=?", (source_id, user_id))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Ошибка при удалении API: {e}")
        return jsonify({'error': str(e)}), 500

# --- ФОНОВЫЙ ПАРСЕР ЧУЖИХ API ---
def api_parser_scheduler():
    """Раз в минуту проверяет чужие API и заносит товары в базу как поставщик"""
    time.sleep(5)
    last_sync_times = {}
    
    with app.app_context():
        while True:
            try:
                db = get_db()
                sources = db.execute("SELECT * FROM api_sources WHERE is_active=1").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for src in sources:
                    src_id = src['id']
                    user_id = src['user_id']
                    interval = src['interval_min']
                    
                    # Проверяем, пришло ли время опрашивать этот API
                    last_run = last_sync_times.get(src_id)
                    if last_run and (now - last_run).total_seconds() / 60 < interval:
                        continue
                        
                    # Формируем URL с токеном (как в вашей системе)
                    url = f"{src['url']}?token={src['token']}"
                    
                    try:
                        resp = requests.get(url, timeout=15)
                        if resp.status_code == 200:
                            data = resp.json()
                            products = data.get('products', [])
                            
                            # Отбираем только валидные товары
                            valid_products = [p for p in products if p.get('name') and p.get('price') is not None]
                            
                            # Создаем "фейковый чат", чтобы API выглядел как поставщик
                            dummy_chat_id = f"api_src_{src_id}"
                            dummy_chat_title = f"🌐 API: {src['name']}"
                            
                            # 1. Собираем все товары в единый текст (построчно)
                            lines = [f"Авто-обновление из API ({len(valid_products)} товаров)"]
                            for p in valid_products:
                                lines.append(f"{p['name']} — {p['price']}")
                                
                            msg_text = "\n".join(lines)
                            sender_name = f"🌐 API: {src['name']}"
                            
                            # Вставляем "сообщение" от этого поставщика с полным списком
                            cursor = db.execute(
                                "INSERT INTO messages (user_id, chat_id, chat_title, text, date, sender_name) VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                                (user_id, dummy_chat_id, dummy_chat_title, msg_text, now, sender_name)
                            )
                            dummy_msg_id = cursor.fetchone()['id']
                            
                            # 2. Привязываем товары с указанием конкретной строки (line_index)
                            for i, p in enumerate(valid_products):
                                name = p.get('name')
                                price = p.get('price')
                                
                                # Ищем товар в базе или создаем новый (без папки)
                                prod_row = db.execute("SELECT id FROM products WHERE name=? AND user_id=?", (name, user_id)).fetchone()
                                if prod_row:
                                    prod_id = prod_row['id']
                                else:
                                    cur = db.execute("INSERT INTO products (user_id, name) VALUES (?, ?) RETURNING id", (user_id, name))
                                    prod_id = cur.fetchone()['id']
                                    
                                # line_index = i + 1, так как 0-я строка - это заголовок "Авто-обновление..."
                                db.execute("""
                                    INSERT INTO product_messages (product_id, message_id, extracted_price, status, is_actual, line_index)
                                    VALUES (?, ?, ?, 'pending', 1, ?)
                                """, (prod_id, dummy_msg_id, price, i + 1))
                                
                            db.commit()
                            notify_clients()
                            print(f"✅ Спарсили {len(valid_products)} товаров из {src['name']}")
                    except Exception as req_err:
                        print(f"❌ Ошибка запроса к API {src['name']}: {req_err}")
                        
                    last_sync_times[src_id] = now
            except Exception as e:
                print(f"Ошибка в планировщике парсинга API: {e}")
                
            time.sleep(60) # Проверяем таймеры каждую минуту



import math
from flask import request, jsonify, session

import math
from flask import request, jsonify, session

@app.route('/api/messages')
def get_messages():
    # Проверка авторизации
    if 'user_id' not in session:
        return jsonify({'messages': [], 'error': 'Not authorized'}), 401
        
    user_id = session['user_id']
    msg_type = request.args.get('type', 'all')
    search = request.args.get('search', '').lower()
    exclude = request.args.get('exclude', '').lower()
    
    sender = request.args.get('sender', '')
    msg_source_type = request.args.get('msg_type', '')
    
    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except ValueError:
        page = 1
        
    try:
        limit = int(request.args.get('limit', 20))
        if limit < 20: limit = 20
        elif limit > 100: limit = 100
    except ValueError:
        limit = 20
        
    offset = (page - 1) * limit

    db = get_db()
    
    # Скрываем сообщения, которые пришли вне тайминга (is_delayed = 1)
    where_clauses = ["m.user_id = ?", "(m.is_blocked IS NULL OR m.is_blocked = 0)", "(m.is_delayed IS NULL OR m.is_delayed = 0)"]
    params = [user_id]

    if msg_type == 'favorites':
        where_clauses.append("m.is_favorite = 1")
    elif msg_type.startswith('folder_'):
        folder_id = msg_type.replace('folder_', '')
        where_clauses.append("m.id IN (SELECT message_id FROM saved_messages WHERE folder_id = ?)")
        params.append(folder_id)
    elif msg_type.startswith('chat_'):
        chat_id = msg_type.replace('chat_', '')
        where_clauses.append("m.chat_id = ?")
        params.append(chat_id)

    if search:
        where_clauses.append("LOWER(m.text) LIKE ?")
        params.append(f"%{search}%")
        
    if exclude:
        where_clauses.append("LOWER(m.text) NOT LIKE ?")
        params.append(f"%{exclude}%")

    if sender and sender != 'all':
        where_clauses.append("(tc.custom_name = ? OR m.chat_title = ? OR REPLACE(m.sender_name, ' None', '') = ?)")
        params.extend([sender, sender, sender])

    if msg_source_type and msg_source_type != 'all':
        where_clauses.append("m.type = ?")
        params.append(msg_source_type)

    where_str = " AND ".join(where_clauses)
    
    try:
        # 1. Достаем все подходящие сообщения, сортируя от новых к старым 
        # (Ограничиваем 2000 для скорости, так как старые дубли все равно скроются)
        data_query = f"""
            SELECT m.*, tc.custom_name
            FROM messages m
            LEFT JOIN tracked_chats tc 
                   ON CAST(m.chat_id AS TEXT) = CAST(tc.chat_id AS TEXT) 
                  AND m.user_id = tc.user_id
            WHERE {where_str} 
            ORDER BY m.date DESC
            LIMIT 2000
        """
        cursor = db.execute(data_query, params)
        all_messages = [dict(row) for row in cursor.fetchall()]

        # === 2. УМНАЯ ФИЛЬТРАЦИЯ ДУБЛИКАТОВ ===
       
        import re
        grouped_messages = []
        
        seen_products_per_seller = {} # Сохраненные товары для каждого продавца
        seller_msg_count = {}         # Счетчик проверенных сообщений от продавца

        for msg in all_messages:
            chat_id = msg['chat_id']
            
            # Инициализируем память для нового продавца
            if chat_id not in seen_products_per_seller:
                seen_products_per_seller[chat_id] = set()
                seller_msg_count[chat_id] = 0
                
            seller_msg_count[chat_id] += 1
            
            # ПРАВИЛО 3: Анализируем только последние 15 сообщений от каждого продавца
            if seller_msg_count[chat_id] > 1000:
                continue

            text = msg['text'] if msg['text'] else ''
            # Разбиваем сообщение на отдельные строки (товары)
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if not lines:
                continue

            # Функция: удаляет цены (числа >1000) и значки валют из строки
            def clean_line(l):
                l = re.sub(r'\b\d{1,3}(?:[ .,]\d{3})+\b|\b\d{4,}\b', '', l.lower())
                l = re.sub(r'[₽$€рRkKкК]', '', l)
                return l.strip()

            # ПРАВИЛО 1: Берем ПЕРВЫЙ осмысленный товар в старом сообщении
            first_product = ""
            for line in lines:
                cleaned = clean_line(line)
                if len(cleaned) > 3:  # Игнорируем пустые строки или одиночные смайлики
                    first_product = cleaned
                    break
            
            # ПРАВИЛО 2: Ищем этот товар в списке того, что продавец уже присылал НЕДАВНО
            if first_product and first_product in seen_products_per_seller[chat_id]:
                # Если нашли — значит это дубликат старого прайса. ПРОПУСКАЕМ (скрываем)!
                continue
                
            # Если мы дошли сюда, значит сообщение содержит новые товары. ПОКАЗЫВАЕМ ЕГО.
            grouped_messages.append(msg)
            
            # А теперь берем 2-й, 3-й и ВСЕ остальные товары из этого сообщения 
            # и записываем их в память, чтобы в будущем отсекать старые дубликаты
            for line in lines:
                cleaned = clean_line(line)
                if len(cleaned) > 3:
                    seen_products_per_seller[chat_id].add(cleaned)
        # ==============================================================================

        # 3. Ручная пагинация обрезанного списка
        total_count = len(grouped_messages)
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        messages_page = grouped_messages[offset : offset + limit]

        return jsonify({
            'messages': messages_page,
            'total': total_count,
            'page': page,
            'limit': limit,
            'total_pages': total_pages
        })

    except Exception as e:
        print(f"Ошибка в API сообщений: {e}")
        return jsonify({'messages': [], 'total': 0, 'page': page, 'limit': limit, 'total_pages': 1, 'error': str(e)})

# --- МАРШРУТЫ ДЛЯ АВТОПУБЛИКАЦИЙ ---

@app.route('/api/publications', methods=['GET'])
@login_required
def get_publications():
    db = get_db()
    user_id = session['user_id'] # Получаем ID текущего пользователя
    pubs = db.execute("SELECT * FROM publications WHERE user_id = ?", (user_id,)).fetchall() # Фильтруем
    return jsonify([dict(p) for p in pubs])

@app.route('/api/publications', methods=['POST'])
@login_required
def save_publication():
    data = request.json
    db = get_db()
    pub_id = data.get('id')
    
    allowed_items_json = json.dumps(data.get('allowed_items', {}))
    markup_type = data.get('markup_type', 'percent')
    markup_value = float(data.get('markup_value', 0))
    rounding = int(data.get('rounding', 100))
    
    if pub_id:
        db.execute("""
            UPDATE publications 
            SET name=?, is_active=?, interval_min=?, chat_id=?, message_id=?, userbot_id=?, template=?, allowed_items=?, markup_type=?, markup_value=?, rounding=?
            WHERE id=?
        """, (data.get('name'), data.get('is_active', 0), data.get('interval_min', 60), 
              data.get('chat_id'), data.get('message_id'), data.get('userbot_id'), 
              data.get('template'), allowed_items_json, markup_type, markup_value, rounding, pub_id))
    else:
        db.execute("""
    INSERT INTO publications (name, is_active, interval_min, chat_id, message_id, userbot_id, template, allowed_items, markup_type, markup_value, rounding, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data.get('name'), data.get('is_active', 0), data.get('interval_min', 60), 
              data.get('chat_id'), data.get('message_id'), data.get('userbot_id'), 
              data.get('template'), allowed_items_json, markup_type, markup_value, rounding,session['user_id']))
        # Получаем ID только что созданной публикации
        pub_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        
    # --- СОХРАНЕНИЕ НАЦЕНОК, КОТОРЫЕ БЫЛИ ДОБАВЛЕНЫ ДО СОЗДАНИЯ ПУБЛИКАЦИИ ---
    temp_markups = data.get('temp_markups', [])
    for m in temp_markups:
        db.execute("""
            INSERT INTO pub_markups (pub_id, folder_id, markup_type, markup_value, rounding) 
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pub_id, folder_id) DO UPDATE SET 
            markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
        """, (pub_id, m['folder_id'], m['markup_type'], m['markup_value'], m['rounding']))
        
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/publications/<int:pub_id>', methods=['DELETE'])
@login_required
def delete_publication(pub_id):
    db = get_db()
    db.execute("DELETE FROM publications WHERE id=?", (pub_id,))
    db.commit()
    notify_clients()
    return jsonify({'success': True})
    
@app.route('/api/publications/<int:pub_id>/toggle', methods=['POST'])
@login_required
def toggle_publication(pub_id):
    data = request.json
    db = get_db()
    db.execute("UPDATE publications SET is_active=? WHERE id=?", (data.get('is_active', 0), pub_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

import re


def smart_folder_sort_key(folder):
    """
    Умная сортировка:
    1. 'По умолчанию' всегда сверху.
    2. Обычные папки идут по алфавиту.
    3. Папки с гигабайтами и терабайтами идут в конце и сортируются по объему.
    """
    name = folder.get('name', '').strip()
    name_lower = name.lower()
    
    # Папка "По умолчанию" всегда будет самой первой (-1)
    if name_lower == 'по умолчанию':
        return (-1, 0, name)
        
    # Ищем названия, состоящие из цифр (64, 128) и опционально "гб" или "тб"
    match = re.fullmatch(r'(\d+)\s*(тб|tb|гб|gb)?', name_lower)
    
    if match:
        val = int(match.group(1))
        unit = match.group(2)
        
        # Переводим терабайты в гигабайты (1 ТБ = 1024 ГБ) для правильной последовательности
        if unit in ['тб', 'tb']:
            val *= 1024
            
        # (1, ...) отправляет такие папки в конец списка, затем сортирует по числу
        return (1, val, name)
        
    # (0, ...) оставляет обычные папки в начале списка и сортирует по алфавиту
    return (0, 0, name)

def sort_folders_recursive(folders_list):
    """Рекурсивно сортирует каждую папку и её подпапки"""
    folders_list.sort(key=smart_folder_sort_key)
    for f in folders_list:
        if f.get('children'):
            sort_folders_recursive(f['children'])


@app.route('/api/folders/tree')
@login_required
def get_folder_tree():
    user_id = session['user_id']
    db = get_db()
    
    # Извлекаем все папки из БД
    cursor = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()
    
    folders = {}
    roots = []
    
    # 1. Заполняем словарь всеми папками
    for row in rows:
        folders[row['id']] = {
            'id': row['id'],
            'name': row['name'],
            'parent_id': row['parent_id'],
            'children': []
        }
        
    # 2. Выстраиваем дерево (кто в какой папке лежит)
    for fid, f in folders.items():
        if f['parent_id'] is None:
            roots.append(f)
        else:
            if f['parent_id'] in folders:
                folders[f['parent_id']]['children'].append(f)
                
    # 3. Применяем умную сортировку ко всему дереву перед отправкой на фронтенд
    sort_folders_recursive(roots)
                
    return jsonify(roots)

@app.route('/api/folders', methods=['POST'])
@login_required
def create_folder():
    user_id = session['user_id']
    data = request.get_json()
    name = data.get('name')
    parent_id = data.get('parent_id')
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    try:
        db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (?, ?, ?)",
                   (user_id, name, parent_id))
        db.commit()
        notify_clients()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Folder exists'}), 400

@app.route('/api/folders/<int:folder_id>/rename', methods=['POST'])
@login_required
def rename_folder_route(folder_id):
    user_id = session['user_id']
    data = request.get_json()
    new_name = data.get('name')
    
    if not new_name:
        return jsonify({'error': 'Имя не может быть пустым'}), 400
        
    db = get_db()
    
    row = db.execute("SELECT name FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id)).fetchone()
    if not row:
        return jsonify({'error': 'Папка не найдена'}), 404
    if row['name'] == 'По умолчанию':
        return jsonify({'error': 'Нельзя переименовать папку по умолчанию'}), 400
        
    try:
        db.execute("UPDATE folders SET name = ? WHERE id = ? AND user_id = ?", (new_name, folder_id, user_id))
        db.commit()
        notify_clients() # Уведомляем клиентов для автообновления
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка переименования: {e}")
        return jsonify({'error': 'Системная ошибка при переименовании'}), 500

@app.route('/api/folders/<int:folder_id>', methods=['DELETE'])
@login_required
def delete_folder(folder_id):
    user_id = session['user_id']
    db = get_db()
    row = db.execute("SELECT name FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id)).fetchone()
    if not row:
        return jsonify({'error': 'Folder not found'}), 404
    if row['name'] == 'По умолчанию':
        return jsonify({'error': 'Cannot delete default folder'}), 400
        
    db.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/folder/<int:folder_id>/messages')
@login_required
def get_folder_messages(folder_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("""
        SELECT m.*, tc.custom_name, tc.chat_title as tc_chat_title
        FROM messages m
        JOIN saved_messages sm ON m.id = sm.message_id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE sm.folder_id = ? AND m.user_id = ?
        AND (m.is_blocked IS NULL OR m.is_blocked = 0)
        ORDER BY sm.saved_date DESC
    """, (folder_id, user_id))
    # ...
    rows = cursor.fetchall()
    messages = [{
        'id': row['id'],
        'telegram_message_id': row['telegram_message_id'], 
        'chat_id': row['chat_id'],                         
        'type': row['type'],
        'text': row['text'],
        'date': row['date'],
        'chat_title': row['chat_title'],
        'sender_name': row['sender_name'] if 'sender_name' in row.keys() else 'Unknown',
        'custom_name': row['custom_name'] if 'custom_name' in row.keys() else None,
        'tc_chat_title': row['tc_chat_title'] if 'tc_chat_title' in row.keys() else None # <--- ДОБАВИТЬ ЭТУ СТРОКУ
    } for row in rows]
    return jsonify(messages)


@app.route('/api/save_messages', methods=['POST'])
@login_required
def save_messages():
    user_id = session['user_id']
    data = request.get_json()
    message_ids = data.get('message_ids', [])
    folder_id = data.get('folder_id')
    if not message_ids or not folder_id:
        return jsonify({'error': 'Missing data'}), 400
    db = get_db()
    cursor = db.execute("SELECT id FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id))
    if not cursor.fetchone():
        return jsonify({'error': 'Folder not found'}), 404
    for mid in message_ids:
        try:
            db.execute("INSERT INTO saved_messages (message_id, folder_id) VALUES (?, ?)", (mid, folder_id))
        except sqlite3.IntegrityError:
            pass
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/userbots', methods=['GET'])
@login_required
def get_userbots():
    user_id = session['user_id']
    db = get_db()
    # ДОБАВИЛИ time_start, time_end, schedule_enabled в SQL-запрос
    try:
        cursor = db.execute("SELECT id, api_id, api_hash, status, account_name, time_start, time_end, schedule_enabled FROM user_telegram_sessions WHERE user_id=?", (user_id,))
    except sqlite3.OperationalError:
        cursor = db.execute("SELECT id, api_id, api_hash, status, time_start, time_end, schedule_enabled FROM user_telegram_sessions WHERE user_id=?", (user_id,))
        
    bots = [dict(row) for row in cursor.fetchall()]
    return jsonify(bots)

# --- НОВЫЙ МАРШРУТ ДЛЯ СОХРАНЕНИЯ РАСПИСАНИЯ ---
@app.route('/api/userbots/<int:bot_id>/schedule', methods=['POST'])
@login_required
def update_userbot_schedule(bot_id):
    user_id = session['user_id']
    data = request.get_json()
    db = get_db()
    db.execute("""
        UPDATE user_telegram_sessions 
        SET time_start = ?, time_end = ?, schedule_enabled = ? 
        WHERE id = ? AND user_id = ?
    """, (data['time_start'], data['time_end'], int(data['schedule_enabled']), bot_id, user_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})
# -----------------------------------------------

@app.route('/api/userbots/<int:bot_id>/toggle', methods=['POST'])
@login_required
def toggle_userbot(bot_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("SELECT user_id, api_id, api_hash, status FROM user_telegram_sessions WHERE id=?", (bot_id,))
    row = cursor.fetchone()
    if not row or row['user_id'] != user_id:
        return jsonify({'error': 'Not found'}), 404

    if row['status'] == 'active':
        stop_message_listener(bot_id)
        new_status = 'inactive'
    else:
        start_message_listener(bot_id, user_id, row['api_id'], row['api_hash'])
        new_status = 'active'

    db.execute("UPDATE user_telegram_sessions SET status = ? WHERE id = ?", (new_status, bot_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True, 'status': new_status})

@app.route('/api/userbots/<int:bot_id>', methods=['DELETE'])
@login_required
def delete_userbot(bot_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("SELECT user_id, session_file FROM user_telegram_sessions WHERE id=?", (bot_id,))
    row = cursor.fetchone()
    if not row or row['user_id'] != user_id:
        return jsonify({'error': 'Not found'}), 404

    if bot_id in user_clients:
        stop_message_listener(bot_id)

    session_file = row['session_file']
    if session_file and os.path.exists(session_file):
        os.remove(session_file)

    db.execute("DELETE FROM user_telegram_sessions WHERE id = ?", (bot_id,))
    db.commit()
    notify_clients()
    return jsonify({'success': True})



@app.route('/api/excel/missing_sheets', methods=['GET'])
@login_required
def get_missing_sheets():
    db = get_db()
    missing = db.execute("SELECT * FROM excel_missing_sheets WHERE user_id = ?", (session['user_id'],)).fetchall()
    return jsonify([dict(m) for m in missing])

@app.route('/api/excel/missing_sheets/<int:m_id>', methods=['DELETE'])
@login_required
def delete_missing_sheet(m_id):
    db = get_db()
    db.execute("DELETE FROM excel_missing_sheets WHERE id = ? AND user_id = ?", (m_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/excel/parse_latest/<chat_id>', methods=['POST'])
@login_required
def parse_latest_excel_route(chat_id):
    chat_id = int(chat_id)
    user_id = session['user_id']    
    
    # 1. Собираем ВСЕ активные сессии текущего пользователя
    active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
    if not active_sessions:
        return jsonify({'error': 'Юзерботы не запущены. Включите хотя бы один аккаунт во вкладке "Подключение".'}), 400

    def background_parser():
        with app.app_context():
            db = get_db()
            tc = db.execute("SELECT chat_title, custom_name FROM tracked_chats WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)).fetchone()
            chat_title = tc['custom_name'] if tc and tc['custom_name'] else (tc['chat_title'] if tc else str(chat_id))

        success = False
        
        # 2. ПЕРЕБИРАЕМ ВСЕ АККАУНТЫ ПО ОЧЕРЕДИ
        for sid in active_sessions:
            if sid not in user_clients: continue
            thread, client, uid = user_clients[sid]
            client_loop = user_loops.get(sid)
            
            if not client_loop: continue

            async def try_parse_for_account():
                try:
                    # Пытаемся получить доступ к каналу
                    entity = await client.get_entity(chat_id)
                    
                    # Ищем последние сообщения
                    async for message in client.iter_messages(entity, limit=100):
                        if message.document and getattr(message.file, 'ext', '').lower() in ['.xlsx', '.xls', '.pdf']:
                            
                            parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                            
                            if parsed_text:
                                msg_type = 'channel' if message.is_channel else 'group' if message.is_group else 'private'
                                sender = await message.get_sender()
                                sender_name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip() or "Unknown"
                                
                                from datetime import timedelta
                                msg_date = (message.date + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

                                # 3. Записываем в БД для вывода в СКЛАД
                                with app.app_context():
                                    db = get_db()
                                    try:
                                        db.execute(
                                            'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                            (user_id, message.id, msg_type, parsed_text, msg_date, chat_id, chat_title, sender_name)
                                        )
                                    except sqlite3.IntegrityError:
                                        db.execute(
                                            "UPDATE messages SET text = ? WHERE telegram_message_id = ? AND user_id = ?", 
                                            (parsed_text, message.id, user_id)
                                        )
                                    db.commit()
                                    # Обновляем Склад в реальном времени
                                    notify_clients()
                                    logger.info(f"✅ Успешно спарсили файл из {chat_title} через аккаунт сессии {sid}")
                                return True # Сообщаем об успехе
                    return False # Файл не найден в последних 100 сообщениях
                except Exception as e:
                    # У этого аккаунта нет доступа к каналу или произошла другая ошибка
                    logger.warning(f"Аккаунт (сессия {sid}) не имеет доступа к чату {chat_id} или произошла ошибка: {e}")
                    return False

            try:
                # Передаем задачу в цикл событий конкретного Telethon-клиента
                future = asyncio.run_coroutine_threadsafe(try_parse_for_account(), client_loop)
                is_parsed = future.result(timeout=60)
                
                if is_parsed:
                    success = True
                    break # 🔥 КАК ТОЛЬКО НАШЛИ И СПАРСИЛИ — ОСТАНАВЛИВАЕМ ПЕРЕБОР
                    
            except Exception as e:
                logger.error(f"Таймаут или критическая ошибка при попытке парсинга через аккаунт {sid}: {e}")
                continue # Пробуем следующий аккаунт

        if not success:
            logger.error(f"❌ Ни один из {len(active_sessions)} аккаунтов не смог спарсить файл для {chat_id}. Возможно, нет доступа или файла нет в последних 100 сообщениях.")

    # Запускаем логику перебора в отдельном потоке, чтобы не подвешивать сервер Flask
    threading.Thread(target=background_parser, daemon=True).start()
    
    return jsonify({'success': True})


@app.route('/api/tracked_chats', methods=['GET'])
@login_required
def get_tracked_chats():
    user_id = session['user_id']
    db = get_db()
    # ДОБАВЛЕНО: custom_name в выборку
    cursor = db.execute("SELECT id, chat_id, chat_title, custom_name FROM tracked_chats WHERE user_id=?", (user_id,))
    chats = [dict(row) for row in cursor.fetchall()]
    return jsonify(chats)

@app.route('/api/tracked_chats', methods=['POST'])
@login_required
def add_tracked_chat():
    data = request.get_json()
    raw_chat_id = data.get('chat_id')
    custom_name = data.get('custom_name') # Получаем кастомное название
    user_id = session['user_id']
  
    if not raw_chat_id:
        return jsonify({'error': 'chat_id required'}), 400

    chat_title = custom_name # Изначально ставим кастомное имя (если оно пустое, ниже получим из ТГ)
    numeric_chat_id = None
    client_to_use = None
    client_loop_to_use = None

    try:
        entity_query = int(raw_chat_id)
    except ValueError:
        entity_query = raw_chat_id

    # Ищем активную сессию юзербота для текущего пользователя
    # Ищем активные сессии юзерботов для текущего пользователя
    active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
    
    # ПЕРЕБИРАЕМ ВСЕХ БОТОВ, ПОКА НЕ НАЙДЕМ ТОГО, У КОТОРОГО ЕСТЬ ДОСТУП
    for sid in active_sessions:
        thread, client, uid = user_clients[sid]
        client_loop = user_loops.get(sid)
        
        async def fetch_chat():
            return await client.get_entity(entity_query)
            
        if client_loop:
            try:
                future = asyncio.run_coroutine_threadsafe(fetch_chat(), client_loop) 
                entity = future.result(timeout=5)
                numeric_chat_id = entity.id 
                
                # Если доступ есть, запоминаем этого бота как рабочего для данного чата
                client_to_use = client
                client_loop_to_use = client_loop
                
                # Записываем название чата
                if not chat_title:
                    if hasattr(entity, 'title'):
                        chat_title = entity.title
                    else:
                        chat_title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                        
                break # Успешно нашли нужного бота, прерываем цикл перебора!
                
            except Exception as e:
                logging.warning(f"Бот сессии {sid} не имеет доступа к чату или произошла ошибка: {e}")
                continue # Пробуем следующего бота
                
                # Если кастомное имя не ввели, берем оригинальное название из Telegram
                if not chat_title:
                    if hasattr(entity, 'title'):
                        chat_title = entity.title
                    else:
                        chat_title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            except Exception as e:
                logging.warning(f"Не удалось получить информацию о чате: {e}")

    # Фолбэк, если юзербот не запущен или произошла ошибка
    if not numeric_chat_id:
        try:
            numeric_chat_id = int(raw_chat_id)
        except ValueError:
            return jsonify({'error': 'Запустите юзербота, чтобы добавлять чаты по ссылке, или введите точный числовой ID'}), 400

    # Если название всё ещё пустое (например, юзербот выключен и ввели просто ID без кастомного имени)
    if not chat_title:
        chat_title = str(numeric_chat_id)

    db = get_db()
    try:
        db.execute(
            "INSERT INTO tracked_chats (user_id, chat_id, chat_title, custom_name) VALUES (?, ?, ?, ?)",
            (user_id, numeric_chat_id, chat_title, custom_name)
        )
        db.commit()
        notify_clients()
        
        # Запускаем парсинг истории сообщений в фоне, если юзербот активен
        # Запускаем парсинг истории сообщений в фоне, если юзербот активен
        if client_to_use and client_loop_to_use and numeric_chat_id:
            target_to_parse = entity if 'entity' in locals() else numeric_chat_id
            asyncio.run_coroutine_threadsafe(
                fetch_chat_history(client_to_use, target_to_parse, chat_title, user_id, numeric_chat_id), 
                client_loop_to_use
            )

        return jsonify({'success': True, 'chat_title': chat_title})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Этот чат уже добавлен в список отслеживаемых'}), 400

@app.route('/api/tracked_chats/<item_id>', methods=['DELETE'])
def delete_tracked_chat(item_id):
    try:
        db = get_db()
        user_id = session.get('user_id')
        
        # 1. Узнаем настоящий Telegram ID этого чата (например, -1001234567) по его ID в таблице (например, 43)
        row = db.execute("SELECT chat_id FROM tracked_chats WHERE id=? AND user_id=?", (item_id, user_id)).fetchone()
        
        if row:
            real_chat_id = row['chat_id']
            
            # 2. Удаляем привязки цен и сообщения
            db.execute("DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE chat_id=? AND user_id=?)", (real_chat_id, user_id))
            db.execute("DELETE FROM messages WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            
            # 3. ДОБАВЛЕНО: Полностью сносим старые настройки файлов
            db.execute("DELETE FROM excel_configs WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
            
        # 4. Удаляем сам чат
        db.execute("DELETE FROM tracked_chats WHERE id=? AND user_id=?", (item_id, user_id))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        print(f"Ошибка при удалении чата: {e}")
        return jsonify({'error': str(e)}), 500

# ---------- Административные маршруты ----------
@app.route('/api/admin/users')
@admin_required
def admin_users():
    db = get_db()
    cursor = db.execute("SELECT id, login, role FROM users")
    users = [{'id': row['id'], 'login': row['login'], 'role': row['role']} for row in cursor.fetchall()]
    return jsonify(users)

@app.route('/api/admin/add_user', methods=['POST'])
@admin_required
def admin_add_user():
    data = request.get_json()
    login = data.get('login')
    password = data.get('password')
    if not login or not password:
        return jsonify({'error': 'Login and password required'}), 400
    pwd_hash = hashlib.sha256(password.encode()).hexdigest()
    db = get_db()
    try:
        db.execute("INSERT INTO users (login, password_hash, role) VALUES (?, ?, 'user')", (login, pwd_hash))
        new_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (?, 'По умолчанию', NULL)", (new_id,))
        db.commit()
        notify_clients()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Login already exists'}), 400




@app.route('/api/admin/clear_messages', methods=['POST'])
@admin_required
def admin_clear_messages():
    data = request.get_json()
    period = data.get('period', 'all')
    db = get_db()
    
    try:
        if period == 'all':
            db.execute("DELETE FROM messages")
        else:
            days = int(period)
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")
            db.execute("DELETE FROM messages WHERE date < ?", (cutoff_str,))
        
        db.commit()
        notify_clients() 
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка при очистке сообщений: {e}")
        return jsonify({'error': str(e)}), 500



# ---------- Шаблоны страниц ----------
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
<link rel="icon" type="image/svg+xml" href="/logo.svg">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <meta charset="UTF-8">
    <title>Вход в систему</title>
    <style>
        body { background: #1a1a1a; color: #e0e0e0; font-family: 'Segoe UI', Tahoma, Arial, sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }
        .login-box { background: #2a2a2a; padding: 30px; border-radius: 8px; border: 1px solid #3a3a3a; text-align: center; width: 100%; max-width: 320px; box-shadow: 0 4px 15px rgba(0,0,0,0.5); }
        .login-box h2 { margin-bottom: 20px; color: #fff; }
        input { width: 100%; padding: 12px; margin: 8px 0 16px 0; background: #1e1e1e; border: 1px solid #3a3a3a; color: #fff; border-radius: 4px; box-sizing: border-box; font-size: 14px; }
        input:focus { outline: none; border-color: #4a90e2; }
        button { width: 100%; padding: 12px; background: #4a90e2; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: 600; font-size: 15px; transition: 0.2s; }
        button:hover { background: #357abd; }
        .error { color: #ff5555; background: rgba(255, 85, 85, 0.1); padding: 10px; border-radius: 4px; margin-bottom: 15px; font-size: 14px; border: 1px solid rgba(255, 85, 85, 0.3); }
        
        /* Новые стили для поля с паролем */
        .password-container { position: relative; margin: 8px 0 16px 0; }
        .password-container input { margin: 0; padding-right: 40px; } /* Отступ справа, чтобы текст не залезал под иконку */
        .toggle-password { position: absolute; right: 12px; top: 50%; transform: translateY(-50%); cursor: pointer; color: #888; font-size: 18px; user-select: none; }
    </style>
</head>
<body>
    <div class="login-box">
        <h2>Вход в систему</h2>
        {% if error %}
            <div class="error">{{ error }}</div>
        {% endif %}
        <form action="/login" method="POST">
            <input type="text" name="login" placeholder="Логин" required>
            
            <div class="password-container">
                <input type="password" id="login-password" name="password" placeholder="Пароль" required>
                <span class="toggle-password" onclick="togglePassword(this)"></span>
            </div>

            <button type="submit">Войти</button>
        </form>
    </div>

    <script>
        function togglePassword(iconElement) {
            const passwordInput = document.getElementById('login-password');
            if (passwordInput.type === 'password') {
                passwordInput.type = 'text';
                iconElement.innerText = '🙈'; // Меняем иконку на закрытый глаз
            } else {
                passwordInput.type = 'password';
                iconElement.innerText = '👁️'; // Меняем иконку на открытый глаз
            }
        }
    </script>
</body>
</html>
"""


@app.route('/api/interaction_bots/<int:id>', methods=['PUT'])
@login_required
def update_interaction_bot(id):
    data = request.get_json()
    user_id = session['user_id']
    db = get_db()
    
    bot_username = data.get('bot_username').strip()
    custom_name = data.get('custom_name')
    userbot_id = data.get('userbot_id')
    commands = json.dumps(data.get('commands', []))
    interval_minutes = data.get('interval_minutes', 60)
    
    # Обновляем настройки бота
    db.execute("""
        UPDATE interaction_bots 
        SET userbot_id=?, bot_username=?, custom_name=?, commands=?, interval_minutes=?
        WHERE id=? AND user_id=?
    """, (userbot_id, bot_username, custom_name, commands, interval_minutes, id, user_id))
    
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/fix_db')
@login_required
def fix_db():
    db = get_db()
    try:
        # Отключаем проверки ключей на секунду, чтобы SQLite разрешил замену
        db.execute("PRAGMA foreign_keys = OFF")
        db.executescript('''
            CREATE TABLE IF NOT EXISTS messages_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                telegram_message_id INTEGER,
                type TEXT,
                text TEXT,
                date TIMESTAMP,
                chat_id INTEGER,
                chat_title TEXT,
                is_blocked INTEGER DEFAULT 0,
                sender_name TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, telegram_message_id)
            );
            INSERT OR IGNORE INTO messages_new (id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name)
            SELECT id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name FROM messages;
            DROP TABLE messages;
            ALTER TABLE messages_new RENAME TO messages;
        ''')
        db.execute("PRAGMA foreign_keys = ON")
        db.commit()
        return "<h1>✅ База данных успешно исправлена!</h1><p>Глобальная блокировка сообщений снята. Теперь парсер будет выкачивать всё.</p><br><a href='/'>Вернуться в парсер</a>"
    except Exception as e:
        return f"<h1>❌ Ошибка:</h1><p>{e}</p>"

@app.route('/api/cleanup_old_bug')
@login_required
def cleanup_old_bug():
    user_id = session['user_id']
    db = get_db()
    
    # Считаем, сколько сломанных сообщений мы найдем
    cursor = db.execute("SELECT COUNT(*) FROM messages WHERE telegram_message_id IS NULL AND user_id = ?", (user_id,))
    bad_count = cursor.fetchone()[0]
    
    if bad_count > 0:
        # Удаляем их (каскадное удаление само почистит привязки к товарам, если они были)
        db.execute("DELETE FROM messages WHERE telegram_message_id IS NULL AND user_id = ?", (user_id,))
        db.commit()
        notify_clients()
        return f"<h1>Уборка завершена! 🧹</h1><p>Удалено сломанных сообщений: <b>{bad_count}</b>.</p><br><a href='/'>Вернуться в парсер</a>"
    else:
        return "<h1>Всё чисто! ✨</h1><p>Сломанных сообщений в базе не найдено.</p><br><a href='/'>Вернуться в парсер</a>"






# ================= УПРАВЛЕНИЕ API И КЛИЕНТАМИ =================

# ✅ А ЭТОТ БЛОК ДОЛЖЕН ОСТАТЬСЯ ✅
@app.route('/api/api_clients/<int:client_id>/filters', methods=['POST'])
@login_required
def update_api_client_filters(client_id):
    data = request.json
    access_rules = json.dumps(data.get('access_rules', {})) # Конвертируем в JSON
    db = get_db()
    db.execute("UPDATE api_clients SET access_rules=? WHERE id=? AND user_id=?", 
               (access_rules, client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


@app.route('/api/api_clients', methods=['GET', 'POST'])
@login_required
def manage_api_clients():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        # Теперь достаем все поля, включая тайминги
        clients = db.execute("SELECT * FROM api_clients WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(c) for c in clients])
    
    # POST: Создаем нового клиента
    data = request.json
    name = data.get('name')
    schedule_enabled = int(data.get('schedule_enabled', 0))
    time_start = data.get('time_start', '00:00')
    time_end = data.get('time_end', '23:59')
    token = secrets.token_hex(24) 
    
    db.execute("""
        INSERT INTO api_clients (user_id, name, token, schedule_enabled, time_start, time_end) 
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, name, token, schedule_enabled, time_start, time_end))
    
    client_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    
    db.execute("INSERT INTO api_markups (client_id, folder_id, markup_type, markup_value, rounding) VALUES (?, 0, 'percent', 0, 100)", (client_id,))
    db.commit()
    return jsonify({'success': True})

# --- ДОБАВЬ НОВЫЙ МАРШРУТ ДЛЯ РЕДАКТИРОВАНИЯ ТАЙМИНГОВ ---
@app.route('/api/api_clients/<int:client_id>/schedule', methods=['POST'])
@login_required
def update_api_client_schedule(client_id):
    data = request.json
    db = get_db()
    db.execute("""
        UPDATE api_clients 
        SET schedule_enabled=?, time_start=?, time_end=? 
        WHERE id=? AND user_id=?
    """, (int(data['schedule_enabled']), data['time_start'], data['time_end'], client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_clients/<int:client_id>', methods=['DELETE'])
@login_required
def delete_api_client(client_id):
    db = get_db()
    db.execute("DELETE FROM api_clients WHERE id=? AND user_id=?", (client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_clients/<int:client_id>/markups', methods=['GET', 'POST'])
@login_required
def manage_markups(client_id):
    db = get_db()
    if request.method == 'GET':
        markups = db.execute("""
            SELECT am.*, f.name as folder_name 
            FROM api_markups am 
            LEFT JOIN folders f ON am.folder_id = f.id 
            WHERE am.client_id=?
        """, (client_id,)).fetchall()
        return jsonify([dict(m) for m in markups])
    
    data = request.json
    folder_id = data.get('folder_id') or 0
    db.execute("""
        INSERT INTO api_markups (client_id, folder_id, markup_type, markup_value, rounding) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(client_id, folder_id) DO UPDATE SET 
        markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
    """, (client_id, folder_id, data['markup_type'], data['markup_value'], data['rounding']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_markups/<int:markup_id>', methods=['DELETE'])
@login_required
def delete_markup(markup_id):
    db = get_db()
    # Запрещаем удалять базовую настройку (folder_id = 0)
    db.execute("DELETE FROM api_markups WHERE id=? AND folder_id != 0 AND client_id IN (SELECT id FROM api_clients WHERE user_id=?)", (markup_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


# ================= ПУБЛИЧНЫЙ API ДЛЯ ВЫДАЧИ КАТАЛОГА =================
@app.route('/api/v1/catalog', methods=['GET'])
def public_api_catalog():
    token = request.args.get('token')
    if not token:
        return jsonify({'error': 'Токен не предоставлен'}), 401
        
    db = get_db()
    client = db.execute("SELECT * FROM api_clients WHERE token=?", (token,)).fetchone()
    if not client:
        return jsonify({'error': 'Неверный токен'}), 401
        
    # --- ПРОВЕРКА РАСПИСАНИЯ ---
    if client['schedule_enabled']:
        from datetime import timedelta
        # МСК время (UTC+3)
        now = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
        start_time = datetime.strptime(client['time_start'], '%H:%M').time()
        end_time = datetime.strptime(client['time_end'], '%H:%M').time()
        
        is_active = start_time <= now <= end_time if start_time <= end_time else (now >= start_time or now <= end_time)
        if not is_active: 
            return jsonify({'categories': [], 'products': []})
            
    user_id = client['user_id']
    client_id = client['id']

    # --- ПОЛУЧАЕМ НАЦЕНКИ ---
    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM api_markups WHERE client_id=?", (client_id,)).fetchall()
    markups = {}
    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
    for m in markups_raw:
        if m['folder_id'] == 0: 
            default_markup = dict(m)
        else: 
            markups[m['folder_id']] = dict(m)

    # --- ПОЛУЧАЕМ ТОВАРЫ (С ПОЛНЫМ НАБОРОМ ПОЛЕЙ И ЦЕНАМИ) ---
    # Используем подзапросы, чтобы сразу вытащить последнюю подтвержденную цену
    products_raw = db.execute("""
        SELECT p.id, p.name, p.folder_id, p.price as manual_price, p.photo_url, p.brand, p.country, p.weight, p.model_number, p.is_on_request,
               p.color, p.storage, p.ram, p.warranty, p.description, p.specs,
               (SELECT pm.extracted_price FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as parsed_price,
               (SELECT pm.is_actual FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as is_actual,
               (SELECT m.chat_id FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as latest_chat_id
        FROM products p
        WHERE p.user_id = ?
    """, (user_id,)).fetchall()
    
    products = []
    active_folder_ids = set()
    
    # Правила доступа (какие товары и каких поставщиков видит этот клиент)
    try:
        access_rules = json.loads(client['access_rules']) if 'access_rules' in client.keys() and client['access_rules'] else {}
    except:
        access_rules = {}

    for p in products_raw:
        # 1. Фильтр доступа: если товар не отмечен в access_rules - пропускаем
        pid_str = str(p['id'])
        if not access_rules or pid_str not in access_rules: 
            continue
            
        # 2. Фильтр поставщиков (чатов)
        allowed_chats = access_rules[pid_str]
        chat_id_str = str(p['latest_chat_id'])
        
        # Если есть цена от конкретного чата, проверяем, разрешен ли он (или разрешены 'all')
        if chat_id_str != 'None':
            if allowed_chats != ['all'] and chat_id_str not in allowed_chats:
                # Цена от этого поставщика запрещена, но мы можем использовать ручную цену p['manual_price']
                # Если ручной цены нет, товар станет "По запросу"
                current_price_source = None 
            else:
                current_price_source = p['parsed_price'] if p['is_actual'] == 1 else None
        else:
            current_price_source = None

        # 3. Расчет базовой цены
        base_price = float(current_price_source if current_price_source is not None else (p['manual_price'] or 0))
        
        # 4. Применение наценок и округления
        if base_price > 0 and not p['is_on_request']:
            mk = markups.get(p['folder_id'], default_markup)
            if mk['markup_type'] == 'percent':
                final_price = base_price * (1 + mk['markup_value'] / 100)
            else:
                final_price = base_price + mk['markup_value']
                
            rnd = mk['rounding']
            if rnd > 0:
                final_price = math.ceil(final_price / rnd) * rnd
            final_price = int(final_price)
        else:
            final_price = "По запросу"

        # 5. Обработка фотографий (формируем полные ссылки)
        photo_links = []
        if p['photo_url']:
            # Базовый URL вашего сервера
            base_img_url = "https://engine.astoredirect.ru/uploads/"
            photo_links = [
                (f"{base_img_url}{link.strip()}" if not link.strip().startswith('http') else link.strip())
                for link in p['photo_url'].split(',') if link.strip()
            ]

        # 6. Обработка технических характеристик (specs)
        specs_data = {}
        if p['specs']:
            try:
                specs_data = json.loads(p['specs'])
            except:
                specs_data = {}

        # 7. Формируем финальный объект товара
        products.append({
            'id': p['id'],
            'name': p['name'],
            'category_id': p['folder_id'],
            'price': final_price, 
            'price2': "По запросу" ,
            "photo_url": photo_links,
            'brand': p['brand'],
            'country': p['country'],
            'weight': p['weight'],
            'model_number': p['model_number'],
            'is_on_request': bool(p['is_on_request']),
            
            # Новые поля
            'color': p['color'],
            'storage': p['storage'],
            'ram': p['ram'],
            'warranty': p['warranty'],
            'description': p['description'],
            'specs': specs_data
        })
        
        # Запоминаем ID папки, чтобы потом показать её в списке категорий
        if p['folder_id']:
            active_folder_ids.add(p['folder_id'])

    # --- ОБРАБОТКА КАТЕГОРИЙ (только те, в которых есть товары) ---
    folders_raw = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    parent_map = {f['id']: f['parent_id'] for f in folders_raw}
    folders_to_keep = set(active_folder_ids)
    
    # Добавляем всех родителей активных папок (чтобы дерево не рассыпалось)
    for fid in active_folder_ids:
        current = fid
        while current in parent_map and parent_map[current] is not None:
            current = parent_map[current]
            folders_to_keep.add(current)
            
    categories = [
        {'id': f['id'], 'name': f['name'], 'parent_id': f['parent_id']} 
        for f in folders_raw if f['id'] in folders_to_keep
    ]
                  
    # --- СОРТИРОВКА (А-Я) ---
    products.sort(key=lambda x: (x['name'] or '').lower())
    categories.sort(key=lambda x: (x['name'] or '').lower())
        
    return jsonify({
        'categories': categories,
        'products': products
    })


# ================= ПАРСИНГ EXCEL ТАБЛИЦ =================
async def parse_excel_message(client, message, chat_id, user_id):
    text_to_save = message.text or ""
    if not message.document:
        return text_to_save
        
    ext = getattr(message.file, 'ext', '').lower()
    if ext not in ['.xlsx', '.xls', '.pdf']:
        return text_to_save

    # Функция для приведения ID к единому формату (без -100)
    def clean_id(cid):
        s = str(cid)
        if s.startswith('-100'): return s[4:]
        if s.startswith('-'): return s[1:]
        return s

    with app.app_context():
        db = get_db()
        target_id = clean_id(chat_id)
        
        # Загружаем все конфиги для этого пользователя
        all_configs = db.execute("SELECT * FROM excel_configs WHERE user_id = ?", (user_id,)).fetchall()
        configs = {}
        for c in all_configs:
            if clean_id(c['chat_id']) == target_id:
                configs[c['sheet_name']] = dict(c)
        
        # Если конфигов нет вообще - выходим сразу
        if not configs:
            logger.warning(f"Нет настроек парсинга для чата {chat_id}")
            return text_to_save

        tmp_path = None
        try:
            # Скачиваем файл во временное хранилище
            suffix = ext
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                await client.download_media(message, tmp.name)
                tmp_path = tmp.name
                
            parsed_lines = []
            unknown_sheets = []
            wildcard_config = configs.get('*')

            # --- ОБРАБОТКА PDF ---
            if ext == '.pdf':
                import pdfplumber
                # Для PDF ищем: 'Страница 1', затем '*', если нет - берем первый попавшийся
                config = configs.get('Страница 1') or wildcard_config
                if not config and configs:
                    config = list(configs.values())[0]
                
                if config:
                    with pdfplumber.open(tmp_path) as pdf:
                        for page in pdf.pages:
                            table = page.extract_table()
                            if not table: continue
                            
                            # Превращаем таблицу в безопасный список строк
                            clean_rows = []
                            for row in table:
                                clean_rows.append([str(cell).replace('\n', ' ') if cell is not None else "" for cell in row])
                            
                            step = int(config['block_step'])
                            start = int(config['start_row'])
                            n_col = int(config['name_col'])
                            p_col = int(config['price_col'])
                            n_off = int(config['name_row_offset'])
                            p_off = int(config['price_row_offset'])
                            is_grouped = config.get('is_grouped', 0)
                            sku_col = config.get('sku_col', -1)

                            if is_grouped == 1:
                                current_group = ""
                                for i in range(start, len(clean_rows)):
                                    row = clean_rows[i]
                                    # Проверка: хватает ли колонок в этой строке PDF?
                                    if len(row) <= max(n_col, p_col, sku_col): continue
                                    
                                    col_n_val = row[n_col].strip()
                                    col_sku_val = row[sku_col].strip() if sku_col != -1 else ""
                                    col_p_val = row[p_col].strip()
                                    
                                    if col_n_val and not col_p_val: current_group = col_n_val
                                    if col_p_val and current_group:
                                        name = col_sku_val if col_sku_val else col_n_val
                                        parsed_lines.append(f"{current_group} {name} - {col_p_val}")
                            else:
                                for i in range(start, len(clean_rows), step):
                                    name_idx = i + n_off
                                    price_idx = i + p_off
                                    if name_idx < len(clean_rows) and price_idx < len(clean_rows):
                                        row_n = clean_rows[name_idx]
                                        row_p = clean_rows[price_idx]
                                        if len(row_n) > n_col and len(row_p) > p_col:
                                            n_val = row_n[n_col].strip()
                                            p_val = row_p[p_col].strip()
                                            if n_val and p_val:
                                                parsed_lines.append(f"{n_val} - {p_val}")

            # --- ОБРАБОТКА EXCEL ---
            else:
                xls = pd.ExcelFile(tmp_path)
                for sheet_name in xls.sheet_names:
                    config = configs.get(sheet_name) or wildcard_config
                    if not config:
                        unknown_sheets.append(sheet_name)
                        continue            
                
                        
                    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                    df = df.fillna("")
                    
                    step = config['block_step']
                    start = config['start_row']
                    n_col = config['name_col']
                    p_col = config['price_col']
                    n_off = config['name_row_offset']
                    p_off = config['price_row_offset']
                    is_grouped = config.get('is_grouped', 0)
                    sku_col = config.get('sku_col', -1)
                    
                    if is_grouped == 1:
                        current_group = ""
                        for i in range(start, len(df)):
                            try:
                                col_n_val = str(df.iat[i, n_col]).strip()
                                col_sku_val = str(df.iat[i, sku_col]).strip() if sku_col != -1 else ""
                                col_p_val = str(df.iat[i, p_col]).strip()
                                
                                is_empty_name = (not col_n_val or col_n_val == 'nan')
                                has_price = (col_p_val and col_p_val != 'nan')
                                
                                if n_col == sku_col:
                                    if col_n_val and not has_price: current_group = col_n_val
                                    elif col_n_val and has_price and current_group:
                                        parsed_lines.append(f"{current_group} {col_n_val} - {col_p_val}")
                                else:
                                    if col_n_val and not is_empty_name: current_group = col_n_val
                                    if col_sku_val and col_sku_val != 'nan' and has_price and current_group:
                                        parsed_lines.append(f"{current_group} {col_sku_val} - {col_p_val}")
                            except Exception:
                                continue
                    else:
                        for i in range(start, len(df), step):
                            try:
                                name_row = i + n_off
                                price_row = i + p_off
                                if name_row < len(df) and price_row < len(df):
                                    name_val = str(df.iat[name_row, n_col]).strip()
                                    price_val = str(df.iat[price_row, p_col]).strip()
                                    
                                    if name_val and name_val != 'nan' and price_val and price_val != 'nan':
                                        parsed_lines.append(f"{name_val} - {price_val}")
                            except Exception:
                                continue
            
            # --- ФОРМИРОВАНИЕ ИТОГОВОГО ТЕКСТА ---
            if parsed_lines:
                doc_type = "PDF" if ext == '.pdf' else "EXCEL"
                text_to_save = f"📊 [{doc_type} ДАННЫЕ]:\n" + "\n".join(parsed_lines)
                
                if unknown_sheets and ext != '.pdf':
                    with app.app_context():
                        db = get_db()
                        all_chats = db.execute("SELECT chat_id, chat_title, custom_name FROM tracked_chats WHERE user_id = ?", (user_id,)).fetchall()
                        chat_title = str(chat_id)
                        for tc in all_chats:
                            if clean_id(tc['chat_id']) == target_id:
                                chat_title = tc['custom_name'] if tc['custom_name'] else tc['chat_title']
                                break
                                
                        for us in unknown_sheets:
                            db.execute("INSERT OR IGNORE INTO excel_missing_sheets (user_id, chat_id, chat_title, sheet_name) VALUES (?, ?, ?, ?)", (user_id, chat_id, chat_title, us))
                        db.commit()
                        
                    warning_text = f"\n\n⚠️ Внимание! В файле найдены ненастроенные листы: {', '.join(unknown_sheets)}. Перейдите в раздел «Парсинг Excel», чтобы их настроить."
                    if text_to_save:
                        text_to_save += warning_text
                    else:
                        text_to_save = warning_text.strip()
                        
        except Exception as e:
            logger.error(f"Ошибка парсинга файла: {e}")
        finally:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    return text_to_save


@app.route('/api/excel/preview_latest/<chat_id>', methods=['GET'])
@login_required
def preview_latest_excel(chat_id):
    chat_id = int(chat_id) # Добавляем преобразование здесь
    user_id = session['user_id']
    
    # Ищем активную сессию юзербота
    client_to_use = None
    client_loop_to_use = None
    for sid, (thread, client, uid) in user_clients.items():
        if uid == user_id:
            client_to_use = client
            client_loop_to_use = user_loops.get(sid)
            break
            
    if not client_to_use:
        return jsonify({'error': 'Юзербот не запущен. Запустите его во вкладке "Юзерботы".'}), 400

    async def fetch_and_preview():
        try:
            entity = await client_to_use.get_entity(chat_id)
            # Ищем последние 50 сообщений
            async for message in client_to_use.iter_messages(entity, limit=50):
                if message.document and getattr(message.file, 'ext', '').lower() in ['.xlsx', '.xls', '.pdf']:
                    ext = getattr(message.file, 'ext', '').lower()
                    
                    # Скачиваем файл во временную директорию
                    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                        await client_to_use.download_media(message, tmp.name)
                        tmp_path = tmp.name
                        
                    try:
                        xls = pd.ExcelFile(tmp_path)
                        data = {}
                        # Читаем по 15 строк для предпросмотра
                        for sheet in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=15)
                            df = df.fillna("")
                            data[sheet] = df.values.tolist()
                        return {'success': True, 'sheets_data': data}
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                            
            return {'success': False, 'error': 'В последних 50 сообщениях чата не найден Excel файл'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # Запускаем асинхронную задачу в цикле юзербота и ждем результат
    future = asyncio.run_coroutine_threadsafe(fetch_and_preview(), client_loop_to_use)
    try:
        result = future.result(timeout=30)
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify({'error': result.get('error')}), 400
    except Exception as e:
        return jsonify({'error': 'Таймаут или ошибка при скачивании файла: ' + str(e)}), 500


@app.route('/api/excel/preview', methods=['POST'])
@login_required
def preview_excel():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    file = request.files['file']
    try:
        xls = pd.ExcelFile(file)
        data = {}
        # Читаем по 15 строк с КАЖДОГО листа
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=15)
            df = df.fillna("")
            data[sheet] = df.values.tolist()
        return jsonify({'success': True, 'sheets_data': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel/save_config', methods=['POST'])
@login_required
def save_excel_config():
    data = request.json
    db = get_db()
    
    chat_id = int(data['chat_id'])
    sheet_name = data['sheet_name']
    is_grouped = data.get('is_grouped', 0)
    sku_col = data.get('sku_col', -1)
    
    db.execute("""
        INSERT INTO excel_configs (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
        name_col=excluded.name_col, name_row_offset=excluded.name_row_offset, price_col=excluded.price_col, 
        price_row_offset=excluded.price_row_offset, block_step=excluded.block_step, start_row=excluded.start_row,
        is_grouped=excluded.is_grouped, sku_col=excluded.sku_col
    """, (session['user_id'], chat_id, sheet_name, data['name_col'], data['name_row_offset'], data['price_col'], data['price_row_offset'], data['block_step'], data['start_row'], is_grouped, sku_col))

    cid_str = str(chat_id)
    core_id = cid_str[4:] if cid_str.startswith('-100') else (cid_str[1:] if cid_str.startswith('-') else cid_str)
    possible_ids = [core_id, f"-{core_id}", f"-100{core_id}"]
    
    for pid in possible_ids:
        if sheet_name == '*':
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND user_id = ?", (pid, session['user_id']))
        else:
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND sheet_name = ? AND user_id = ?", (pid, sheet_name, session['user_id']))
    
    db.commit()
    return jsonify({'success': True})



@app.route('/api/excel/configs', methods=['GET'])
@login_required
def get_excel_configs():
    db = get_db()
    # Используем GROUP_CONCAT для объединения названий листов в одну строку
    query = """
        SELECT chat_id, source_type, sheet_url, 
               GROUP_CONCAT(sheet_name, ', ') as sheet_names,
               MAX(id) as id
        FROM excel_configs 
        WHERE user_id = ?
        GROUP BY chat_id, source_type, sheet_url
    """
    configs = db.execute(query, (session['user_id'],)).fetchall()
    return jsonify([dict(c) for c in configs])

@app.route('/api/excel/configs/<int:config_id>', methods=['DELETE'])
@login_required
def delete_excel_config(config_id):
    db = get_db()
    db.execute("DELETE FROM excel_configs WHERE id = ? AND user_id = ?", (config_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


import sqlite3

def update_database():
    conn = sqlite3.connect('app.db')
    cursor = conn.cursor()
    
    print("Начинаю проверку базы данных...")

    # --- ДОБАВЛЯЕМ КОЛОНКУ IS_DELAYED ---
    try:
        cursor.execute("ALTER TABLE messages ADD COLUMN is_delayed INTEGER DEFAULT 0")
        print("✅ Колонка is_delayed успешно добавлена в таблицу messages!")
    except sqlite3.OperationalError:
        pass

    # Полный список всех колонок для работы парсера (Google, PDF, Excel)
    columns_to_add = {
        "source_type": "TEXT DEFAULT 'excel'",
        "sheet_url": "TEXT",
        "parse_interval": "INTEGER DEFAULT 60",
        "is_grouped": "INTEGER DEFAULT 0",
        "sku_col": "INTEGER DEFAULT -1",
        "name_col": "INTEGER DEFAULT -1",
        "name_row_offset": "INTEGER DEFAULT 0",
        "price_row_offset": "INTEGER DEFAULT 0",
        "block_step": "INTEGER DEFAULT 1",
        "sheet_name": "TEXT DEFAULT '*'"
    }

    for col, col_type in columns_to_add.items():
        try:
            cursor.execute(f"ALTER TABLE excel_configs ADD COLUMN {col} {col_type}")
            print(f"✅ Колонка {col} успешно добавлена.")
        except sqlite3.OperationalError:
            print(f"⏩ Колонка {col} уже существует, пропускаем.")

    # Устанавливаем тип 'excel' для всех записей, где он еще не задан
    cursor.execute("UPDATE excel_configs SET source_type = 'excel' WHERE source_type IS NULL OR source_type = ''")

    # --- ДОБАВЛЯЕМ КОЛОНКИ FOLDER_ID ---
    try:
        cursor.execute("ALTER TABLE target_chats ADD COLUMN folder_id INTEGER;")
        print("✅ Колонка folder_id добавлена в таблицу target_chats")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE excel_configs ADD COLUMN folder_id INTEGER;")
        print("✅ Колонка folder_id добавлена в таблицу excel_configs")
    except sqlite3.OperationalError:
        pass

    # Фиксируем все изменения ОДИН РАЗ и закрываем базу
    conn.commit()
    conn.close()
    
    print("✅ Данные обновлены, старые конфиги теперь помечены как excel")
    print("🎉 Полное обновление базы данных завершено!")




@app.route('/api/fix_delayed')
def fix_delayed():
    try:
        db = get_db()
        db.execute("ALTER TABLE messages ADD COLUMN is_delayed INTEGER DEFAULT 0")
        db.commit()
        return "<h1>✅ База данных успешно обновлена! Колонка is_delayed добавлена.</h1>"
    except Exception as e:
        return f"<h1>Уже добавлена или произошла ошибка:</h1><p>{e}</p>"

from datetime import datetime

def is_parsing_allowed(session_id):
    """
    Проверяет, попадает ли текущее время в разрешенный интервал тайминга.
    """
    try:
        with app.app_context():
            db = get_db()
            # Добавили проверку schedule_enabled
            row = db.execute("SELECT schedule_enabled, time_start, time_end FROM user_telegram_sessions WHERE id = ?", (session_id,)).fetchone()
            
            # Если галочка "Расписание" не стоит или данных нет — разрешаем 24/7
            if not row or not row['schedule_enabled']:
                return True 
            
            # Сдвигаем время сервера на +3 часа (Москва).
            current_time = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
            
            start_time = datetime.strptime(row['time_start'], '%H:%M').time()
            end_time = datetime.strptime(row['time_end'], '%H:%M').time()

            if start_time <= end_time:
                return start_time <= current_time <= end_time
            else:
                return current_time >= start_time or current_time <= end_time

    except Exception as e:
        logger.error(f"Ошибка проверки тайминга для сессии {session_id}: {e}")
        return True

def delayed_messages_scheduler():
    while True:
        try:
            with app.app_context():
                db = get_db()
                # Проверяем все активные аккаунты
                cursor = db.execute("SELECT id as session_id, user_id FROM user_telegram_sessions WHERE status = 'active'")
                for session_row in cursor.fetchall():
                    session_id = session_row['session_id']
                    user_id = session_row['user_id']
                    
                    # Если для этого аккаунта СЕЙЧАС рабочее время
                    if is_parsing_allowed(session_id):
                        # Достаем все его скрытые сообщения
                        msg_cursor = db.execute("SELECT * FROM messages WHERE is_delayed = 1 AND user_id = ? ORDER BY date ASC", (user_id,))
                        delayed_messages = msg_cursor.fetchall()
                        
                        for msg in delayed_messages:
                            msg_id = msg['id']
                            parsed_text = msg['text']
                            chat_id = msg['chat_id']
                            
                            # 1. Снимаем флаг 'скрыто'
                            db.execute("UPDATE messages SET is_delayed = 0 WHERE id = ?", (msg_id,))
                            
                            # 2. ЗАПУСКАЕМ ОБРАБОТКУ (миграция привязок к товарам)
                            old_bindings = db.execute("""
                                SELECT pm.id, pm.line_index, p.synonyms
                                FROM product_messages pm
                                JOIN products p ON pm.product_id = p.id
                                JOIN messages m ON pm.message_id = m.id
                                WHERE m.chat_id = ? AND m.user_id = ? AND m.id != ?
                            """, (chat_id, user_id, msg_id)).fetchall()

                            if old_bindings and parsed_text:
                                lines = parsed_text.split('\n')
                                text_lower = parsed_text.lower()
                                
                                for b in old_bindings:
                                    synonyms = [s.strip().lower() for s in (b['synonyms'] or '').split(',') if s.strip()]
                                    line_idx = b['line_index']
                                    match_found = False
                                    new_price = None
                                    
                                    if line_idx == -1:
                                        if any(syn in text_lower for syn in synonyms):
                                            match_found = True
                                            new_price = extract_price(parsed_text)
                                    else:
                                        for i, line in enumerate(lines):
                                            if any(syn in line.lower() for syn in synonyms):
                                                match_found = True
                                                new_price = extract_price(line)
                                                if not new_price and i + 1 < len(lines):
                                                    new_price = extract_price(lines[i+1])
                                                if not new_price and i + 2 < len(lines):
                                                    new_price = extract_price(lines[i+2])
                                                line_idx = i
                                                break
                                     
                                    if match_found:
                                        db.execute("""
                                            UPDATE product_messages 
                                            SET message_id = ?, line_index = ?, extracted_price = ?, is_actual = 1 
                                            WHERE id = ?
                                        """, (msg_id, line_idx, new_price, b['id']))
                            
                            # Очистка старых сообщений (зазор 5 минут)
                            cutoff_time = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 1. Помечаем товары как неактуальные
                            db.execute("""
                                UPDATE product_messages 
                                SET is_actual = 0 
                                WHERE message_id IN (
                                    SELECT id FROM messages 
                                    WHERE chat_id = ? AND user_id = ? AND id != ? AND date < ?
                                )
                            """, (chat_id, user_id, msg_id, cutoff_time))
                            
                            # 2. Удаляем старые сообщения
                            db.execute("""
                                DELETE FROM messages 
                                WHERE chat_id = ? AND user_id = ? AND id != ? AND date < ?
                                  AND id NOT IN (SELECT message_id FROM product_messages WHERE is_actual = 1)
                            """, (chat_id, user_id, msg_id, cutoff_time))

                            db.commit()
                            
                            # 3. Отправляем уведомление на фронтенд
                            notify_clients()
                            
        except Exception as e:
            print(f"Ошибка в планировщике отложенных сообщений: {e}")
            
        time.sleep(60) # Проверяем тайминги каждую минуту

# ---------- Запуск приложения ----------

if __name__ == '__main__':
    update_database()
    threading.Thread(target=publish_scheduler, daemon=True).start()
    threading.Thread(target=api_parser_scheduler, daemon=True).start()
    threading.Thread(target=delayed_messages_scheduler, daemon=True).start()
    
    with app.app_context():
        db = get_db()
        cursor = db.execute("SELECT id, user_id, api_id, api_hash FROM user_telegram_sessions WHERE status = 'active'")
        for row in cursor.fetchall():
            start_message_listener(row['id'], row['user_id'], row['api_id'], row['api_hash'])
    # Запускаем планировщик взаимодействия с ботами
    threading.Thread(target=bot_interaction_scheduler, daemon=True).start()
    socketio.run(app, debug=True, host='0.0.0.0', port=5000, use_reloader=False, allow_unsafe_werkzeug=True)
