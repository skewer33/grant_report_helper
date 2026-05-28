import logging
import os
import re
import json
import asyncio
import pandas as pd
from functools import wraps
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from logging.handlers import TimedRotatingFileHandler

from aiogram.client.default import DefaultBotProperties
import yadisk
from yadisk.sessions.requests_session import RequestsSession
import requests
import posixpath


# --- НАСТРОЙКИ ---
load_dotenv(".env")
API_TOKEN = os.getenv('API_TOKEN')
YANDEX_TOKEN = os.getenv('YANDEX_TOKEN')
AUTHORIZED_USERS_FILE = "authorized_users.txt"
YADISK_HOME_PATH = os.getenv('YADISK_HOME_PATH')
TEMPLATE_STATE_FILE = "current_template.json"

# путь к файлу логов
TEMPLATES_FOLDER = os.path.join(YADISK_HOME_PATH, "Шаблоны")
# путь к файлу логов
REPORT_FOLDER = os.path.join(YADISK_HOME_PATH, "Документы")
# путь к папке логов
LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
# путь к локальной директории, хранящей Шаблоны
LOCAL_TEMPLATES_FOLDER = os.path.join(os.getcwd(), "Templates")
os.makedirs(LOCAL_TEMPLATES_FOLDER, exist_ok=True)
# json таблица, хранящая инфу о юзерах и ипользуемых ими шаблонах
USER_TEMPLATES_FILE = "user_templates.json"

# --- НАСТРОЙКА ЛОГГЕРА ---
os.makedirs(LOG_DIR, exist_ok=True)

time_handler = TimedRotatingFileHandler(
    os.path.join(LOG_DIR, "bot.log"),
    when="midnight",
    interval=1,
    backupCount=14,
    encoding="utf-8"
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        time_handler,
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# --- ИНИЦИАЛИЗАЦИЯ ---
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
yadisk_client = yadisk.YaDisk(token=YANDEX_TOKEN)

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

# Авторизация
def is_authorized(user_id: int) -> bool:
    # проверка авторизации
    try:
        with open(AUTHORIZED_USERS_FILE, "r") as f:
            return str(user_id) in f.read()
    except FileNotFoundError:
        logger.warning("Файл authorized_users.txt не найден")
        return False

def require_authorization(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        user_id = message.from_user.id
        if not is_authorized(user_id):
            await message.answer("\u274C Доступ запрещён. Вы не авторизованы.")
            return
        return await handler(message, *args, **kwargs)
    return wrapper

# Работа с шаблонами

def normalize_template_name(name: str) -> str:
    '''Нормализация имени шаблона: удаление пробелов по краям и суффикса .xlsx, если он есть'''
    
    return name.strip().removesuffix(".xlsx")

def get_local_template_path(template_name: str) -> str:
    '''
    получение локального пути шаблона
    '''
    return os.path.join(LOCAL_TEMPLATES_FOLDER, f"{template_name}.xlsx")


def ensure_local_template(template_name: str):
    '''
    Always download fresh template from Yandex.Disk to ensure cache consistency.
    '''
    local_path = get_local_template_path(template_name)
    
    # Always delete old cache to ensure fresh version
    if os.path.exists(local_path):
        os.remove(local_path)
        logger.info(f"Старый кеш {template_name} удален")
    
    # Download fresh template from Yandex.Disk
    yadisk_client.download(posixpath.join(TEMPLATES_FOLDER, f"{template_name}.xlsx"), local_path)
    logger.info(f"Шаблон {template_name} скачан в {local_path}")
    return local_path


def load_user_templates() -> dict:
    '''
    Загрузка пользовательского шаблона. Смотрит, пользовался ли пользователь шаблоном ранее и загружает его
    '''
    if not os.path.exists(USER_TEMPLATES_FILE):
        return {}
    with open(USER_TEMPLATES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_user_templates(user_templates: dict):
    '''
    Сохранение инфы о шаблоне, который использовал юзер
    '''
    with open(USER_TEMPLATES_FILE, "w", encoding="utf-8") as f:
        json.dump(user_templates, f, ensure_ascii=False, indent=2)

def set_user_template(user_id: int, template_name: str):
    '''
    Установка шаблона пользователя
    '''
    user_templates = load_user_templates()
    user_templates[str(user_id)] = template_name
    save_user_templates(user_templates)

def get_user_template(user_id: int) -> str | None:
    '''
    Получение шаблона пользователя
    '''
    user_templates = load_user_templates()
    return user_templates.get(str(user_id))

def get_savedir(user_id: str) -> str:
    """
    Возвращает имя шаблона пользователя для папки на Яндекс.Диске
    """
    template_name = get_user_template(user_id)
    if not template_name:
        logger.warning(f"Шаблон не найден для пользователя {user_id}")
        return None
    return template_name

def get_template_names():
    return [
        os.path.splitext(f['name'])[0]
        for f in yadisk_client.listdir(TEMPLATES_FOLDER)
        if f['path'].endswith(".xlsx")
    ]

def get_template_dataframe(name: str) -> pd.DataFrame:
    '''
    Получаем dataframe шаблона
    '''
    local_path = get_local_template_path(name)
    return pd.read_excel(local_path)

def get_expenses_csv_path(template_name: str) -> str:
    '''Get template-scoped expenses.csv path'''
    data_dir = os.path.join(os.getcwd(), "data", template_name)
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "expenses.csv")

def get_attachments_csv_path(template_name: str) -> str:
    '''Get template-scoped attachments.csv path'''
    data_dir = os.path.join(os.getcwd(), "data", template_name)
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, "attachments.csv")

# Работа с расходами (expenses.csv) - now template-scoped
def initialize_expenses_csv(template_name: str):
    '''
    Инициализация CSV файла расходов для шаблона, если его нет
    '''
    csv_path = get_expenses_csv_path(template_name)
    if not os.path.exists(csv_path):
        df = pd.DataFrame(columns=[
            "expense_id", "date", "title", "category", 
            "expense_item", "amount"
        ])
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Создан файл {csv_path}")

def get_next_expense_id(template_name: str) -> str:
    '''
    Получить следующий expense_id для шаблона (EXP001, EXP002, ...)
    '''
    initialize_expenses_csv(template_name)
    csv_path = get_expenses_csv_path(template_name)
    try:
        df = pd.read_csv(csv_path)
        if df.empty:
            return "EXP001"
        # Извлекаем номер из последнего ID
        last_id = df.iloc[-1]["expense_id"]
        if isinstance(last_id, str) and last_id.startswith("EXP"):
            try:
                num = int(last_id[3:])
                return f"EXP{num + 1:03d}"
            except ValueError:
                return "EXP001"
        return "EXP001"
    except Exception as e:
        logger.error(f"Ошибка при получении next expense_id: {e}")
        return "EXP001"

def read_expenses(template_name: str) -> list:
    '''
    Прочитать все расходы из CSV для шаблона
    '''
    initialize_expenses_csv(template_name)
    csv_path = get_expenses_csv_path(template_name)
    try:
        df = pd.read_csv(csv_path)
        return df.to_dict('records')
    except Exception as e:
        logger.error(f"Ошибка при чтении расходов: {e}")
        return []

def write_expense(template_name: str, expense_dict: dict):
    '''
    Добавить расход в CSV
    expense_dict должен содержать: expense_id, date, title, category, expense_item, amount
    '''
    initialize_expenses_csv(template_name)
    csv_path = get_expenses_csv_path(template_name)
    try:
        df = pd.read_csv(csv_path)
        new_row = pd.DataFrame([expense_dict])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Добавлен расход {expense_dict.get('expense_id')}")
    except Exception as e:
        logger.error(f"Ошибка при записи расхода: {e}")

def get_expense_by_id(template_name: str, expense_id: str) -> dict | None:
    '''
    Получить расход по ID для шаблона
    '''
    expenses = read_expenses(template_name)
    for exp in expenses:
        if exp.get("expense_id") == expense_id:
            return exp
    return None

# Работа с приложениями (attachments.csv) - now template-scoped
def initialize_attachments_csv(template_name: str):
    '''Инициализация CSV файла приложений для шаблона, если его нет'''
    csv_path = get_attachments_csv_path(template_name)
    if not os.path.exists(csv_path):
        df = pd.DataFrame(columns=[
            "expense_id", "file_type", "filename", "relative_path", "public_url"
        ])
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Создан файл {csv_path}")

def write_attachment(template_name: str, attachment_dict: dict):
    '''Добавить приложение в CSV'''
    initialize_attachments_csv(template_name)
    csv_path = get_attachments_csv_path(template_name)
    try:
        df = pd.read_csv(csv_path)
        new_row = pd.DataFrame([attachment_dict])
        df = pd.concat([df, new_row], ignore_index=True)
        df.to_csv(csv_path, index=False, encoding="utf-8")
        logger.info(f"Добавлено приложение для {attachment_dict.get('expense_id')}")
    except Exception as e:
        logger.error(f"Ошибка при записи приложения: {e}")

# РАБОТА С КЛАВИАТУРОЙ
def build_reply_keyboard(options: list[str]) -> ReplyKeyboardMarkup:
    options.append("🔙 Назад")
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=opt)] for opt in options],
        resize_keyboard=True,
        one_time_keyboard=True
    )

# РАБОТА С ФАЙЛАМИ
def get_unique_filename(folder_path: str, filename: str) -> str:
    '''
    Сделать имя файла уникальным, добавив _index на конце, если такой файл уже существует
    '''
    base, ext = os.path.splitext(filename)
    index = 1
    candidate = f"{base}{ext}"
    while yadisk_client.exists(posixpath.join(folder_path, candidate)):
        candidate = f"{base}_{index}{ext}"
        index += 1
    return candidate

async def create_file_public_url(path):
    '''
    Создание публичной ссылки на файл по его пути на Я.Диске
    '''
    meta = yadisk_client.get_meta(path)
    attempts = 5
    waiting_times = [(i+1)**2 - (i) for i in range(attempts)]
    public_url = None
    for i in range(attempts):
        meta = yadisk_client.get_meta(path)
        public_url = meta.public_url if hasattr(meta, "public_url") and meta.public_url else None
        if public_url:
            break
        logger.info(f"URL isn`t exist, sleeping {waiting_times[i]} seconds and try again")
        await asyncio.sleep(waiting_times[i])
    if not public_url:
        public_url = "Ссылка недоступна\nВозможно такой файл уже был загружен ранее, если нет, то произошла ошибка, попробуйте ещё раз"
    return public_url

async def processing_document(message: types.Message):
    '''
    функция для обработки документов
    Поддерживает два workflow:
    1. Старый (backward compatibility): категория/статья/тип/сумма/дата в шаблонную папку
    2. Новый (Phase 3-4): расход с документом в папку расхода
    '''
    
    user_id = str(message.from_user.id)
    state_data = dp.workflow_data.get(user_id, {})

    # Определяем, что прислал пользователь
    if message.document:
        file_id = message.document.file_id
        orig_filename = message.document.file_name
    elif message.photo:
        file_id = message.photo[-1].file_id
        orig_filename = f"photo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    else:
        await message.answer("❌ Не удалось получить файл. Попробуйте снова.")
        return

    file_info = await bot.get_file(file_id)
    local_path = os.path.join(os.getcwd(), orig_filename)
    await bot.download_file(file_info.file_path, destination=local_path)
    ext = os.path.splitext(orig_filename)[1]

    current_template = get_user_template(user_id)
    if not current_template:
        await message.answer("❌ Не удалось определить текущий шаблон.")
        return

    savedir_name = get_savedir(user_id)
    
    # === NEW WORKFLOW (Phase 3-4): Expense-linked documents ===
    if "selected_expense_id" in state_data:
        selected_expense_id = state_data.get("selected_expense_id")
        file_type = state_data.get("file_type", "Док")
        
        # Get expense details for category and other info
        expense = get_expense_by_id(current_template, selected_expense_id)
        if not expense:
            await message.answer(f"❌ Расход {selected_expense_id} не найден.")
            return
        
        amount = expense.get("amount", "0")
        date = expense.get("date", datetime.now().strftime("%Y-%m-%d"))
        category = expense.get("category", "Прочее")
        
        # Format filename: EXP001_receipt_22000_2025-06-17.pdf
        base_name = f"{selected_expense_id}_{file_type}_{amount}_{date}"
        
        # Upload to category folder on Yandex.Disk
        category_folder = posixpath.join(REPORT_FOLDER, savedir_name, category)
        filename = get_unique_filename(category_folder, base_name + ext)
        file_path_on_disk = posixpath.join(category_folder, filename)
        
        try:
            yadisk_client.mkdir(category_folder, parents=True)
        except yadisk.exceptions.PathExistsError:
            pass
        
        yadisk_client.upload(local_path, file_path_on_disk)
        
        try:
            yadisk_client.publish(file_path_on_disk)
        except yadisk.exceptions.PathAlreadyPublicError:
            pass
        
        public_url = await create_file_public_url(file_path_on_disk)
        
        # Record in attachments.csv with new format: template/category/filename
        relative_path = f"{savedir_name}/{category}/{filename}"
        attachment_dict = {
            "expense_id": selected_expense_id,
            "file_type": file_type,
            "filename": filename,
            "relative_path": relative_path,
            "public_url": public_url
        }
        write_attachment(current_template, attachment_dict)
        
        logger.info(f"Загружен документ '{filename}' для расхода {selected_expense_id}")
        await message.answer(
            f"✅ Документ загружен для расхода <b>{selected_expense_id}</b>\n"
            f"Файл: {filename}\n"
            f"Ссылка: {public_url}"
        )
    
    
    dp.workflow_data.pop(user_id, None)
    await cmd_start(message)
    
    # Удаляем локальный файл
    try:
        os.remove(local_path)
        logger.info(f"Локальный файл {local_path} удалён после загрузки.")
    except Exception as e:
        logger.warning(f"Не удалось удалить локальный файл {local_path}: {e}")


# --- СЛОВАРЬ ТИПОВ ДОКУМЕНТОВ ---
d_typedoc = {
    "Кассовый чек": "Чек", "Ведомость на вручение": "ВВруч", "Акт": "Акт",
    "Товарная накладная": "ТНакл", "Договор": "Дгвр", "Гарантийный Талон": "Гарант", "Документ": "Док"
}


# --- ХЭНДЛЕРЫ ---
@dp.message(CommandStart())
@require_authorization
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    logger.info(f"Пользователь {user_id} начал работу с ботом")
    print(f"\U0001F511 USER ID: {user_id}")

    if not is_authorized(message.from_user.id):
        await message.answer(f"Ваш Telegram ID: {user_id}")
        await message.answer("\u274C Доступ запрещён. Вы не авторизованы.")
        return

    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Новый расход"), KeyboardButton(text="📎 Добавить документ")],
            [KeyboardButton(text="🔍 Посмотреть расход"), KeyboardButton(text="📋 Последние расходы")],
            [KeyboardButton(text="📄 Выбрать шаблон"), KeyboardButton(text="✏️ Создать шаблон")],
            [KeyboardButton(text="📤 Экспорт")],
        ], resize_keyboard=True
    )
    await message.answer("✅ Добро пожаловать! Выберите действие:", reply_markup=kb)

@dp.message(F.text == "➕ Новый расход")
@require_authorization
async def handle_new_expense_prompt(message: types.Message):
    user_id = str(message.from_user.id)
    current_template = get_user_template(user_id)
    if not current_template:
        await message.answer("❌ Сначала выберите шаблон с помощью '📄 Выбрать шаблон'")
        return
    
    logger.info(f"{user_id} создаёт новый расход")
    dp.workflow_data[user_id] = {"state": "awaiting_expense_title"}
    await message.answer("Введите название расхода (например: 'Аренда беседки на базе'):")

@dp.message(F.text == "✏️ Создать шаблон")
@require_authorization
async def handle_create_template_name(message: types.Message):
    user_id = str(message.from_user.id)
    logger.info(f"{user_id} создаёт шаблон")
    await message.answer("Введите название шаблона:")
    dp.workflow_data[str(user_id)] = {"state": "awaiting_template_name"}

@dp.message(F.text == "📄 Выбрать шаблон")
@require_authorization
async def handle_choose_template(message: types.Message):
    user_id = str(message.from_user.id)
    templates = get_template_names()
    if not templates:
        await message.answer("❌ Шаблоны не найдены.")
        return
    kb = build_reply_keyboard(templates)
    dp.workflow_data[user_id] = {"state": "awaiting_template_choice"}
    await message.answer("Выберите шаблон:", reply_markup=kb)

@dp.message(F.text == "📎 Добавить документ")
@require_authorization
async def handle_add_document_prompt(message: types.Message):
    user_id = str(message.from_user.id)
    template_name = get_user_template(user_id)
    if not template_name:
        await message.answer("❌ Сначала выберите шаблон с помощью '📄 Выбрать шаблон'")
        return
    
    expenses = read_expenses(template_name)
    if not expenses:
        await message.answer("❌ Нет расходов. Сначала создайте расход через '➕ Новый расход'")
        return
    
    expense_list = [f"{i+1}. {exp['expense_id']} | {exp['title']} | {exp['amount']} ₽" 
                    for i, exp in enumerate(expenses)]
    kb = build_reply_keyboard(expense_list)
    dp.workflow_data[user_id] = {
        "state": "awaiting_expense_selection",
        "expenses": expenses
    }
    await message.answer("Выберите расход для добавления документа:", reply_markup=kb)

@dp.message(F.text == "🔍 Посмотреть расход")
@require_authorization
async def handle_view_expense(message: types.Message):
    user_id = str(message.from_user.id)
    template_name = get_user_template(user_id)
    if not template_name:
        await message.answer("❌ Сначала выберите шаблон с помощью '📄 Выбрать шаблон'")
        return
    
    expenses = read_expenses(template_name)
    if not expenses:
        await message.answer("❌ Нет расходов.")
        return
    
    expense_list = [f"{i+1}. {exp['expense_id']} | {exp['title']} | {exp['amount']} ₽" 
                    for i, exp in enumerate(expenses)]
    kb = build_reply_keyboard(expense_list + ["🔙 Назад"])
    dp.workflow_data[user_id] = {
        "state": "viewing_expense_selection",
        "expenses": expenses,
        "template_name": template_name
    }
    await message.answer("Выберите расход:", reply_markup=kb)

@dp.message(F.text == "📋 Последние расходы")
@require_authorization
async def handle_recent_expenses(message: types.Message):
    user_id = str(message.from_user.id)
    template_name = get_user_template(user_id)
    if not template_name:
        await message.answer("❌ Сначала выберите шаблон с помощью '📄 Выбрать шаблон'")
        return
    
    expenses = read_expenses(template_name)
    if not expenses:
        await message.answer("❌ Нет расходов.")
        return
    
    # Show last 10 expenses
    recent = expenses[-10:] if len(expenses) > 10 else expenses
    text = "📋 <b>Последние расходы:</b>\n\n"
    for i, exp in enumerate(recent, 1):
        text += f"{i}. <b>{exp['expense_id']}</b> | {exp['title']} | {exp['amount']} ₽\n"
    
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏠 Главное меню")]], 
        resize_keyboard=True
    )
    await message.answer(text, reply_markup=kb)

# @dp.message()
# async def debug(message: types.Message):
#     logger.info((repr(message.text)))

@dp.message(F.text == "📤 Экспорт")
@require_authorization
async def handle_export(message: types.Message):
    user_id = str(message.from_user.id)
    template_name = get_user_template(user_id)
    if not template_name:
        await message.answer("❌ Сначала выберите шаблон с помощью '📄 Выбрать шаблон'")
        return
    
    expenses = read_expenses(template_name)
    if not expenses:
        await message.answer("❌ Нет расходов для экспорта.")
        return
    
    # Create Excel export with multiple sheets
    export_file = f"export_{template_name}_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    export_path = os.path.join(os.getcwd(), export_file)
    
    try:
        df_expenses = pd.DataFrame(expenses)
        
        # Try to load attachments
        csv_path = get_attachments_csv_path(template_name)
        try:
            df_attachments = pd.read_csv(csv_path)
        except FileNotFoundError:
            df_attachments = pd.DataFrame()
        
        # Create Excel with multiple sheets
        with pd.ExcelWriter(export_path, engine='openpyxl') as writer:
            df_expenses.to_excel(writer, sheet_name='Расходы', index=False)
            if not df_attachments.empty:
                df_attachments.to_excel(writer, sheet_name='Документы', index=False)
        
        # Send file
        await message.answer_document(
            document=types.FSInputFile(export_path),
            caption=f"✅ Экспорт {len(expenses)} расходов"
        )
        
        logger.info(f"Экспорт {user_id}: создан файл {export_path}")
        os.remove(export_path)
            
    except Exception as e:
        logger.exception("Ошибка при экспорте")
        await message.answer(f"❌ Ошибка при создании экспорта: {e}")


@dp.message(F.text)
@require_authorization
async def handle_text_inputs(message: types.Message):
    user_id = str(message.from_user.id)
    text = message.text.strip()
    state_data = dp.workflow_data.get(user_id, {})
    state = state_data.get("state")

    if text == "🔙 Назад":
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)
        return

    # Workflow: добавление документа к расходу
    if state == "awaiting_expense_selection":
        expenses = state_data.get("expenses", [])
        try:
            idx = int(text.split(".")[0]) - 1
            if 0 <= idx < len(expenses):
                selected_expense = expenses[idx]
                dp.workflow_data[user_id]["selected_expense_id"] = selected_expense["expense_id"]
                dp.workflow_data[user_id]["state"] = "awaiting_file_type"
                
                typedoc_options = list(d_typedoc.keys())
                kb = build_reply_keyboard(typedoc_options)
                await message.answer("Выберите тип документа:", reply_markup=kb)
            else:
                await message.answer("❌ Неверный выбор. Попробуйте снова.")
        except (ValueError, IndexError):
            await message.answer("❌ Пожалуйста, выберите расход из списка.")
    
    elif state == "viewing_expense_selection":
        expenses = state_data.get("expenses", [])
        try:
            idx = int(text.split(".")[0]) - 1
            if 0 <= idx < len(expenses):
                expense = expenses[idx]
                expense_id = expense["expense_id"]
                
                # Build expense details message
                msg = f"<b>{expense_id}</b>\n\n"
                msg += f"<b>Название:</b> {expense['title']}\n"
                msg += f"<b>Категория:</b> {expense['category']}\n"
                msg += f"<b>Статья:</b> {expense['expense_item']}\n"
                msg += f"<b>Сумма:</b> {expense['amount']} ₽\n"
                msg += f"<b>Дата:</b> {expense['date']}\n\n"
                
                # Get attachments for this expense
                template_name = state_data.get("template_name", "")
                try:
                    csv_path = get_attachments_csv_path(template_name)
                    df = pd.read_csv(csv_path)
                    attachments = df[df["expense_id"] == expense_id]
                    if not attachments.empty:
                        msg += f"<b>Документы:</b>\n"
                        for _, att in attachments.iterrows():
                            msg += f"✓ {att['file_type']} - {att['filename']}\n"
                    else:
                        msg += f"<b>Документы:</b> нет\n"
                except FileNotFoundError:
                    msg += f"<b>Документы:</b> нет\n"
                
                dp.workflow_data.pop(user_id, None)
                kb = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="🏠 Главное меню")]], 
                    resize_keyboard=True
                )
                await message.answer(msg, reply_markup=kb)
            else:
                await message.answer("❌ Неверный выбор. Попробуйте снова.")
        except (ValueError, IndexError):
            await message.answer("❌ Пожалуйста, выберите расход из списка.")
    
    elif state == "awaiting_file_type":
        file_type = d_typedoc.get(text, text[:6])
        dp.workflow_data[user_id]["file_type"] = file_type
        dp.workflow_data[user_id]["state"] = "awaiting_file_upload"
        await message.answer("📄 Загрузите документ или фото:", reply_markup=ReplyKeyboardRemove())
    
    # Workflow: создание нового расхода
    elif state == "awaiting_expense_title":
        dp.workflow_data[user_id]["expense_title"] = text
        dp.workflow_data[user_id]["state"] = "awaiting_expense_category"
        
        current_template = get_user_template(user_id)
        df = get_template_dataframe(current_template)
        categories = df['Категория'].dropna().unique().tolist()
        kb = build_reply_keyboard(categories)
        await message.answer("Выберите категорию:", reply_markup=kb)
    
    elif state == "awaiting_expense_category":
        current_template = get_user_template(user_id)
        df = get_template_dataframe(current_template)
        
        if text not in df["Категория"].values:
            await message.answer("❌ Неверная категория. Попробуйте снова.")
            return
        
        dp.workflow_data[user_id]["expense_category"] = text
        dp.workflow_data[user_id]["state"] = "awaiting_expense_item"
        
        articles = df[df["Категория"] == text]["Статья Расходов"].dropna().unique().tolist()
        kb = build_reply_keyboard(articles)
        await message.answer("Выберите статью расходов:", reply_markup=kb)
    
    elif state == "awaiting_expense_item":
        current_template = get_user_template(user_id)
        df = get_template_dataframe(current_template)
        category = dp.workflow_data[user_id].get("expense_category")
        
        # Проверяем, существует ли такая статья в этой категории
        valid_rows = df[(df["Категория"] == category) & (df["Статья Расходов"] == text)]
        if valid_rows.empty:
            await message.answer("❌ Неверная статья расходов. Попробуйте снова.")
            return
        
        dp.workflow_data[user_id]["expense_item"] = text
        dp.workflow_data[user_id]["state"] = "awaiting_expense_amount"
        await message.answer("Введите сумму в рублях:")
    
    elif state == "awaiting_expense_amount":
        if not re.match(r"^\d+(\.\d{1,2})?$", text):
            await message.answer("❌ Введите корректную сумму (только цифры, можно с точкой).")
            return
        
        dp.workflow_data[user_id]["expense_amount"] = text
        dp.workflow_data[user_id]["state"] = "awaiting_expense_date_choice"
        kb = build_reply_keyboard(["Сегодня", "Своя дата"])
        await message.answer("Выберите дату:", reply_markup=kb)
    
    elif state == "awaiting_expense_date_choice":
        if text == "Сегодня":
            dp.workflow_data[user_id]["expense_date"] = datetime.now().strftime("%Y-%m-%d")
            dp.workflow_data[user_id]["state"] = "awaiting_expense_confirmation"
            
            # Показываем подтверждение
            title = dp.workflow_data[user_id].get("expense_title")
            category = dp.workflow_data[user_id].get("expense_category")
            item = dp.workflow_data[user_id].get("expense_item")
            amount = dp.workflow_data[user_id].get("expense_amount")
            date = dp.workflow_data[user_id].get("expense_date")
            
            confirmation_text = (
                f"📋 Подтверждение расхода:\n\n"
                f"<b>Название:</b> {title}\n"
                f"<b>Категория:</b> {category}\n"
                f"<b>Статья:</b> {item}\n"
                f"<b>Сумма:</b> {amount} ₽\n"
                f"<b>Дата:</b> {date}\n\n"
                f"Всё верно?"
            )
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ Подтвердить")],
                    [KeyboardButton(text="❌ Отмена")],
                ], resize_keyboard=True
            )
            await message.answer(confirmation_text, reply_markup=kb)
        
        elif text == "Своя дата":
            dp.workflow_data[user_id]["state"] = "awaiting_expense_custom_date"
            await message.answer("Введите дату в формате YYYY-MM-DD (например: 2025-06-17):")
        else:
            await message.answer("❌ Пожалуйста, выберите 'Сегодня' или 'Своя дата'")
    
    elif state == "awaiting_expense_custom_date":
        try:
            datetime.strptime(text, "%Y-%m-%d")
            dp.workflow_data[user_id]["expense_date"] = text
            dp.workflow_data[user_id]["state"] = "awaiting_expense_confirmation"
            
            # Показываем подтверждение
            title = dp.workflow_data[user_id].get("expense_title")
            category = dp.workflow_data[user_id].get("expense_category")
            item = dp.workflow_data[user_id].get("expense_item")
            amount = dp.workflow_data[user_id].get("expense_amount")
            date = dp.workflow_data[user_id].get("expense_date")
            
            confirmation_text = (
                f"📋 Подтверждение расхода:\n\n"
                f"<b>Название:</b> {title}\n"
                f"<b>Категория:</b> {category}\n"
                f"<b>Статья:</b> {item}\n"
                f"<b>Сумма:</b> {amount} ₽\n"
                f"<b>Дата:</b> {date}\n\n"
                f"Всё верно?"
            )
            kb = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✅ Подтвердить")],
                    [KeyboardButton(text="❌ Отмена")],
                ], resize_keyboard=True
            )
            await message.answer(confirmation_text, reply_markup=kb)
        except ValueError:
            await message.answer("❌ Некорректный формат даты. Введите в формате YYYY-MM-DD")
    
    elif state == "awaiting_expense_confirmation":
        if text == "✅ Подтвердить":
            try:
                # Создаём расход
                current_template = get_user_template(user_id)
                expense_id = get_next_expense_id(current_template)

                title = dp.workflow_data[user_id].get("expense_title")
                category = dp.workflow_data[user_id].get("expense_category")
                item = dp.workflow_data[user_id].get("expense_item")
                amount = dp.workflow_data[user_id].get("expense_amount")
                date = dp.workflow_data[user_id].get("expense_date")

                # Сначала гарантируем папки на Яндекс.Диске
                savedir_name = get_savedir(user_id)

                template_folder = posixpath.join(REPORT_FOLDER, savedir_name)
                category_folder = posixpath.join(template_folder, category)

                try:
                    yadisk_client.mkdir(template_folder, parents=True)
                except yadisk.exceptions.PathExistsError:
                    pass

                try:
                    yadisk_client.mkdir(category_folder, parents=True)
                    logger.info(f"Создана папка для категории {category}")
                except yadisk.exceptions.PathExistsError:
                    logger.info(f"Папка для категории {category} уже существует")

                # И ТОЛЬКО ПОТОМ записываем расход
                expense_dict = {
                    "expense_id": expense_id,
                    "date": date,
                    "title": title,
                    "category": category,
                    "expense_item": item,
                    "amount": amount
                }

                write_expense(current_template, expense_dict)

                # Чистим state
                dp.workflow_data.pop(user_id, None)

                await message.answer(
                    f"✅ <b>{expense_id}</b> создан\n\n"
                    f"Название: {title}\n"
                    f"Сумма: {amount} ₽\n\n"
                    f"Что дальше?",
                    reply_markup=ReplyKeyboardMarkup(
                        keyboard=[
                            [KeyboardButton(text="📎 Добавить документ")],
                            [KeyboardButton(text="🏠 Главное меню")],
                        ],
                        resize_keyboard=True
                    )
                )

            except Exception as e:
                logger.exception("Ошибка при создании расхода")

                dp.workflow_data.pop(user_id, None)

                await message.answer(
                    f"❌ Ошибка при создании расхода:\n{e}"
                )

                await cmd_start(message)
        
        elif text == "❌ Отмена":
            dp.workflow_data.pop(user_id, None)
            await message.answer("❌ Отменено.")
            await cmd_start(message)
        else:
            await message.answer("❌ Пожалуйста, выберите 'Подтвердить' или 'Отмена'")
    
    elif text == "🏠 Главное меню":
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)
        return
    
    elif state == "awaiting_template_name":
        template_name = normalize_template_name(text)
        existing = get_template_names()
        
        if template_name in existing:
            # Template already exists - ask for confirmation
            dp.workflow_data[user_id] = {
                "state": "confirming_template_overwrite",
                "template_name": template_name
            }
            kb = build_reply_keyboard(["✅ Да", "❌ Нет"])
            await message.answer(f"Шаблон '{template_name}' уже существует. Перезаписать?", reply_markup=kb)
        else:
            # New template - proceed directly to upload
            dp.workflow_data[user_id] = {
                "state": "awaiting_template_file",
                "template_name": template_name
            }
            await message.answer("Отправьте шаблон Excel-файлом (.xlsx), содержащим нужные столбцы.")

    elif state == "confirming_template_overwrite":
        template_name = state_data.get("template_name")
        
        if text == "✅ Да":
            # User confirmed overwrite - proceed to upload
            dp.workflow_data[user_id] = {
                "state": "awaiting_template_file",
                "template_name": template_name,
                "overwrite": True
            }
            await message.answer("Отправьте новый файл шаблона (.xlsx).")
        elif text == "❌ Нет":
            # User cancelled - go back to main menu
            dp.workflow_data.pop(user_id, None)
            await cmd_start(message)
        else:
            await message.answer("❌ Пожалуйста, выберите 'Да' или 'Нет'")

    elif state == "awaiting_template_choice":
        template_name = normalize_template_name(text)
        existing = get_template_names()
        if template_name not in existing:
            await message.answer("❌ Такого шаблона нет. Попробуйте снова.")
            return
        set_user_template(user_id, template_name)
        ensure_local_template(template_name)
        
        # BUG FIX 1: Ensure Yandex.Disk template folder exists
        try:
            template_folder = posixpath.join(REPORT_FOLDER, template_name)
            yadisk_client.mkdir(template_folder, parents=True)
            logger.info(f"Папка шаблона {template_name} создана на Яндекс.Диске")
        except yadisk.exceptions.PathExistsError:
            logger.info(f"Папка шаблона {template_name} уже существует")
        except Exception as e:
            logger.warning(f"Ошибка при создании папки шаблона: {e}")
        
        await message.answer(f"✅ Шаблон '{template_name}' выбран.")
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)


@dp.message(F.document)
@require_authorization
async def handle_document_upload(message: types.Message):
    user_id = str(message.from_user.id)
    state_data = dp.workflow_data.get(user_id, {})
    state = state_data.get("state")

    document = message.document
    if not document:
        await message.answer("❌ Не удалось получить файл. Попробуйте снова.")
        return

    file_info = await bot.get_file(document.file_id)
    temp_path = os.path.join(os.getcwd(), "temp", document.file_name)

    os.makedirs(os.path.dirname(temp_path), exist_ok=True)

    await bot.download_file(file_info.file_path, destination=temp_path)

    if state == "awaiting_template_file":
        if not document.file_name.endswith(".xlsx"):
            await message.answer("❌ Поддерживаются только .xlsx-файлы.")
            return

        try:
            df = pd.read_excel(temp_path)
        except Exception:
            logger.exception("Ошибка при чтении Excel-файла")
            await message.answer("❌ Не удалось прочитать файл. Убедитесь, что это Excel.")
            return

        required_cols = {"Категория", "Статья Расходов"}
        if not required_cols.issubset(set(df.columns)):
            await message.answer("❌ Файл должен содержать столбцы: " + ", ".join(required_cols))
            return

        template_name = state_data["template_name"]
        disk_path = posixpath.join(TEMPLATES_FOLDER, f"{template_name}.xlsx")

        try:
            yadisk_client.upload(temp_path, disk_path, overwrite=True)
            logger.info(f"✅ Шаблон '{template_name}' загружен на Я.Диск пользователем {message.from_user.id}")
        except Exception:
            logger.exception("Ошибка загрузки шаблона на Яндекс.Диск")
            await message.answer("❌ Не удалось загрузить файл на Яндекс.Диск.")
            return
        
        # If this is an overwrite, clear local cache to force fresh download
        if state_data.get("overwrite"):
            local_template_path = get_local_template_path(template_name)
            if os.path.exists(local_template_path):
                os.remove(local_template_path)
                logger.info(f"Локальный кеш шаблона '{template_name}' удален для перезагрузки")
        
        # Создаём папку для документов этого шаблона
        template_docs_folder = posixpath.join(REPORT_FOLDER, template_name)
        try:
            yadisk_client.mkdir(template_docs_folder, parents=True)
            logger.info(f"Папка для документов шаблона '{template_name}' создана на Я.Диске")
        except yadisk.exceptions.PathExistsError:
            logger.info(f"Папка для документов шаблона '{template_name}' уже существует на Я.Диске")
            
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)

    elif state == "awaiting_file_upload":
        await processing_document(message)
    else:
        await message.answer("❌ Сейчас бот не ожидает загрузки файла.")
    os.remove(temp_path)

@dp.message(F.photo)
@require_authorization
async def handle_photo_upload(message: types.Message):
    user_id = str(message.from_user.id)
    state_data = dp.workflow_data.get(user_id, {})
    state = state_data.get("state")
    if state == "awaiting_file_upload":
        await processing_document(message)
    else:
        await message.answer("❌ Сейчас бот не ожидает загрузки файла.")

if __name__ == '__main__':
    import asyncio
    from aiogram import Router
    router = Router()
    dp.include_router(router)
    dp.workflow_data = {}
    asyncio.run(dp.start_polling(bot))
