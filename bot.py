import logging
import os
import re
import json
import pandas as pd
from functools import wraps
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove

from aiogram.client.default import DefaultBotProperties
import yadisk
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
# путь к файлу логов
LOG_FILE = os.path.join(os.path.dirname(__file__), 'logs.txt')
# путь к локальной директории, хранящей Шаблоны
LOCAL_TEMPLATES_FOLDER = os.path.join(os.getcwd(), "Templates")
os.makedirs(LOCAL_TEMPLATES_FOLDER, exist_ok=True)
# json таблица, хранящая инфу о юзерах и ипользуемых ими шаблонах
USER_TEMPLATES_FILE = "user_templates.json"

# --- НАСТРОЙКА ЛОГГЕРА ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
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
def get_local_template_path(template_name: str) -> str:
    '''
    получение локального пути шаблона
    '''
    return os.path.join(LOCAL_TEMPLATES_FOLDER, template_name)


def ensure_local_template(template_name: str):
    '''
    Скачивание шаблона, если его нет, возвращает локальный путь
    '''
    local_path = get_local_template_path(template_name)
    if not os.path.exists(local_path):
        yadisk_client.download(posixpath.join(TEMPLATES_FOLDER, template_name), local_path)
        logger.info(f"Шаблон {template_name} скачан в {local_path}")
    else:
        logger.info(f"Шаблон {template_name} уже есть локально")
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
    Возвращает имя папки для документов пользователя без префикса 'Шаблон_'
    """
    template_name = get_user_template(user_id)
    if not template_name:
        logger.warning(f"Шаблон не найден для пользователя {user_id}")
        return None
    return template_name.removeprefix('Шаблон_')

def get_template_names():
    return [f['name'] for f in yadisk_client.listdir(TEMPLATES_FOLDER) if f['path'].endswith(".xlsx")]

def get_template_dataframe(name: str) -> pd.DataFrame:
    '''
    Получаем dataframe шаблона
    '''
    local_path = get_local_template_path(name)
    return pd.read_excel(local_path)

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
    Меняет имя документа исходя из его типа, категории, статьи расхода, цены и даты
    Загружает на диск и возвращает ссылку на просмотр 
    '''
    
    user_id = str(message.from_user.id)
    state_data = dp.workflow_data.get(user_id, {})

    # Определяем, что прислал пользователь
    if message.document:
        file_id = message.document.file_id
        orig_filename = message.document.file_name
    elif message.photo:
        # Берём самое большое фото
        file_id = message.photo[-1].file_id
        # Формируем имя файла для фото
        orig_filename = f"photo_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
    else:
        await message.answer("❌ Не удалось получить файл. Попробуйте снова.")
        return

    file_info = await bot.get_file(file_id)
    local_path = os.path.join(os.getcwd(), orig_filename)
    await bot.download_file(file_info.file_path, destination=local_path)

    short_category = state_data.get("short_category", "БезКат")
    short_item = state_data.get("short_item", "БезСтат")
    typedoc = state_data.get("typedoc", "Док")
    summ = state_data.get("summ", "0")
    date = state_data.get("date", datetime.now().strftime("%d-%m-%y"))

    base_name = f"{short_category}_{short_item}_{typedoc}_{summ}_{date}"
    ext = os.path.splitext(orig_filename)[1]

    #загружаем файл в папку шаблона
    current_template = get_user_template(user_id)
    if not current_template:
        await message.answer("❌ Не удалось определить текущий шаблон.")
        return

    savedir_name = get_savedir(user_id)
    template_docs_folder = posixpath.join(REPORT_FOLDER, savedir_name)
    filename = get_unique_filename(template_docs_folder, base_name + ext)
    file_path_on_disk = posixpath.join(template_docs_folder, filename)

    try:
        yadisk_client.mkdir(template_docs_folder, parents=True)
    except yadisk.exceptions.PathExistsError:
        pass

    yadisk_client.upload(local_path, file_path_on_disk)

    try:
        yadisk_client.publish(file_path_on_disk)
    except yadisk.exceptions.PathAlreadyPublicError:
        pass
    
    # создание публичной ссылки для загружаемого файла
    public_url = await create_file_public_url(file_path_on_disk)
    logger.info(f"Загружен файл '{filename}' в директорию '{template_docs_folder}'")

    await message.answer(
        f"✅ Файл успешно загружен как {filename}\nСсылка на файл: {public_url}"
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
            [KeyboardButton(text="✏️ Создать шаблон")],
            [KeyboardButton(text="📄 Выбрать шаблон")],
            [KeyboardButton(text="📂 Загрузить файл")],
        ], resize_keyboard=True
    )
    await message.answer("✅ Добро пожаловать! Выберите действие:", reply_markup=kb)

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

@dp.message(F.text == "📂 Загрузить файл")
@require_authorization
async def handle_upload_file_prompt(message: types.Message):
    user_id = str(message.from_user.id)
    current_template = get_user_template(user_id)
    if not current_template:
        await message.answer("❌ Сначала выберите шаблон с помощью команды '📄 Выбрать шаблон'")
        return
    df = get_template_dataframe(current_template)
    categories = df['Категория'].dropna().unique().tolist()
    kb = build_reply_keyboard(categories)
    dp.workflow_data[str(message.from_user.id)] = {
        "state": "awaiting_category",
        "df": df.to_dict()
    }
    await message.answer("Выберите категорию:", reply_markup=kb)

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

    if state == "awaiting_template_name":
        template_name = text
        existing = get_template_names()
        if f"{template_name}.xlsx" in existing:
            await message.answer("❌ Шаблон с таким названием уже существует. Попробуйте другое.")
            return
        dp.workflow_data[user_id] = {
            "state": "awaiting_template_file",
            "template_name": template_name
        }
        await message.answer("Отправьте шаблон Excel-файлом (.xlsx), содержащим нужные столбцы.")

    elif state == "awaiting_template_choice":
        template_name = text
        existing = get_template_names()
        if template_name not in existing:
            await message.answer("❌ Такого шаблона нет. Попробуйте снова.")
            return
        set_user_template(user_id, template_name)
        ensure_local_template(template_name)
        await message.answer(f"✅ Шаблон '{template_name}' выбран.")
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)

    elif state == "awaiting_category":
        df = pd.DataFrame(state_data["df"])
        if text not in df["Категория"].values:
            await message.answer("❌ Неверная категория. Попробуйте снова.")
            return
        articles = df[df["Категория"] == text]["Статья Расходов"].dropna().unique().tolist()
        dp.workflow_data[user_id]["category"] = text
        dp.workflow_data[user_id]["state"] = "awaiting_item"
        dp.workflow_data[user_id]["df"] = df.to_dict()
        kb = build_reply_keyboard(articles)
        await message.answer("Выберите статью расходов:", reply_markup=kb)

    elif state == "awaiting_item":
        df = pd.DataFrame(state_data["df"])
        category = state_data.get("category")
        valid_rows = df[(df["Категория"] == category) & (df["Статья Расходов"] == text)]
        if valid_rows.empty:
            valid_rows = df[df["Категория"] == category]
            row = valid_rows.iloc[0]
            dp.workflow_data[user_id].update({
                "state": "awaiting_typedoc",
                "short_category": row["Категория_Short"],
                "short_item": text[:6]
            })
        else:
            row = valid_rows.iloc[0]
            dp.workflow_data[user_id].update({
                "state": "awaiting_typedoc",
                "short_category": row["Категория_Short"],
                "short_item": row["Статья Расходов_Short"]
            })
            
        typedoc_options = list(d_typedoc.keys())
        kb = build_reply_keyboard(typedoc_options)
        await message.answer("Выберите тип документа:", reply_markup=kb)
        
    elif state == "awaiting_typedoc":
        typedoc = d_typedoc.get(message.text, message.text[:6])
        dp.workflow_data[user_id]["typedoc"] = typedoc
        dp.workflow_data[user_id]["state"] = "awaiting_sum"
        await message.answer("Укажите сумму в рублях:")

    elif state == "awaiting_sum":
        if not re.match(r"^\d+(\.\d{1,2})?$", message.text):
            await message.answer("❌ Введите корректную сумму (только цифры, можно с точкой).")
            return
        dp.workflow_data[user_id]["summ"] = message.text
        dp.workflow_data[user_id]["state"] = "awaiting_date_choice"
        kb = build_reply_keyboard(["Сегодня", "Своя дата"])
        await message.answer("Выберите дату:", reply_markup=kb)

    elif state == "awaiting_date_choice":
        if message.text == "Сегодня":
            dp.workflow_data[user_id]["date"] = datetime.now().strftime("%d-%m-%y")
            dp.workflow_data[user_id]["state"] = "uploading_file"
            await message.answer("📄 Загрузите файл. Если это картинка, прикрепите её, как документ", reply_markup=ReplyKeyboardRemove())
        elif message.text == "Своя дата":
            dp.workflow_data[user_id]["state"] = "awaiting_custom_date"
            await message.answer("Введите дату в формате ДД-ММ-ГГ:")
        else:
            await message.answer("❌ Пожалуйста, выберите 'Сегодня' или 'Своя дата'")


    elif state == "awaiting_custom_date":
        try:
            datetime.strptime(message.text, "%d-%m-%y")
            dp.workflow_data[user_id]["date"] = message.text
            dp.workflow_data[user_id]["state"] = "uploading_file"
            await message.answer("📄 Загрузите файл. Если это картинка, прикрепите её, как документ", reply_markup=ReplyKeyboardRemove())
        except ValueError:
            await message.answer("❌ Некорректный формат даты. Введите в формате ДД-ММ-ГГ")
            return

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
    local_path = os.path.join(os.getcwd(), document.file_name)
    await bot.download_file(file_info.file_path, destination=local_path)

    if state == "awaiting_template_file":
        if not document.file_name.endswith(".xlsx"):
            await message.answer("❌ Поддерживаются только .xlsx-файлы.")
            return

        try:
            df = pd.read_excel(local_path)
        except Exception:
            logger.exception("Ошибка при чтении Excel-файла")
            await message.answer("❌ Не удалось прочитать файл. Убедитесь, что это Excel.")
            return

        required_cols = {"Категория", "Статья Расходов", "Категория_Short", "Статья Расходов_Short"}
        if not required_cols.issubset(set(df.columns)):
            await message.answer("❌ Файл должен содержать столбцы: " + ", ".join(required_cols))
            return

        template_name = state_data["template_name"]
        disk_path = posixpath.join(TEMPLATES_FOLDER, f"{template_name}.xlsx")

        try:
            yadisk_client.upload(local_path, disk_path, overwrite=True)
            logger.info(f"✅ Шаблон '{template_name}' загружен на Я.Диск пользователем {message.from_user.id}")
        except Exception:
            logger.exception("Ошибка загрузки шаблона на Яндекс.Диск")
            await message.answer("❌ Не удалось загрузить файл на Яндекс.Диск.")
            return
        
        # Создаём папку для документов этого шаблона
        savedir_name = get_savedir(user_id)
        template_docs_folder = posixpath.join(REPORT_FOLDER, savedir_name)
        try:
            yadisk_client.mkdir(template_docs_folder, parents=True)
            logger.info(f"Папка для документов шаблона '{template_name}' создана на Я.Диске")
        except yadisk.exceptions.PathExistsError:
            logger.info(f"Папка для документов шаблона '{template_name}' уже существует на Я.Диске")
            
        dp.workflow_data.pop(user_id, None)
        await cmd_start(message)

    elif state == "uploading_file":
        await processing_document(message)
    else:
        await message.answer("❌ Сейчас бот не ожидает загрузки файла.")

@dp.message(F.photo)
@require_authorization
async def handle_photo_upload(message: types.Message):
    user_id = str(message.from_user.id)
    state_data = dp.workflow_data.get(user_id, {})
    state = state_data.get("state")
    if state == "uploading_file":
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
