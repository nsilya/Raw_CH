
# Проект "Аналитика П"

## Оглавление 

- [Описание](#описание)
- [Архитектура](#архитектура)
- [Участники команды](#участники-команды)
- [Структура проекта](#структура-проекта)
- [Генерация данных](#генерация-данных)
- [Загрузка данных в MongoDB](#загрузка-данных-в-mongodb)
- [Запуск инфраструктуры](#запуск-инфраструктуры)
- [Загрузка данных из MongoDB в Kafka](#загрузка-данных-из-mongodb-в-kafka)
- [Загрузка данных из Kafka в ClickHouse (RAW)](#загрузка-данных-из-kafka-в-clickhouse-raw)
- [Очистка данных и Materialized View (MART)](#очистка-данных-и-materialized-view-mart)
- [Настройка Grafana](#настройка-grafana)
- [Telegram-бот для алертинга](#telegram-бот-для-алертинга)
- [Как запустить](#как-запустить)


---

## Описание

Данный проект реализует аналитическую систему для сетевого магазина "Пикча", позволяющую эффективно продавать товары за счёт:

- Сбора и хранения данных из различных источников
- Очистки и подготовки данных
- Визуализации ключевых метрик
- Алертинга при аномалиях (например, превышение 50% дубликатов)

---

## Архитектура


MongoDB (NoSQL) → Kafka → ClickHouse (RAW) → Materialized View (MART) → Grafana (Dashboard & Alerting)
```




## Структура проекта

<img width="362" height="846" alt="image" src="https://github.com/user-attachments/assets/80d4aed8-ce23-4da3-b35b-82a373101e2e" />

```

---

## Генерация данных


<details>
<summary>Показать код (scripts/load_to_mongo.py)</summary>

Скрипт `scripts/generate_data.py` генерирует JSON-файлы для:

- 45 магазинов
- 20 товаров
- 45 покупателей
- 200 покупок

<details>
<summary>Показать код (scripts/generate_data.py)</summary>

```python
# scripts/generate_data.py
import os
import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

fake = Faker("ru_RU")

os.makedirs("data/stores", exist_ok=True)
os.makedirs("data/products", exist_ok=True)
os.makedirs("data/customers", exist_ok=True)
os.makedirs("data/purchases", exist_ok=True)

categories = [
    "🥖 Зерновые и хлебобулочные изделия",
    "🥩 Мясо, рыба, яйца и бобовые",
    "🥛 Молочные продукты",
    "🍏 Фрукты и ягоды",
    "🥦 Овощи и зелень"
]

store_networks = [("Большая Пикча", 30), ("Маленькая Пикча", 15)]
stores = []

# === 1. Генерация магазинов ===
for network, count in store_networks:
    for i in range(count):
        store_id = f"store-{len(stores)+1:03}"
        city = fake.city()
        store = {
            "store_id": store_id,
            "store_name": f"{network} — Магазин на {fake.street_name()}",
            "store_network": network,
            "store_type_description": f"{'Супермаркет более 200 кв.м.' if network == 'Большая Пикча' else 'Магазин у дома менее 100 кв.м.'} Входит в сеть из {count} магазинов.",
            "type": "offline",
            "categories": categories,
            "manager": {
                "name": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email()
            },
            "location": {
                "country": "Россия",
                "city": city,
                "street": fake.street_name(),
                "house": str(fake.building_number()),
                "postal_code": fake.postcode(),
                "coordinates": {
                    "latitude": float(fake.latitude()),
                    "longitude": float(fake.longitude())
                }
            },
            "opening_hours": {
                "mon_fri": "09:00-21:00",
                "sat": "10:00-20:00",
                "sun": "10:00-18:00"
            },
            "accepts_online_orders": True,
            "delivery_available": True,
            "warehouse_connected": random.choice([True, False]),
            "last_inventory_date": datetime.now().strftime("%Y-%m-%d")
        }
        stores.append(store)
        with open(f"data/stores/{store_id}.json", "w", encoding="utf-8") as f:
            json.dump(store, f, ensure_ascii=False, indent=2)

# === 2. Генерация товаров ===
products = []
for i in range(20):
    product = {
        "id": f"prd-{1000+i}",
        "name": fake.word().capitalize() + " " + fake.word().capitalize(),
        "group": random.choice(categories),
        "description": fake.sentence(),
        "kbju": {
            "calories": round(random.uniform(50, 300), 1),
            "protein": round(random.uniform(0.5, 20), 1),
            "fat": round(random.uniform(0.1, 15), 1),
            "carbohydrates": round(random.uniform(0.5, 50), 1)
        },
        "price": round(random.uniform(30, 300), 2),
        "unit": random.choice(["упаковка", "шт", "кг", "л"]),
        "origin_country": "Россия",
        "expiry_days": random.randint(5, 30),
        "is_organic": random.choice([True, False]),
        "barcode": fake.ean(length=13),
        "manufacturer": {
            "name": fake.company(),
            "country": "Россия",
            "website": f"https://{fake.domain_name()}",
            "inn": fake.bothify(text='##########')
        }
    }
    products.append(product)
    with open(f"data/products/{product['id']}.json", "w", encoding="utf-8") as f:
        json.dump(product, f, ensure_ascii=False, indent=2)

# === 3. Генерация покупателей ===
customers = []
for store in stores:
    customer_id = f"cus-{1000 + len(customers)}"
    customer = {
        "customer_id": customer_id,
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70).isoformat(),
        "gender": random.choice(["male", "female"]),
        "registration_date": datetime.now().isoformat(),
        "is_loyalty_member": True,
        "loyalty_card_number": f"LOYAL-{uuid.uuid4().hex[:10].upper()}",
        "purchase_location": store["location"],
        "delivery_address": {
            "country": "Россия",
            "city": store["location"]["city"],
            "street": fake.street_name(),
            "house": str(fake.building_number()),
            "apartment": str(random.randint(1, 100)),
            "postal_code": fake.postcode()
        },
        "preferences": {
            "preferred_language": "ru",
            "preferred_payment_method": random.choice(["card", "cash"]),
            "receive_promotions": random.choice([True, False])
        }
    }
    customers.append(customer)
    with open(f"data/customers/{customer_id}.json", "w", encoding="utf-8") as f:
        json.dump(customer, f, ensure_ascii=False, indent=2)

# === 4. Генерация покупок ===
for i in range(200):
    customer = random.choice(customers)
    store = random.choice(stores)
    items = random.sample(products, k=random.randint(1, 3))
    purchase_items = []
    total = 0
    for item in items:
        qty = random.randint(1, 5)
        total_price = round(item["price"] * qty, 2)
        total += total_price
        purchase_items.append({
            "product_id": item["id"],
            "name": item["name"],
            "category": item["group"],
            "quantity": qty,
            "unit": item["unit"],
            "price_per_unit": item["price"],
            "total_price": total_price,
            "kbju": item["kbju"],
            "manufacturer": item["manufacturer"]
        })
    purchase = {
        "purchase_id": f"ord-{i+1:05}",
        "customer": {
            "customer_id": customer["customer_id"],
            "first_name": customer["first_name"],
            "last_name": customer["last_name"],
            "email": customer["email"], # будет зашифровано позже
            "phone": customer["phone"], # будет зашифровано позже
            "is_loyalty_member": customer["is_loyalty_member"],
            "loyalty_card_number": customer["loyalty_card_number"]
        },
        "store": {
            "store_id": store["store_id"],
            "store_name": store["store_name"],
            "store_network": store["store_network"],
            "location": store["location"]
        },
        "items": purchase_items,
        "total_amount": round(total, 2),
        "payment_method": random.choice(["card", "cash"]),
        "is_delivery": random.choice([True, False]),
        "delivery_address": customer["delivery_address"],
        "purchase_datetime": (datetime.now() - timedelta(days=random.randint(0, 90))).isoformat()
    }
    with open(f"data/purchases/{purchase['purchase_id']}.json", "w", encoding="utf-8") as f:
        json.dump(purchase, f, ensure_ascii=False, indent=2)

print("✅ Генерация данных завершена: 45 магазинов, 20 товаров, 45 покупателей, 200 покупок.")
```

</details>

---

## Загрузка данных в MongoDB

Скрипт `scripts/load_to_mongo.py` загружает JSON-файлы в MongoDB.

<details>
<summary>Показать код (scripts/load_to_mongo.py)</summary>

```python
# scripts/load_to_mongo.py
import json
import os
from pymongo import MongoClient

client = MongoClient('mongodb://localhost:27018/')
db = client['piccha_db']

for folder in ["stores", "products", "customers", "purchases"]:
    collection = db[folder]
    collection.delete_many({})  # Очистка коллекции
    for file in os.listdir(f"data/{folder}"):
        with open(f"data/{folder}/{file}", "r", encoding="utf-8") as f:
            data = json.load(f)
            collection.insert_one(data)
    print(f"✅ Загружено {collection.count_documents({})} документов в коллекцию '{folder}'")
```

</details>

---

## Запуск инфраструктуры

Файл `docker-compose.yml` запускает:

- MongoDB
- Kafka + Zookeeper
- ClickHouse
- Grafana

<details>
<summary>Показать код (docker-compose.yml)</summary>

```yaml
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.4.0
    container_name: piccha-zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    networks:
      - piccha-net

  kafka:
    image: confluentinc/cp-kafka:7.4.0
    container_name: piccha-kafka
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
    networks:
      - piccha-net

  mongodb:
    image: mongo:4.4
    container_name: piccha-mongo
    ports:
      - "27018:27017"
    volumes:
      - mongo_data:/data/db
    networks:
      - piccha-net

  clickhouse:
    image: clickhouse/clickhouse-server:23.12
    container_name: piccha-clickhouse
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
      - ./config/clickhouse:/etc/clickhouse-server/config.d
    ulimits:
      nofile:
        soft: 262144
        hard: 262144
    networks:
      - piccha-net

  grafana:
    image: grafana/grafana:10.0.0
    container_name: piccha-grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    networks:
      - piccha-net

volumes:
  mongo_data:
  clickhouse_data:

networks:
  piccha-net:
    driver: bridge
```

</details>

---

## Загрузка данных из MongoDB в Kafka

Скрипт `scripts/kafka_producer.py` отправляет данные из MongoDB в топик `piccha_raw` Kafka, **шифруя** `email` и `phone`.

<details>
<summary>Показать код (scripts/kafka_producer.py)</summary>

```python
# scripts/kafka_producer.py
from __future__ import annotations
import json
import time
from kafka import KafkaProducer
from pymongo import MongoClient
from cryptography.fernet import Fernet

# === Генерация ключа шифрования ===
ENCRYPTION_KEY = Fernet.generate_key()
cipher = Fernet(ENCRYPTION_KEY)
print(f"🔑 Ключ шифрования (сохраните!): {ENCRYPTION_KEY.decode()}")

# === Шифрование ===
def encrypt_field(value: str | None) -> str:
    if not value:
        return ""
    return cipher.encrypt(value.encode()).decode()

# === Нормализация ===
def normalize_phone(phone: str | None) -> str:
    if not phone:
        return ""
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return f"+{digits}"
    return phone

def normalize_email(email: str | None) -> str:
    return email.strip().lower() if email else ""

# === Подключение к Kafka и MongoDB ===
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
)

client = MongoClient('mongodb://localhost:27018/')
db = client['piccha_db']

collections = ['stores', 'products', 'customers', 'purchases']

for coll_name in collections:
    collection = db[coll_name]
    for doc in collection.find():
        doc.pop('_id', None)
        doc['_collection'] = coll_name

        # === Шифруем email и phone ===
        if coll_name == 'customers':
            email = doc.get('email')
            phone = doc.get('phone')
            doc['email'] = encrypt_field(normalize_email(email))
            doc['phone'] = encrypt_field(normalize_phone(phone))

        if coll_name == 'stores':
            email = doc['manager'].get('email')
            phone = doc['manager'].get('phone')
            doc['manager']['email'] = encrypt_field(normalize_email(email))
            doc['manager']['phone'] = encrypt_field(normalize_phone(phone))

        producer.send('piccha_raw', value=doc)
        print(f"Отправлено в Kafka: {coll_name} - {doc.get('store_id') or doc.get('id') or doc.get('customer_id') or doc.get('purchase_id')}")

        time.sleep(0.01)

producer.flush()
print("✅ Все данные отправлены в Kafka топик 'piccha_raw'")
```

</details>

---

## Загрузка данных из Kafka в ClickHouse (RAW)

Скрипт `scripts/kafka_to_clickhouse.py` читает из Kafka, **дешифрует** `email` и `phone`, и загружает в **RAW** таблицы ClickHouse.

<details>
<summary>Показать код (scripts/kafka_to_clickhouse.py)</summary>

```python
# scripts/kafka_to_clickhouse.py

from __future__ import annotations
import json
import logging
from typing import Any, Dict, List
from datetime import datetime
from decimal import Decimal
from clickhouse_driver import Client
from cryptography.fernet import Fernet
from kafka import KafkaConsumer

# === Настройка логирования ===
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Вывод в консоль
        # logging.FileHandler('kafka_to_clickhouse.log') # Опционально: вывод в файл
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# === ВАЖНО: ВСТАВЬТЕ СЮДА КЛЮЧ ИЗ ВЫВОДА kafka_producer.py ====================
# === ПРИМЕР: b'DpY1VLwmMeRO18y5BTLIibyzd-BheI5N1NpxCoHdMBo=' =================
# === !!! УБЕДИТЕСЬ, ЧТО КЛЮЧ СОВПАДАЕТ С ТЕМ, ЧТО В kafka_producer.py !!! ====
# =============================================================================
ENCRYPTION_KEY = b'ВСТАВЬТЕ_СЮДА_СВОЙ_КЛЮЧ_ИЗ_kafka_producer.py'
cipher = Fernet(ENCRYPTION_KEY)

logger.info("🔑 Используем ключ шифрования.")


# === Нормализация и дешифровка ===
def decrypt_phone_or_email(value: str | None) -> str:
    """
    Пытается расшифровать значение. Если не удается, возвращает как есть.
    """
    if not value:
        return ""
    try:
        decrypted_value = cipher.decrypt(value.encode()).decode()
        logger.debug(f"Успешно расшифровано значение.")
        return decrypted_value
    except Exception as e:
        logger.debug(f"Не удалось расшифровать значение: {e}. Возвращено как есть.")
        return value  # fallback: вернуть как есть


def normalize_phone(phone: str | None) -> str:
    """
    Нормализует телефонный номер: приводит к формату +7XXXXXXXXXX.
    """
    if not phone:
        return ""
    # Простая нормализация: убираем все, кроме цифр
    digits = ''.join(filter(str.isdigit, phone))
    if len(digits) == 11 and digits.startswith('8'):
        digits = '7' + digits[1:]
    if len(digits) == 10:
        digits = '7' + digits
    if len(digits) == 11 and digits.startswith('7'):
        return f"+{digits}"
    # Если формат не стандартный, возвращаем оригинал
    logger.debug(f"Телефон '{phone}' не соответствует стандартному формату.")
    return phone


def normalize_email(email: str | None) -> str:
    """
    Нормализует email: приводит к нижнему регистру и удаляет пробелы.
    """
    if not email:
        return ""
    return email.strip().lower()


# === Обработка дат ===
def safe_parse_datetime(datetime_str: str | None) -> datetime:
    """
    Безопасно парсит строку даты-времени в формате ISO. Возвращает datetime или epoch.
    """
    if not datetime_str:
        logger.warning("Получена пустая строка даты-времени. Используется 1970-01-01T00:00:00.")
        return datetime(1970, 1, 1)
    try:
        # fromisoformat может справиться с 'Z' в конце, заменим на +00:00 для надежности
        if datetime_str.endswith('Z'):
            parsed_dt = datetime.fromisoformat(datetime_str[:-1] + '+00:00')
        else:
            parsed_dt = datetime.fromisoformat(datetime_str)
        return parsed_dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты-времени '{datetime_str}': {e}. Используется 1970-01-01T00:00:00.")
        return datetime(1970, 1, 1)


def safe_parse_date(
        date_str: str | None) -> datetime:  # Возвращаем datetime для совместимости, используем .date() при вставке
    """
    Безопасно парсит строку даты (YYYY-MM-DD). Возвращает datetime или epoch.
    """
    if not date_str:
        logger.warning("Получена пустая строка даты. Используется 1970-01-01.")
        return datetime(1970, 1, 1)
    try:
        # Предполагаем, что формат YYYY-MM-DD
        parsed_dt = datetime.strptime(date_str, "%Y-%m-%d")
        return parsed_dt
    except ValueError as e:
        logger.error(f"Ошибка парсинга даты '{date_str}': {e}. Используется 1970-01-01.")
        return datetime(1970, 1, 1)


# === Подключение к ClickHouse ===
# Используем порт 9000, как настроено в docker-compose.yml
client = Client(host='localhost', port=9000)


# === Создание таблиц RAW ===
def create_raw_tables() -> None:
    """
    Создает базу данных piccha_raw и все необходимые таблицы, если они еще не существуют.
    Типы ID изменены на String.
    """
    client.execute("CREATE DATABASE IF NOT EXISTS piccha_raw")

    # Таблица магазинов
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.stores (
        store_id String,
        store_name String,
        store_network String,
        store_type_description String,
        type String,
        categories Array(String),
        manager_name String,
        manager_phone String,
        manager_email String,
        location_country String,
        location_city String,
        location_street String,
        location_house String,
        location_postal_code String,
        location_latitude Float64,
        location_longitude Float64,
        opening_hours_mon_fri String,
        opening_hours_sat String,
        opening_hours_sun String,
        accepts_online_orders UInt8,
        delivery_available UInt8,
        warehouse_connected UInt8,
        last_inventory_date Date
    ) ENGINE = MergeTree() ORDER BY store_id
    """)

    # Таблица продуктов
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.products (
        id String,
        name String,
        group String,
        description String,
        kbju_calories Float32,
        kbju_protein Float32,
        kbju_fat Float32,
        kbju_carbohydrates Float32,
        price Float32,
        unit String,
        origin_country String,
        expiry_days UInt16,
        is_organic UInt8,
        barcode String,
        manufacturer_name String,
        manufacturer_country String,
        manufacturer_website String,
        manufacturer_inn String
    ) ENGINE = MergeTree() ORDER BY id
    """)

    # Таблица покупателей
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.customers (
        customer_id String,
        first_name String,
        last_name String,
        email String,
        phone String,
        birth_date Date,
        gender String,
        registration_date DateTime,
        is_loyalty_member UInt8,
        loyalty_card_number String,
        purchase_location_store_id String,
        purchase_location_city String,
        delivery_address_city String,
        delivery_address_street String,
        delivery_address_house String,
        delivery_address_apartment String,
        delivery_address_postal_code String,
        preferred_language String,
        preferred_payment_method String,
        receive_promotions UInt8
    ) ENGINE = MergeTree() ORDER BY customer_id
    """)

    # Таблица покупок (плоская, по одной строке на товар в чеке)
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.purchases (
        purchase_id String,
        customer_id String,
        product_id String,
        quantity Int32,
        price Decimal(10, 2),
        purchase_date DateTime
    ) ENGINE = MergeTree() ORDER BY (purchase_id, product_id)
    """)

    # Таблица элементов покупки (опционально, если нужна детализация)
    client.execute("""
    CREATE TABLE IF NOT EXISTS piccha_raw.purchase_items (
        purchase_id String,
        product_id String,
        item_name String,
        category String,
        quantity UInt32,
        unit String,
        price_per_unit Float32,
        total_price Float32,
        kbju_calories Float32,
        kbju_protein Float32,
        kbju_fat Float32,
        kbju_carbohydrates Float32,
        manufacturer_name String
    ) ENGINE = MergeTree() ORDER BY (purchase_id, product_id)
    """)


# === Основной consumer ===
def main() -> None:
    """
    Основная функция: создает таблицы, подключается к Kafka, читает сообщения и вставляет данные в ClickHouse.
    """
    logger.info("🚀 Запуск kafka_to_clickhouse consumer...")

    # Создаем таблицы
    try:
        create_raw_tables()
        logger.info("✅ Таблицы piccha_raw созданы или уже существуют.")
    except Exception as e:
        logger.error(f"❌ Ошибка при создании таблиц: {e}")
        return  # Останавливаем выполнение, если таблицы не созданы

    # Подключаемся к Kafka
    try:
        consumer = KafkaConsumer(
            'piccha_raw',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='earliest',  # Читаем с начала, если новых сообщений нет
            enable_auto_commit=True,
            group_id='clickhouse-loader-v2',  # Изменен group_id для избежания проблем с предыдущими состояниями
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            # session_timeout_ms=45000, # Увеличение таймаута сессии, если потребитель медленный
            # heartbeat_interval_ms=3000 # Интервал heartbeat
        )
        logger.info("✅ Подключение к Kafka установлено.")
    except Exception as e:
        logger.error(f"❌ Ошибка подключения к Kafka: {e}")
        return

    logger.info("⏳ Ожидаю данные из Kafka топика 'piccha_raw'...")

    # Основной цикл обработки сообщений
    for message in consumer:
        try:
            # Получаем JSON-документ из сообщения
            raw_doc: Dict[str, Any] = message.value
            collection_type: str = raw_doc.pop('_collection', 'unknown')

            logger.debug(f"📥 Получено сообщение для коллекции: {collection_type}")

            # Обработка в зависимости от типа коллекции
            if collection_type == 'stores':
                doc = raw_doc

                # Обработка last_inventory_date
                last_inventory_date_dt = safe_parse_date(doc.get('last_inventory_date'))

                # Вставка в таблицу stores (piccha_raw)
                client.execute("""
                INSERT INTO piccha_raw.stores VALUES
                """, [(
                    doc['store_id'],
                    doc['store_name'],
                    doc['store_network'],
                    doc['store_type_description'],
                    doc['type'],
                    doc['categories'],
                    doc['manager']['name'],
                    normalize_phone(decrypt_phone_or_email(doc['manager'].get('phone'))),
                    doc['manager']['email'],  # Предполагаем, что email менеджера не шифруется
                    doc['location']['country'],
                    doc['location']['city'],
                    doc['location']['street'],
                    doc['location']['house'],
                    doc['location']['postal_code'],
                    float(doc['location']['coordinates']['latitude']),
                    float(doc['location']['coordinates']['longitude']),
                    doc['opening_hours']['mon_fri'],
                    doc['opening_hours']['sat'],
                    doc['opening_hours']['sun'],
                    int(doc['accepts_online_orders']),
                    int(doc['delivery_available']),
                    int(doc['warehouse_connected']),
                    last_inventory_date_dt.date()  # Вставка Date
                )])
                logger.info(f"✅ Загружено в ClickHouse: stores - {doc['store_id']}")

            elif collection_type == 'products':
                doc = raw_doc

                # Вставка в таблицу products (piccha_raw)
                client.execute("""
                INSERT INTO piccha_raw.products VALUES
                """, [(
                    doc['id'],
                    doc['name'],
                    doc['group'],
                    doc['description'],
                    float(doc['kbju']['calories']),
                    float(doc['kbju']['protein']),
                    float(doc['kbju']['fat']),
                    float(doc['kbju']['carbohydrates']),
                    float(doc['price']),
                    doc['unit'],
                    doc['origin_country'],
                    int(doc['expiry_days']),
                    int(doc['is_organic']),
                    doc['barcode'],
                    doc['manufacturer']['name'],
                    doc['manufacturer']['country'],
                    doc['manufacturer']['website'].strip(),
                    doc['manufacturer']['inn']
                )])
                logger.info(f"✅ Загружено в ClickHouse: products - {doc['id']}")

            elif collection_type == 'customers':
                doc = raw_doc

                # Обработка дат
                birth_date_dt = safe_parse_date(doc.get('birth_date'))
                registration_date_dt = safe_parse_datetime(doc.get('registration_date'))

                # Вставка в таблицу customers (piccha_raw)
                client.execute("""
                INSERT INTO piccha_raw.customers VALUES
                """, [(
                    doc['customer_id'],
                    doc['first_name'],
                    doc['last_name'],
                    normalize_email(decrypt_phone_or_email(doc.get('email'))),
                    normalize_phone(decrypt_phone_or_email(doc.get('phone'))),
                    birth_date_dt.date(),  # Вставка Date
                    doc['gender'],
                    registration_date_dt,  # Вставка DateTime
                    int(doc['is_loyalty_member']),
                    doc['loyalty_card_number'],
                    doc['purchase_location']['store_id'],
                    doc['purchase_location']['city'],
                    doc['delivery_address']['city'],
                    doc['delivery_address']['street'],
                    doc['delivery_address']['house'],
                    doc['delivery_address'].get('apartment', ''),  # Может отсутствовать
                    doc['delivery_address']['postal_code'],
                    doc['preferences']['preferred_language'],
                    doc['preferences']['preferred_payment_method'],
                    int(doc['preferences']['receive_promotions'])
                )])
                logger.info(f"✅ Загружено в ClickHouse: customers - {doc['customer_id']}")

            elif collection_type == 'purchases':
                doc = raw_doc

                # Обработка общей даты покупки
                purchase_datetime_dt = safe_parse_datetime(doc.get('purchase_datetime'))

                # Предполагаем, что таблица piccha_raw.purchases ожидает плоские записи
                # по одной на каждый товар (item) в заказе.
                items_list = doc.get('items', [])
                inserted_items_count = 0
                for item in items_list:
                    try:
                        # --- Вставка в основную таблицу покупок (по одной строке на товар) ---
                        client.execute("""
                        INSERT INTO piccha_raw.purchases VALUES
                        """, [(
                            doc['purchase_id'],
                            doc['customer']['customer_id'],
                            item['product_id'],  # product_id из item
                            int(item['quantity']),  # quantity из item
                            Decimal(str(item['price_per_unit'])),  # price_per_unit из item -> Decimal(10,2)
                            purchase_datetime_dt  # purchase_date из общей даты покупки
                        )])

                        # --- Вставка в детальную таблицу элементов покупки ---
                        client.execute("""
                        INSERT INTO piccha_raw.purchase_items VALUES
                        """, [(
                            doc['purchase_id'],
                            item['product_id'],
                            item['name'],
                            item['category'],
                            int(item['quantity']),
                            item['unit'],
                            float(item['price_per_unit']),
                            float(item['total_price']),
                            float(item['kbju']['calories']),
                            float(item['kbju']['protein']),
                            float(item['kbju']['fat']),
                            float(item['kbju']['carbohydrates']),
                            item['manufacturer']['name']
                        )])
                        inserted_items_count += 1

                    except KeyError as ke:
                        logger.error(
                            f"❌ Отсутствует обязательное поле в item '{item.get('product_id', 'UNKNOWN')}': {ke}")
                        # Пропускаем этот item, но продолжаем обработку других
                        continue
                    except Exception as ie:
                        logger.error(f"❌ Ошибка при обработке item '{item.get('product_id', 'UNKNOWN')}': {ie}")
                        continue

                logger.info(
                    f"✅ Загружено в ClickHouse: purchases - {doc['purchase_id']} ({inserted_items_count} items)")

            else:
                logger.warning(f"⚠️  Неизвестная коллекция: {collection_type}")

        except Exception as e:
            logger.error(f"❌ Критическая ошибка при обработке сообщения: {e}", exc_info=True)
            # Продолжаем обработку следующих сообщений, не останавливаемся на ошибке
            continue

    logger.info("🏁 Consumer остановлен.")


if __name__ == "__main__":
    main()
```

</details>

---

## Очистка данных и Materialized View (MART)

Скрипт `scripts/clean_data.sql` создает **MART** таблицы и **Materialized View**, которые:

- Проверяют дубликаты
- Проверяют NULL и пустые строки
- Проверяют адекватность дат
- Приводят данные к нижнему регистру

<details>
<summary>Показать код (scripts/clean_data.sql)</summary>

```sql
-- scripts/clean_data.sql

-- Создание базы MART
CREATE DATABASE IF NOT EXISTS piccha_mart;

-- Создание MART-таблиц
CREATE TABLE IF NOT EXISTS piccha_mart.purchases_mart (
    purchase_id String,
    customer_id String,
    store_id String,
    total_amount Float32,
    payment_method String,
    is_delivery UInt8,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,
    purchase_datetime DateTime
) ENGINE = MergeTree()
ORDER BY (purchase_datetime, purchase_id);

CREATE TABLE IF NOT EXISTS piccha_mart.customers_mart (
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date Date,
    gender String,
    registration_date DateTime,
    is_loyalty_member UInt8,
    loyalty_card_number String,
    purchase_location_store_id String,
    purchase_location_city String,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,
    preferred_language String,
    preferred_payment_method String,
    receive_promotions UInt8
) ENGINE = MergeTree()
ORDER BY customer_id;

-- Materialized View для очистки и загрузки покупок
CREATE MATERIALIZED VIEW piccha_mart.purchases_mart_mv
TO piccha_mart.purchases_mart
AS SELECT
    purchase_id,
    customer_id,
    store_id,
    total_amount,
    lower(payment_method) AS payment_method,
    is_delivery,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    purchase_datetime
FROM piccha_raw.purchases
WHERE
    purchase_id != '' AND purchase_id IS NOT NULL
    AND customer_id != '' AND customer_id IS NOT NULL
    AND store_id != '' AND store_id IS NOT NULL
    AND purchase_datetime <= now()
    AND total_amount > 0;

-- Materialized View для очистки и загрузки покупателей
CREATE MATERIALIZED VIEW piccha_mart.customers_mart_mv
TO piccha_mart.customers_mart
AS SELECT
    customer_id,
    lower(first_name) AS first_name,
    lower(last_name) AS last_name,
    lower(email) AS email,
    phone,
    birth_date,
    lower(gender) AS gender,
    registration_date,
    is_loyalty_member,
    loyalty_card_number,
    purchase_location_store_id,
    lower(purchase_location_city) AS purchase_location_city,
    lower(delivery_address_city) AS delivery_address_city,
    lower(delivery_address_street) AS delivery_address_street,
    delivery_address_house,
    delivery_address_apartment,
    delivery_address_postal_code,
    lower(preferred_language) AS preferred_language,
    lower(preferred_payment_method) AS preferred_payment_method,
    receive_promotions
FROM piccha_raw.customers
WHERE
    customer_id != '' AND customer_id IS NOT NULL
    AND first_name != '' AND first_name IS NOT NULL
    AND last_name != '' AND last_name IS NOT NULL
    AND birth_date <= today()
    AND registration_date <= now();

-- Таблица для логирования дубликатов
CREATE TABLE IF NOT EXISTS piccha_mart.duplicates_log (
    table_name String,
    duplicate_count UInt64,
    total_count UInt64,
    duplicate_percentage Float32,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (table_name, timestamp);

-- Materialized View для логирования дубликатов (пример для purchases)
-- В реальности, для точного подсчета дубликатов может потребоваться более сложный запрос или триггер.
-- Этот MV показывает общий принцип.
CREATE MATERIALIZED VIEW piccha_mart.duplicates_log_mv
TO piccha_mart.duplicates_log
AS
SELECT
    'purchases' AS table_name,
    (SELECT count(*) - uniq(purchase_id) FROM piccha_raw.purchases WHERE purchase_id != '' AND purchase_id IS NOT NULL) AS duplicate_count,
    (SELECT count(*) FROM piccha_raw.purchases WHERE purchase_id != '' AND purchase_id IS NOT NULL) AS total_count,
    (duplicate_count * 100.0) / total_count AS duplicate_percentage
FROM system.one
WHERE duplicate_percentage > 50;
```

</details>

---

## Настройка Grafana

### 7.1. Добавление источника данных ClickHouse

1. Открой Grafana: `http://localhost:3000`
2. Перейди в **Configuration → Data Sources**
3. Нажми **Add data source**
4. Выбери **ClickHouse**
5. Укажи:
   - **URL**: `http://clickhouse:8123` (если Grafana в Docker) или `http://localhost:8123` (если на хосте)
   - **Database**: `piccha_raw`
   - **User**: `default`
   - **Password**: (оставь пустым)

### 7.2. Создание дашборда

1. Перейди в **Create → Dashboard**
2. Добавь панель:
   - **Query**:
     ```sql
     SELECT COUNT(*) AS "Количество покупок" FROM piccha_raw.purchases
     ```
   - **Format as**: `SingleStat`
3. Добавь ещё одну панель:
   - **Query**:
     ```sql
     SELECT COUNT(DISTINCT store_id) AS "Количество магазинов" FROM piccha_raw.stores
     ```
   - **Format as**: `SingleStat`

   <img width="629" height="519" alt="1 задание" src="https://github.com/user-attachments/assets/a5bbef9d-ecd2-4ab7-adeb-2cbb08d08a52" />


### 7.3. Настройка алертинга дубликатов

1. Перейди в **Alerting → Contact points**
2. Добавь **Telegram**:
   - Вставь **токен бота** и **ID чата**
3. Перейди в **Alerting → Alert rules**
4. Создай правило:
   - **Query**:
     ```sql
     SELECT duplicate_percentage FROM piccha_mart.duplicates_log ORDER BY timestamp DESC LIMIT 1
     ```
   - **Condition**: `IS ABOVE 50`
   - **Contact point**: Telegram
5. Сохрани алерт.
6. Сделай скриншот и сохрани как `telegram_alert_screenshot.png` в корне проекта.

---

## Telegram-бот для алертинга

- Название: `PicchaAlertBot`
- Токен: `123456789:ABCdefGHIjklMNOpqrSTUvwxYZ`
- Скриншот работы: `telegram_alert_screenshot.png`

---

## Как запустить

1. `docker-compose up -d`
2. `python scripts/generate_data.py`
3. `python scripts/load_to_mongo.py`
4. `python scripts/kafka_producer.py` (скопируйте ключ шифрования из вывода)
5. Вставьте ключ в `scripts/kafka_to_clickhouse.py`
6. `python scripts/kafka_to_clickhouse.py`
7. `clickhouse-client < scripts/clean_data.sql`
8. Настрой Grafana → создай дашборд → настрой алертинг → сделай скриншоты

---


---
```
