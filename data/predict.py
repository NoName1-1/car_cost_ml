import uvicorn
from fastapi import FastAPI, Query
import requests
import json
import time
import random
import pickle
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from sklearn.preprocessing import LabelEncoder
from sklearn.impute import SimpleImputer
import re

import os
import pickle

# Получаем путь к текущей папке, где находится скрипт
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Полные пути к файлам
MODEL_PATH = os.path.join(BASE_DIR, "model.pkl")
ENCODER_PATH = os.path.join(BASE_DIR, "encoder.pkl")

# Проверяем, существуют ли файлы
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError(f"❌ Файл модели не найден: {MODEL_PATH}")

if not os.path.exists(ENCODER_PATH):
    raise FileNotFoundError(f"❌ Файл LabelEncoder не найден: {ENCODER_PATH}")

# Загружаем модель
with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

# Загружаем LabelEncoder
with open(ENCODER_PATH, "rb") as f:
    encoder = pickle.load(f)

print("✅ Модель и LabelEncoder успешно загружены!")


# ==== 2. FASTAPI ====
app = FastAPI(title="Car Price Prediction API")

# ==== 3. ФУНКЦИЯ ПАРСИНГА ====
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]

def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}

def fetch_car_data(url):
    """Парсит JSON со страницы объявления"""
    try:
        headers = get_headers()
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 429:
            print("⚠️ Блокировка! Ждём 90 сек...")
            time.sleep(90)
            return fetch_car_data(url)
        if response.status_code == 200:
            html = response.text
            match = re.search(r"window\.digitalData\s*=\s*({.*?});", html, re.DOTALL)
            if match:
                return json.loads(match.group(1)).get("product", {})
    except Exception as e:
        print(f"⚠ Ошибка парсинга {url}: {e}")
    return None

# ==== 4. ПРЕОБРАЗОВАНИЕ ДАННЫХ ====
def preprocess_data(car_data):
    """Обрабатывает данные перед отправкой в модель"""
    print("📊 Исходные данные перед DataFrame:", car_data)

    df = pd.DataFrame([car_data]) if isinstance(car_data, dict) else pd.DataFrame(car_data)
    # 🔍 Проверяем правильность названия столбцов
    df.columns = df.columns.str.strip()  # Убираем лишние пробелы
    pattern = r"\b(19|20)\d{2}\b"

    df.columns = df.columns.str.strip()

    # 🔍 Дебаг: Проверяем названия столбцов (ещё раз)
    print("🔎 Итоговые столбцы после очистки:", df.columns.tolist())

    # 💡 Если 'name' пропал, создаём его вручную!
    if "name" not in df.columns:
        df["name"] = car_data.get("name", "Unknown")

    # Проверяем снова
    print("✅ Финальный список столбцов:", df.columns.tolist())

    if "name" in df:
        df["year"] = pd.to_numeric(df["name"].fillna("").str.extract(f"({pattern})")[0], errors="coerce").astype(
            "Int64")
    else:
        print("❌ Ошибка: 'name' отсутствует в df")

    print("🔎 Итоговые столбцы:", df.columns.tolist())  # Выведет все названия

    print("📝 DataFrame после преобразования:\n", df.head())

    # Удаляем ненужные столбцы
    drop_cols = ["name", "id", "seller", "section", "categoryString", "region", "city"]
    df.drop(columns=[col for col in drop_cols if col in df.columns], axis=1, inplace=True)

    # Извлекаем год из названия
    pattern = r"\b(19|20)\d{2}\b"
    df["year"] = pd.to_numeric(df["name"].fillna("").str.extract(f"({pattern})")[0], errors="coerce").astype("Int64")

    # Обрабатываем пробег
    if "parameters_Пробег" in df.columns:
        df["mileage"] = pd.to_numeric(df["parameters_Пробег"].str.replace(r"\D+", "", regex=True), errors="coerce").astype("Int64")
        df.drop(columns=["parameters_Пробег"], inplace=True)

    # Заполняем пропущенные значения пробега
    df["mileage"].fillna(df["mileage"].median(), inplace=True)

    # Категориальные фичи
    categorical_features = ["parameters_Коробка передач", "parameters_Привод", "parameters_Руль", "parameters_Растаможен в Казахстане", "fuel_type"]
    for col in categorical_features:
        if col in df.columns:
            df[col] = encoder.fit_transform(df[col].astype(str))

    # Кодируем кузов и бренд
    df["Car_encoded"] = encoder.fit_transform(df["name"])
    df["Body_encoded"] = encoder.fit_transform(df["parameters_Кузов"].fillna("Неизвестно"))

    # Заполняем пропуски средними значениями
    imputer = SimpleImputer(strategy="mean")
    df.iloc[:, :] = imputer.fit_transform(df)

    # Приводим фичи к тому же виду, что был при обучении
    df = df.reindex(columns=model.feature_names_in_, fill_value=0)

    return df

# ==== 5. API ЭНДПОИНТ ====
@app.get("/predict")
def predict_price(url: str = Query(..., description="Ссылка на объявление Kolesa.kz")):
    """Парсит данные и возвращает предсказанную цену"""
    car_data = fetch_car_data(url)

    print("📊 Полученные данные:", json.dumps(car_data, indent=4, ensure_ascii=False))

    if not car_data:
        return {"error": "Не удалось получить данные с сайта"}

    df = preprocess_data(car_data)
    predicted_price = model.predict(df)[0]
    return {"predicted_price": round(predicted_price, 2)}

# ==== 6. ЗАПУСК СЕРВЕРА ====
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
