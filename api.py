from fastapi import FastAPI, Query
import pickle
import pandas as pd
import numpy as np
import os
# Загружаем модель
with open("data/model.pkl", "rb") as f:
    model = pickle.load(f)

# Загружаем label encoding для названий машин и кузова

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    label_mapping_name = pd.read_csv(os.path.join(BASE_DIR, "data", "label_mapping_name.csv"))

    label_mapping_body = pd.read_csv(os.path.join(BASE_DIR, "data", "label_mapping_body.csv"))
except FileNotFoundError:
    raise FileNotFoundError(
        "⚠️ Файл 'data/label_mapping_name.csv' или 'data/label_mapping_body.csv' не найден! Проверьте путь к файлам.")

# Создаем FastAPI
app = FastAPI(title="Car Price Predictor API",
              description="API для предсказания цены автомобиля по введенным данным.\n"
                          "Заполните все параметры и получите предсказание цены в тенге (KZT).\n"
                          "Используется модель машинного обучения, обученная на данных Kolesa.kz.",
              version="1.0")


# Функции кодирования
def encode_car_name(car_name: str):
    match = label_mapping_name[label_mapping_name['Original_name'] == car_name]
    if not match.empty:
        return int(match.iloc[0]['Car_encoded'])
    return 0  # Если нет в таблице


def encode_body_type(body_type: str):
    match = label_mapping_body[label_mapping_body['Original_name'] == body_type]
    if not match.empty:
        return int(match.iloc[0]['Car_encoded'])
    return 0  # Если нет в таблице


@app.get("/predict/", summary="Предсказание цены автомобиля", tags=["Предсказание"])
def predict(
        car_name: str = Query(..., title="Марка и модель", example="BMW 530"),
        year: int = Query(..., title="Год выпуска", example=2022),
        mileage: int = Query(..., title="Пробег в км", example=35000),
        engine_volume: float = Query(..., title="Объем двигателя, л", example=2.5),
        fuel_type: int = Query(..., title="Тип топлива", example=0, description="0 - бензин, 1 - дизель"),
        gearbox: str = Query(..., title="Коробка передач", example="Автомат"),
        drive: str = Query(..., title="Привод", example="Передний привод"),
        wheel: str = Query(..., title="Руль", example="Слева"),
        customs_clear: str = Query(..., title="Растаможен", example="Да"),
        body_type: str = Query(..., title="Тип кузова", example="Седан")
):
    """
    📌 Введи данные о машине:
    🔹 Марка и модель
    🔹 Год выпуска
    🔹 Пробег в км
    🔹 Объем двигателя, л
    🔹 Тип топлива (0 - бензин, 1 - дизель)
    🔹 Коробка передач (Механика, Автомат, Вариатор, Робот)
    🔹 Привод (Полный привод, Передний привод, Задний привод)
    🔹 Руль (Слева, Справа)
    🔹 Растаможен (Да, Нет)
    🔹 Тип кузова (например, 'Седан')
    """

    # Кодируем параметры
    car_encoded = encode_car_name(car_name)
    body_encoded = encode_body_type(body_type)
    gearbox_mapping = {"Механика": 0, "Автомат": 1, "Вариатор": 2, "Робот": 3}
    drive_mapping = {"Полный привод": 0, "Передний привод": 1, "Задний привод": 2}
    wheel_mapping = {"Слева": 1, "Справа": 0}
    customs_mapping = {"Нет": 0, "Да": 1}

    gearbox = gearbox_mapping.get(gearbox, 0)
    drive = drive_mapping.get(drive, 1)
    wheel = wheel_mapping.get(wheel, 0)
    customs_clear = customs_mapping.get(customs_clear, 1)

    # Создаем DataFrame с входными данными
    input_data = pd.DataFrame([{
        'isNewAuto': 0,
        'parameters_Коробка передач': gearbox,
        'parameters_Привод': drive,
        'parameters_Руль': wheel,
        'parameters_Растаможен в Казахстане': customs_clear,
        'year': year,
        'mileage': mileage,
        'Car_encoded': car_encoded,
        'Body_encoded': body_encoded,
        'engine_volume': engine_volume,
        'fuel_type': fuel_type
    }])

    # Делаем предсказание
    predicted_price = model.predict(input_data)[0]

    return {"predicted_price": round(predicted_price, 2)}
