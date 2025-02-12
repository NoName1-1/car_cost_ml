import requests
import re
import json
import time
from bs4 import BeautifulSoup

import os

# Получаем путь к папке "data" на уровень выше
output_path = os.path.join(os.path.dirname(__file__), "..", "data", "cars_data.json")


# Заголовки для обхода блокировки
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# Функция для получения списка ссылок на объявления
def get_car_links(page=1):
    url = f"https://kolesa.kz/cars/?page={page}"
    response = requests.get(url, headers=HEADERS, allow_redirects=False)


    if response.status_code != 200:
        print(f"Ошибка {response.status_code} при запросе {url}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    car_links = []

    # Ищем ссылки на объявления
    for link in soup.find_all("a", class_="a-card__link"):
        car_url = "https://kolesa.kz" + link.get("href")
        car_links.append(car_url)

    return car_links


# Функция для парсинга JSON из страницы объявления
def get_car_data(url):
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Ошибка {response.status_code} при запросе {url}")
        return None

    html = response.text

    # Ищем JSON в коде страницы
    match = re.search(r"window\.digitalData\s*=\s*({.*?});", html, re.DOTALL)
    if match:
        json_text = match.group(1)
        data = json.loads(json_text)
        return data.get("product", {})
    else:
        print(f"Не найден digitalData на {url}")
        return None


# Основная функция парсинга
def parse_kolesa(num_pages=1):
    all_cars = []

    for page in range(1, num_pages + 1):
        print(f"Парсим страницу {page}...")
        car_links = get_car_links(page)

        for car_url in car_links:
            print(f"Собираем данные с {car_url}")
            car_data = get_car_data(car_url)

            if car_data:
                all_cars.append(car_data)

            time.sleep(1)  # Делаем паузу, чтобы не забанили

    # Сохраняем в JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_cars, f, indent=4, ensure_ascii=False)

    print(f"✅ Собрано {len(all_cars)} объявлений! Данные сохранены в {output_path}")


# Запуск парсера (например, 3 страницы)
parse_kolesa(num_pages=3)
