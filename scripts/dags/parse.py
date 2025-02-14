from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import re
import json
import time
from bs4 import BeautifulSoup
import os

# Заголовки для обхода блокировки
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
}

# Список URL для фильтрации
FILTER_URLS = [
    "https://kolesa.kz/cars/?sort_by=add_date-desc",
    "https://kolesa.kz/cars/?sort_by=price-asc",
    "https://kolesa.kz/cars/?sort_by=year.price-desc.asc",
    "https://kolesa.kz/cars/?sort_by=price-desc",
]

# Сохраняем вне контейнера
data_dir = os.path.abspath(os.path.join(os.getcwd(), "data/unprocessed_json"))
os.makedirs(data_dir, exist_ok=True)

# Файл для отслеживания последней страницы
last_page_file = os.path.join(data_dir, "last_page.json")

def load_last_page():
    if os.path.exists(last_page_file):
        with open(last_page_file, "r") as f:
            return json.load(f)
    return {}

def save_last_page(data):
    with open(last_page_file, "w") as f:
        json.dump(data, f, indent=4)

def get_car_links(url, pages=15, start_page=1):
    links = set()
    pattern = re.compile(r"https://kolesa.kz/a/show/\d+")

    for page in range(start_page, start_page + pages):
        if len(links) >= 100:
            break
        full_url = f"{url}?page={page}"
        try:
            print(f"🔍 Запрос страницы: {full_url}")
            response = requests.get(full_url, headers=HEADERS, timeout=10)
            if response.status_code != 200:
                print(f"⚠️ Ошибка запроса: {full_url} - статус {response.status_code}")
                time.sleep(60)
                continue

            soup = BeautifulSoup(response.text, "html.parser")
            found_links = {"https://kolesa.kz" + a["href"] for a in soup.select('a[href^="/a/show/"]')}
            filtered_links = {link for link in found_links if pattern.match(link)}

            links.update(filtered_links)
            print(f"🔗 Страница {page}: найдено {len(filtered_links)} ссылок")

            time.sleep(30)

        except requests.exceptions.RequestException as e:
            print(f"❌ Ошибка при запросе {full_url}: {e}. Повтор через 60 секунд.")
            time.sleep(60)

    print(f"✅ Всего собрано ссылок: {len(links)}")
    return list(links)[:100]

def get_car_data(url, processed_ids):
    retries = 3
    for attempt in range(retries):
        try:
            print(f"🌐 Запрос данных по ссылке: {url}")
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                html = response.text
                match = re.search(r"window\.digitalData\s*=\s*({.*?});", html, re.DOTALL)
                if match:
                    data = json.loads(match.group(1))
                    car = data.get("product", {})
                    car_id = car.get("id")
                    if car_id and car_id not in processed_ids:
                        processed_ids.add(car_id)
                        print(f"✅ Данные успешно извлечены для: {url}")
                        return car
                    else:
                        print(f"⚠️ Повтор записи с ID {car_id}, пропускаем")
                else:
                    print(f"⚠️ Данные не найдены для: {url}")
                return None
            elif response.status_code in [403, 429, 503]:
                print(f"🚨 Сайт заблокировал парсинг (код {response.status_code}). Ожидаем 300 секунд.")
                time.sleep(300)
            else:
                print(f"⚠️ Ошибка при запросе: {url}, статус {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Ошибка при запросе {url}: {e}. Повтор через 60 секунд.")
            time.sleep(60)

    print(f"❌ Не удалось получить данные после {retries} попыток: {url}")
    return None

def parse_cars(url, label="filtered", pages=50, max_links=20):
    print(f"🚗 Начинаем парсинг: {url} | Метка: {label} | Страниц: {pages}")
    last_pages = load_last_page()
    start_page = last_pages.get(label, 1)
    cars = []
    processed_ids = set()
    links = get_car_links(url, pages, start_page)[:max_links]
    for i, link in enumerate(links):
        print(f"🔄 Обработка ссылки {i+1}/{len(links)}: {link}")
        car = get_car_data(link, processed_ids)
        if car:
            cars.append(car)
        time.sleep(5)

    if cars:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(data_dir, f"cars_{label}_{timestamp}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(cars, f, indent=4, ensure_ascii=False)
        print(f"✅ Собрано {len(cars)} объявлений! Данные сохранены в {file_path}")

        last_pages[label] = start_page + pages
        save_last_page(last_pages)
    else:
        print("⚠️ Нет данных для сохранения.")

def parse_new_cars():
    print("🚀 Запуск парсинга новых автомобилей")
    parse_cars("https://kolesa.kz/cars/", label="new", pages=100, max_links=100)

def parse_filtered_cars():
    for url in FILTER_URLS:
        label = url.split("=")[-1]
        print(f"🛠️ Запуск парсинга с фильтром: {label}")
        parse_cars(url, label=label, pages=20, max_links=20)

default_args = {
    "owner": "temirlan",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 14),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "kolesa_car_parser",
    default_args=default_args,
    schedule_interval="0 */4 * * *",  # Каждые 4 часа
    catchup=False,
    tags=["kolesa", "parser"]
)

new_cars_task = PythonOperator(
    task_id="parse_new_cars",
    python_callable=parse_new_cars,
    dag=dag
)

filtered_cars_task = PythonOperator(
    task_id="parse_filtered_cars",
    python_callable=parse_filtered_cars,
    dag=dag,
)

new_cars_task >> filtered_cars_task
