from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import re
import json
import time
from bs4 import BeautifulSoup
import os
import random
from concurrent.futures import ThreadPoolExecutor

# Список user-agents для ротации
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
]

FILTER_URLS = [
    "https://kolesa.kz/cars/?sort_by=add_date-desc",
    "https://kolesa.kz/cars/?sort_by=price-asc",
    "https://kolesa.kz/cars/?sort_by=year.price-desc.asc",
    "https://kolesa.kz/cars/?sort_by=price-desc",
]

data_dir = os.path.abspath(os.path.join(os.getcwd(), "data/unprocessed_json"))
os.makedirs(data_dir, exist_ok=True)
last_page_file = os.path.join(data_dir, "last_page.json")

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=3)
session.mount("https://", adapter)


def get_headers():
    return {"User-Agent": random.choice(USER_AGENTS)}


def load_last_page():
    if os.path.exists(last_page_file):
        with open(last_page_file, "r") as f:
            return json.load(f)
    return {}


def save_last_page(data):
    with open(last_page_file, "w") as f:
        json.dump(data, f, indent=4)


def fetch_page(url):
    try:
        headers = get_headers()
        time.sleep(random.uniform(10, 20))  # Увеличенная задержка
        response = session.get(url, headers=headers, timeout=30)
        if response.status_code == 429:
            print("⚠️ Сайт блокирует! Ждём 90 секунд...")
            time.sleep(90)
            return fetch_page(url)
        if response.status_code == 200:
            return response.text
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Ошибка запроса {url}: {e}")
    return None


def get_car_links(url, pages=10, start_page=1):
    links = set()
    pattern = re.compile(r"https://kolesa.kz/a/show/\d+")

    with ThreadPoolExecutor(max_workers=3) as executor:  # Уменьшено число потоков
        urls = [f"{url}?page={page}" for page in range(start_page, start_page + pages)]
        results = list(executor.map(fetch_page, urls))

    for html in results:
        if html:
            soup = BeautifulSoup(html, "html.parser")
            found_links = {"https://kolesa.kz" + a["href"] for a in soup.select('a[href^="/a/show/"]')}
            filtered_links = {link for link in found_links if pattern.match(link)}
            links.update(filtered_links)
        time.sleep(random.uniform(5, 10))

    print(f"✅ Найдено {len(links)} ссылок")
    return list(links)


def get_car_data(url, processed_ids):
    try:
        print(f"🌐 Обрабатываю: {url}")
        headers = get_headers()
        time.sleep(random.uniform(10, 20))  # Увеличенная задержка
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code == 429:
            print("⚠️ Сайт блокирует! Ждём 90 секунд...")
            time.sleep(90)
            return get_car_data(url, processed_ids)
        if response.status_code == 200:
            html = response.text
            time.sleep(random.uniform(5, 10))
            match = re.search(r"window\.digitalData\s*=\s*({.*?});", html, re.DOTALL)
            if match:
                data = json.loads(match.group(1))
                car = data.get("product", {})
                car_id = car.get("id")
                if car_id and car_id not in processed_ids:
                    processed_ids.add(car_id)
                    return car
    except Exception as e:
        print(f"⚠️ Ошибка при обработке {url}: {e}")
    return None


def parse_cars(url, label="filtered", pages=10, max_links=100):
    print(f"🚗 Парсим: {url} | Метка: {label} | Страниц: {pages}")
    last_pages = load_last_page()
    start_page = last_pages.get(label, 1)

    cars = []
    processed_ids = set()
    links = get_car_links(url, pages, start_page)[:max_links]

    with ThreadPoolExecutor(max_workers=3) as executor:  # Уменьшено число потоков
        results = list(executor.map(lambda link: get_car_data(link, processed_ids), links))

    for car in results:
        if car:
            cars.append(car)

    if cars:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(data_dir, f"cars_{label}_{timestamp}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(cars, f, indent=4, ensure_ascii=False)
        print(f"✅ Сохранено {len(cars)} объявлений в {file_path}")

        last_pages[label] = start_page + pages
        save_last_page(last_pages)
    else:
        print("⚠️ Нет данных для сохранения.")


def parse_new_cars():
    print("🚀 Запуск парсинга новых автомобилей")
    parse_cars("https://kolesa.kz/cars/", label="new", pages=5, max_links=100)


def parse_filtered_cars():
    for url in FILTER_URLS:
        label = url.split("=")[-1]
        print(f"🛠️ Запуск парсинга с фильтром: {label}")
        parse_cars(url, label=label, pages=2, max_links=25)


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
    schedule_interval="0 */2 * * *",  # Запуск раз в 2 часа
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
