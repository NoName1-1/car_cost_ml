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



def get_car_links(url, pages=15):
    links = []
    start_page = 2  # Начинаем со 2-й страницы
    for page in range(start_page, start_page + pages):
        full_url = f"{url}?page={page}"
        response = requests.get(full_url, headers=HEADERS)
        if response.status_code != 200:
            print(f"⚠️ Ошибка запроса: {full_url}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        found_links = [a["href"] for a in soup.select('a[href^="/cars/"]')]
        links += ["https://kolesa.kz" + link for link in found_links]

        print(f"🔗 Страница {page}: найдено {len(found_links)} ссылок")
        time.sleep(2)

    return links



def get_car_data(url):
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        return None
    html = response.text
    match = re.search(r"window\.digitalData\s*=\s*({.*?});", html, re.DOTALL)
    if match:
        data = json.loads(match.group(1))
        return data.get("product", {})
    return None


def parse_cars(url, label="filtered", pages=50):
    cars = []
    links = get_car_links(url, pages)
    for i, link in enumerate(links):
        try:
            response = requests.get(link, headers=HEADERS)
            if response.status_code in [403, 429, 503]:
                print(f"🚨 Сайт заблокировал парсинг (код {response.status_code}). Сохраняем и останавливаемся.")
                break
            if response.status_code != 200:
                print(f"⚠️ Ошибка при запросе: {link}, статус {response.status_code}")
                continue

            car = get_car_data(link)
            if car:
                cars.append(car)

            time.sleep(2)  # Задержка для обхода блокировки

        except Exception as e:
            print(f"❌ Ошибка при обработке {link}: {e}")

    # Сохранение данных даже при остановке
    if cars:
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(data_dir, f"cars_{label}_{timestamp}.json")
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(cars, f, indent=4, ensure_ascii=False)
        print(f"✅ Собрано {len(cars)} объявлений! Данные сохранены в {file_path}")

    else:
        print("⚠️ Нет данных для сохранения.")



def parse_new_cars():
    # Передаём URL без page
    parse_cars("https://kolesa.kz/cars/", label="new", pages=10)


def parse_filtered_cars():
    for url in FILTER_URLS:
        label = url.split("=")[-1]
        parse_cars(url, label=label, pages=20)


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
    schedule_interval="0 15 * * *",  # Каждый день в 15:00
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
