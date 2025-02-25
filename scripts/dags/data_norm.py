from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import json
import pandas as pd
import os
from sklearn.preprocessing import OneHotEncoder, StandardScaler

# Пути к файлам
BASE_DIR = os.getcwd()
RAW_JSON_PATH = os.path.abspath(os.path.join(BASE_DIR, "data/unprocessed_json"))
PROCESSED_JSON_PATH = os.path.abspath(os.path.join(BASE_DIR, "data/processed_json"))
FEATURE_ENGINEERING_PATH = "/opt/airflow/data/feature_engineering.json"

# Создаём директории, если их нет
os.makedirs(RAW_JSON_PATH, exist_ok=True)
os.makedirs(PROCESSED_JSON_PATH, exist_ok=True)
os.makedirs(os.path.dirname(FEATURE_ENGINEERING_PATH), exist_ok=True)


def load_json(**kwargs):
    files = [f for f in os.listdir(RAW_JSON_PATH) if f.endswith(".json")]
    data = []
    for file in files:
        with open(os.path.join(RAW_JSON_PATH, file), "r", encoding="utf-8") as f:
            json_data = json.load(f)
            if isinstance(json_data, list):
                data.extend(json_data)
            else:
                data.append(json_data)
    kwargs['ti'].xcom_push(key='raw_data', value=data)


def clean_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='load_json', key='raw_data')
    if not data:
        raise ValueError("No data received from load_json")

    flattened_data = []
    for item in data:
        if not isinstance(item, dict):
            continue
        flattened_item = {
            'id': item.get('id'),
            'name': item.get('name'),
            'unitPrice': item.get('unitPrice'),
            'availability': item.get('availability'),
            'region': item.get('region'),
            'city': item.get('city'),
            'brand': item.get('attributes', {}).get('brand'),
            'model': item.get('attributes', {}).get('model'),
            'year': item.get('parameters', {}).get('Поколение'),
            'engine_volume': item.get('parameters', {}).get('Объем двигателя, л'),
            'transmission': item.get('parameters', {}).get('Коробка передач'),
            'drive': item.get('parameters', {}).get('Привод'),
            'steering': item.get('parameters', {}).get('Руль'),
            'color': item.get('parameters', {}).get('Цвет'),
            'damaged': item.get('parameters', {}).get('Аварийная', "Нет"),
            'not_running': item.get('parameters', {}).get('Не на ходу', "Нет"),
            'url': item.get('url')
        }
        flattened_data.append(flattened_item)

    df = pd.DataFrame(flattened_data)
    kwargs['ti'].xcom_push(key='clean_data', value=df.to_dict(orient='records'))


def feature_engineering(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='clean_data', key='clean_data')
    if not data:
        raise ValueError("No data received from clean_data")



    df = pd.DataFrame(data)

    def remove_outliers_iqr(df, column):
        Q1 = df[column].quantile(0.25)
        Q3 = df[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        return df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]

    df = remove_outliers_iqr(df, 'unitPrice')

    categorical_features = ['availability', 'region', 'city', 'brand', 'model', 'transmission', 'drive', 'steering',
                            'color', 'damaged', 'not_running']
    ohe = OneHotEncoder(handle_unknown='ignore', sparse_output=False)
    encoded_features = ohe.fit_transform(df[categorical_features])
    encoded_df = pd.DataFrame(encoded_features, columns=ohe.get_feature_names_out(categorical_features))

    numerical_features = ['engine_volume']
    scaler = StandardScaler()

    df[numerical_features] = df[numerical_features].replace(r'[^0-9.]', '', regex=True).astype(float)

    scaled_features = scaler.fit_transform(df[numerical_features])
    scaled_df = pd.DataFrame(scaled_features, columns=numerical_features)

    df = pd.concat([df.drop(columns=categorical_features + numerical_features), encoded_df, scaled_df], axis=1)

    df.to_json(FEATURE_ENGINEERING_PATH, orient="records")
    kwargs['ti'].xcom_push(key='feature_engineering_path', value=FEATURE_ENGINEERING_PATH)


def save_data(**kwargs):
    feature_engineering_path = kwargs['ti'].xcom_pull(task_ids='feature_engineering', key='feature_engineering_path')
    if not feature_engineering_path or not os.path.exists(feature_engineering_path):
        raise ValueError("No valid feature engineering file found")

    df = pd.read_json(feature_engineering_path)
    df.to_json(os.path.join(PROCESSED_JSON_PATH, 'processed_data.json'), orient='records', force_ascii=False)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'start_date': datetime(2025, 2, 22),
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'process_car_json',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
)

load_json_task = PythonOperator(
    task_id='load_json',
    python_callable=load_json,
    dag=dag
)

clean_data_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag
)

feature_engineering_task = PythonOperator(
    task_id='feature_engineering',
    python_callable=feature_engineering,
    dag=dag
)

save_data_task = PythonOperator(
    task_id='save_data',
    python_callable=save_data,
    dag=dag
)

load_json_task >> clean_data_task >> feature_engineering_task >> save_data_task
