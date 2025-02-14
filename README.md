# car_cost_ml
# Инициализация базы данных
airflow db init

# Создание пользователя
airflow users create \
    --username admin \
    --password admin \
    --role Admin \
    --email admin@example.com \
    --firstname Air \
    --lastname Flow

# Запуск веб-интерфейса и планировщика
airflow webserver -p 8080
airflow scheduler
