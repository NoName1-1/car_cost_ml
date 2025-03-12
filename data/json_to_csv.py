import json
import pandas as pd
import os

# Папка с неотработанными JSON-файлами
input_folder = 'unprocessed_json'
output_file = 'combined_output.csv'

# Список для хранения DataFrame из всех файлов
all_dfs = []

# Перебор всех JSON-файлов в папке
for filename in os.listdir(input_folder):
    if filename.endswith('.json'):
        file_path = os.path.join(input_folder, filename)
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)

        # Денормализация JSON
        df = pd.json_normalize(data, sep='_')

        # Раскрытие списков в отдельные строки (если есть)
        for column in df.columns:
            if df[column].apply(lambda x: isinstance(x, list)).any():
                df = df.explode(column)

        # Добавляем столбец с именем файла (опционально)
        df['source_file'] = filename

        all_dfs.append(df)

# Объединение всех DataFrame
if all_dfs:
    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Сохранение в CSV с корректным энкодингом
    combined_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f'CSV файл успешно создан: {output_file}')
else:
    print('JSON файлы не найдены.')
