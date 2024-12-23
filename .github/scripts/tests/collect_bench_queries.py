#!/usr/bin/env python
import os
repo_path = '/home/kirrysin/fork/ydb/'
def combine_sql_files(queries_path, output_file):
    """
    Собирает все SQL файлы из указанной директории и сохраняет в один выходной файл.
    
    :param queries_path: Путь к директории с SQL запросами.
    :param output_file: Имя итогового файла, в который будут записаны объединенные запросы.
    """
    # Получаем список всех .sql файлов в директории
    sql_files = sorted([f for f in os.listdir(queries_path) if f.endswith('.sql')])

    # Открываем выходной файл для записи объединенных запросов
    with open(output_file, 'w') as outfile:
        for sql_file in sql_files:
            with open(os.path.join(queries_path, sql_file), 'r') as infile:
                outfile.write('-- ' + sql_file + '\n')  # Добавление заголовка с именем файла
                outfile.write(infile.read() + '\n')  # Запись содержимого файла в выходной файл

    print(f'Объединенные запросы были записаны в {output_file}')

# Пример использования для разных бенчмарков:
# Путь к директории с SQL запросами для разных бенчмарков
tpch_path = 'ydb/library/benchmarks/queries/tpch/yql/'
tpcds_path = 'ydb/library/benchmarks/queries/tpcds/yql'
clickbench_path = 'ydb/library/workload/clickbench/'

# Объединение запросов для каждого бенчмарка
combine_sql_files(repo_path + tpch_path, 'combined_tpch_queries.sql')
combine_sql_files(repo_path + tpcds_path, 'combined_tpcds_queries.sql')
combine_sql_files(repo_path + clickbench_path, 'combined_clickbench_queries.sql')