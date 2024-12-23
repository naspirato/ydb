import re
import pandas as pd

repo_path = '/home/kirrysin/fork/ydb/'
# Задаем пути к файлам
file_paths = [
    '/home/kirrysin/fork/ydb/combined_clickbench_queries.sql',
    '/home/kirrysin/fork/ydb/combined_tpcds_queries.sql',
    '/home/kirrysin/fork/ydb/combined_tpch_queries.sql'
]

# Имена для каждого бенчмарка на основе имен файлов
benchmark_names_updated = {
    '/home/kirrysin/fork/ydb/combined_clickbench_queries.sql': 'ClickBench',
    '/home/kirrysin/fork/ydb/combined_tpcds_queries.sql': 'TPC-DS',
    '/home/kirrysin/fork/ydb/combined_tpch_queries.sql': 'TPC-H'
}

# Чтение содержимого файлов
file_contents = []
for path in file_paths:
    with open(path, 'r', encoding='utf-8') as file:
        file_contents.append(file.read())

def classify_query(query):
    operations_found = set()
    
    # Перевод в верхний регистр для нечувствительности к регистру
    upper_query = query.upper()
    
    # Проверка на наличие каждого типа операции
    if 'SELECT' in upper_query:
        operations_found.add('Выборка (Select)')
    if any(agg in upper_query for agg in ['SUM', 'AVG', 'COUNT', 'MIN', 'MAX']):
        operations_found.add('Агрегация (Aggregation)')
    if 'GROUP BY' in upper_query:
        operations_found.add('Группировка (Grouping)')
    if 'WHERE' in upper_query:
        operations_found.add('Фильтрация (Filtering)')
    if 'ORDER BY' in upper_query:
        operations_found.add('Порядок (Ordering)')
    if 'LIMIT' in upper_query:
        operations_found.add('Лимит (Limiting)')
    if 'JOIN' in upper_query:
        operations_found.add('Multiple Join')
    if 'UNION' in upper_query:
        operations_found.add('Cross Table Query')
    
    return "; ".join(sorted(operations_found))

def extract_queries_info(file_content, benchmark_name):
    # Разделяем содержимое на запросы по разделителю "-- q10" или "-- q10.sql"
    split_queries = re.split(r'(-- q\d+(?:\.sql)?)\n', file_content)
    
    queries_info = []
    for i in range(1, len(split_queries), 2):
        identifier = split_queries[i].strip()  # например, "-- q10" или "-- q10.sql"
        query = split_queries[i + 1].strip()
        
        # Выделяем номер запроса
        query_number_match = re.search(r'\d+', identifier)
        if query_number_match:
            query_number = query_number_match.group()
        else:
            query_number = 'Unknown'
        
        # Классифицируем операции запроса
        operations = classify_query(query)
        
        # Сохраняем детальную информацию
        queries_info.append((query_number, benchmark_name, operations, query))
    
    return queries_info

# Обрабатываем содержимое каждого файла с добавлением детальной информации
detailed_results_updated = []
for file_path, file_content in zip(file_paths, file_contents):
    benchmark_name = benchmark_names_updated[file_path]
    detailed_results_updated.extend(extract_queries_info(file_content, benchmark_name))

# Преобразуем результаты в DataFrame
df_detailed_updated = pd.DataFrame(detailed_results_updated, columns=['QueryNumber', 'BenchmarkName', 'Operations', 'QueryText'])

# Сохраняем результаты в CSV
detailed_csv_file_path_updated = repo_path+ 'query_operations_detailed_updated.csv'
df_detailed_updated.to_csv(detailed_csv_file_path_updated, index=False, encoding='utf-8')