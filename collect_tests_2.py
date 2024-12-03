import ast
import os
import csv
import re
import pandas as pd

from pathlib import Path
from treelib import Node, Tree
from itertools import count
import networkx as nx
import matplotlib.pyplot as plt
from tqdm import tqdm
from typing import List, Dict


def count_py_files(directory):
    count = 0
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                count += 1
    return count

def find_all_pytest_tests(directory):
    """Найти все тесты pytest в указанной директории."""
    tests = []
    py_file_count = count_py_files(directory)
    processed_files = 0

    for file_path in Path(directory).rglob('*.py'):
        processed_files += 1
        print(f"Processing file {processed_files}/{py_file_count}: {file_path}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    tree = ast.parse(f.read(), filename=str(file_path))
                    for node in ast.walk(tree):
                        if isinstance(node, ast.FunctionDef) and node.name.startswith('test_'):
                            asserts = [n for n in ast.walk(node) if isinstance(n, ast.Assert)]
                            assert_lines = [f"Line {n.lineno}: {ast.dump(n.test)}" for n in asserts]
                            tests.append({
                                'file': str(file_path),
                                'name': node.name,
                                'asserts': assert_lines
                            })
                except SyntaxError:
                    pass  # Пропустить файлы с ошибками синтаксиса
        except Exception as e:
            pass  # Пропустить файлы, которые невозможно открыть

    return tests

def save_to_csv(tests, csv_filename):
    """Сохранить информацию о тестах в CSV файл."""
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['file', 'name', 'assert']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for test in tests:
            for assert_line in test['asserts']:
                writer.writerow({'file': test['file'], 'name': test['name'], 'assert': assert_line})

def load_from_csv(csv_filename):
    """Загрузить информацию о тестах из CSV файла."""
    tests = []

    with open(csv_filename, 'r', newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            tests.append({
                'file': row['file'],
                'name': row['name'],
                'assert': row['assert']
            })

    return tests


def sanitize_label(label) -> str:
    """Очистка строки для использования в качестве идентификатора в Graphviz."""
    if label is None:
        return "Missing"
    label = str(label)
    #label = re.sub(r'[.\s:]', '_', label)
    #label = re.sub(r'[^a-zA-Z0-9_]', '', label)
    return label

def build_tree_graph(tests: pd.DataFrame) -> nx.DiGraph:
    """Построить граф дерева информации о тестах с использованием `networkx`."""
    graph = nx.DiGraph()
    graph.add_node("Tests")
    
    for _, test in tqdm(tests.iterrows(), total=tests.shape[0], desc="Building tree graph", unit="test"):
        # Обработка пути файла для создания узлов директории
        file_value = test['file'] if pd.notna(test['file']) else 'MissingFile'
        filepath = os.path.normpath(str(file_value))
        directories = (filepath.split(os.sep))[1:]
        previous_node = "Tests"
        path_accumulator = ""

        # Создаем узлы для каждой директории в пути
        for directory in directories:  # Все кроме имени файла
            path_accumulator = os.path.join(path_accumulator, directory)
            directory_label = sanitize_label(path_accumulator)
            if not graph.has_node(directory_label):
                graph.add_node(directory_label, label=directory)  # Используем оригинальное имя для отображения
            if not graph.has_edge(previous_node, directory_label):
                graph.add_edge(previous_node, directory_label)
            previous_node = directory_label

        # Добавляем последний элемент как файл и тест

        test_name = sanitize_label(test['name'])
        test_label = f"{test_name}"

        graph.add_node(test_label)
        graph.add_edge(previous_node, test_label)

        # Инициализируем предыдущий узел для assert
        previous_assert_node = test_label

        # Добавляем узел и ребро для каждого утверждения
       # assert_value = test['assert'] if pd.notna(test['assert']) else 'MissingAssertion'
       # assertion_label = sanitize_label(f"{test_label}-{assert_value}")
       # graph.add_node(assertion_label)
       # graph.add_edge(previous_assert_node, assertion_label)
    
    return graph



def visualize_graph(graph: nx.DiGraph, output_filename='large_graph.svg'):
    """Визуализировать и сохранить граф."""
    ag = nx.nx_agraph.to_agraph(graph)

    # Настройка дополнительных параметров
    ag.graph_attr.update(overlap='false', splines='true')
    ag.node_attr.update(shape='rectangle', fixedsize='false')

    # Используем алгоритм dot, чтобы разместить узлы вертикально
    ag.layout(prog='neato')

    ag.draw(output_filename, format='svg')







def save_graph_as_png(graph, filename):
    """Сохранить граф в формате PNG."""
    plt.figure(figsize=(12, 8))
    pos = nx.nx_agraph.graphviz_layout(graph, prog='dot')
    nx.draw(graph, pos, with_labels=True, node_size=3000, node_color="lightblue", font_size=8, arrowsize=10)
    plt.savefig(filename, format="SVG")
    plt.close()


if __name__ == "__main__":
    project_directory = "/home/kirrysin/fork/ydb/"  # Укажите путь к вашему проекту
    csv_filename = "test_results.csv"
    png_filename = "test_tree.png"

    # Найти тесты и сохранить их в CSV
    tests = find_all_pytest_tests(project_directory)
    save_to_csv(tests, csv_filename)

    # Чтение данных из CSV
    #csv_file = 'your_data.csv'
   # df = pd.read_csv(csv_filename)

  

    # Строим граф
    #graph = build_tree_graph(df)

    # Сохранить граф как PNG
    #save_graph_as_png(tree_graph, png_filename)
    #visualize_graph(graph)
    #print(f"Graph has been saved to {png_filename}")


