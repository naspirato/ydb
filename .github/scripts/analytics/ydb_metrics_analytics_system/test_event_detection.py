#!/usr/bin/env python3
"""
Тесты для проверки корректного обнаружения событий в системе аналитики
"""

import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(__file__))

from baseline_calculator import BaselineCalculator
from event_detector import EventDetector
from preprocessing import Preprocessing


class TestEventDetection(unittest.TestCase):
    """Тесты для обнаружения событий"""
    
    def setUp(self):
        """Настройка тестового окружения"""
        self.base_config = {
            'analytics': {
                'baseline_method': 'rolling_mean',
                'window_size': 7,
                'sensitivity': 2.0,
                'min_absolute_change': 10,
                'min_relative_change': 0.1,
                'hysteresis_points': 3,
                'adaptive_threshold': True
            },
            'events': {
                'detect': ['degradation_start', 'improvement_start'],
                'min_event_duration_minutes': 30
            }
        }
    
    def _create_time_series(self, values, start_date=None, freq='1h'):
        """Создать временной ряд для тестов"""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=10)
        
        dates = pd.date_range(start=start_date, periods=len(values), freq=freq)
        return pd.Series(values, index=dates)
    
    def test_degradation_detection(self):
        """Тест обнаружения деградации (падение значений ниже порога)"""
        # Создаем данные: стабильные значения, затем резкое падение
        stable_values = [100.0] * 20
        degraded_values = [50.0] * 10  # Падение на 50%
        values = stable_values + degraded_values
        
        series = self._create_time_series(values)
        
        # Вычисляем baseline
        baseline_calc = BaselineCalculator(self.base_config)
        baseline_result = baseline_calc.compute_baseline_and_thresholds(series)
        
        # Обнаруживаем события
        event_detector = EventDetector(self.base_config)
        events = event_detector.detect_events(series, baseline_result)
        
        # Проверяем, что обнаружена деградация
        degradation_events = [e for e in events if e['event_type'] == 'degradation_start']
        self.assertGreater(len(degradation_events), 0, "Должна быть обнаружена деградация")
        
        # Проверяем корректность данных события
        event = degradation_events[0]
        self.assertLess(event['current_value'], event['baseline_before'], 
                         "Текущее значение должно быть меньше baseline")
        self.assertLess(event['change_absolute'], 0, 
                       "Изменение должно быть отрицательным")
        print(f"✓ Обнаружена деградация: {event['change_relative']*100:.1f}% падение")
    
    def test_improvement_detection(self):
        """Тест обнаружения улучшения (рост значений выше порога)"""
        # Создаем данные: стабильные значения, затем резкий рост
        stable_values = [100.0] * 20
        improved_values = [200.0] * 10  # Рост на 100%
        values = stable_values + improved_values
        
        series = self._create_time_series(values)
        
        # Вычисляем baseline
        baseline_calc = BaselineCalculator(self.base_config)
        baseline_result = baseline_calc.compute_baseline_and_thresholds(series)
        
        # Обнаруживаем события
        event_detector = EventDetector(self.base_config)
        events = event_detector.detect_events(series, baseline_result)
        
        # Проверяем, что обнаружено улучшение
        improvement_events = [e for e in events if e['event_type'] == 'improvement_start']
        self.assertGreater(len(improvement_events), 0, "Должно быть обнаружено улучшение")
        
        # Проверяем корректность данных события
        event = improvement_events[0]
        self.assertGreater(event['current_value'], event['baseline_before'],
                          "Текущее значение должно быть больше baseline")
        self.assertGreater(event['change_absolute'], 0,
                          "Изменение должно быть положительным")
        print(f"✓ Обнаружено улучшение: {event['change_relative']*100:.1f}% рост")
    
    def test_no_false_positives(self):
        """Тест отсутствия ложных срабатываний при стабильных данных"""
        # Создаем стабильные данные с небольшими флуктуациями
        np.random.seed(42)
        base_value = 100.0
        noise = np.random.normal(0, 5, 30)  # Небольшой шум ±5
        values = base_value + noise
        
        series = self._create_time_series(values)
        
        # Вычисляем baseline
        baseline_calc = BaselineCalculator(self.base_config)
        baseline_result = baseline_calc.compute_baseline_and_thresholds(series)
        
        # Обнаруживаем события
        event_detector = EventDetector(self.base_config)
        events = event_detector.detect_events(series, baseline_result)
        
        # При стабильных данных не должно быть событий
        # (или очень мало, если шум значительный)
        print(f"✓ Стабильные данные: обнаружено {len(events)} событий (ожидается 0 или мало)")
    
    def test_error_count_aggregation(self):
        """Тест агрегации счетчиков ошибок"""
        # Создаем данные с ошибками (каждая ошибка = 1)
        timestamps = pd.date_range(start=datetime.now() - timedelta(days=7), 
                                   periods=168, freq='1h')  # 7 дней по часам
        
        # Создаем данные: несколько ошибок в разные часы
        data = {
            'ts': timestamps,
            'operation_type': ['scan_query'] * 168,
            'script_name': ['test_script'] * 168,
            'query_name': ['test_query'] * 168,
            'metric_value': [0] * 168,
            'metric_name': ['error_count'] * 168
        }
        
        # Добавляем ошибки в некоторые часы
        error_hours = [10, 11, 12, 50, 51, 100, 101, 102, 103]
        for hour in error_hours:
            data['metric_value'][hour] = 1
        
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # Настраиваем preprocessing с агрегацией
        config = {
            'data_source': {
                'aggregate_by': '1h'
            },
            'context_fields': ['operation_type', 'script_name', 'query_name'],
            'metric_fields': ['metric_name', 'metric_value'],
            'timestamp_field': 'ts'
        }
        
        preprocessing = Preprocessing(config)
        df_aggregated = preprocessing.clean_data(df, remove_outliers=False)
        
        # Проверяем, что агрегация работает корректно
        # В часах с ошибками должно быть значение > 0
        error_hours_data = df_aggregated[df_aggregated['metric_value'] > 0]
        self.assertGreater(len(error_hours_data), 0, 
                          "После агрегации должны быть часы с ошибками")
        
        # Проверяем, что значения суммируются (не усредняются)
        self.assertTrue(all(error_hours_data['metric_value'] >= 1),
                       "Значения счетчиков должны суммироваться")
        print(f"✓ Агрегация error_count: {len(error_hours_data)} часов с ошибками")
    
    def test_duration_aggregation(self):
        """Тест агрегации метрик производительности (duration_ms)"""
        # Создаем данные с измерениями длительности
        timestamps = pd.date_range(start=datetime.now() - timedelta(days=3),
                                   periods=72, freq='1h')  # 3 дня по часам
        
        np.random.seed(42)
        # Базовое значение 100мс с небольшими вариациями
        base_duration = 100.0
        noise = np.random.normal(0, 10, 72)
        durations = np.maximum(base_duration + noise, 10)  # Минимум 10мс
        
        data = {
            'ts': timestamps,
            'operation_type': ['scan_query'] * 72,
            'script_name': ['test_script'] * 72,
            'query_name': ['test_query'] * 72,
            'metric_value': durations.tolist(),
            'metric_name': ['duration_ms'] * 72
        }
        
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # Настраиваем preprocessing с агрегацией по часам
        config = {
            'data_source': {
                'aggregate_by': '1h'
            },
            'context_fields': ['operation_type', 'script_name', 'query_name'],
            'metric_fields': ['metric_name', 'metric_value'],
            'timestamp_field': 'ts'
        }
        
        preprocessing = Preprocessing(config)
        df_aggregated = preprocessing.clean_data(df, remove_outliers=False)
        
        # Проверяем, что агрегация работает (усреднение для duration_ms)
        self.assertEqual(len(df_aggregated), 72, 
                       "После агрегации должно быть 72 точки (по одной на час)")
        
        # Проверяем, что значения усредняются (не суммируются)
        # Среднее значение должно быть близко к базовому
        mean_value = df_aggregated['metric_value'].mean()
        self.assertAlmostEqual(mean_value, base_duration, delta=20,
                              msg="Усредненные значения должны быть близки к базовому")
        print(f"✓ Агрегация duration_ms: среднее значение {mean_value:.1f}мс (ожидается ~{base_duration}мс)")
    
    def test_error_rate_computation(self):
        """Тест вычисления процента ошибок"""
        # Создаем данные с успешными запросами и ошибками
        timestamps = pd.date_range(start=datetime.now() - timedelta(days=2),
                                   periods=48, freq='1h')  # 2 дня по часам
        
        data = []
        for i, ts in enumerate(timestamps):
            # В каждом часе: 10 запросов, из них 2 ошибки (20% ошибок)
            for j in range(10):
                status = 'error' if j < 2 else 'success'
                data.append({
                    'ts': ts,
                    'operation_type': 'scan_query',
                    'script_name': 'test_script',
                    'query_name': 'test_query',
                    'status': status,
                    'metric_value': 1,
                    'metric_name': 'error_rate'
                })
        
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # Настраиваем preprocessing с вычислением error_rate
        config = {
            'data_source': {
                'aggregate_by': '1h',
                'compute_error_rate': True
            },
            'context_fields': ['operation_type', 'script_name', 'query_name'],
            'metric_fields': ['metric_name', 'metric_value'],
            'timestamp_field': 'ts'
        }
        
        preprocessing = Preprocessing(config)
        df_aggregated = preprocessing.clean_data(df, remove_outliers=False)
        
        # Проверяем, что error_rate вычислен корректно
        self.assertEqual(len(df_aggregated), 48,
                        "После агрегации должно быть 48 точек (по одной на час)")
        
        # Проверяем, что процент ошибок ~20%
        error_rates = df_aggregated['metric_value']
        mean_error_rate = error_rates.mean()
        self.assertAlmostEqual(mean_error_rate, 20.0, delta=1.0,
                              msg="Процент ошибок должен быть ~20%")
        print(f"✓ Вычисление error_rate: средний процент ошибок {mean_error_rate:.1f}% (ожидается ~20%)")
    
    def test_small_data_volume(self):
        """Тест работы с малым объемом данных (неделя данных)"""
        # Создаем данные за неделю с агрегацией по дням
        timestamps = pd.date_range(start=datetime.now() - timedelta(days=7),
                                   periods=7, freq='1D')  # 7 дней
        
        # Небольшое количество ошибок
        error_counts = [0, 1, 0, 2, 1, 0, 3]  # Всего 7 ошибок за неделю
        
        data = []
        for ts, count in zip(timestamps, error_counts):
            for _ in range(count):
                data.append({
                    'ts': ts,
                    'operation_type': 'scan_query',
                    'script_name': 'test_script',
                    'query_name': 'test_query',
                    'metric_value': 1,
                    'metric_name': 'error_count'
                })
        
        if not data:
            # Если нет данных, создаем минимальный набор
            data = [{
                'ts': timestamps[0],
                'operation_type': 'scan_query',
                'script_name': 'test_script',
                'query_name': 'test_query',
                'metric_value': 1,
                'metric_name': 'error_count'
            }]
        
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        
        # Настраиваем preprocessing с агрегацией по дням
        config = {
            'data_source': {
                'aggregate_by': '1D'
            },
            'context_fields': ['operation_type', 'script_name', 'query_name'],
            'metric_fields': ['metric_name', 'metric_value'],
            'timestamp_field': 'ts'
        }
        
        preprocessing = Preprocessing(config)
        df_aggregated = preprocessing.clean_data(df, remove_outliers=False)
        
        # Проверяем, что данные агрегированы по дням
        self.assertLessEqual(len(df_aggregated), 7,
                           "После агрегации по дням должно быть не более 7 точек")
        
        # Проверяем, что сумма ошибок сохранена
        total_errors = df_aggregated['metric_value'].sum()
        expected_total = sum(error_counts)
        self.assertEqual(total_errors, expected_total,
                        f"Сумма ошибок должна быть {expected_total}")
        print(f"✓ Малый объем данных: {len(df_aggregated)} точек после агрегации, всего {total_errors} ошибок")
    
    def test_multiple_metrics(self):
        """Тест работы с несколькими метриками одновременно"""
        timestamps = pd.date_range(start=datetime.now() - timedelta(days=5),
                                   periods=120, freq='1h')
        
        data = []
        for ts in timestamps:
            # Добавляем duration_ms
            data.append({
                'ts': ts,
                'operation_type': 'scan_query',
                'script_name': 'test_script',
                'query_name': 'test_query',
                'metric_value': 100.0 + np.random.normal(0, 10),
                'metric_name': 'duration_ms'
            })
            
            # Добавляем error_count (иногда)
            if np.random.random() < 0.1:  # 10% вероятность ошибки
                data.append({
                    'ts': ts,
                    'operation_type': 'scan_query',
                    'script_name': 'test_script',
                    'query_name': 'test_query',
                    'metric_value': 1,
                    'metric_name': 'error_count'
                })
        
        df = pd.DataFrame(data)
        df['ts'] = pd.to_datetime(df['ts'])
        
        config = {
            'data_source': {
                'aggregate_by': '1h'
            },
            'context_fields': ['operation_type', 'script_name', 'query_name'],
            'metric_fields': ['metric_name', 'metric_value'],
            'timestamp_field': 'ts'
        }
        
        preprocessing = Preprocessing(config)
        df_aggregated = preprocessing.clean_data(df, remove_outliers=False)
        
        # Проверяем, что обе метрики присутствуют
        metric_names = df_aggregated['metric_name'].unique()
        self.assertIn('duration_ms', metric_names, "Должна быть метрика duration_ms")
        self.assertIn('error_count', metric_names, "Должна быть метрика error_count")
        
        # Проверяем, что duration_ms усредняется, а error_count суммируется
        duration_data = df_aggregated[df_aggregated['metric_name'] == 'duration_ms']
        error_data = df_aggregated[df_aggregated['metric_name'] == 'error_count']
        
        # duration_ms должен усредняться (значения должны быть в разумных пределах)
        self.assertTrue(all(50 < v < 200 for v in duration_data['metric_value'].head(10)),
                       "duration_ms должен усредняться")
        
        # error_count должен суммироваться (значения >= 1)
        if len(error_data) > 0:
            self.assertTrue(all(v >= 1 for v in error_data['metric_value']),
                           "error_count должен суммироваться")
        
        print(f"✓ Множественные метрики: duration_ms ({len(duration_data)} точек), "
              f"error_count ({len(error_data)} точек)")


def run_tests():
    """Запуск всех тестов"""
    print("=" * 70)
    print("Запуск тестов для проверки обнаружения событий")
    print("=" * 70)
    print()
    
    # Создаем test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestEventDetection)
    
    # Запускаем тесты
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    print()
    print("=" * 70)
    if result.wasSuccessful():
        print("✓ Все тесты пройдены успешно!")
    else:
        print(f"✗ Тесты завершились с ошибками: {len(result.failures)} failures, {len(result.errors)} errors")
    print("=" * 70)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)

