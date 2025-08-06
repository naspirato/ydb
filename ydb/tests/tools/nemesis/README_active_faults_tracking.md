# Система отслеживания активных нарушений Nemesis

## Обзор

Система позволяет отслеживать активные нарушения, внесенные nemesis процессами, через веб-интерфейс на localhost.

## 🎯 **ПОЛНАЯ ИНТЕГРАЦИЯ**

**Система отслеживания теперь полностью интегрирована в существующий код nemesis!**

### ✅ **Что изменилось:**

1. **Автоматическое отслеживание** - все nemesis теперь автоматически отслеживаются
2. **Новые аргументы командной строки** - можно включать/отключать отслеживание
3. **Умные экстракторы** - автоматически определяют тип кластера и используют соответствующие экстракторы
4. **Обратная совместимость** - все существующие функции работают как прежде

## Компоненты

### 1. ActiveFaultsTracker (`active_faults_tracker.py`)
- Отслеживает все активные и завершенные нарушения
- Потокобезопасный трекер с использованием RLock
- Автоматическая очистка старых нарушений

### 2. NemesisTrackerWrapper (`nemesis_tracker_wrapper.py`) ⭐ **ИНТЕГРИРОВАНО**
- **Wrapper для автоматического добавления отслеживания к существующим nemesis**
- **НЕ требует переписывания существующего кода**
- Автоматически извлекает цели и описания нарушений
- Предопределенные экстракторы для популярных типов nemesis

### 3. TrackedNemesis (`tracked_nemesis.py`)
- Базовый класс для nemesis с автоматическим отслеживанием
- Требует переписывания существующих nemesis
- Используйте только если нужна полная кастомизация

### 4. Веб-интерфейс (`monitor.py`)
- Главная страница: `http://localhost:8666/`
- API для активных нарушений: `http://localhost:8666/active-faults`
- API для всех нарушений: `http://localhost:8666/all-faults`

## Использование

### 🚀 **Автоматическое использование (ПО УМОЛЧАНИЮ)**

**Теперь отслеживание работает автоматически!** Просто запустите nemesis как обычно:

```bash
python3 driver/__main__.py \
    --ydb-cluster-template /path/to/template \
    --ydb-binary-path /path/to/binary
```

**Отслеживание включено по умолчанию!** Откройте `http://localhost:8666/` для просмотра активных нарушений.

### 🔧 **Управление отслеживанием**

#### Включить отслеживание (по умолчанию):
```bash
python3 driver/__main__.py \
    --ydb-cluster-template /path/to/template \
    --ydb-binary-path /path/to/binary \
    --enable-fault-tracking
```

#### Отключить отслеживание:
```bash
python3 driver/__main__.py \
    --ydb-cluster-template /path/to/template \
    --ydb-binary-path /path/to/binary \
    --disable-fault-tracking
```

### 📊 **Что вы получите автоматически:**

- **Bridge Pile кластеры**: Отслеживание pile, узлов, портов
- **Обычные кластеры**: Отслеживание узлов, tablet, datacenter
- **Умные описания**: Автоматическое извлечение деталей нарушений
- **Веб-интерфейс**: Красивый интерфейс с автообновлением

## Доступ к веб-интерфейсу

После запуска nemesis, откройте в браузере:
- `http://localhost:8666/` - главная страница с активными нарушениями
- `http://localhost:8666/active-faults` - JSON API активных нарушений
- `http://localhost:8666/all-faults` - JSON API всех нарушений

## Пример ответа API

```json
{
  "timestamp": "2025-01-15T10:30:00",
  "active_faults_count": 2,
  "active_faults": [
    {
      "fault_id": "BridgePileStopNodesNemesis_1_1705312200",
      "nemesis_name": "BridgePileStopNodesNemesis",
      "fault_type": "BridgePileStopNodesNemesis",
      "target": "pile_1",
      "inject_time": "2025-01-15T10:25:00",
      "extract_time": null,
      "description": "BridgePileStopNodesNemesis: 3 nodes (node1, node2, node3)",
      "status": "active",
      "duration_seconds": 300
    }
  ]
}
```

## Тестирование интеграции

Запустите тестовый скрипт для проверки интеграции:

```bash
cd ydb/tests/tools/nemesis
python3 test_tracking_integration.py
```

## API Endpoints

### GET /active-faults
Возвращает только активные нарушения.

**Параметры:** нет

**Ответ:**
```json
{
  "timestamp": "2025-01-15T10:30:00",
  "active_faults_count": 2,
  "active_faults": [...]
}
```

### GET /all-faults
Возвращает все нарушения (активные и завершенные).

**Параметры:**
- `limit` (int, опционально): максимальное количество нарушений (по умолчанию 100)

**Ответ:**
```json
{
  "timestamp": "2025-01-15T10:30:00",
  "total_faults_count": 5,
  "faults": [...]
}
```

### GET /
Главная страница с веб-интерфейсом для просмотра активных нарушений.

## Статусы нарушений

- `active` - нарушение активно
- `extracted` - нарушение успешно извлечено
- `failed` - попытка извлечения нарушения не удалась

## Автоматическая очистка

Система автоматически удаляет старые завершенные нарушения через 24 часа для экономии памяти.

## Мониторинг

Все операции логируются с помощью стандартного Python logging. Проверьте логи nemesis процесса для отладочной информации.

## Преимущества полной интеграции

✅ **Работает из коробки** - никаких изменений в коде не требуется
✅ **Автоматическое определение типа кластера** - bridge pile vs обычный кластер
✅ **Умные экстракторы** - автоматическое извлечение целей и описаний
✅ **Обратная совместимость** - все существующие функции работают
✅ **Простое управление** - аргументы командной строки для включения/отключения
✅ **Красивый веб-интерфейс** - с автообновлением каждые 30 секунд

## Миграция существующего кода

**Никаких изменений не требуется!** Система отслеживания работает автоматически.

### Было:
```bash
python3 driver/__main__.py --ydb-cluster-template /path/to/template --ydb-binary-path /path/to/binary
```

### Стало:
```bash
# То же самое! Отслеживание включено по умолчанию
python3 driver/__main__.py --ydb-cluster-template /path/to/template --ydb-binary-path /path/to/binary

# Или явно включить/отключить
python3 driver/__main__.py --ydb-cluster-template /path/to/template --ydb-binary-path /path/to/binary --enable-fault-tracking
```

## Примеры использования

Смотрите файлы:
- `test_tracking_integration.py` - тестирование интеграции
- `tracked_existing_nemesis_example.py` - примеры использования wrapper'а
- `tracked_bridge_pile_example.py` - примеры с переписыванием (для кастомизации)

## Логирование

Система отслеживания логирует свои действия:

```
2025-01-15 10:25:00 - catalog - INFO - Using bridge pile extractors for tracking
2025-01-15 10:25:01 - catalog - INFO - Added tracking to 15 nemesis
2025-01-15 10:25:02 - active_faults_tracker - INFO - Registered fault injection: BridgePileStopNodesNemesis on pile_1
2025-01-15 10:25:32 - active_faults_tracker - INFO - Registered fault extraction: BridgePileStopNodesNemesis on pile_1
``` 